"""Offline regression tests; run: python -m unittest discover -s tests -v."""
import contextlib
import csv
import importlib.util
import io
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
DELIMITERS = (',', '|', '\t', ';')
INVALID = (None, 1, True, b'|', [], {}, '', '||', '\\t', '"', '\r', '\n', '\0')


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Operator:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


# Stub only imports, then restore sys.modules. No optional dependency is needed.
modules = {}
for name in ('google', 'google.auth', 'google.auth.impersonated_credentials',
             'google.cloud', 'google.cloud.storage', 'google.cloud.secretmanager',
             'paramiko', 'airflow', 'airflow.providers', 'airflow.providers.google',
             'airflow.providers.google.cloud', 'airflow.providers.google.cloud.hooks',
             'airflow.providers.google.cloud.hooks.gcs', 'airflow.providers.standard',
             'airflow.providers.standard.operators', 'airflow.providers.standard.operators.python',
             'airflow.providers.ssh', 'airflow.providers.ssh.operators',
             'airflow.providers.ssh.operators.ssh', 'framework', 'framework.logger'):
    module = modules[name] = types.ModuleType(name)
    module.__path__ = []
    parent, _, leaf = name.rpartition('.')
    if parent in modules:
        setattr(modules[parent], leaf, module)
modules['google.cloud.storage'].Client = Mock()
modules['airflow.providers.google.cloud.hooks.gcs'].GCSHook = Mock()
modules['airflow.providers.standard.operators.python'].PythonOperator = Operator
modules['airflow.providers.ssh.operators.ssh'].SSHOperator = Operator
modules['framework.logger'].task_failure_callback = Mock()
modules['framework.logger'].task_success_callback = Mock()
utils = load('delimiter_utils', ROOT / 'dags/framework/utils.py')
modules['framework.utils'] = utils
with patch.dict(sys.modules, modules):
    runner = load('delimiter_runner', ROOT / 'tools/sftp-runner/gcs_to_sftp.py')
    gcs = load('delimiter_gcs', ROOT / 'dags/framework/gcs_executor.py')
    executor = load('delimiter_executor', ROOT / 'dags/framework/sftp_runner_executor.py')


def encoded(rows, delimiter):
    stream = io.StringIO(newline='')
    csv.writer(stream, delimiter=delimiter, lineterminator='\n').writerows(rows)
    return stream.getvalue().encode('utf-8')


def parsed(data, delimiter):
    return list(csv.reader(io.StringIO(data.decode('utf-8'), newline=''), delimiter=delimiter))


class Blob:
    def __init__(self, name, contents):
        self.name, self.contents = name, contents
        self.deleted = False

    @property
    def size(self):
        return len(self.contents[self.name])

    def exists(self):
        return self.name in self.contents

    def download_to_filename(self, filename):
        Path(filename).write_bytes(self.contents[self.name])

    def upload_from_filename(self, filename):
        self.contents[self.name] = Path(filename).read_bytes()

    def upload_from_string(self, data, **kwargs):
        self.contents[self.name] = data.encode('utf-8')

    def delete(self):
        self.deleted = True
        del self.contents[self.name]


class Bucket:
    def __init__(self, contents):
        self.contents = contents

    def blob(self, name):
        return Blob(name, self.contents)

    def get_blob(self, name):
        return self.blob(name) if name in self.contents else None


class SFTP:
    def __init__(self):
        self.files = {}
        self.directories = {'/'}
        self.events = []
        self.closed = False
        self.bad_size = False

    def stat(self, name):
        if name in self.files:
            return types.SimpleNamespace(st_size=len(self.files[name]) + self.bad_size)
        if name in self.directories:
            return types.SimpleNamespace(st_size=0)
        raise FileNotFoundError(name)

    def mkdir(self, name):
        self.directories.add(name)

    def remove(self, name):
        self.events.append(('remove', name))
        if name not in self.files:
            raise FileNotFoundError(name)
        del self.files[name]

    def put(self, local, remote, confirm):
        assert confirm is True
        self.events.append(('put', remote))
        self.files[remote] = Path(local).read_bytes()

    def rename(self, source, destination):
        self.events.append(('rename', source, destination))
        self.files[destination] = self.files.pop(source)

    @contextlib.contextmanager
    def open(self, name, mode):
        self.events.append(('open', name, mode))
        if mode not in ('xb', 'wb'):
            raise AssertionError('Unexpected direct upload mode: ' + mode)
        if mode == 'xb' and name in self.files:
            raise FileExistsError(name)
        self.files[name] = b''
        stream = io.BytesIO()
        try:
            yield stream
        finally:
            self.files[name] = stream.getvalue()
            stream.close()

    def close(self):
        self.closed = True


def fixture(delimiter, header=True):
    heading = [['id', 'text']]
    rows = [['1', 'contains' + delimiter + 'delimiter'], ['2', 'line one\nline two\r\nline three "quoted"']]
    lead = heading if header else []
    return {'in/b.csv': encoded(lead + rows[1:], delimiter),
            'in/a.csv': encoded(lead + rows[:1], delimiter),
            'in/0.csv': b'', 'in/c.csv': encoded(lead, delimiter)}, lead + rows


class DelimiterTests(unittest.TestCase):
    def test_validation_accepts_exact_characters(self):
        for delimiter in DELIMITERS + (' ', "'", '-', '$', '`', '\\', '&', 'é'):
            for validator in (utils.validate_csv_delimiter, runner.validate_csv_delimiter):
                with self.subTest(delimiter=repr(delimiter), validator=validator):
                    self.assertEqual(validator(delimiter), delimiter)

    def test_validation_rejects_without_coercing_or_trimming(self):
        for value in INVALID:
            for validator in (utils.validate_csv_delimiter, runner.validate_csv_delimiter):
                with self.subTest(value=repr(value), validator=validator):
                    with self.assertRaises(ValueError):
                        validator(value)

    def test_runner_merge_and_count_all_delimiters(self):
        for delimiter in DELIMITERS:
            for header in (True, False):
                with self.subTest(delimiter=repr(delimiter), header=header), tempfile.TemporaryDirectory() as tmp:
                    contents, expected = fixture(delimiter, header)
                    path, count, counts = runner.merge_csv_blobs(
                        [Blob(name, contents) for name in contents], Path(tmp), header, 'utf-8', delimiter)
                    self.assertEqual(parsed(path.read_bytes(), delimiter), expected)
                    self.assertEqual(count, 2)
                    self.assertEqual(counts, [('in/0.csv', 0), ('in/a.csv', 1), ('in/b.csv', 1), ('in/c.csv', 0)])
                    self.assertEqual(runner.count_csv_rows(path, header, 'utf-8', delimiter), 2)
                    self.assertEqual(list(Path(tmp).iterdir()), [path])

    def test_runner_legacy_positional_defaults_and_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            contents, expected = fixture(',')
            path, count, _ = runner.merge_csv_blobs([Blob(n, contents) for n in contents], directory, True, 'utf-8')
            self.assertEqual(parsed(path.read_bytes(), ','), expected)
            self.assertEqual(runner.count_csv_rows(path, True, 'utf-8'), count)
            for data, header, expected_rows in ((b'', True, []), (b'a,b\n', True, [['a', 'b']]), (b'', False, [])):
                contents = {'empty.csv': data}
                path, count, _ = runner.merge_csv_blobs([Blob('empty.csv', contents)], directory, header, 'utf-8')
                self.assertEqual(count, 0)
                self.assertEqual(parsed(path.read_bytes(), ','), expected_rows)

    def test_runner_helpers_validate_before_io(self):
        for value in INVALID:
            with self.subTest(value=repr(value)):
                with self.assertRaises(ValueError):
                    runner.count_csv_rows(Path('/not-present'), True, 'utf-8', value)
                with self.assertRaises(ValueError):
                    runner.merge_csv_blobs([], Path('/not-present'), True, 'utf-8', value)


class GCSMergeTests(unittest.TestCase):
    def merge(self, contents, **kwargs):
        bucket = Bucket(contents)
        hook = Mock()
        hook.list.return_value = list(contents)
        hook.get_conn.return_value.bucket.return_value = bucket
        with patch.object(gcs, 'GCSHook', return_value=hook):
            return gcs.merge_gcs_csv(bucket='test', source_prefix='in/', destination_object='out/merged.csv', **kwargs)

    def test_merge_and_fin_all_delimiters(self):
        for delimiter in DELIMITERS:
            for header in (True, False):
                with self.subTest(delimiter=repr(delimiter), header=header):
                    contents, expected = fixture(delimiter, header)
                    result = self.merge(contents, csv_delimiter=delimiter, csv_header=header,
                                        fin_filename='out/rows.fin', file_count_fin_filename='out/files.fin')
                    self.assertEqual(parsed(contents['out/merged.csv'], delimiter), expected)
                    self.assertEqual(result['merged_rows'], 2)
                    self.assertEqual(result['source_count'], 4)
                    self.assertEqual(contents['out/rows.fin'], b'2\n')
                    self.assertEqual(contents['out/files.fin'], b'1\n')

    def test_legacy_default_and_empty_objects(self):
        contents, expected = fixture(',')
        self.assertEqual(self.merge(contents)['merged_rows'], 2)
        self.assertEqual(parsed(contents['out/merged.csv'], ','), expected)
        for data in (b'', b'a,b\n'):
            contents = {'in/a.csv': data}
            self.assertEqual(self.merge(contents)['merged_rows'], 0)
            self.assertEqual(contents['out/merged.csv'], data)
        self.assertEqual(self.merge({}, allow_empty=True)['source_count'], 0)
        with self.assertRaises(FileNotFoundError):
            self.merge({})

    def test_invalid_delimiter_fails_before_cloud_calls(self):
        for value in INVALID:
            with self.subTest(value=repr(value)), patch.object(gcs, 'GCSHook') as hook:
                with self.assertRaises(ValueError):
                    gcs.merge_gcs_csv(bucket='test', source_prefix='in/', destination_object='out.csv', csv_delimiter=value)
                hook.assert_not_called()


class ExecutorTests(unittest.TestCase):
    def task(self, kind, options=None, defaults=None):
        grape = {'grape_id': 'test_delimiter', 'type': kind,
                 'source': {'bucket': 'test', 'prefix': 'in/'},
                 'destination': {'object': 'out.csv', 'target': 'test', 'remote_dir': '/out'},
                 'runner': {'ssh_conn_id': 'runner', 'command': 'python runner.py'},
                 'options': options or {}}
        if kind == 'gcs_merge_csv':
            return gcs.GCSExecutor.create_task(dag=None, grape=grape, defaults=defaults or {}, runtime={})
        with patch.dict(sys.modules, modules):
            return executor.SftpRunnerExecutor.create_task(dag=None, grape=grape, defaults=defaults or {})

    def test_gcs_forwarding_default_override_and_no_coercion(self):
        self.assertEqual(self.task('gcs_merge_csv').op_kwargs['csv_delimiter'], ',')
        for delimiter in DELIMITERS + (' ',):
            self.assertEqual(self.task('gcs_merge_csv', {'csv_delimiter': delimiter}).op_kwargs['csv_delimiter'], delimiter)
        self.assertEqual(self.task('gcs_merge_csv', defaults={'csv_delimiter': '|'}).op_kwargs['csv_delimiter'], '|')
        self.assertEqual(self.task('gcs_merge_csv', {'csv_delimiter': ';'}, {'csv_delimiter': '|'}).op_kwargs['csv_delimiter'], ';')

    def test_sftp_shell_roundtrip_preserves_one_literal_argument(self):
        for delimiter in DELIMITERS + (' ', "'", '-', '$', '`', '\\', '&', '(', ')', '<', '>', '*', '?', '#'):
            with self.subTest(delimiter=repr(delimiter)):
                command = self.task('gcs_to_sftp_runner', {'csv_delimiter': delimiter}).command
                expected = shlex.split(command)[2:]
                # Execute only a shell printf, never the generated Python/SSH command.
                actual = subprocess.check_output(['/bin/sh', '-c', 'printf "%s\\0" ' + command[len('python runner.py '):]])
                actual = actual.decode().split('\0')[:-1]
                self.assertEqual(actual, expected)
                self.assertEqual([x for x in actual if x.startswith('--csv-delimiter=')], ['--csv-delimiter=' + delimiter])
        self.assertIn('--csv-delimiter=,', shlex.split(self.task('gcs_to_sftp_runner').command))
        self.assertIn('--csv-delimiter=|', shlex.split(self.task('gcs_to_sftp_runner', defaults={'csv_delimiter': '|'}).command))

    def test_nested_merge_options_and_delimiter_override(self):
        task = self.task('gcs_to_sftp_runner', {
            'csv_delimiter': ';',
            'merge': {'enabled': True, 'filename': 'nested.csv', 'header': True, 'encoding': 'utf-8'},
            'fin': {'enabled': True, 'filename': 'nested.fin'},
        }, {'csv_delimiter': '|'})
        argv = shlex.split(task.command)
        self.assertIn('--csv-delimiter=;', argv)
        self.assertEqual(argv[argv.index('--merge-filename') + 1], 'nested.csv')
        self.assertEqual(argv[argv.index('--fin-filename') + 1], 'nested.fin')
        self.assertEqual(argv[argv.index('--csv-encoding') + 1], 'utf-8')
        self.assertIn('--csv-header', argv)

    def test_sftp_upload_mode_default_forwarding_and_validation(self):
        for mode in ('atomic', 'direct'):
            argv = shlex.split(self.task('gcs_to_sftp_runner', {'upload_mode': mode}).command)
            self.assertEqual(argv[argv.index('--upload-mode') + 1], mode)
        argv = shlex.split(self.task('gcs_to_sftp_runner').command)
        self.assertEqual(argv[argv.index('--upload-mode') + 1], 'atomic')
        argv = shlex.split(self.task('gcs_to_sftp_runner', defaults={'upload_mode': 'direct'}).command)
        self.assertEqual(argv[argv.index('--upload-mode') + 1], 'direct')
        for mode in (None, '', 'DIRECT', 'invalid', True, 1, [], {}):
            with self.subTest(mode=repr(mode)), self.assertRaises(ValueError):
                self.task('gcs_to_sftp_runner', {'upload_mode': mode})

    def test_both_executors_reject_invalid_options(self):
        for kind in ('gcs_merge_csv', 'gcs_to_sftp_runner'):
            for value in INVALID:
                with self.subTest(kind=kind, value=repr(value)), self.assertRaises(ValueError):
                    self.task(kind, {'csv_delimiter': value})


class RunnerCLITests(unittest.TestCase):
    def run_main(self, contents, extra):
        with tempfile.TemporaryDirectory() as tmp:
            bucket = Bucket(contents)
            client = Mock()
            client.bucket.return_value = bucket
            client.list_blobs.return_value = [bucket.blob(name) for name in contents]
            config = {'runner': {'temp_dir': tmp}, 'targets': {'test': {'host': 'host', 'username': 'user'}}}
            ssh, sftp = Mock(), SFTP()
            argv = ['runner', '--target', 'test', '--bucket', 'bucket', '--prefix', 'in/', '--remote-dir', '/out'] + extra
            with patch.object(sys, 'argv', argv), patch.object(runner, 'load_config', return_value=config), \
                    patch.object(runner, 'setup_logging'), patch.object(runner, 'build_google_credentials', return_value=(None, 'test')), \
                    patch.object(runner.storage, 'Client', return_value=client), patch.object(runner, 'connect', return_value=(ssh, sftp)):
                self.assertEqual(runner.main(), 0)
            self.assertEqual(list(Path(tmp).iterdir()), [])
            ssh.close.assert_called_once()
            self.assertTrue(sftp.closed)
            return sftp

    def test_cli_delimiter_merge_fin_and_atomic_upload(self):
        for delimiter in DELIMITERS:
            with self.subTest(delimiter=repr(delimiter)):
                contents, expected = fixture(delimiter)
                sftp = self.run_main(contents, ['--csv-delimiter=' + delimiter, '--csv-header', '--merge-filename', 'merged.csv',
                                                '--fin-filename', 'rows.fin', '--file-count-fin-filename', 'files.fin'])
                self.assertEqual(parsed(sftp.files['/out/merged.csv'], delimiter), expected)
                self.assertEqual(sftp.files['/out/rows.fin'], b'2\n')
                self.assertEqual(sftp.files['/out/files.fin'], b'1\n')
                self.assertEqual([e[2] for e in sftp.events if e[0] == 'rename'], ['/out/merged.csv', '/out/rows.fin', '/out/files.fin'])
                self.assertFalse(any(n.endswith('.uploading') for n in sftp.files))

    def test_cli_default_comma_and_raw_transfer_byte_identity(self):
        contents, expected = fixture(',')
        sftp = self.run_main(contents, ['--csv-header', '--merge-filename', 'merged.csv'])
        self.assertEqual(parsed(sftp.files['/out/merged.csv'], ','), expected)
        for delimiter in DELIMITERS:
            contents = {'in/sub/raw.csv': b'\xef\xbb\xbf"x|y";z\r\n"embedded\nnewline";\xff\x00'}
            sftp = self.run_main(contents, ['--csv-delimiter=' + delimiter])
            self.assertEqual(sftp.files, {'/out/sub/raw.csv': contents['in/sub/raw.csv']})

    def test_cli_direct_merge_fin_and_raw_all_delimiters(self):
        for delimiter in DELIMITERS:
            with self.subTest(delimiter=repr(delimiter)):
                contents, expected = fixture(delimiter)
                sftp = self.run_main(contents, ['--upload-mode', 'direct', '--csv-delimiter=' + delimiter,
                    '--csv-header', '--merge-filename', 'merged.csv', '--fin-filename', 'rows.fin',
                    '--file-count-fin-filename', 'files.fin'])
                self.assertEqual(parsed(sftp.files['/out/merged.csv'], delimiter), expected)
                self.assertEqual(sftp.files['/out/rows.fin'], b'2\n')
                self.assertEqual(sftp.files['/out/files.fin'], b'1\n')
                self.assertEqual(sftp.events, [('open', '/out/merged.csv', 'xb'),
                    ('open', '/out/rows.fin', 'xb'), ('open', '/out/files.fin', 'xb')])
                raw = b'\xef\xbb\xbfraw|data;with\r\nembedded\nbytes\x00\xff'
                sftp = self.run_main({'in/sub/raw.csv': raw}, ['--upload-mode', 'direct', '--overwrite',
                    '--csv-delimiter=' + delimiter])
                self.assertEqual(sftp.files, {'/out/sub/raw.csv': raw})
                self.assertEqual(sftp.events, [('open', '/out/sub/raw.csv', 'wb')])

    def test_upload_direct_exclusive_overwrite_and_size_verification(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'data'
            # More than one chunk exercises the full stream loop.
            payload = b'x' * (1024 * 1024 + 7) + b'\x00\xff\r\n'
            path.write_bytes(payload)
            sftp = SFTP()
            runner.upload_direct(sftp, path, '/out/data', False)
            self.assertEqual(sftp.files['/out/data'], payload)
            self.assertEqual(sftp.events, [('open', '/out/data', 'xb')])
            path.write_bytes(b'short')
            with self.assertRaises(FileExistsError):
                runner.upload_direct(sftp, path, '/out/data', False)
            self.assertEqual(sftp.files['/out/data'], payload)
            runner.upload_direct(sftp, path, '/out/data', True)
            self.assertEqual(sftp.files['/out/data'], b'short')
            self.assertTrue(all(event[0] == 'open' for event in sftp.events))
            sftp = SFTP()
            sftp.bad_size = True
            with self.assertRaises(RuntimeError):
                runner.upload_direct(sftp, path, '/out/data', False)
            self.assertEqual(sftp.events, [('open', '/out/data', 'xb')])

    def test_cli_rejects_invalid_before_loading_config(self):
        for delimiter in ('', '||', '\\t', '"', '\r', '\n', '\0'):
            with self.subTest(delimiter=repr(delimiter)), patch.object(sys, 'argv',
                    ['runner', '--target', 'test', '--bucket', 'b', '--prefix', 'in/', '--remote-dir', '/out', '--csv-delimiter=' + delimiter]), \
                    patch.object(runner, 'load_config') as config, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    runner.main()
                self.assertEqual(caught.exception.code, 2)
                config.assert_not_called()

    def test_upload_atomic_overwrite_refusal_and_size_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'data'
            path.write_bytes(b'new\x00bytes\r\n')
            sftp = SFTP()
            sftp.files['/out/data'] = b'old'
            with self.assertRaises(FileExistsError):
                runner.upload_atomic(sftp, path, '/out/data', False)
            self.assertEqual(sftp.files['/out/data'], b'old')
            sftp.files['/out/data.uploading'] = b'stale'
            runner.upload_atomic(sftp, path, '/out/data', True)
            self.assertEqual(sftp.files['/out/data'], path.read_bytes())
            self.assertNotIn('/out/data.uploading', sftp.files)
            sftp = SFTP()
            sftp.bad_size = True
            with self.assertRaises(RuntimeError):
                runner.upload_atomic(sftp, path, '/out/data', False)
            self.assertNotIn('/out/data', sftp.files)
            self.assertFalse(any(e[0] == 'rename' for e in sftp.events))


if __name__ == '__main__':
    unittest.main()
