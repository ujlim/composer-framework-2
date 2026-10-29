"""Offline tests: real executor/launcher with Airflow, Beam and cloud APIs doubled."""
import argparse
import ast
import importlib
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = ROOT / 'tools/dataflow/jdbc-export/docker/main.py'


def modules_for_executor():
    modules = {}
    def module(name):
        if name not in modules:
            modules[name] = types.ModuleType(name)
            modules[name].__path__ = []
            parent, _, leaf = name.rpartition('.')
            if parent:
                setattr(module(parent), leaf, modules[name])
        return modules[name]
    class Operator:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
    module('airflow.models').BaseOperator = Operator
    module('airflow.hooks.base').BaseHook = MagicMock()
    module('airflow.providers.google.cloud.hooks.gcs').GCSHook = MagicMock()
    module('googleapiclient.discovery').build = MagicMock()
    module('framework').__path__ = [str(ROOT / 'dags/framework')]
    return modules


class ExecutorTests(unittest.TestCase):
    def setUp(self):
        p = patch.dict(sys.modules, {})
        p.start()
        self.addCleanup(p.stop)
        for name in list(sys.modules):
            if name == 'framework' or name.startswith('framework.'):
                del sys.modules[name]
        sys.modules.update(modules_for_executor())
        self.m = importlib.import_module('framework.dataflow_executor')
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        directory = Path(self.tmp.name)
        (directory / 'query.sql').write_text('SELECT 1 AS id FROM dual')
        (directory / 'schema.json').write_text('[{"name":"id","type":"INTEGER"}]')
        self.config = self.m.LoadedConfig(data={}, source_path=directory / 'config.yaml')
        self.df = dict(project_id='test-project', region='asia-northeast3',
                       template_gcs_path='gs://test/template.json', system_bucket='test',
                       subnetwork='regions/asia-northeast3/subnetworks/test',
                       service_account_email='worker@test.iam.gserviceaccount.com')
        self.grape = dict(grape_id='export', type='jdbc_to_bigquery', sql_file='query.sql',
                          source=dict(db_type='oracle', connection_id='oracle', driver_jars='gs://test/ojdbc.jar'),
                          destination=dict(table='test:staging.output', schema_file='schema.json'))
    def task(self):
        return self.m.DataflowExecutor.create_task(dag=object(), vine_config=self.config,
                    grape=self.grape, runtime={'dataflow': self.df}, defaults={})
    def execute(self, task):
        self.m.BaseHook.get_connection.return_value = types.SimpleNamespace(
            extra_dejson={'password_secret': 'projects/test/secrets/password/versions/latest'},
            login='user', host='oracle', schema='service', port=1521)
        build = sys.modules['googleapiclient.discovery'].build
        service = build.return_value
        service.projects.return_value.locations.return_value.flexTemplates.return_value.launch.return_value.execute.return_value = {'job': {'id': 'job-id'}}
        service.projects.return_value.locations.return_value.jobs.return_value.get.return_value.execute.return_value = {'currentState': 'JOB_STATE_DONE'}
        task.execute({'run_id': 'run-1', 'dag': types.SimpleNamespace(dag_id='test'), 'ts_nodash': '20261029T000000'})
        return service.projects.return_value.locations.return_value.flexTemplates.return_value.launch.call_args.kwargs['body']['launchParameter']
    def test_unset_preserves_request(self):
        result = self.execute(self.task())
        self.assertNotIn('machineType', result['environment'])
        self.assertNotIn('fetch_size', result['parameters'])
        self.assertEqual(result['parameters']['write_disposition'], 'WRITE_APPEND')
    def test_null_preserves_request(self):
        self.df['machine_type'] = None
        self.grape['source']['fetch_size'] = None
        result = self.execute(self.task())
        self.assertNotIn('machineType', result['environment'])
        self.assertNotIn('fetch_size', result['parameters'])
    def test_explicit_values_forwarded(self):
        self.df['machine_type'] = ' n2-highmem-2 '
        self.grape['source']['fetch_size'] = 1000
        result = self.execute(self.task())
        self.assertEqual(result['environment']['machineType'], 'n2-highmem-2')
        self.assertEqual(result['parameters']['fetch_size'], '1000')
        self.assertEqual(result['environment']['ipConfiguration'], 'WORKER_IP_PRIVATE')
    def test_fetch_boundaries(self):
        for value in [1, 32767]:
            with self.subTest(value=value):
                self.grape['source']['fetch_size'] = value
                self.assertEqual(self.execute(self.task())['parameters']['fetch_size'], str(value))
    def test_invalid_fetch_fails_before_api(self):
        for value in [True, False, 0, -1, 32768, 1000.0, '1000', [], {}]:
            with self.subTest(value=value):
                self.grape['source']['fetch_size'] = value
                with self.assertRaisesRegex(ValueError, 'source.fetch_size'):
                    self.task()
        sys.modules['googleapiclient.discovery'].build.assert_not_called()
        self.m.GCSHook.assert_not_called()
    def test_invalid_machine(self):
        for value in ['', ' ', True, 2, [], {}]:
            with self.subTest(value=value):
                self.df['machine_type'] = value
                with self.assertRaisesRegex(ValueError, 'machine_type'):
                    self.task()
    def test_machine_only(self):
        self.df['machine_type'] = 'n2-highmem-2'
        result = self.execute(self.task())
        self.assertNotIn('fetch_size', result['parameters'])
        self.assertEqual(result['environment']['machineType'], 'n2-highmem-2')
    def test_fetch_only_gcs(self):
        self.grape.update(type='jdbc_to_gcs', destination={'path': 'gs://test/output'})
        self.grape['source']['fetch_size'] = 1000
        result = self.execute(self.task())
        self.assertNotIn('machineType', result['environment'])
        self.assertEqual(result['parameters']['fetch_size'], '1000')
        self.assertEqual(result['parameters']['destination_type'], 'gcs')
    def test_options_do_not_override_sources(self):
        self.grape['options'] = {'fetch_size': 500, 'machine_type': 'bad-location'}
        result = self.execute(self.task())
        self.assertNotIn('machineType', result['environment'])
        self.assertNotIn('fetch_size', result['parameters'])


class LauncherTests(unittest.TestCase):
    def run_launcher(self, fetch=None):
        # Compile the actual run() AST; all cloud / Beam endpoints are doubles.
        tree = ast.parse(LAUNCHER.read_text())
        run = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'run')
        jdbc = MagicMock()
        bq = MagicMock()
        namespace = dict(argparse=argparse, logging=MagicMock(), google=MagicMock(),
                         beam=MagicMock(), PipelineOptions=MagicMock(), ReadFromJdbc=jdbc,
                         WriteToBigQuery=bq, DRIVER_CLASS={'oracle': 'oracle.jdbc.OracleDriver'},
                         read_gcs_text=MagicMock(return_value='SELECT 1'), access_secret=MagicMock(return_value='secret'),
                         row_dict=lambda row: row, json=json)
        namespace['google'].auth.default.return_value = (object(), 'test')
        exec(compile(ast.Module(body=[run], type_ignores=[]), str(LAUNCHER), 'exec'), namespace)
        argv = ['main.py', '--db_type=oracle', '--connection_url=jdbc:test', '--username=test',
                '--password_secret=secret-resource', '--driver_jars=test.jar', '--query_uri=gs://test/sql',
                '--destination_type=bigquery', '--output_table=test:staging.output', '--bq_schema=[]',
                '--runner=DataflowRunner']
        if fetch is not None:
            argv.append('--fetch_size=' + str(fetch))
        with patch.object(sys, 'argv', argv):
            namespace['run']()
        return namespace, jdbc, bq
    def test_fetch_forwarded_and_file_loads_unchanged(self):
        namespace, jdbc, bq = self.run_launcher(1000)
        self.assertEqual(jdbc.call_args.kwargs['fetch_size'], 1000)
        self.assertEqual(jdbc.call_args.kwargs['query'], 'SELECT 1')
        self.assertEqual(bq.call_args.kwargs['method'], bq.Method.FILE_LOADS)
        self.assertEqual(namespace['PipelineOptions'].call_args.args[0], ['--runner=DataflowRunner'])
    def test_unset_fetch(self):
        _, jdbc, _ = self.run_launcher()
        self.assertIsNone(jdbc.call_args.kwargs['fetch_size'])
    def test_launcher_boundaries(self):
        for value in [1, 32767]:
            _, jdbc, _ = self.run_launcher(value)
            self.assertEqual(jdbc.call_args.kwargs['fetch_size'], value)
    def test_invalid_cli(self):
        for value in ['true', '1000.0', 0, -1, 32768]:
            with self.subTest(value=value), self.assertRaises(SystemExit), patch('sys.stderr'):
                self.run_launcher(value)
    def test_metadata_optional(self):
        metadata = json.loads((LAUNCHER.parent / 'metadata.json').read_text())
        fields = [p for p in metadata['parameters'] if p['name'] == 'fetch_size']
        self.assertEqual(len(fields), 1)
        self.assertTrue(fields[0]['isOptional'])
        self.assertFalse(metadata['streaming'])


if __name__ == '__main__':
    unittest.main()
