"""Offline Runner lifecycle tests; run: python -m unittest discover -s tests -v."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import tempfile
from threading import Event
import unittest
from unittest.mock import Mock, patch

from test_csv_delimiters import Blob, Bucket, SFTP, runner


class RunnerConnectionOrderTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.temp_dir = Path(directory.name)
        self.contents = {'in/b.csv': b'id,name\n2,beta\n',
                         'in/a.csv': b'id,name\n1,alpha\n'}
        self.original_contents = dict(self.contents)
        self.blobs = [Blob(name, self.contents) for name in self.contents]
        self.events = []
        self.ssh, self.sftp = Mock(), SFTP()
        self.config = {'runner': {'temp_dir': directory.name},
                       'targets': {'test': {'host': 'host', 'username': 'user',
                                            'auth_type': 'private_key', 'key_file': '/fake/key'}}}
        client = Mock()
        client.bucket.return_value = Bucket(self.contents)
        client.list_blobs.return_value = self.blobs
        self.patch(runner, 'load_config', return_value=self.config)
        self.patch(runner, 'setup_logging')
        self.patch(runner, 'build_google_credentials', return_value=(None, 'test'))
        self.patch(runner.storage, 'Client', return_value=client)
        self.real_connect = runner.connect
        self.connect = self.patch(runner, 'connect', side_effect=self.connected)
        self.real_upload = runner.upload_file
        self.upload = self.patch(runner, 'upload_file', side_effect=self.uploaded)
        real_count = runner.count_csv_rows

        def count(path, *args):
            result = real_count(path, *args)
            if path.name.startswith('sftp-runner-merged-'):
                self.events.append('validated')
            return result

        self.count = self.patch(runner, 'count_csv_rows', side_effect=count)

    def patch(self, target, name, **kwargs):
        patcher = patch.object(target, name, **kwargs)
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    def connected(self, *args):
        self.events.append('connected')
        return self.ssh, self.sftp

    def uploaded(self, sftp, path, remote_path, overwrite, upload_mode):
        self.events.append(remote_path)
        return self.real_upload(sftp, path, remote_path, overwrite, upload_mode)

    def run_main(self, *extra, merge=True):
        argv = ['runner', '--target', 'test', '--bucket', 'bucket', '--prefix', 'in/',
                '--remote-dir', '/out', '--delete-source']
        if merge:
            argv += ['--merge-filename', 'merged.csv', '--csv-header',
                     '--fin-filename', 'rows.fin', '--file-count-fin-filename', 'files.fin']
        with patch.object(sys, 'argv', argv + list(extra)):
            return runner.main()

    def assert_clean(self, connected):
        self.assertEqual(list(self.temp_dir.iterdir()), [])
        if connected:
            self.ssh.close.assert_called_once_with()
            self.assertTrue(self.sftp.closed)
        else:
            self.ssh.close.assert_not_called()
            self.assertFalse(self.sftp.closed)

    def assert_sources_untouched(self):
        self.assertEqual(self.contents, self.original_contents)
        self.assertFalse(any(blob.deleted for blob in self.blobs))

    def test_slow_merge_does_not_open_sftp_until_ready(self):
        # Block a real merge's download to simulate slow preparation without sleeping.
        started, release = Event(), Event()
        real_download = self.blobs[0].download_to_filename

        def slow_download(filename):
            started.set()
            if not release.wait(timeout=5):
                raise RuntimeError('Test did not release the blocked download')
            real_download(filename)

        self.patch(self.blobs[0], 'download_to_filename', side_effect=slow_download)
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(self.run_main)
            try:
                self.assertTrue(started.wait(timeout=5))
                self.connect.assert_not_called()
                self.upload.assert_not_called()
                self.assertEqual(self.sftp.files, {})
                self.assert_sources_untouched()
            finally:
                release.set()
            self.assertEqual(result.result(timeout=5), 0)
        self.assertEqual(self.events, ['validated', 'connected', '/out/merged.csv',
                                      '/out/rows.fin', '/out/files.fin'])
        self.assert_clean(connected=True)

    def test_row_verification_failure_never_connects(self):
        self.count.side_effect = [1, 1, 3]
        with self.assertRaisesRegex(RuntimeError, 'CSV row verification failed'):
            self.run_main()
        self.connect.assert_not_called()
        self.upload.assert_not_called()
        self.assert_sources_untouched()
        self.assert_clean(connected=False)

    def test_download_failure_cleans_partial_files_without_connecting(self):
        def fail_download(filename):
            Path(filename).write_bytes(b'partial')
            raise OSError('GCS download failed')

        self.patch(self.blobs[0], 'download_to_filename', side_effect=fail_download)
        with self.assertRaisesRegex(OSError, 'GCS download failed'):
            self.run_main()
        self.connect.assert_not_called()
        self.upload.assert_not_called()
        self.assert_sources_untouched()
        self.assert_clean(connected=False)

    def test_connect_failure_cleans_completed_merge(self):
        def fail_connect(*args):
            self.assertEqual(self.events, ['validated'])
            self.assertEqual(len(list(self.temp_dir.glob('sftp-runner-merged-*'))), 1)
            raise OSError('SFTP connection failed')

        self.connect.side_effect = fail_connect
        with self.assertRaisesRegex(OSError, 'SFTP connection failed'):
            self.run_main()
        self.connect.assert_called_once()
        self.upload.assert_not_called()
        self.assert_sources_untouched()
        self.assert_clean(connected=False)

    def test_existing_connect_retries_close_failed_clients_and_clean_merge(self):
        clients = [Mock() for _ in range(3)]
        for client in clients:
            client.open_sftp.side_effect = OSError('SFTP subsystem failed')
        factory = self.patch(runner.paramiko, 'SSHClient', side_effect=clients, create=True)
        self.patch(runner.paramiko, 'RejectPolicy', return_value=object(), create=True)
        sleep = self.patch(runner.time, 'sleep')
        self.connect.side_effect = self.real_connect
        with self.assertRaisesRegex(OSError, 'SFTP subsystem failed'):
            self.run_main()
        self.assertEqual(factory.call_count, 3)
        self.assertEqual([call.args for call in sleep.call_args_list], [(5,), (5,)])
        for client in clients:
            client.close.assert_called_once_with()
        self.assertEqual(self.events, ['validated'])
        self.upload.assert_not_called()
        self.assert_sources_untouched()
        self.assert_clean(connected=False)

    def test_existing_connect_retry_can_succeed_after_merge(self):
        failed = Mock()
        failed.connect.side_effect = OSError('SSH connection failed')
        self.ssh.open_sftp.return_value = self.sftp
        self.patch(runner.paramiko, 'SSHClient', side_effect=[failed, self.ssh], create=True)
        self.patch(runner.paramiko, 'RejectPolicy', return_value=object(), create=True)
        sleep = self.patch(runner.time, 'sleep')
        self.connect.side_effect = self.real_connect
        self.assertEqual(self.run_main(), 0)
        failed.close.assert_called_once_with()
        sleep.assert_called_once_with(5)
        self.assertEqual(self.sftp.files['/out/rows.fin'], b'2\n')
        self.assertEqual(self.sftp.files['/out/files.fin'], b'1\n')
        self.assert_clean(connected=True)

    def test_failed_data_upload_never_uploads_fin_or_deletes_sources(self):
        for mode in ('atomic', 'direct'):
            with self.subTest(mode=mode):
                self.sftp = SFTP()
                self.sftp.bad_size = True
                self.ssh.reset_mock()
                self.upload.reset_mock()
                with self.assertRaisesRegex(RuntimeError, 'size verification failed'):
                    self.run_main('--upload-mode', mode)
                self.assertEqual([call.args[2] for call in self.upload.call_args_list], ['/out/merged.csv'])
                self.assertFalse(any('.fin' in name for name in self.sftp.files))
                self.assert_sources_untouched()
                self.assert_clean(connected=True)

    def test_fin_failure_preserves_sources_and_cleans_local_files(self):
        for failed_fin in ('rows.fin', 'files.fin'):
            with self.subTest(failed_fin=failed_fin):
                self.sftp = SFTP()
                self.ssh.reset_mock()
                self.upload.reset_mock()

                def upload(sftp, path, remote, overwrite, mode):
                    if remote == '/out/' + failed_fin:
                        raise OSError('FIN upload failed')
                    return self.real_upload(sftp, path, remote, overwrite, mode)

                self.upload.side_effect = upload
                with self.assertRaisesRegex(OSError, 'FIN upload failed'):
                    self.run_main()
                self.assertIn('/out/merged.csv', self.sftp.files)
                self.assertNotIn('/out/' + failed_fin, self.sftp.files)
                self.assert_sources_untouched()
                self.assert_clean(connected=True)

    def test_success_uploads_data_then_fin_before_source_deletion(self):
        for mode in ('atomic', 'direct'):
            with self.subTest(mode=mode):
                self.contents.update(self.original_contents)
                for blob in self.blobs:
                    blob.deleted = False
                self.sftp = SFTP()
                self.ssh.reset_mock()
                self.events.clear()

                def upload(sftp, path, remote, overwrite, upload_mode):
                    self.assert_sources_untouched()
                    return self.uploaded(sftp, path, remote, overwrite, upload_mode)

                self.upload.side_effect = upload
                self.assertEqual(self.run_main('--upload-mode', mode), 0)
                self.assertEqual(self.events, ['validated', 'connected', '/out/merged.csv',
                                              '/out/rows.fin', '/out/files.fin'])
                self.assertEqual(self.sftp.files['/out/merged.csv'], b'id,name\n1,alpha\n2,beta\n')
                self.assertEqual(self.sftp.files['/out/rows.fin'], b'2\n')
                self.assertEqual(self.sftp.files['/out/files.fin'], b'1\n')
                self.assertEqual(self.contents, {})
                self.assertTrue(all(blob.deleted for blob in self.blobs))
                self.assert_clean(connected=True)

    def test_non_merge_still_connects_before_downloading(self):
        def download(filename):
            self.connect.assert_called_once()
            Path(filename).write_bytes(self.original_contents[self.blobs[0].name])

        self.patch(self.blobs[0], 'download_to_filename', side_effect=download)
        merge = self.patch(runner, 'merge_csv_blobs')
        self.assertEqual(self.run_main(merge=False), 0)
        merge.assert_not_called()
        self.assertEqual(self.sftp.files, {'/out/a.csv': self.original_contents['in/a.csv'],
                                          '/out/b.csv': self.original_contents['in/b.csv']})
        self.assert_clean(connected=True)

    def test_sftp_close_failure_still_closes_ssh(self):
        self.patch(self.sftp, 'close', side_effect=OSError('SFTP close failed'))
        with self.assertRaisesRegex(OSError, 'SFTP close failed'):
            self.run_main()
        self.assertEqual(list(self.temp_dir.iterdir()), [])
        self.ssh.close.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
