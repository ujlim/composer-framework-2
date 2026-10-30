"""Offline observations only: no Beam Java, Oracle, Dataflow or BQ execution."""
import ast
import json
import logging
import os
from pathlib import Path
import socket
import time
import types
import unittest
from unittest.mock import MagicMock, patch

SOURCE = Path(__file__).resolve().parents[1] / 'tools/dataflow/jdbc-export/docker/main.py'


class LauncherProgressTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse(SOURCE.read_text())
        wanted = {'_progress_identifier', '_emit_progress', 'ObservePostJdbcRows', 'ObserveFileLoadOutput'}
        nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in wanted]
        self.metrics = MagicMock()
        self.ns = dict(beam=types.SimpleNamespace(DoFn=object), Metrics=self.metrics,
                       os=os, socket=socket, time=time, logging=logging, json=json)
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), 'exec'), self.ns)
        self.real_emit = self.ns['_emit_progress']
        self.events = []
        self.ns['_emit_progress'] = self.events.append

    def test_counter_preserves_identity_no_row_log(self):
        row = {'secret_value': 'do not log'}
        observer = self.ns['ObservePostJdbcRows']()
        self.assertIs(list(observer.process(row))[0], row)
        self.metrics.counter.assert_called_once_with('jdbc_export_progress', 'post_jdbc_output_rows')
        self.metrics.counter.return_value.inc.assert_called_once_with()
        self.assertEqual(self.events, [])

    def test_counter_failure_does_not_drop_row(self):
        self.metrics.counter.return_value.inc.side_effect = RuntimeError('secret')
        self.assertEqual(list(self.ns['ObservePostJdbcRows']().process(123)), [123])
        self.metrics.counter.side_effect = RuntimeError('secret')
        self.assertEqual(list(self.ns['ObservePostJdbcRows']().process(456)), [456])

    def test_file_actual_uri_bytes_and_sampling(self):
        observer = self.ns['ObserveFileLoadOutput']('gcs_temp_file_observed', 'test-job')
        with patch.object(time, 'monotonic', side_effect=[0, 5, 61]):
            for i in range(3):
                self.assertEqual(list(observer.process(('p:d.t', ('gs://b/temp/bq_load/id/file', 12)))), [])
        self.assertEqual(len(self.events), 2)
        self.assertEqual(self.events[1]['suppressed_since_previous'], 1)
        self.assertTrue(self.events[0]['sample_only'])
        self.assertEqual(self.events[0]['state'], 'FILE_CLOSED_OUTPUT_OBSERVED')
        self.assertEqual(self.events[0]['size_bytes'], 12)

    def test_load_reference_is_not_success_or_live_status(self):
        ref = types.SimpleNamespace(projectId='p', jobId='load_123', location='asia-northeast3')
        observer = self.ns['ObserveFileLoadOutput']('bq_load_reference_observed')
        list(observer.process(('p:d.t', ref)))
        self.assertEqual(self.events[0]['bq_job_id'], 'load_123')
        self.assertEqual(self.events[0]['state'], 'SUBMITTED_REFERENCE_OBSERVED')
        self.assertEqual(self.events[0]['current_state'], 'unknown')
        self.assertIn('ONGOING_JOBS', self.events[0]['status_source'])

    def test_missing_location_unknown_not_inferred(self):
        observer = self.ns['ObserveFileLoadOutput']('bq_copy_completion_observed')
        list(observer.process(('p:d.t', {'jobId': 'copy_123'})))
        self.assertEqual(self.events[0]['bq_location'], 'unknown')
        self.assertEqual(self.events[0]['bq_project'], 'unknown')
        self.assertEqual(self.events[0]['state'], 'SUCCESS_OUTPUT_OBSERVED')
        self.assertEqual(self.events[0]['current_state'], 'unknown')
        self.assertIn('copy completion output', self.events[0]['status_source'])

    def test_malformed_records_and_logging_failure_ignored(self):
        observer = self.ns['ObserveFileLoadOutput']('bq_load_reference_observed')
        for item in [None, ('p:d.t', None), ('p:d.t', {'jobId': None})]:
            self.assertEqual(list(observer.process(item)), [])
        self.assertEqual(self.events, [])
        self.ns['_emit_progress'] = MagicMock(side_effect=RuntimeError('do not log'))
        self.assertEqual(list(observer.process(('p:d.t', {'jobId': 'j'}))), [])

    def test_identifier_strips_signed_query_and_controls(self):
        self.assertEqual(self.ns['_progress_identifier']('gs://b/a?token=SECRET\n'), 'gs://b/a')
        self.assertEqual(self.ns['_progress_identifier'](None), 'unknown')
        self.assertEqual(self.ns['_progress_identifier']('abc\nxyz'), 'abcxyz')

    def test_real_emitter_pid_host_and_no_worker_id_inference(self):
        with patch.object(logging, 'info') as log, patch.object(os, 'getpid', return_value=123), patch.object(socket, 'gethostname', return_value='host-1'):
            self.real_emit({'event': 'test'})
        payload = json.loads(log.call_args.args[1])
        self.assertEqual(payload['observer_pid'], 123)
        self.assertEqual(payload['observer_host'], 'host-1')
        self.assertEqual(payload['worker_id'], 'unknown')
        self.assertTrue(payload['may_repeat_on_retry'])

    def test_real_emitter_failure_is_ignored(self):
        with patch.object(logging, 'info', side_effect=RuntimeError('secret')):
            self.real_emit({'event': 'test'})
        with patch.object(socket, 'gethostname', side_effect=RuntimeError('secret')):
            self.real_emit({'event': 'test'})

    def test_pipeline_write_method_and_source_unchanged(self):
        tree = ast.parse(SOURCE.read_text())
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
        jdbc = [n for n in calls if isinstance(n.func, ast.Name) and n.func.id == 'ReadFromJdbc'][0]
        self.assertNotIn('output_parallelization', {k.arg for k in jdbc.keywords})
        self.assertNotIn('fetch_size', {k.arg for k in jdbc.keywords})
        bq = [n for n in calls if isinstance(n.func, ast.Name) and n.func.id == 'WriteToBigQuery'][0]
        method = next(k.value for k in bq.keywords if k.arg == 'method')
        self.assertEqual(ast.unparse(method), 'WriteToBigQuery.Method.FILE_LOADS')
        source = SOURCE.read_text()
        for attr in ['destination_file_pairs', 'destination_load_jobid_pairs', 'destination_copy_jobid_pairs']:
            self.assertIn('write_result.' + attr, source)


if __name__ == '__main__':
    unittest.main()
