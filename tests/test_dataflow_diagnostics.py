# Author: LIM UI JIN
# Created: 2026-10-23

import ast
import importlib.util
import logging
import json
from pathlib import Path
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('diagnostics', ROOT / 'dags/framework/dataflow_diagnostics.py')
d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(d)


class Response:
    def __init__(self, data):
        self.data = data
    def raise_for_status(self):
        pass
    def iter_content(self, chunk_size):
        yield json.dumps(self.data).encode()
    def close(self):
        pass


class DiagnosticsTest(unittest.TestCase):
    def call(self, responses=(), **kwargs):
        logger = Mock()
        session = Mock()
        session.request.side_effect = [r if isinstance(r, Exception) else Response(r) for r in responses]
        values = dict(logger=logger, credentials=object(), project_id='project', region='region',
                      job_id='job123', job_name='job-name', launch_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                      now=datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc),
                      session_factory=lambda c: session)
        values.update(kwargs)
        d.collect_failure_diagnostics(**values)
        text = '\n'.join(str(c) for c in logger.warning.call_args_list)
        return session, text

    def test_messages_and_info_container_logs(self):
        s, text = self.call([{'entries': [{'severity': 'INFO', 'textPayload': 'root cause', 'logName': 'launcher'}]}, {'entries': []}])
        self.assertIn('root cause', text)
        self.assertEqual(s.request.call_count, 2)
        body = s.request.call_args_list[0].kwargs['json']
        self.assertIn('resource.labels.job_id="job123"', body['filter'])
        self.assertNotIn('severity', body['filter'])
        self.assertEqual(body['orderBy'], 'timestamp asc')
        s.close.assert_called_once()

    def test_no_id_does_not_fetch(self):
        s, text = self.call(job_id=None)
        s.request.assert_not_called()
        self.assertIn('No verified Dataflow job ID', text)

    def test_permission_error_not_printed_and_other_source_attempted(self):
        s, text = self.call([PermissionError('password=highlysecret'), {'entries': [{'textPayload': 'context'}]}])
        self.assertIn('PermissionError', text)
        self.assertNotIn('highlysecret', text)
        self.assertIn('context', text)

    def test_no_wait_or_retries_when_logs_delayed(self):
        s, text = self.call([{}, {}])
        self.assertEqual(s.request.call_count, 2)
        self.assertIn('delayed', text)

    def test_pages_bounded_and_deduplicated(self):
        page = {'entries': [{'textPayload': 'duplicate'}], 'nextPageToken': 'again'}
        s, text = self.call([page, page, {'entries': []},])
        self.assertEqual(s.request.call_count, 3)
        self.assertEqual(text.count('duplicate'), 1)
        self.assertEqual(s.request.call_args_list[1].kwargs['json']['pageToken'], 'again')

    def test_deadline_and_request_timeouts(self):
        clock = iter([0, 21, 22, 23, 24])
        s, text = self.call(monotonic=lambda: next(clock))
        s.request.assert_not_called()
        s, _ = self.call([{}, {}])
        for call in s.request.call_args_list:
            self.assertLessEqual(call.kwargs['timeout'], 5)
            self.assertLessEqual(call.kwargs['max_allowed_time'], 20)

    def test_limits_and_redaction(self):
        entries = [{'textPayload': f'item{i} password="p@ss" ' + 'x' * 10000} for i in range(10)]
        _, text = self.call([{}, {'entries': entries}])
        self.assertNotIn('p@ss', text)
        self.assertIn('[truncated]', text)
        self.assertIn('[REDACTED]', text)
        self.assertNotIn('TimeoutError', text)
        self.assertLess(len(text), 140000)

    def test_response_size_cap(self):
        _, text = self.call([{'entries': [{'textPayload': 'x' * (d.MAX_RESPONSE_BYTES + 1)}]}, {}])
        self.assertIn('TimeoutError', text)

    def test_secret_formats(self):
        text = d.redact('jdbc:oracle:thin:user/pass@host token=abc Authorization: Bearer def https://u:p@example.test "password": "secret space" knownvalue', ('knownvalue',))
        for value in ('user/pass', 'abc', 'Bearer def', 'u:p@', 'secret space', 'knownvalue'):
            self.assertNotIn(value, text)

    def test_jdbc_section_reserved_and_independently_emitted(self):
        row = {'textPayload': 'JDBC QUERY RESULT result1', 'timestamp': '2026-01-01T00:00:00Z', 'logName': 'worker'}
        s, text = self.call([{'entries': [row]}, {'entries': [row]}])
        self.assertEqual(text.count('JDBC QUERY RESULT result1'), 2)
        self.assertIn('textPayload:"JDBC QUERY RESULT"', s.request.call_args_list[1].kwargs['json']['filter'])
        noisy = [{'textPayload': str(i) + 'x' * 10000} for i in range(10)]
        _, text = self.call([{'entries': noisy}, {'entries': [row]}])
        self.assertIn('result1', text)

    def test_json_message_and_newlines_preserved(self):
        _, text = self.call([{'entries': [{'jsonPayload': {'message': 'Traceback\nerror line'}, 'logName': 'launcher'}]}, {}])
        self.assertIn('launcher', text)
        self.assertIn('Traceback', text)
        self.assertIn('error line', text)

    def test_session_setup_failure_is_best_effort(self):
        _, text = self.call(session_factory=Mock(side_effect=ValueError('private')))
        self.assertIn('ValueError', text)
        self.assertNotIn('private', text)


def execute_method():
    tree = ast.parse((ROOT / 'dags/framework/dataflow_executor.py').read_text())
    klass = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'DataflowJdbcOperator')
    method = next(n for n in klass.body if isinstance(n, ast.FunctionDef) and n.name == 'execute')
    return compile(ast.Module(body=[method], type_ignores=[]), '<executor>', 'exec')


class OperatorTest(unittest.TestCase):
    def run_operator(self, launch=None, states=(), launch_error=None, diagnostic_error=None, conn_override=None):
        conn = conn_override or SimpleNamespace(extra_dejson={}, login='user', password='pass')
        creds = object()
        service = Mock()
        regional = service.projects.return_value.locations.return_value
        regional.flexTemplates.return_value.launch.return_value.execute.return_value = launch or {'job': {'id': '123'}}
        regional.flexTemplates.return_value.launch.return_value.execute.side_effect = launch_error
        regional.jobs.return_value.get.return_value.execute.side_effect = [{'currentState': s} for s in states]
        diagnostic = Mock(side_effect=diagnostic_error)
        env = {'BaseHook': Mock(get_connection=Mock(return_value=conn)), 'GCSHook': Mock(),
               'unused': None,
               '_jdbc_url': lambda *a: 'jdbc:url', '_safe_job_name': lambda x: x,
               'datetime': datetime, 'timezone': timezone, 'LOGGER': Mock(),
               'collect_failure_diagnostics': diagnostic, 'redact': d.redact, 'json': json,
               '_TERMINAL_STATES': {'JOB_STATE_DONE', 'JOB_STATE_FAILED'}, '_SUCCESS_STATES': {'JOB_STATE_DONE'},
               'time': Mock()}
        exec(execute_method(), env)
        op = SimpleNamespace(connection_id='conn', password_secret='secret-ref', sql='select 1', task_id='task',
            gcp_conn_id='configured', impersonation_chain='impersonated', system_bucket='bucket', db_type='oracle',
            driver_jars='jar', destination_type='gcs', destination={'path':'gs://out'}, project_id='project', region='region',
            job_name_prefix='prefix', template_gcs_path='gs://template', service_account_email='worker',
            subnetwork='subnet', staging_location='gs://staging', temp_location='gs://temp', log=Mock(), poll_interval_seconds=1)
        context = {'run_id':'run', 'dag':SimpleNamespace(dag_id='dag'), 'ts_nodash':'20260101T000000'}
        # googleapiclient is deliberately not required for offline tests.
        import sys
        auth = SimpleNamespace(default=Mock(return_value=(creds, 'project')))
        modules = {'google': SimpleNamespace(auth=auth), 'google.auth': auth, 'googleapiclient': Mock(), 'googleapiclient.discovery': SimpleNamespace(build=Mock(return_value=service))}
        with patch.dict(sys.modules, modules):
            try:
                result = env['execute'](op, context)
                error = None
            except Exception as exc:
                result, error = None, exc
        return diagnostic, result, error, env

    def test_success_does_not_collect(self):
        diag, result, error, env = self.run_operator(states=['JOB_STATE_DONE'])
        self.assertIsNone(error)
        diag.assert_not_called()
        self.assertEqual(result['job_id'], '123')
        # The launch identity remains ADC rather than switching to the configured GCS impersonation.

    def test_failed_state_collects_before_failure(self):
        diag, _, error, _ = self.run_operator(states=['JOB_STATE_FAILED'])
        diag.assert_called_once()
        self.assertIn('JOB_STATE_FAILED', str(error))

    def test_launch_error_preserved(self):
        original = ValueError('launch failed')
        diag, _, error, _ = self.run_operator(launch_error=original)
        diag.assert_called_once()
        self.assertIn("ValueError", str(error))
        self.assertIn("launch failed", str(error))
        self.assertTrue(error.__suppress_context__)
        self.assertIsNone(diag.call_args.kwargs['job_id'])

    def test_password_lookup_cannot_mask_dataflow_failure(self):
        class Connection:
            extra_dejson = {}
            login = 'user'
            @property
            def password(self):
                raise ValueError('decryption failed')
        diag, _, error, _ = self.run_operator(states=['JOB_STATE_FAILED'], conn_override=Connection())
        diag.assert_called_once()
        self.assertIn('JOB_STATE_FAILED', str(error))
        self.assertNotIn('decryption', str(error))

    def test_diagnostic_failure_does_not_mask_original_state(self):
        _, _, error, _ = self.run_operator(states=['JOB_STATE_FAILED'], diagnostic_error=PermissionError('private'))
        self.assertIn('JOB_STATE_FAILED', str(error))
        self.assertNotIn('private', str(error))

    def test_launch_api_error_is_actionable_without_raw_body(self):
        class FakeHttpError(Exception):
            content = b'{"error": {"message": "Permission denied password=abc", "private": "not-for-logs"}}'
            resp = SimpleNamespace(status=403)
        _, _, error, _ = self.run_operator(launch_error=FakeHttpError('raw-body-secret'))
        self.assertIn('Permission denied', str(error))
        self.assertIn('403', str(error))
        for secret in ('abc', 'not-for-logs', 'raw-body-secret'):
            self.assertNotIn(secret, str(error))

    def test_missing_id_no_raw_response(self):
        diag, _, error, _ = self.run_operator(launch={'private': 'password-value'})
        diag.assert_called_once()
        self.assertNotIn('password-value', str(error))


if __name__ == '__main__':
    unittest.main()
