# Author: 임의진 (LIM UI JIN)
# Created: 2026-10-30
"""Offline API/clock doubles; no Dataflow, JDBC or BigQuery integration claims."""
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]


def load_module():
    # Avoid importing Airflow; restore sys.modules for other offline test suites.
    with patch.dict(sys.modules, {}):
        framework = types.ModuleType('framework')
        framework.__path__ = [str(ROOT / 'dags/framework')]
        sys.modules['framework'] = framework
        spec = importlib.util.spec_from_file_location('progress', ROOT / 'dags/framework/dataflow_progress.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


p = load_module()


class Response:
    def __init__(self, data):
        self.data = data
    def raise_for_status(self):
        pass
    def iter_content(self, chunk_size):
        yield json.dumps(self.data).encode()
    def close(self):
        pass


class ProgressTests(unittest.TestCase):
    def reporter(self, responses=(), **kwargs):
        self.tick = 0
        self.session = Mock()
        self.session.request.side_effect = [x if isinstance(x, Exception) else Response(x) for x in responses]
        self.logger = Mock()
        values = dict(logger=self.logger, credentials=object(), project_id='p', region='r', job_id='j',
                      dag_id='dag', run_id='run', task_id='task', details=True,
                      session_factory=lambda _: self.session, monotonic=lambda: self.tick,
                      now=lambda: datetime(2026, 10, 30, 0, 5, tzinfo=timezone.utc))
        values.update(kwargs)
        return p.ProgressReporter(**values)
    def text(self):
        return '\n'.join(str(call) for call in self.logger.info.call_args_list)
    def test_missing_is_unknown_not_zero(self):
        r = self.reporter([{}, {}, {}]); result = r.poll('JOB_STATE_RUNNING')
        self.assertEqual(result['source_jdbc_rows'], 'unknown')
        self.assertEqual(result['memory_usage'], 'unknown')
        self.assertEqual(result['metrics'], [])
        self.assertIn('does not prove', result['limitation'])
    def test_interval_and_cached_final(self):
        r = self.reporter([{}, {}, {}]); r.poll(); self.tick=59; r.poll()
        self.assertEqual(self.session.request.call_count, 3)
        r.poll('JOB_STATE_FAILED', final=True)
        self.assertEqual(self.session.request.call_count, 3)
        self.assertIn('"cached": true', self.text())
        self.assertIn('JOB_STATE_FAILED', self.text())
    def test_invalid_intervals(self):
        for value in [True, 0, 59, 60.0, '60', None]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.reporter(interval=value)
        self.reporter(interval=60)
    def test_permission_failure_preserves_other_observations(self):
        r = self.reporter([PermissionError('password=secret'), PermissionError('bearer secret'), {}])
        result = r.poll()
        self.assertEqual(len(result['unavailable']), 2)
        self.assertNotIn('secret', self.text())
        self.assertEqual(self.session.request.call_count, 3)
    def test_map_exact_stage_and_running_elapsed(self):
        desc = {'pipelineDescription': {'executionPipelineStage': [{'id':'s1','name':'Read JDBC',
                'componentTransform':[{'userName':'Reparallelize'}], 'prerequisiteStage':['s0']}]}}
        stage = {'stageId':'s1','state':'EXECUTION_STATE_RUNNING','startTime':'2026-10-30T00:00:00Z',
                 'endTime':'2030-01-01T00:00:00Z', 'progress':{'currentProgress':0.5}}
        r = self.reporter([desc, {'stages':[stage]}, {}, {'workers':[{'workerName':'w', 'workItems':[{'taskId':'t','attemptId':'a','state':'EXECUTION_STATE_RUNNING'}]}]}])
        result = r.poll(); s=result['stages'][0]
        self.assertEqual(s['mapped_name'], 'Read JDBC'); self.assertEqual(s['elapsed_seconds'],300)
        self.assertEqual(s['stage_mapping']['prerequisites'],['s0'])
        self.assertEqual(result['worker_sample'][0]['attempt_id'],'a')
        self.assertNotIn('active_worker_count',result)
    def test_no_fuzzy_stage_mapping(self):
        r = self.reporter([{'pipelineDescription':{'executionPipelineStage':[{'id':'abc','name':'Read'}]}},
                           {'stages':[{'stageId':'abc1'}]}, {}])
        self.assertEqual(r.poll()['stages'][0]['mapped_name'],'unknown')
    def test_metrics_dedup_tentative_preserved_no_summing(self):
        committed = {'name':{'origin':'dataflow/v1b3','name':'ElementCount','context':{'step':'s1'}},'scalar':10}
        tentative = {'name':{'origin':'dataflow/v1b3','name':'ElementCount','context':{'step':'s1','tentative':'true'}},'scalar':12}
        result=p._metrics([committed, committed, tentative], 'getMetrics')
        self.assertEqual(len(result),2); self.assertEqual([x['scalar'] for x in result],[10,12])
        self.assertEqual([x['tentative'] for x in result],[False,True])
        self.assertIsNone(result[0]['stage_id']); self.assertEqual(result[0]['unit'],'unknown')
    def test_non_scalar_and_memory_accounting_not_inferred(self):
        result=p._metrics([{'name':{'name':'ElementCount'},'distribution':{'count':5}},
                           {'name':{'name':'TotalMemoryUsage'},'scalar':999}], 'getMetrics')
        self.assertEqual(len(result),1); self.assertIsNone(result[0]['scalar'])
    def test_state_changes_not_inferred_retry(self):
        r=self.reporter([{}, {'stages':[{'stageId':'s','state':'EXECUTION_STATE_NOT_STARTED'}]}, {},
                         {'stages':[{'stageId':'s','state':'EXECUTION_STATE_COMPLETE'}]}, {}])
        r.poll(); self.tick=60; result=r.poll()
        self.assertTrue(result['stages'][0]['state_changed'])
        self.assertEqual(result['stages'][0]['previous_state'],'EXECUTION_STATE_NOT_STARTED')
        self.assertNotIn('retry_count',result)
    def test_pagination_is_bounded(self):
        r=self.reporter([{}, {'stages':[], 'nextPageToken':'p2'}, {'stages':[], 'nextPageToken':'p3'}, {}])
        result=r.poll(); self.assertTrue(result['partial']); self.assertEqual(self.session.request.call_count,4)
    def test_response_budget_timeout_safe(self):
        r=self.reporter([{'x':'x'*(p.MAX_RESPONSE_BYTES+1)}, {}, {}])
        self.assertEqual(r.poll()['unavailable'][0]['error_type'],'TimeoutError')
    def test_fail_open_session_and_logging(self):
        r=self.reporter(session_factory=Mock(side_effect=RuntimeError('private')))
        self.assertIsNone(r.poll(final=True)); self.assertNotIn('private',self.text())
        self.logger.info.side_effect=RuntimeError('logger down'); r.emit('test')
    def test_redaction_context_mapping(self):
        r=self.reporter([{'pipelineDescription':{'executionPipelineStage':[{'id':'s','name':'password=xyz SELECT SECRET'}]}},
            {'stages':[{'stageId':'s','state':'EXECUTION_STATE_COMPLETE'}]},
            {'metrics':[{'name':{'name':'ElementCount','context':{'password':'abc','note':'SELECT SECRET'}},'scalar':1}]}],secrets=['SELECT SECRET'])
        r.poll(); text=self.text()
        for secret in ['xyz','abc','SELECT SECRET']:
            self.assertNotIn(secret,text)
    def test_requests_are_read_only_scoped_timeout(self):
        r=self.reporter([{}, {}, {}]); r.poll()
        for call in self.session.request.call_args_list:
            self.assertEqual(call.args[0],'GET')
            self.assertTrue(call.args[1].startswith('https://dataflow.googleapis.com/v1b3/projects/p/locations/r/jobs/j'))
            self.assertLessEqual(call.kwargs['timeout'],3)
            self.assertLessEqual(call.kwargs['max_allowed_time'],12)
        self.session.close.assert_called_once()
    def test_log_byte_cap_for_large_stage_and_unicode_context(self):
        r=self.reporter([{'pipelineDescription': {'executionPipelineStage': [{'id':'s','name':'x'*100000}]}},
                         {'stages':[{'stageId':'s'}]}, {}])
        r.poll()
        for call in self.logger.info.call_args_list:
            if "detail" not in call.args[0]:
                continue
            self.assertLessEqual(len(call.args[1].encode('utf-8')),48000)
            json.loads(call.args[1])
        r.emit('unicode', arbitrary='한'*30000)
        self.assertLessEqual(len(self.logger.info.call_args.args[1].encode('utf-8')),48000)
        self.assertIn('log_truncated',self.logger.info.call_args.args[1])

    def test_redaction_expansion_remains_bounded(self):
        r=self.reporter(secrets=['x'])
        r.emit('test', text='x'*40000)
        rendered=self.logger.info.call_args.args[1]
        self.assertLessEqual(len(rendered.encode('utf-8')),48000)
        json.loads(rendered)

    def test_post_jdbc_counter_prioritized(self):
        metrics=[{'name':{'name':'ElementCount','context':{'step':str(i)}},'scalar':1} for i in range(60)]
        metrics.append({'name':{'name':'post_jdbc_output_rows'},'scalar':2})
        self.assertEqual(p._metrics(metrics,'test')[0]['name'],'post_jdbc_output_rows')

    def test_source_metrics_scalar_zero_is_observed_not_unknown(self):
        result=p._metrics([{'name':{'name':'post_jdbc_output_rows'},'scalar':0}], 'getMetrics')
        self.assertEqual(result[0]['scalar'],0)


if __name__ == '__main__':
    unittest.main()
