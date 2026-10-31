# Author: 임의진 (LIM UI JIN)
# Created: 2026-10-30
"""Human summaries and explicit detail mode; no live services."""
import json
import unittest
from unittest.mock import Mock
from test_dataflow_progress import p, Response
import test_dataflow_machine_type as machine_tests


class ReadabilityTests(unittest.TestCase):
    def make(self, **kwargs):
        self.log=Mock(); self.tick=0
        params=dict(logger=self.log,credentials=object(),project_id='p',region='r',job_id='job-123',
                    dag_id='dag',run_id='run',task_id='export',monotonic=lambda:self.tick)
        params.update(kwargs)
        return p.ProgressReporter(**params)
    def rendered(self):
        return '\n'.join(str(c.args[1]) for method in [self.log.info,self.log.warning] for c in method.call_args_list)
    def test_default_korean_no_massive_json_and_dashboard_prefix(self):
        r=self.make();r.emit('snapshot',snapshot={'job_state':'JOB_STATE_RUNNING','stages':[],'metrics':[],'arbitrary':'x'*50000})
        self.assertEqual(self.log.info.call_count,1)
        self.assertEqual(self.log.info.call_args.args[0],'Dataflow progress %s')
        text=self.rendered();self.assertIn('Job=job-123',text);self.assertIn('실행 중',text)
        self.assertIn('원천읽기/BQ확정적재=미확인',text);self.assertNotIn('{',text);self.assertNotIn('arbitrary',text)
    def test_opt_in_preserves_detail_event_model(self):
        r=self.make(details=True);r.emit('snapshot',snapshot={'stages':[],'metrics':[]},cached=False)
        self.assertEqual(self.log.info.call_count,2)
        call=self.log.info.call_args;self.assertEqual(call.args[0],'Dataflow progress detail %s')
        doc=json.loads(call.args[1]);self.assertEqual(doc['job_id'],'job-123');self.assertEqual(doc['event'],'snapshot')
        self.assertFalse(doc['cached']);self.assertIn('snapshot',doc)
    def test_strict_boolean(self):
        for value in ['false','true',0,1,None,[],{}]:
            with self.subTest(value=value),self.assertRaises(ValueError):self.make(details=value)
    def test_state_change_only(self):
        r=self.make();r.job_state('JOB_STATE_RUNNING');r.job_state('JOB_STATE_RUNNING');r.job_state('JOB_STATE_FAILED')
        self.assertEqual(self.log.info.call_count,2);self.assertIn('실패',self.rendered());self.assertIn('이전=실행 중',self.rendered())
    def test_missing_final_and_cached_final_are_explicit(self):
        r=self.make();r.emit('final_snapshot',snapshot=None,cached=True,job_state='JOB_STATE_FAILED')
        text=self.rendered();self.assertIn('관측자료 없음',text);self.assertIn('미확인',text);self.assertIn('현재 최종값 아님',text)
    def test_final_observer_failure_keeps_current_failed_state(self):
        session=Mock();session.request.side_effect=[Response({}),Response({}),Response({})]
        factory=Mock(side_effect=[session,RuntimeError('private')])
        r=self.make(session_factory=factory);r.poll('JOB_STATE_RUNNING')
        self.tick=60;r.poll('JOB_STATE_FAILED',final=True)
        final=self.log.info.call_args.args[1]
        self.assertIn('[최종 관측]',final);self.assertIn('상태=실패',final)
        self.assertNotIn('상태=실행 중',final);self.assertIn('캐시 관측',final)
        self.assertNotIn('private',self.rendered())

    def test_stage_sample_not_job_percent(self):
        r=self.make();r.emit('snapshot',snapshot={'job_state':'JOB_STATE_RUNNING','stages':[
            {'stage_id':'s1','state':'EXECUTION_STATE_RUNNING','elapsed_seconds':120,'progress':.99},
            {'stage_id':'s2','state':'EXECUTION_STATE_COMPLETE','state_changed':True}], 'metrics':[],'partial':True})
        text=self.rendered();self.assertIn('경과 120초',text);self.assertIn('단계 변경=s2',text)
        self.assertIn('전체 보장 아님',text);self.assertNotIn('%',text);self.assertIn('일부 관측 누락',text)
    def test_post_jdbc_variants_not_summed_or_bq_count(self):
        r=self.make();metrics=p._metrics([
            {'name':{'name':'post_jdbc_output_rows','context':{'step':'s'}},'scalar':10},
            {'name':{'name':'post_jdbc_output_rows','context':{'step':'s','tentative':'true'}},'scalar':12}],'getMetrics')
        r.emit('snapshot',snapshot={'metrics':metrics})
        text=self.rendered();self.assertIn('관측=10',text);self.assertNotIn('관측=22',text);self.assertIn('BQ 적재건수 아님',text)
    def test_multiple_transforms_not_aggregated(self):
        r=self.make();metrics=p._metrics([{'name':{'name':'post_jdbc_output_rows','context':{'step':x}},'scalar':1}for x in ['a','b']],'getMetrics')
        r.emit('snapshot',snapshot={'metrics':metrics});self.assertIn('복수 시리즈(합산 안 함)',self.rendered())
    def test_observed_zero_vs_absent(self):
        r=self.make();metrics=p._metrics([{'name':{'name':'post_jdbc_output_rows'},'scalar':0}],'getMetrics')
        r.emit('snapshot',snapshot={'metrics':metrics});self.assertIn('관측=0',self.rendered());self.assertIn('원천읽기/BQ확정적재=미확인',self.rendered())
    def test_process_element_duration_not_count(self):
        metrics=p._metrics([{'name':{'name':'ExecutionTime_ProcessElement'},'scalar':999},
            {'name':{'name':'ElementCount'},'scalar':12}],'test')
        self.assertEqual([m['name']for m in metrics],['ElementCount'])
    def test_redaction_size_and_control_characters(self):
        r=self.make(secrets=['secret']);r.emit('job_started',machine_type_configured='secret\n'+('가'*10000))
        text=self.rendered();self.assertNotIn('secret',text);self.assertNotIn('\n',text)
        for call in self.log.info.call_args_list:self.assertLessEqual(len(call.args[1].encode()),2400)
    def test_heartbeat_interval_preserved_with_error_and_recovery(self):
        session=Mock();session.request.side_effect=[Response({}),PermissionError('private'),Response({}),Response({}),Response({})]
        r=self.make(session_factory=lambda _:session);r.poll('JOB_STATE_RUNNING');self.tick=59;r.poll('JOB_STATE_RUNNING')
        self.assertEqual(session.request.call_count,3);self.tick=60;r.poll('JOB_STATE_RUNNING')
        self.assertEqual(session.request.call_count,5);self.assertIn('일부 관측 누락',self.rendered());self.assertNotIn('private',self.rendered())
    def test_warning_and_fail_open_logger(self):
        r=self.make();r.emit('observer_unavailable',error_type='PermissionError');self.assertEqual(self.log.warning.call_count,1)
        self.log.info.side_effect=RuntimeError('logger');r.emit('snapshot',snapshot={})


class ReadabilityExecutorTests(unittest.TestCase):
    # Reuse actual executor setup without rerunning inherited test cases.
    setUp = machine_tests.ExecutorTests.setUp
    task = machine_tests.ExecutorTests.task
    execute = machine_tests.ExecutorTests.execute
    def test_details_default_and_option(self):
        self.assertIs(self.task().progress_log_details,False)
        self.grape['options']={'progress_log_details':True};task=self.task();self.assertIs(task.progress_log_details,True)
        original=self.execute(task);self.grape['options']={};self.assertEqual(original,self.execute(self.task()))
    def test_details_invalid_yaml_values(self):
        for value in ['true',1,None]:
            self.grape['options']={'progress_log_details':value}
            with self.subTest(value=value),self.assertRaises(ValueError):self.task()

if __name__ == '__main__':unittest.main()
