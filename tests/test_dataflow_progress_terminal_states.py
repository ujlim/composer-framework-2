# Author: 임의진 (LIM UI JIN)
# Created: 2026-11-01
# Updated: 2026-11-01
"""Terminal-state regression tests using offline API/clock doubles only."""
from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import Mock

from test_dataflow_progress import p, Response


class TerminalStateTests(unittest.TestCase):
    start = '2026-11-01T05:06:28.281Z'
    end = '2026-11-01T05:06:28.453Z'
    now = datetime(2026, 11, 1, 5, 7, 16, 281000, tzinfo=timezone.utc)
    terminals = ('EXECUTION_STATE_SUCCEEDED', 'EXECUTION_STATE_FAILED',
                 'EXECUTION_STATE_CANCELLED', 'EXECUTION_STATE_COMPLETE')

    def reporter(self, stages=(), details=True):
        self.logger = Mock()
        self.session = Mock()
        self.session.request.side_effect = [Response({}), Response({'stages': stages}), Response({})]
        return p.ProgressReporter(logger=self.logger, credentials=object(), project_id='p',
            region='r', job_id='j', dag_id='dag', run_id='run', task_id='task',
            session_factory=lambda _: self.session, monotonic=lambda: 0,
            now=lambda: self.now, details=details)

    def test_terminal_duration_uses_reported_end_and_does_not_grow(self):
        for state in self.terminals:
            for now in (self.now, self.now + timedelta(hours=1)):
                with self.subTest(state=state, now=now):
                    self.assertAlmostEqual(p._elapsed(self.start, self.end, state, now), .172)

    def test_terminal_missing_invalid_or_naive_end_stays_unknown(self):
        for state in self.terminals:
            for end in (None, '', 'invalid', '2026-11-01T05:06:28.453', {}, 123):
                with self.subTest(state=state, end=end):
                    self.assertIsNone(p._elapsed(self.start, end, state, self.now))

    def test_missing_invalid_or_naive_start_stays_unknown(self):
        for state in self.terminals + ('EXECUTION_STATE_RUNNING',):
            for start in (None, '', 'invalid', '2026-11-01T05:06:28.281', {}, 123):
                with self.subTest(state=state, start=start):
                    self.assertIsNone(p._elapsed(start, self.end, state, self.now))

    def test_running_ignores_predicted_or_missing_end(self):
        for end in (self.end, '2030-01-01T00:00:00Z', None, 'invalid'):
            with self.subTest(end=end):
                self.assertEqual(p._elapsed(self.start, end, 'EXECUTION_STATE_RUNNING', self.now), 48)
        self.assertIsNone(p._elapsed(self.start, self.end, 'EXECUTION_STATE_RUNNING', None))
        self.assertIsNone(p._elapsed(self.start, self.end, 'EXECUTION_STATE_RUNNING', self.now.replace(tzinfo=None)))

    def test_utc_offset_and_negative_duration_compatibility(self):
        self.assertAlmostEqual(p._elapsed('2026-11-01T14:06:28.281+09:00', self.end,
                                         'EXECUTION_STATE_SUCCEEDED', self.now), .172)
        self.assertEqual(p._elapsed(self.end, self.start, 'EXECUTION_STATE_SUCCEEDED', self.now), 0)

    def test_photo_f53_and_twelve_succeeded_stages_poll_and_detail(self):
        stages = [dict(stageId='F53' if i == 0 else f'F{i}', state='EXECUTION_STATE_SUCCEEDED',
                       startTime=self.start, endTime=self.end) for i in range(12)]
        r = self.reporter(stages)
        snapshot = r.poll('JOB_STATE_DONE', final=True)
        self.assertEqual(snapshot['stages'][0]['stage_id'], 'F53')
        self.assertAlmostEqual(snapshot['stages'][0]['elapsed_seconds'], .172)
        self.assertEqual(snapshot['stages'][0]['end_time_reported'], self.end)
        summary = r._summary('final_snapshot', dict(snapshot=snapshot))
        self.assertIn('완료 12', summary)
        self.assertNotIn('기타/미확인', summary)
        self.assertIn('원천읽기/BQ확정적재=미확인', summary)
        self.assertIn('JDBC 후단 관측=미확인', summary)
        details = [json.loads(c.args[1]) for c in self.logger.info.call_args_list
                   if c.args[0] == 'Dataflow progress detail %s']
        self.assertAlmostEqual(details[0]['snapshot']['stages'][0]['elapsed_seconds'], .172)
        self.assertEqual(self.session.request.call_count, 3)

    def test_mixed_summary_counts_alias_once_and_keeps_unknowns(self):
        states = self.terminals + ('EXECUTION_STATE_RUNNING', 'EXECUTION_STATE_NOT_STARTED',
                                  'EXECUTION_STATE_UNKNOWN', 'FUTURE_STATE', None)
        stages = [dict(stage_id=str(i), state=state) for i, state in enumerate(states)]
        summary = self.reporter()._summary('snapshot', dict(snapshot={'stages': stages}))
        for text in ('실행 1', '완료 2', '실패 1', '취소 1', '기타/미확인 4'):
            self.assertIn(text, summary)

    def test_terminal_transition_labels(self):
        for state, label in zip(self.terminals, ('완료', '실패', '취소', '완료')):
            with self.subTest(state=state):
                snapshot = {'stages': [dict(stage_id='F53', state=state, state_changed=True)]}
                self.assertIn('단계 변경=F53:' + label,
                              self.reporter()._summary('snapshot', dict(snapshot=snapshot)))

    def test_multi_table_configured_summary_and_detail_retained(self):
        r = self.reporter()
        r.emit('table_configured', table_id='emp', destination='project:stage.emp',
               write_disposition='WRITE_TRUNCATE')
        calls = self.logger.info.call_args_list
        summary = next(c.args[1] for c in calls if c.args[0] == 'Dataflow progress %s')
        for text in ('[테이블 설정]', '테이블=emp', '목적지=project:stage.emp',
                     '쓰기=WRITE_TRUNCATE', '설정 확인만 완료; 원천읽기/BQ확정적재=미확인'):
            self.assertIn(text, summary)
        detail = next(json.loads(c.args[1]) for c in calls
                      if c.args[0] == 'Dataflow progress detail %s')
        self.assertEqual(detail['event'], 'table_configured')
        self.assertEqual(detail['table_id'], 'emp')
        self.assertEqual(detail['destination'], 'project:stage.emp')

    def test_poll_terminal_missing_end_is_null_not_elapsed_now(self):
        for state in self.terminals:
            with self.subTest(state=state):
                snapshot = self.reporter([dict(stageId='F53', state=state, startTime=self.start)]).poll()
                self.assertIsNone(snapshot['stages'][0]['elapsed_seconds'])
                self.assertIsNone(snapshot['stages'][0]['end_time_reported'])


if __name__ == '__main__':
    unittest.main()
