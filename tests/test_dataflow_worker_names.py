# Author: 임의진 (LIM UI JIN)
# Created: 2026-11-01
# Updated: 2026-11-01
"""Observed worker names are bounded samples, never a current VM inventory."""
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock
from test_dataflow_progress import p, Response


class WorkerNameTests(unittest.TestCase):
    def setUp(self):
        self.tick = 0
        self.logger = Mock()
        self.session = Mock()
        self.reporter = p.ProgressReporter(logger=self.logger, credentials=object(), project_id='p',
            region='r', job_id='j', dag_id='d', run_id='run', task_id='t',
            session_factory=lambda _: self.session, monotonic=lambda: self.tick,
            now=lambda: datetime(2026, 11, 1, tzinfo=timezone.utc), secrets=('secret-value',))

    def poll(self, names=(), error=False, no_items=False):
        responses = [] if self.reporter.description_checked else [Response({})]
        responses += [Response({'stages':[{'stageId':'s','state':'EXECUTION_STATE_RUNNING'}]}), Response({})]
        workers = {'workers':[{'workerName':name, 'workItems':[] if no_items else [{'taskId':'t'}]}
                              for name in names]}
        responses.append(PermissionError('private') if error else Response(workers))
        self.session.request.side_effect = responses
        self.tick += 60
        return self.reporter.poll()

    def summaries(self):
        return [c.args[1] for c in self.logger.info.call_args_list if c.args[0] == 'Dataflow progress %s']

    def name_summaries(self):
        return [s for s in self.summaries() if '관측 워커 VM 이름(일부)=' in s]

    def test_first_duplicate_reordered_and_changed_nonempty_sets(self):
        self.assertTrue(self.poll(['vm-b', 'vm-a', 'vm-a'])['worker_names_changed'])
        self.assertIn('vm-a, vm-b', self.name_summaries()[0])
        self.assertIn('전체/현재 VM 목록 아님', self.name_summaries()[0])
        self.assertFalse(self.poll(['vm-a', 'vm-b'])['worker_names_changed'])
        self.assertEqual(len(self.name_summaries()), 1)
        self.assertTrue(self.poll(['vm-a', 'vm-c'])['worker_names_changed'])
        self.assertEqual(len(self.name_summaries()), 2)
        self.assertIn('vm-c', self.name_summaries()[-1])

    def test_empty_error_cache_do_not_reset_or_reannounce(self):
        self.poll(['vm-a'])
        requests = self.session.request.call_count
        self.reporter.poll(final=True)
        self.assertEqual(self.session.request.call_count, requests)
        self.assertEqual(len(self.name_summaries()), 1)
        self.assertFalse(self.poll([])['worker_names_changed'])
        self.assertFalse(self.poll(error=True)['worker_names_changed'])
        self.assertEqual(self.reporter.previous_worker_names, frozenset({'vm-a'}))
        self.assertFalse(self.poll(['vm-a'])['worker_names_changed'])
        self.assertEqual(len(self.name_summaries()), 1)
        self.assertNotIn('private', '\n'.join(self.summaries()))

    def test_blank_invalid_and_worker_without_items_not_inferred(self):
        self.assertFalse(self.poll([None, '', '  ', 123])['worker_names_changed'])
        self.assertFalse(self.poll(['vm-no-items'], no_items=True)['worker_names_changed'])
        self.assertEqual(self.name_summaries(), [])
        self.assertEqual(self.reporter.previous_worker_names, frozenset())

    def test_names_are_bounded_sanitized_and_redacted(self):
        self.poll(['vm-secret-value\n\t\x00x', 'b'*200, 'c'*200, 'd'*200])
        summary = self.name_summaries()[0]
        self.assertIn('외 생략', summary)
        self.assertLessEqual(len(summary.encode('utf-8')), 2400)
        self.assertNotIn('b'*101, summary)
        # Put the sensitive/control-containing name inside the first-three display sample.
        self.poll(['a-secret-value\n\t\x00x'])
        summary = self.name_summaries()[-1]
        self.assertNotIn('secret-value', summary)
        for char in ('\n', '\t', '\x00'):
            self.assertNotIn(char, summary)

    def test_no_extra_api_requests_or_worker_inventory_calls(self):
        self.poll(['vm-a'])
        self.assertEqual(self.session.request.call_count, 4)
        self.poll(['vm-b'])
        self.assertEqual(self.session.request.call_count, 7)
        for call in self.session.request.call_args_list:
            self.assertEqual(call.args[0], 'GET')
            self.assertTrue(call.args[1].startswith('https://dataflow.googleapis.com/'))
        self.assertEqual(sum('/stages/' in c.args[1] for c in self.session.request.call_args_list), 2)


if __name__ == '__main__':
    unittest.main()
