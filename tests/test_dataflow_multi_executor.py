# Author: 임의진 (LIM UI JIN)
# Created: 2026-10-31
"""Offline multi-table factory and common launch lifecycle regression tests."""
import importlib
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from test_dataflow_machine_type import modules_for_executor

ROOT = Path(__file__).resolve().parents[1]


class MultiExecutorTests(unittest.TestCase):
    def setUp(self):
        scope = patch.dict(sys.modules, {})
        scope.start(); self.addCleanup(scope.stop)
        for name in list(sys.modules):
            if name == 'framework' or name.startswith('framework.'):
                del sys.modules[name]
        sys.modules.update(modules_for_executor())
        self.m = importlib.import_module('framework.dataflow_executor')
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        (self.directory/'query.sql').write_text('SELECT 1 AS "id" FROM dual')
        (self.directory/'schema.json').write_text('{"fields":[{"name":"id","type":"INTEGER"}]}')
        self.config = self.m.LoadedConfig(data={}, source_path=self.directory/'config.yaml')
        self.df = dict(project_id='test-project', region='asia-northeast3',template_gcs_path='gs://test/template.json',system_bucket='test',subnetwork='private',service_account_email='worker@test',machine_type='n2-highmem-2')
        self.grape = dict(grape_id='export',type='jdbc_tables_to_bigquery',source=dict(db_type='oracle',connection_id='oracle',driver_jars='gs://test/ojdbc.jar'),tables=[dict(id=n,sql_file='query.sql',destination=dict(table='test-project:stage.'+n,schema_file='schema.json',write_disposition='WRITE_TRUNCATE')) for n in ('emp','dept','sales')])
        self.m.BaseHook.get_connection.return_value=types.SimpleNamespace(extra_dejson={'password_secret':'projects/test/secrets/password'},login='reader',host='oracle',schema='service',port=1521,password='SECRET')
        self.service=sys.modules['googleapiclient.discovery'].build.return_value
        self.launch=self.service.projects.return_value.locations.return_value.flexTemplates.return_value.launch
        self.launch.return_value.execute.return_value={'job':{'id':'job-1'}}
        self.get=self.service.projects.return_value.locations.return_value.jobs.return_value.get
        self.get.return_value.execute.return_value={'currentState':'JOB_STATE_DONE'}
        self.context=dict(run_id='manual/../../test',dag=types.SimpleNamespace(dag_id='test'),ts_nodash='20261031T000000')

    def task(self):
        return self.m.DataflowExecutor.create_task(dag=object(),vine_config=self.config,grape=self.grape,runtime={'dataflow':self.df},defaults={})

    def test_one_launch_and_credential_free_manifest(self):
        task=self.task();result=task.execute(self.context)
        self.assertEqual(self.launch.call_count,1)
        uploads=self.m.GCSHook.return_value.upload.call_args_list
        self.assertEqual(len(uploads),7)
        manifest=json.loads(uploads[-1].kwargs['data'])
        self.assertEqual(len(manifest['tables']),3)
        self.assertNotIn('SECRET',json.dumps(manifest));self.assertNotIn('password',json.dumps(manifest))
        self.assertIn('tables_manifest_uri',result)
        parameters=self.launch.call_args.kwargs['body']['launchParameter']
        self.assertEqual(parameters['environment']['machineType'],'n2-highmem-2')
        self.assertEqual(parameters['environment']['ipConfiguration'],'WORKER_IP_PRIVATE')
        self.assertNotIn('query_uri',parameters['parameters'])
        self.assertEqual(task.sql,'');self.assertIn('tables',task.template_fields)
        for c in uploads:self.assertNotIn('..',c.kwargs['object_name'])

    def test_per_table_logs_are_configured_not_fake_counts(self):
        self.task().execute(self.context)
        reporter=self.m.ProgressReporter.return_value
        events=[c for c in reporter.emit.call_args_list if c.args[0]=='table_configured']
        self.assertEqual(len(events),3)
        for c in events:
            self.assertEqual(c.kwargs['source_jdbc_rows'],'unknown')
            self.assertEqual(c.kwargs['committed_bigquery_rows'],'unknown')
        reporter.poll.assert_called_once_with(job_state='JOB_STATE_DONE',final=True)

    def test_duplicate_ids_and_casefold_targets_rejected(self):
        self.grape['tables'][1]['id']='emp'
        with self.assertRaises(ValueError):self.task()
        self.grape['tables'][1]['id']='dept'
        self.grape['tables'][1]['destination']['table']='test-project:stage.EMP'
        with self.assertRaises(ValueError):self.task()
        self.launch.assert_not_called()

    def test_path_and_symlink_escapes_sanitized(self):
        for path in ('../private.sql','/tmp/private.sql'):
            self.grape['tables'][0]['sql_file']=path
            with self.assertRaisesRegex(ValueError,'Cannot load') as exc:self.task()
            self.assertNotIn('private',str(exc.exception))
        with tempfile.NamedTemporaryFile() as f:
            (self.directory/'link.sql').symlink_to(f.name)
            self.grape['tables'][0]['sql_file']='link.sql'
            with self.assertRaises(ValueError):self.task()

    def test_schemas_reject_duplicates_and_unsupported_modes(self):
        for fields in ([{'name':'id','type':'INTEGER'},{'name':'ID','type':'INTEGER'}],[{'name':'id','type':'STRING','mode':'REPEATED'}]):
            (self.directory/'schema.json').write_text(json.dumps({'fields':fields}))
            with self.assertRaises(ValueError):self.task()

    def test_unrendered_sql_rejected_before_upload(self):
        task=self.task();task.tables[0]['sql']='SELECT {{ missing }} FROM dual'
        with self.assertRaisesRegex(ValueError,'fully rendered'):task.execute(self.context)
        self.m.GCSHook.assert_not_called();self.launch.assert_not_called()

    def test_nested_destination_templates_validated_after_render(self):
        self.grape['tables'][0]['destination']['table']='test-project:stage.emp_{{ ds_nodash }}'
        task=self.task()
        # Simulate Airflow nested-field render; actual Jinja integration is a deploy check.
        task.tables[0]['destination']['table']='test-project:stage.emp_20261031'
        task.execute(self.context)
        self.assertEqual(self.launch.call_count,1)

    def test_invalid_rendered_destination_rejected(self):
        task=self.task();task.tables[0]['destination']['table']='invalid/target'
        with self.assertRaises(ValueError):task.execute(self.context)
        self.launch.assert_not_called()

    def test_attempt_assets_not_overwritten(self):
        task=self.task();task.execute(self.context);task.execute(self.context)
        uploads=self.m.GCSHook.return_value.upload.call_args_list
        self.assertNotEqual(uploads[6].kwargs['object_name'],uploads[13].kwargs['object_name'])

    def test_failure_and_updated_state_not_multitable_success(self):
        with patch.object(self.m,'collect_failure_diagnostics') as diag:
            for state in ('JOB_STATE_FAILED','JOB_STATE_UPDATED'):
                self.get.return_value.execute.return_value={'currentState':state}
                with self.assertRaises(RuntimeError):self.task().execute(self.context)
            self.assertEqual(diag.call_count,2)

    def test_no_top_level_single_output_allowed(self):
        self.grape['sql_file']='query.sql'
        with self.assertRaises(ValueError):self.task()

if __name__=='__main__':unittest.main()
