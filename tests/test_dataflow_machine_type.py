"""Offline machine-type-only regression tests; no cloud calls."""
import importlib
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]


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
            self.log = MagicMock()
    module('airflow.models').BaseOperator = Operator
    module('airflow.hooks.base').BaseHook = MagicMock()
    module('airflow.providers.google.cloud.hooks.gcs').GCSHook = MagicMock()
    module('googleapiclient.discovery').build = MagicMock()
    # Progress branch compatibility: keep observer/ADC calls offline; machine
    # validation and request construction remain the actual executor code.
    module('google.auth').default = lambda **kwargs: (object(), 'test-project')
    module('framework.dataflow_progress').ProgressReporter = MagicMock()
    module('framework.dataflow_progress').validate_progress_interval = lambda value: value
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
    def test_unset_preserves_environment(self):
        result = self.execute(self.task())
        self.assertEqual(result['environment'], {
            'serviceAccountEmail': self.df['service_account_email'],
            'subnetwork': self.df['subnetwork'], 'ipConfiguration': 'WORKER_IP_PRIVATE',
            'stagingLocation': 'gs://test/staging', 'tempLocation': 'gs://test/temp',
        })
        self.assertEqual(result['parameters']['write_disposition'], 'WRITE_APPEND')

    def test_null_identical_to_absent(self):
        absent = self.execute(self.task())
        self.df['machine_type'] = None
        self.assertEqual(absent, self.execute(self.task()))

    def test_explicit_trimmed_value_only_changes_machine(self):
        baseline = self.execute(self.task())
        self.df['machine_type'] = ' n2-highmem-2 '
        result = self.execute(self.task())
        self.assertEqual(result['environment'].pop('machineType'), 'n2-highmem-2')
        self.assertEqual(result, baseline)

    def test_reject_invalid_before_api(self):
        for value in ['', ' ', True, False, 2, 2.0, [], {}]:
            with self.subTest(value=value):
                self.df['machine_type'] = value
                with self.assertRaisesRegex(ValueError, 'machine_type'):
                    self.task()
        sys.modules['googleapiclient.discovery'].build.assert_not_called()
        self.m.GCSHook.assert_not_called()

    def test_grape_options_cannot_set_machine(self):
        self.grape['options'] = {'machine_type': 'n2-highmem-8'}
        self.assertNotIn('machineType', self.execute(self.task())['environment'])

    def test_gcs_forwarding(self):
        self.grape.update(type='jdbc_to_gcs', destination={'path':'gs://test/output'})
        self.df['machine_type'] = 'n2-highmem-2'
        result = self.execute(self.task())
        self.assertEqual(result['environment']['machineType'], 'n2-highmem-2')
        self.assertEqual(result['parameters']['destination_type'], 'gcs')

    def test_does_not_change_other_vine_defaults(self):
        self.df['machine_type'] = 'n2-highmem-2'
        selected = self.task()
        del self.df['machine_type']
        other = self.task()
        self.assertEqual(selected.machine_type, 'n2-highmem-2')
        self.assertIsNone(other.machine_type)

    def test_existing_temp_override_preserved(self):
        self.df.update(machine_type='n2-highmem-2', temp_location='gs://test/custom-temp')
        result = self.execute(self.task())
        self.assertEqual(result['environment']['tempLocation'], 'gs://test/custom-temp')
        self.assertEqual(result['environment']['machineType'], 'n2-highmem-2')


if __name__ == '__main__':
    unittest.main()
