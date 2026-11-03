# Author: 임의진 (LIM UI JIN)
# Created: 2026-11-02
# Updated: 2026-11-02
"""Offline parsing-context/catalog integration; no Airflow installation or cloud calls."""
import importlib
import importlib.util
import io
import logging
from pathlib import Path
import runpy
import shutil
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
FRAMEWORK = ROOT / 'dags/framework'


def module(name, **attributes):
    result = types.ModuleType(name)
    result.__path__ = []
    result.__dict__.update(attributes)
    return result


class DAG:
    def __init__(self, dag_id):
        self.dag_id = dag_id


class ParsingIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.directory = Path(self.directory.name)
        self.pipelines = self.directory / 'pipelines'
        self.pipelines.mkdir()
        self.calls = []
        self.failed = set()
        self.target = None
        self.logger = Mock()
        case = self

        class VineFactory:
            def __init__(self, config):
                self.config = config

            def create(self):
                case.calls.append(('vine', self.config.id))
                if self.config.id in case.failed:
                    raise ValueError('deliberate factory failure')
                return DAG(self.config.id)

        class RootFactory:
            def __init__(self, config, available_vine_ids):
                self.config = config
                self.available_vine_ids = available_vine_ids

            def create(self):
                case.calls.append(('root', self.config.id, self.available_vine_ids))
                for vine in self.config.data.get('vines', []):
                    if vine['vine_id'] not in self.available_vine_ids:
                        raise ValueError('missing referenced Vine')
                if self.config.id in case.failed:
                    raise ValueError('deliberate factory failure')
                return DAG(self.config.id)

        framework = module('framework')
        framework.__path__ = [str(FRAMEWORK)]
        modules = {
            'framework': framework,
            'framework.logger': module('framework.logger', get_logger=lambda _: self.logger),
            'framework.vine_factory': module('framework.vine_factory', VineFactory=VineFactory),
            'framework.root_factory': module('framework.root_factory', RootFactory=RootFactory),
            'framework.parsing_context': module('framework.parsing_context', get_target_dag_id=lambda: self.target),
            'airflow': module('airflow'),
            'airflow.models': module('airflow.models'),
            'airflow.models.dag': module('airflow.models.dag', DAG=DAG),
        }
        patcher = patch.dict(sys.modules, modules)
        patcher.start()
        self.addCleanup(patcher.stop)
        for name in ('framework.config_loader', 'framework.models', 'framework.utils'):
            sys.modules.pop(name, None)
        self.loader = importlib.import_module('framework.config_loader')

    def config(self, kind, folder, identifier, **extra):
        path = self.pipelines / (kind + 's') / folder / 'config.yaml'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump({kind: {kind + '_id': identifier}, **extra}), encoding='utf-8')
        return path

    def run_main(self):
        path = self.directory / 'main.py'
        shutil.copyfile(ROOT / 'dags/main.py', path)
        return runpy.run_path(str(path))

    def fixtures(self):
        self.config('vine', 'folder-not-vine-id', 'vine_a')
        self.config('vine', 'another-folder', 'vine_b')
        self.config('root', 'root-folder', 'root_a', vines=[{'vine_id': 'vine_a'}, {'vine_id': 'vine_b'}])

    def test_full_scheduler_builds_all_in_stable_order(self):
        self.fixtures()
        result = self.run_main()
        self.assertEqual(result['registered_dag_ids'], {'vine_a', 'vine_b', 'root_a'})
        self.assertEqual(self.calls, [('vine', 'vine_b'), ('vine', 'vine_a'), ('root', 'root_a', {'vine_a', 'vine_b'})])

    def test_target_vine_uses_yaml_id_not_folder(self):
        self.fixtures()
        self.target = 'vine_a'
        result = self.run_main()
        self.assertEqual(result['registered_dag_ids'], {'vine_a'})
        self.assertEqual(self.calls, [('vine', 'vine_a')])

    def test_target_root_preserves_complete_vine_catalog_without_building_vines(self):
        self.fixtures()
        self.target = 'root_a'
        result = self.run_main()
        self.assertEqual(result['registered_dag_ids'], {'root_a'})
        self.assertEqual(self.calls, [('root', 'root_a', {'vine_a', 'vine_b'})])

    def test_unknown_target_is_empty_without_full_parse_or_false_failure(self):
        self.fixtures()
        self.target = 'not_in_this_file'
        result = self.run_main()
        self.assertEqual(result['registered_dag_ids'], set())
        self.assertEqual(self.calls, [])

    def test_factory_error_isolation_full_parse(self):
        self.fixtures()
        self.failed.add('vine_a')
        result = self.run_main()
        self.assertEqual(result['registered_dag_ids'], {'vine_b', 'root_a'})
        self.assertEqual(len(result['registration_errors']), 1)

    def test_unselected_factory_failure_is_not_executed(self):
        self.fixtures()
        self.target = 'vine_a'
        self.failed.update({'vine_b', 'root_a'})
        result = self.run_main()
        self.assertEqual(result['registration_errors'], [])

    def test_selected_factory_failure_remains_visible(self):
        self.fixtures()
        self.target = 'vine_a'
        self.failed.add('vine_a')
        with self.assertRaises(RuntimeError):
            self.run_main()

    def test_empty_full_catalog_remains_error(self):
        with self.assertRaises(RuntimeError):
            self.run_main()

    def test_selected_root_missing_dependency_remains_error(self):
        self.fixtures()
        self.config('root', 'root-folder', 'root_a', vines=[{'vine_id': 'missing'}])
        self.target = 'root_a'
        with self.assertRaises(RuntimeError):
            self.run_main()

    def test_same_kind_duplicate_rejected_even_when_not_selected(self):
        self.fixtures()
        self.config('vine', 'duplicate', 'vine_b')
        self.target = 'vine_a'
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            self.run_main()
        self.assertEqual(self.calls, [])

    def test_cross_kind_duplicate_rejected_even_when_not_selected(self):
        self.fixtures()
        self.config('root', 'duplicate', 'vine_b')
        self.target = 'vine_a'
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            self.run_main()
        self.assertEqual(self.calls, [])

    def test_catalog_failure_marker_in_rendered_error_log(self):
        # Use the production logger and an actual handler/formatter, rather than
        # asserting only the LOGGER.exception arguments on a mock.
        spec = importlib.util.spec_from_file_location('production_logger', FRAMEWORK / 'logger.py')
        logger_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(logger_module)
        logger = logger_module.get_logger('<run_path>')
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter('%(levelname)s:%(message)s'))
        old_level, old_propagate = logger.level, logger.propagate
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        self.addCleanup(logger.removeHandler, handler)
        self.addCleanup(logger.setLevel, old_level)
        self.addCleanup(setattr, logger, 'propagate', old_propagate)

        with patch.dict(sys.modules, {'framework.logger': logger_module}):
            # Normal registrations do not carry the critical marker.
            self.fixtures()
            self.run_main()
            self.assertNotIn('[SMS.CRITICAL]', stream.getvalue())
            stream.seek(0)
            stream.truncate(0)

            # An isolated factory error is not a catalog failure.
            self.failed.add('vine_a')
            self.run_main()
            self.assertIn('Failed to register Vine DAG', stream.getvalue())
            self.assertNotIn('[SMS.CRITICAL]', stream.getvalue())
            self.failed.clear()
            stream.seek(0)
            stream.truncate(0)

            # A cross-kind collision is caught even for an unselected DAG.
            original = self.pipelines / 'vines' / 'another-folder' / 'config.yaml'
            duplicate = self.config('root', 'duplicate', 'vine_b')
            self.target = 'vine_a'
            with self.assertRaisesRegex(ValueError, 'Duplicate DAG ID'):
                self.run_main()
            rendered = stream.getvalue()
            self.assertIn('ERROR:[SMS.CRITICAL] Failed to load pipeline catalog', rendered)
            self.assertIn('Traceback (most recent call last)', rendered)
            self.assertIn("Duplicate DAG ID 'vine_b'", rendered)
            self.assertIn(str(original), rendered)
            self.assertIn(str(duplicate), rendered)

    def test_same_kind_duplicate_has_critical_marker(self):
        self.fixtures()
        self.config('vine', 'duplicate', 'vine_b')
        with self.assertRaisesRegex(ValueError, 'Duplicate vine ID'):
            self.run_main()
        self.logger.exception.assert_called_once_with(
            '[SMS.CRITICAL] Failed to load pipeline catalog',
            extra={'event': 'pipeline_catalog_load_failed', 'pipelines_dir': str(self.pipelines)},
        )

    def test_bad_yaml_isolated_and_good_config_retained(self):
        self.fixtures()
        path = self.config('vine', 'bad', 'bad')
        path.write_text('vine: [unterminated', encoding='utf-8')
        with self.assertLogs('framework.config_loader', level='ERROR'):
            result = self.run_main()
        self.assertEqual(result['registered_dag_ids'], {'vine_a', 'vine_b', 'root_a'})

    def test_operation_counts_yaml_still_full_factory_work_selective(self):
        self.fixtures()
        original = yaml.safe_load
        with patch.object(yaml, 'safe_load', wraps=original) as reads:
            self.run_main()
        self.assertEqual(reads.call_count, 3)
        self.assertEqual(len(self.calls), 3)
        self.calls.clear()
        self.target = 'vine_a'
        with patch.object(yaml, 'safe_load', wraps=original) as reads:
            self.run_main()
        self.assertEqual(reads.call_count, 3)
        self.assertEqual(len(self.calls), 1)

    def test_yaml_change_seen_next_parse_without_persistent_cache(self):
        self.fixtures()
        self.target = 'vine_a'
        self.assertEqual(self.run_main()['registered_dag_ids'], {'vine_a'})
        self.config('vine', 'folder-not-vine-id', 'renamed_vine')
        self.calls.clear()
        self.assertEqual(self.run_main()['registered_dag_ids'], set())
        self.target = 'renamed_vine'
        self.assertEqual(self.run_main()['registered_dag_ids'], {'renamed_vine'})


    def test_whitespace_id_matches_target_and_full_factory_id(self):
        self.real_fixtures()
        self.config('vine', 'folder_a', '  vine_a  ', grapes=[{
            'grape_id': 'wait_fin', 'type': 'gcs_fin_file_sensor',
            'source': {'bucket': 'input-bucket', 'object': 'incoming/batch.fin'}}])
        full = self.run_main()
        self.assertEqual(full['registered_dag_ids'], {'vine_a', 'vine_b', 'root_a'})
        self.target = 'vine_a'
        selected = self.run_main()
        self.assertEqual(selected['registered_dag_ids'], {'vine_a'})
        self.assertEqual(self.graph(selected['vine_a']), self.graph(full['vine_a']))

    def test_normalized_duplicate_ids_rejected_before_selection(self):
        self.fixtures()
        self.config('vine', 'duplicate', '  vine_b  ')
        self.target = 'vine_a'
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            self.run_main()
        self.assertEqual(self.calls, [])

    def test_normalized_cross_kind_duplicate_rejected(self):
        self.fixtures()
        self.config('root', 'duplicate', ' vine_b ')
        self.target = 'vine_a'
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            self.run_main()
        self.assertEqual(self.calls, [])

    def test_ambiguous_dual_section_config_is_isolated(self):
        self.fixtures()
        self.config('vine', 'ambiguous', 'ambiguous_vine', root={'root_id': 'ambiguous_root'})
        with self.assertLogs('framework.config_loader', level='ERROR'):
            result = self.run_main()
        self.assertEqual(result['registered_dag_ids'], {'vine_a', 'vine_b', 'root_a'})
        self.assertFalse(any('ambiguous' in call[1] for call in self.calls))

    def use_real_factories(self):
        # Only external Airflow/provider objects are doubled; factory/executor
        # graph construction, configuration and sensor options are production code.
        from test_runtime_options import _runtime_modules, DAG as RuntimeDAG
        for name in list(sys.modules):
            if name == 'framework' or name.startswith('framework.'):
                del sys.modules[name]
        sys.modules.update(_runtime_modules())
        sys.modules['airflow.models.dag'] = module('airflow.models.dag', DAG=RuntimeDAG)
        sys.modules['framework.parsing_context'] = module(
            'framework.parsing_context', get_target_dag_id=lambda: self.target)
        for filename, cls in [('gcs_executor', 'GCSExecutor'),
                              ('dataflow_executor', 'DataflowExecutor'),
                              ('postgres_executor', 'PostgresExecutor'),
                              ('sftp_runner_executor', 'SftpRunnerExecutor')]:
            sys.modules['framework.' + filename] = module('framework.' + filename, **{cls: Mock()})

    def real_fixtures(self):
        self.use_real_factories()
        sensor = {'grape_id': 'wait_fin', 'type': 'gcs_fin_file_sensor',
                  'source': {'bucket': 'input-bucket', 'object': 'incoming/batch.fin'},
                  'options': {'poke_interval_seconds': 95, 'deferrable': False}}
        self.config('vine', 'folder_a', 'vine_a', grapes=[sensor])
        self.config('vine', 'folder_b', 'vine_b', grapes=[sensor])
        self.config('root', 'root_folder', 'root_a', vines=[
            {'vine_id': 'vine_a'}, {'vine_id': 'vine_b', 'depends_on': ['vine_a']}])

    @staticmethod
    def graph(dag):
        return {name: (task.upstream_task_ids, task.downstream_task_ids)
                for name, task in dag.task_dict.items()}

    def test_real_root_dependency_graph_matches_full_parse(self):
        self.real_fixtures()
        full = self.run_main()['root_a']
        self.target = 'root_a'
        selected = self.run_main()
        self.assertEqual(selected['registered_dag_ids'], {'root_a'})
        self.assertEqual(self.graph(selected['root_a']), self.graph(full))
        trigger = selected['root_a'].task_dict['trigger_vine_b']
        self.assertEqual(trigger.upstream_task_ids, {'trigger_vine_a'})
        self.assertEqual(trigger.trigger_dag_id, 'vine_b')
        self.assertTrue(trigger.wait_for_completion)

    def test_real_file_sensor_and_vine_graph_match_full_parse(self):
        self.real_fixtures()
        full = self.run_main()['vine_a']
        self.target = 'vine_a'
        selected = self.run_main()
        self.assertEqual(selected['registered_dag_ids'], {'vine_a'})
        self.assertEqual(self.graph(selected['vine_a']), self.graph(full))
        full_sensor = full.task_dict['wait_fin']
        selected_sensor = selected['vine_a'].task_dict['wait_fin']
        for attribute in ('bucket', 'object', 'poke_interval', 'deferrable'):
            self.assertEqual(getattr(selected_sensor, attribute), getattr(full_sensor, attribute))
        self.assertEqual(selected_sensor.poke_interval, 95)
        self.assertFalse(selected_sensor.deferrable)


class ContextCompatibilityTests(unittest.TestCase):
    def call(self, sdk=None, legacy=None):
        framework = module('framework')
        framework.__path__ = [str(FRAMEWORK)]
        modules = {'framework': framework, 'airflow': module('airflow'),
                   'airflow.sdk': module('airflow.sdk'), 'airflow.utils': module('airflow.utils'),
                   'airflow.utils.dag_parsing_context': module('airflow.utils.dag_parsing_context')}
        if sdk is not None:
            modules['airflow.sdk'].get_parsing_context = sdk
        if legacy is not None:
            modules['airflow.utils.dag_parsing_context'].get_parsing_context = legacy
        with patch.dict(sys.modules, modules):
            sys.modules.pop('framework.parsing_context', None)
            helper = importlib.import_module('framework.parsing_context')
            return helper.get_target_dag_id()

    def test_sdk_api_preferred(self):
        sdk = Mock(return_value=types.SimpleNamespace(dag_id='sdk_dag', task_id='task'))
        legacy = Mock(return_value=types.SimpleNamespace(dag_id='legacy_dag'))
        self.assertEqual(self.call(sdk, legacy), 'sdk_dag')
        legacy.assert_not_called()

    def test_airflow_2_legacy_api_fallback(self):
        self.assertEqual(self.call(legacy=lambda: types.SimpleNamespace(dag_id='legacy_dag')), 'legacy_dag')

    def test_no_apis_falls_back_to_full(self):
        self.assertIsNone(self.call())

    def test_full_context_ignores_task_id(self):
        self.assertIsNone(self.call(sdk=lambda: types.SimpleNamespace(dag_id=None, task_id='task')))

    def test_context_read_failure_falls_back_to_full(self):
        self.assertIsNone(self.call(sdk=Mock(side_effect=RuntimeError('unavailable'))))

    def test_malformed_context_falls_back_to_full(self):
        for value in (object(), types.SimpleNamespace(dag_id=42), types.SimpleNamespace(dag_id=''), types.SimpleNamespace(dag_id=[])):
            with self.subTest(value=value):
                self.assertIsNone(self.call(sdk=lambda: value))


if __name__ == '__main__':
    unittest.main()
