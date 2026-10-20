"""Offline regression tests for operator options, independent of DAG discovery.

Run with ``python -m unittest discover -s tests -p test_runtime_options.py -v``.
Only Airflow/provider runtime objects and pendulum's date parser are doubled;
the Root factory, executors, configuration model, and option merging are real.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import importlib
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo


FRAMEWORK_DIR = Path(__file__).resolve().parents[1] / "dags" / "framework"
_NOT_PASSED = object()


class DAG:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.default_args = kwargs.get("default_args", {})
        self.task_dict = {}


class Param:
    def __init__(self, default=None, **schema):
        self.default = default
        self.schema = schema


class Operator:
    def __init__(self, *, task_id, dag, **kwargs):
        self.task_id = task_id
        self.dag = dag
        # Keep explicit arguments separate so provider defaults cannot mask a
        # framework regression that stops forwarding one of these options.
        self.kwargs = kwargs
        self.__dict__.update(dag.default_args)
        self.__dict__.update(kwargs)
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()
        if task_id in dag.task_dict:
            raise ValueError(f"Duplicate task ID: {task_id}")
        dag.task_dict[task_id] = self

    def __rshift__(self, other):
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)
        return other


class EmptyOperator(Operator):
    pass


class PythonOperator(Operator):
    pass


class TriggerDagRunOperator(Operator):
    pass


class BigQueryInsertJobOperator(Operator):
    def __init__(self, *, durable=_NOT_PASSED, **kwargs):
        if durable is not _NOT_PASSED:
            kwargs["durable"] = durable
        super().__init__(**kwargs)


class LegacyBigQueryInsertJobOperator(Operator):
    def __init__(self, **kwargs):
        if "durable" in kwargs:
            raise TypeError("Legacy BaseOperator rejects unknown argument: durable")
        super().__init__(**kwargs)


class GCSObjectExistenceSensor(Operator):
    pass


def _runtime_modules():
    """Build standalone import stubs without loading another test suite."""
    modules = {}

    def module(name):
        if name in modules:
            return modules[name]
        result = modules[name] = types.ModuleType(name)
        result.__path__ = []
        parent, _, leaf = name.rpartition(".")
        if parent:
            setattr(module(parent), leaf, result)
        return result

    module("airflow").DAG = DAG
    module("airflow.models.param").Param = Param
    module("airflow.sdk").get_current_context = lambda: {}
    module("airflow.providers.standard.operators.empty").EmptyOperator = EmptyOperator
    module("airflow.providers.standard.operators.python").PythonOperator = PythonOperator
    module("airflow.providers.standard.operators.trigger_dagrun").TriggerDagRunOperator = TriggerDagRunOperator
    module("airflow.providers.google.cloud.operators.bigquery").BigQueryInsertJobOperator = BigQueryInsertJobOperator
    module("airflow.providers.google.cloud.sensors.gcs").GCSObjectExistenceSensor = GCSObjectExistenceSensor
    module("pendulum").parse = lambda value, tz: datetime.fromisoformat(value).replace(tzinfo=ZoneInfo(tz))
    module("framework").__path__ = [str(FRAMEWORK_DIR)]
    return modules


class RuntimeOptionsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        (self.directory / "query.sql").write_text("SELECT 1;\n", encoding="utf-8")

        # Restore all original imports after each test, including any framework
        # modules installed by other offline suites in the same test process.
        module_patch = patch.dict(sys.modules, {})
        module_patch.start()
        self.addCleanup(module_patch.stop)
        for name in list(sys.modules):
            if name == "framework" or name.startswith("framework."):
                del sys.modules[name]
        sys.modules.update(_runtime_modules())
        self.LoadedConfig = importlib.import_module("framework.models").LoadedConfig
        self.bigquery_module = importlib.import_module("framework.bigquery_executor")
        self.BigQueryExecutor = self.bigquery_module.BigQueryExecutor
        self.RootFactory = importlib.import_module("framework.root_factory").RootFactory
        self.FinFileSensorExecutor = importlib.import_module(
            "framework.fin_file_sensor_executor"
        ).FinFileSensorExecutor

    def _bigquery(self, *, defaults=None, options=None, grape_type="bigquery_sql"):
        config = self.LoadedConfig(
            data={"vine": {"vine_id": "runtime_vine"}},
            source_path=self.directory / "config.yaml",
        )
        return self.BigQueryExecutor.create_task(
            dag=DAG(dag_id="runtime_vine"),
            vine_config=config,
            grape={
                "grape_id": "query",
                "type": grape_type,
                "sql_file": "query.sql",
                "procedure": "reporting.refresh",
                "options": options or {},
            },
            runtime={},
            defaults=defaults or {},
        )

    def _root_trigger(self, *, defaults=None, options=None):
        config = self.LoadedConfig(
            data={
                "root": {"root_id": "runtime_root"},
                "defaults": defaults or {},
                "vines": [{"vine_id": "runtime_vine", "options": options or {}}],
            },
            source_path=self.directory / "config.yaml",
        )
        dag = self.RootFactory(config, {"runtime_vine"}).create()
        return dag.task_dict["trigger_runtime_vine"]

    def _fin_sensor(self, *, defaults=None, options=None):
        return self.FinFileSensorExecutor.create_task(
            dag=DAG(dag_id="runtime_vine"),
            grape={
                "grape_id": "wait_for_fin",
                "type": "gcs_fin_file_sensor",
                "source": {"bucket": "input-bucket", "object": "incoming/batch.fin"},
                "options": options or {},
            },
            runtime={},
            defaults=defaults or {},
        )

    def test_bigquery_explicitly_forwards_durable_false_by_default(self):
        for grape_type in ("bigquery_sql", "bigquery_procedure"):
            with self.subTest(grape_type=grape_type):
                task = self._bigquery(grape_type=grape_type)
                self.assertIs(task.kwargs["durable"], False)
                self.assertIs(task.kwargs["deferrable"], True)

    def test_bigquery_durable_defaults_and_grape_overrides(self):
        cases = [
            ({}, {"durable": True}, True),
            ({"durable": True}, {}, True),
            ({"durable": True}, {"durable": False}, False),
            ({"durable": False}, {"durable": True}, True),
        ]
        for grape_type in ("bigquery_sql", "bigquery_procedure"):
            for defaults, options, expected in cases:
                with self.subTest(grape_type=grape_type, defaults=defaults, options=options):
                    task = self._bigquery(
                        defaults=defaults, options=options, grape_type=grape_type,
                    )
                    self.assertIs(task.kwargs["durable"], expected)
                    self.assertIs(task.kwargs["deferrable"], True)

    def test_bigquery_durable_does_not_change_deferrable_precedence(self):
        cases = [
            ({"deferrable": False}, {}, False),
            ({"deferrable": True}, {"deferrable": False}, False),
            ({"deferrable": False}, {"deferrable": True}, True),
        ]
        for durable in (False, True):
            for defaults, options, expected in cases:
                with self.subTest(durable=durable, defaults=defaults, options=options):
                    task = self._bigquery(defaults={**defaults, "durable": durable}, options=options)
                    self.assertIs(task.kwargs["durable"], durable)
                    self.assertIs(task.kwargs["deferrable"], expected)

    def test_legacy_bigquery_provider_imports_and_omits_disabled_durable(self):
        provider = sys.modules["airflow.providers.google.cloud.operators.bigquery"]
        # Exercise import as well as execution with only the legacy provider
        # class available. **kwargs must not be mistaken for durable support.
        with patch.object(provider, "BigQueryInsertJobOperator", LegacyBigQueryInsertJobOperator):
            del sys.modules["framework.bigquery_executor"]
            self.BigQueryExecutor = importlib.import_module(
                "framework.bigquery_executor"
            ).BigQueryExecutor
            cases = [
                ({}, {}),
                ({"durable": False}, {}),
                ({}, {"durable": False}),
                ({"durable": True}, {"durable": False}),
            ]
            for defaults, options in cases:
                with self.subTest(defaults=defaults, options=options):
                    task = self._bigquery(defaults=defaults, options=options)
                    self.assertIsInstance(task, LegacyBigQueryInsertJobOperator)
                    self.assertNotIn("durable", task.kwargs)
                    self.assertIs(task.kwargs["deferrable"], True)

    def test_legacy_bigquery_provider_rejects_enabled_durable_clearly(self):
        cases = [
            ({"durable": True}, {}),
            ({}, {"durable": True}),
            ({"durable": False}, {"durable": True}),
        ]
        with patch.object(
            self.bigquery_module, "BigQueryInsertJobOperator", LegacyBigQueryInsertJobOperator,
        ):
            for defaults, options in cases:
                with self.subTest(defaults=defaults, options=options):
                    with self.assertRaisesRegex(
                        ValueError, "durable=true requires.*BigQueryInsertJobOperator supports durable",
                    ):
                        self._bigquery(defaults=defaults, options=options)

    def test_bigquery_durable_requires_a_boolean_on_every_provider(self):
        for operator in (BigQueryInsertJobOperator, LegacyBigQueryInsertJobOperator):
            with patch.object(self.bigquery_module, "BigQueryInsertJobOperator", operator):
                for value in (None, "false", "true", 0, 1, [], {}):
                    for location in ("defaults", "options"):
                        with self.subTest(operator=operator.__name__, value=value, location=location):
                            arguments = {location: {"durable": value}}
                            with self.assertRaisesRegex(ValueError, "durable must be a YAML boolean"):
                                self._bigquery(**arguments)

    def test_root_polling_defaults_to_120_seconds(self):
        task = self._root_trigger()
        self.assertEqual(task.kwargs["poke_interval"], 120)
        self.assertIs(task.kwargs["deferrable"], True)
        self.assertIs(task.kwargs["wait_for_completion"], True)
        self.assertIs(task.kwargs["reset_dag_run"], False)
        self.assertEqual(task.kwargs["execution_timeout"], timedelta(seconds=21600))

    def test_root_polling_defaults_and_item_overrides_preserve_explicit_30(self):
        cases = [
            ({"poke_interval_seconds": 30}, {}, 30),
            ({}, {"poke_interval_seconds": 30}, 30),
            ({"poke_interval_seconds": 240}, {}, 240),
            ({"poke_interval_seconds": 240}, {"poke_interval_seconds": 30}, 30),
            ({"poke_interval_seconds": 30}, {"poke_interval_seconds": 45}, 45),
        ]
        for defaults, options, expected in cases:
            with self.subTest(defaults=defaults, options=options):
                task = self._root_trigger(defaults=defaults, options=options)
                self.assertEqual(task.kwargs["poke_interval"], expected)

    def test_root_other_runtime_options_keep_defaults_and_item_precedence(self):
        defaults = {
            "deferrable": False,
            "wait_for_completion": False,
            "reset_dag_run": True,
            "execution_timeout_seconds": 1800,
        }
        task = self._root_trigger(defaults=defaults)
        self.assertEqual(task.kwargs["poke_interval"], 120)
        self.assertIs(task.kwargs["deferrable"], False)
        self.assertIs(task.kwargs["wait_for_completion"], False)
        self.assertIs(task.kwargs["reset_dag_run"], True)
        self.assertEqual(task.kwargs["execution_timeout"], timedelta(seconds=1800))

        task = self._root_trigger(defaults=defaults, options={
            "deferrable": True,
            "wait_for_completion": True,
            "reset_dag_run": False,
            "execution_timeout_seconds": 900,
        })
        self.assertEqual(task.kwargs["poke_interval"], 120)
        self.assertIs(task.kwargs["deferrable"], True)
        self.assertIs(task.kwargs["wait_for_completion"], True)
        self.assertIs(task.kwargs["reset_dag_run"], False)
        self.assertEqual(task.kwargs["execution_timeout"], timedelta(seconds=900))

    def test_fin_polling_defaults_to_120_seconds(self):
        task = self._fin_sensor()
        self.assertEqual(task.kwargs["poke_interval"], 120)
        self.assertIs(task.kwargs["deferrable"], True)
        self.assertEqual(task.kwargs["mode"], "reschedule")
        self.assertEqual(task.kwargs["timeout"], 21600)
        self.assertEqual(task.kwargs["execution_timeout"], timedelta(seconds=21600))
        self.assertIs(task.kwargs["soft_fail"], False)

    def test_fin_polling_defaults_and_grape_overrides_preserve_explicit_60(self):
        cases = [
            ({"poke_interval_seconds": 60}, {}, 60),
            ({}, {"poke_interval_seconds": 60}, 60),
            ({"poke_interval_seconds": 240}, {}, 240),
            ({"poke_interval_seconds": 240}, {"poke_interval_seconds": 60}, 60),
            ({"poke_interval_seconds": 60}, {"poke_interval_seconds": 45}, 45),
        ]
        for defaults, options, expected in cases:
            with self.subTest(defaults=defaults, options=options):
                task = self._fin_sensor(defaults=defaults, options=options)
                self.assertEqual(task.kwargs["poke_interval"], expected)

    def test_fin_other_runtime_options_keep_defaults_and_grape_precedence(self):
        defaults = {
            "deferrable": False,
            "mode": "poke",
            "soft_fail": True,
            "timeout_seconds": 1800,
        }
        task = self._fin_sensor(defaults=defaults)
        self.assertEqual(task.kwargs["poke_interval"], 120)
        self.assertIs(task.kwargs["deferrable"], False)
        self.assertEqual(task.kwargs["mode"], "poke")
        self.assertIs(task.kwargs["soft_fail"], True)
        self.assertEqual(task.kwargs["timeout"], 1800)
        self.assertEqual(task.kwargs["execution_timeout"], timedelta(seconds=1800))

        task = self._fin_sensor(defaults=defaults, options={
            "deferrable": True,
            "mode": "reschedule",
            "soft_fail": False,
            "timeout_seconds": 900,
        })
        self.assertEqual(task.kwargs["poke_interval"], 120)
        self.assertIs(task.kwargs["deferrable"], True)
        self.assertEqual(task.kwargs["mode"], "reschedule")
        self.assertIs(task.kwargs["soft_fail"], False)
        self.assertEqual(task.kwargs["timeout"], 900)
        self.assertEqual(task.kwargs["execution_timeout"], timedelta(seconds=900))

    def test_fin_timeout_fallback_and_precedence_are_unchanged(self):
        cases = [
            ({"execution_timeout_seconds": 700}, {}, 700),
            ({"execution_timeout_seconds": 700}, {"execution_timeout_seconds": 800}, 800),
            ({"execution_timeout_seconds": 700}, {"timeout_seconds": 900}, 900),
            ({"timeout_seconds": 600}, {"execution_timeout_seconds": 800}, 600),
            ({"timeout_seconds": 600}, {"timeout_seconds": 900}, 900),
        ]
        for defaults, options, expected in cases:
            with self.subTest(defaults=defaults, options=options):
                task = self._fin_sensor(defaults=defaults, options=options)
                self.assertEqual(task.kwargs["timeout"], expected)
                self.assertEqual(task.kwargs["execution_timeout"], timedelta(seconds=expected))


if __name__ == "__main__":
    unittest.main()
