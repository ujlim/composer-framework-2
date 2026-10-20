"""Offline integration tests for full and execution-time DAG construction.

Run with ``python -m unittest discover -s tests -v``. The factories, catalog,
entry point, business-date callbacks, and BigQuery SQL loader are real; only
Airflow/provider runtime objects and unused executor imports are doubled.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
import importlib
import os
from pathlib import Path
import runpy
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

import yaml


ROOT = Path(__file__).resolve().parents[1]
FRAMEWORK_DIR = ROOT / "dags" / "framework"


class DAG:
    created_ids: list[str] = []

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.task_dict = {}
        self.created_ids.append(self.dag_id)

    @property
    def tasks(self):
        return list(self.task_dict.values())

    @property
    def task_ids(self):
        return list(self.task_dict)


class Param:
    def __init__(self, default=None, **schema):
        self.default = default
        self.schema = schema


class Operator:
    def __init__(self, *, task_id, dag, **kwargs):
        self.task_id, self.dag = task_id, dag
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
    pass


class UnusedExecutor:
    @classmethod
    def create_task(cls, **kwargs):
        raise AssertionError("Fixture unexpectedly invoked a non-BigQuery executor")


def _runtime_modules(context):
    """Install import-only stubs, never replace either DAG factory."""
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
    module("airflow.models").DAG = DAG
    module("airflow.models.dag").DAG = DAG
    module("airflow.models.param").Param = Param
    module("airflow.sdk").get_parsing_context = lambda: context["value"]
    module("airflow.sdk").get_current_context = lambda: {}
    module("airflow.providers.standard.operators.empty").EmptyOperator = EmptyOperator
    module("airflow.providers.standard.operators.python").PythonOperator = PythonOperator
    module("airflow.providers.standard.operators.trigger_dagrun").TriggerDagRunOperator = TriggerDagRunOperator
    module("airflow.providers.google.cloud.operators.bigquery").BigQueryInsertJobOperator = BigQueryInsertJobOperator
    module("pendulum").parse = lambda value, tz: datetime.fromisoformat(value).replace(tzinfo=ZoneInfo(tz))
    module("framework").__path__ = [str(FRAMEWORK_DIR)]
    for filename, name in (
        ("dataflow_executor", "DataflowExecutor"),
        ("fin_file_sensor_executor", "FinFileSensorExecutor"),
        ("gcs_executor", "GCSExecutor"),
        ("postgres_executor", "PostgresExecutor"),
        ("sftp_runner_executor", "SftpRunnerExecutor"),
    ):
        setattr(module(f"framework.{filename}"), name, UnusedExecutor)
    return modules


def _normalized(value):
    if isinstance(value, Param):
        return {"default": value.default, "schema": _normalized(value.schema)}
    if isinstance(value, dict):
        return {key: _normalized(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_normalized(item) for item in value]
    if isinstance(value, set):
        return sorted(value)
    if callable(value):
        return f"{value.__module__}.{value.__qualname__}"
    return value


def _snapshot(dag):
    """Capture every configured DAG/task field, including both edge directions."""
    settings = {key: value for key, value in vars(dag).items() if key != "task_dict"}
    tasks = {
        task.task_id: {
            "operator": type(task).__name__,
            **{key: value for key, value in vars(task).items() if key != "dag"},
        }
        for task in dag.tasks
    }
    return _normalized({"settings": settings, "tasks": tasks})


class SelectiveDagEquivalenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.pipelines = self.directory / "pipelines"
        self.entry_point = self.directory / "main.py"
        self.entry_point.write_text((ROOT / "dags" / "main.py").read_text(encoding="utf-8"), encoding="utf-8")
        self.context = {"value": types.SimpleNamespace(dag_id=None, task_id=None)}
        environment_patch = patch.dict(os.environ, {"FRAMEWORK_DISABLE_SELECTIVE_PARSING": "0"})
        environment_patch.start()
        self.addCleanup(environment_patch.stop)

        # Isolate imports from other offline suites, and restore their modules
        # afterward. Actual framework files are imported from the repository.
        self.module_patch = patch.dict(sys.modules, {})
        self.module_patch.start()
        self.addCleanup(self.module_patch.stop)
        for name in list(sys.modules):
            if name == "framework" or name.startswith("framework."):
                del sys.modules[name]
        sys.modules.update(_runtime_modules(self.context))
        self.config_paths = {}
        self.sql_paths = {}
        self._write_fixtures()

    def _write_config(self, kind, folder, data):
        path = self.pipelines / f"{kind}s" / folder / "config.yaml"
        path.parent.mkdir(parents=True)
        path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
        dag_id = data[kind][f"{kind}_id"]
        self.config_paths[dag_id] = path
        return path.parent

    def _write_fixtures(self):
        # Folder names deliberately differ from configured IDs.
        vine_directory = self._write_config("vine", "first-vine-folder", {
            "vine": {
                "vine_id": "analytics_vine", "description": "Branched SQL pipeline",
                "timezone": "Asia/Seoul", "start_date": "2026-02-03",
                "owner": "analytics", "max_active_runs": 3,
                "dagrun_timeout_seconds": 12345, "tags": ["warehouse", "vine", "warehouse"],
            },
            "runtime": {
                "project_id": "sample-project", "location": "asia-northeast3",
                "gcp_conn_id": "warehouse_conn", "impersonation_chain": ["worker@example.test"],
                "labels": {"environment": "TEST"},
            },
            "business_date": {
                "type": "monthly",
                "date_offsets": {"previous_month": {"months": -1, "adjust": "month_end"}},
            },
            "grape_defaults": {
                "retries": 4, "retry_delay_seconds": 47, "execution_timeout_seconds": 900,
                "pool": "warehouse_pool", "priority_weight": 9, "priority": "BATCH",
                "deferrable": True, "force_rerun": False,
            },
            "grapes": [
                {
                    "grape_id": "extract", "type": "bigquery_sql", "sql_file": "sql/extract.sql",
                    "parameters": {"run_date": {"type": "DATE", "value": "{{ params.business_date }}"}},
                },
                {
                    "grape_id": "transform", "type": "bigquery_sql", "sql_file": "sql/transform.sql",
                    "depends_on": ["extract"],
                    "options": {"retries": 7, "retry_delay_seconds": 13, "pool": "urgent_pool",
                                "priority_weight": 15, "deferrable": False, "force_rerun": True},
                },
                {
                    "grape_id": "audit", "type": "bigquery_sql", "sql_file": "sql/audit.sql",
                    "depends_on": ["extract"], "parameters": {"enabled": {"type": "BOOL", "value": True}},
                },
                {
                    "grape_id": "publish", "type": "bigquery_procedure",
                    "procedure": "sample-project.reporting.publish",
                    "depends_on": ["transform", "audit"],
                    "parameters": {"count": {"type": "INT64", "value": 12}},
                },
            ],
        })
        for name in ("extract", "transform", "audit"):
            path = vine_directory / "sql" / f"{name}.sql"
            path.parent.mkdir(exist_ok=True)
            path.write_text(f"SELECT '{name}' AS step;\n", encoding="utf-8")
            self.sql_paths[name] = path

        for dag_id, folder in (("sibling_vine", "second-vine-folder"), ("unrelated_vine", "third-vine-folder")):
            directory = self._write_config("vine", folder, {
                "vine": {"vine_id": dag_id},
                "grapes": [{"grape_id": "only_task", "type": "bigquery_sql", "sql_file": "sql/only.sql"}],
            })
            path = directory / "sql" / "only.sql"
            path.parent.mkdir()
            path.write_text(f"SELECT '{dag_id}';\n", encoding="utf-8")
            self.sql_paths[dag_id] = path

        self._write_config("root", "first-root-folder", {
            "root": {
                "root_id": "orchestrator_root", "description": "Root dependency graph",
                "timezone": "Asia/Seoul", "start_date": "2026-02-03",
                "schedule": "15 3 * * *", "catchup": True, "owner": "integration",
                "max_active_runs": 2, "dagrun_timeout_seconds": 45678,
                "tags": ["batch", "root", "batch"],
            },
            "business_date": {
                "type": "monthly", "source": "data_interval_end", "offset_days": -2,
                "date_offsets": {"previous_month": {"months": -1, "adjust": "month_end"}},
            },
            "parameters": {"tenant": {"value": "example"}, "region": "apac"},
            "defaults": {
                "wait_for_completion": True, "poke_interval_seconds": 23,
                "reset_dag_run": False, "deferrable": True, "execution_timeout_seconds": 4321,
            },
            "vines": [
                {"vine_id": "analytics_vine", "trigger_id": "trigger_analytics"},
                {
                    "vine_id": "sibling_vine", "depends_on": ["analytics_vine"],
                    "options": {"wait_for_completion": False, "poke_interval_seconds": 7,
                                "reset_dag_run": True, "deferrable": False,
                                "execution_timeout_seconds": 987},
                },
                {"vine_id": "unrelated_vine", "depends_on": ["analytics_vine"]},
            ],
        })
        self._write_config("root", "second-root-folder", {
            "root": {"root_id": "unrelated_root", "schedule": None},
            "vines": [{"vine_id": "unrelated_vine"}],
        })

    def _manifest(self, enabled):
        if enabled:
            importlib.import_module("framework.pipeline_manifest").write_manifest(self.pipelines)
        else:
            (self.pipelines / ".dag-manifest.json").unlink(missing_ok=True)

    def _parse(self, dag_id=None, task_id=None, *, registration_error_count=0):
        self.context["value"] = types.SimpleNamespace(dag_id=dag_id, task_id=task_id)
        DAG.created_ids.clear()
        reads = []
        original_open = Path.open

        def tracked_open(path, mode="r", *args, **kwargs):
            if "r" in mode:
                reads.append(path.resolve())
            return original_open(path, mode, *args, **kwargs)

        with patch.object(Path, "open", tracked_open):
            namespace = runpy.run_path(str(self.entry_point))
        self.assertEqual(len(namespace["registration_errors"]), registration_error_count)
        # Airflow scans module values, including harmless loop-variable aliases.
        dags = {value.dag_id: value for value in namespace.values() if isinstance(value, DAG)}
        self.assertEqual(set(dags), namespace["registered_dag_ids"])
        registered = {
            value.dag_id: value for key, value in namespace.items()
            if key.startswith("_framework_registered_dag_") and isinstance(value, DAG)
        }
        self.assertEqual(registered, dags)
        return dags, reads, list(DAG.created_ids)

    def _assert_only_target(self, dags, built_ids, target):
        self.assertEqual(set(dags), {target})
        self.assertEqual(built_ids, [target], "Unselected factories must not create discarded DAGs")

    def _invalid_configurations(self):
        originals = {
            dag_id: yaml.safe_load(self.config_paths[dag_id].read_text(encoding="utf-8"))
            for dag_id in ("orchestrator_root", "analytics_vine")
        }
        cases = [
            ("unknown Vine", "orchestrator_root",
             lambda data: data["vines"][0].update(vine_id="missing_vine"), "unregistered Vine"),
            ("missing Root dependency", "orchestrator_root",
             lambda data: data["vines"][1].update(depends_on=["missing_parent"]), "depends on unknown ID"),
            ("cyclic Root dependency", "orchestrator_root",
             lambda data: data["vines"][0].update(depends_on=["sibling_vine"]), "dependency cycle"),
            ("duplicate Root graph ID", "orchestrator_root",
             lambda data: data["vines"].append(dict(data["vines"][0])), "duplicate IDs"),
            ("duplicate trigger task ID", "orchestrator_root",
             lambda data: data["vines"][1].update(trigger_id="trigger_analytics"), "Duplicate task ID"),
            ("missing Grape dependency", "analytics_vine",
             lambda data: data["grapes"][0].update(depends_on=["missing_parent"]), "depends on unknown ID"),
            ("cyclic Grape dependency", "analytics_vine",
             lambda data: data["grapes"][0].update(depends_on=["publish"]), "dependency cycle"),
            ("duplicate Grape task ID", "analytics_vine",
             lambda data: data["grapes"].append(dict(data["grapes"][0])), "duplicate IDs"),
        ]
        for label, dag_id, mutate, expected_error in cases:
            data = deepcopy(originals[dag_id])
            mutate(data)
            yield label, dag_id, data, expected_error

    def test_full_parse_registers_every_dag_with_or_without_manifest(self):
        expected = None
        for manifest in (False, True):
            with self.subTest(manifest=manifest):
                self._manifest(manifest)
                dags, reads, built_ids = self._parse()
                self.assertEqual(set(dags), set(self.config_paths))
                self.assertCountEqual(built_ids, self.config_paths)
                self.assertEqual({path for path in reads if path.suffix == ".sql"}, set(self.sql_paths.values()))
                snapshots = {key: _snapshot(dag) for key, dag in dags.items()}
                if expected is None:
                    expected = snapshots
                else:
                    self.assertEqual(snapshots, expected)

    def test_targeted_vine_preserves_all_tasks_edges_parameters_and_runtime_options(self):
        for manifest in (False, True):
            self._manifest(manifest)
            full, _, _ = self._parse()
            expected = _snapshot(full["analytics_vine"])
            for task_id in (None, "extract", "transform", "publish"):
                with self.subTest(manifest=manifest, task_id=task_id):
                    dags, _, built = self._parse("analytics_vine", task_id)
                    self._assert_only_target(dags, built, "analytics_vine")
                    actual = dags["analytics_vine"]
                    self.assertEqual(_snapshot(actual), expected)
                    self.assertEqual(set(actual.task_ids), {
                        "vine_start", "resolve_business_date", "extract", "transform", "audit", "publish", "vine_end",
                    })
                    self.assertEqual(actual.task_dict["extract"].downstream_task_ids, {"transform", "audit"})
                    self.assertEqual(actual.task_dict["publish"].upstream_task_ids, {"transform", "audit"})
                    self.assertEqual(actual.task_dict["publish"].downstream_task_ids, {"vine_end"})
                    defaulted = actual.task_dict["extract"]
                    self.assertEqual((defaulted.retries, defaulted.retry_delay, defaulted.pool),
                                     (4, timedelta(seconds=47), "warehouse_pool"))
                    self.assertTrue(defaulted.deferrable)
                    overridden = actual.task_dict["transform"]
                    self.assertEqual((overridden.retries, overridden.retry_delay, overridden.pool),
                                     (7, timedelta(seconds=13), "urgent_pool"))
                    self.assertFalse(overridden.deferrable)
                    self.assertTrue(overridden.force_rerun)
                    self.assertEqual(defaulted.configuration["query"]["queryParameters"], [{
                        "name": "run_date", "parameterType": {"type": "DATE"},
                        "parameterValue": {"value": "{{ params.business_date }}"},
                    }])

    def test_targeted_root_preserves_all_triggers_dependencies_and_business_context(self):
        for manifest in (False, True):
            self._manifest(manifest)
            full, _, _ = self._parse()
            expected = _snapshot(full["orchestrator_root"])
            for task_id in (None, "resolve_business_date", "trigger_analytics", "trigger_sibling_vine"):
                with self.subTest(manifest=manifest, task_id=task_id):
                    dags, _, built = self._parse("orchestrator_root", task_id)
                    self._assert_only_target(dags, built, "orchestrator_root")
                    dag = dags["orchestrator_root"]
                    self.assertEqual(_snapshot(dag), expected)
                    self.assertEqual(set(dag.task_ids), {
                        "root_start", "resolve_business_date", "trigger_analytics",
                        "trigger_sibling_vine", "trigger_unrelated_vine", "root_end",
                    })
                    trigger = dag.task_dict["trigger_analytics"]
                    self.assertEqual(trigger.trigger_dag_id, "analytics_vine")
                    self.assertEqual(trigger.downstream_task_ids, {"trigger_sibling_vine", "trigger_unrelated_vine"})
                    self.assertEqual(trigger.trigger_run_id,
                                     "root__orchestrator_root__{{ dag_run.run_id }}__analytics_vine")
                    self.assertEqual((trigger.wait_for_completion, trigger.deferrable, trigger.poke_interval),
                                     (True, True, 23))
                    self.assertEqual(trigger.execution_timeout, timedelta(seconds=4321))
                    self.assertEqual(trigger.conf["tenant"], "example")
                    self.assertEqual(trigger.conf["region"], "apac")
                    for name in ("business_date", "period_type", "period_start", "period_end", "previous_month"):
                        self.assertEqual(trigger.conf[name],
                                         "{{ ti.xcom_pull(task_ids='resolve_business_date')['" + name + "'] }}")
                    overridden = dag.task_dict["trigger_sibling_vine"]
                    self.assertEqual((overridden.wait_for_completion, overridden.deferrable,
                                      overridden.reset_dag_run, overridden.poke_interval), (False, False, True, 7))
                    self.assertEqual(overridden.execution_timeout, timedelta(seconds=987))
                    self.assertEqual(dag.task_dict["root_end"].upstream_task_ids,
                                     {"trigger_sibling_vine", "trigger_unrelated_vine"})

    def test_targeted_vine_reads_all_its_sql_but_no_other_vine_sql(self):
        expected_sql = {self.sql_paths[name] for name in ("extract", "transform", "audit")}
        for manifest in (False, True):
            with self.subTest(manifest=manifest):
                self._manifest(manifest)
                dags, reads, built = self._parse("analytics_vine", "transform")
                self._assert_only_target(dags, built, "analytics_vine")
                self.assertCountEqual([path for path in reads if path.suffix == ".sql"], expected_sql)
                if manifest:
                    self.assertEqual([path for path in reads if path.name == "config.yaml"],
                                     [self.config_paths["analytics_vine"]])

    def test_targeted_root_uses_vine_metadata_without_reading_sql_or_vine_yaml(self):
        for manifest in (False, True):
            with self.subTest(manifest=manifest):
                self._manifest(manifest)
                dags, reads, built = self._parse("orchestrator_root", "trigger_analytics")
                self._assert_only_target(dags, built, "orchestrator_root")
                self.assertEqual([path for path in reads if path.suffix == ".sql"], [])
                if manifest:
                    self.assertEqual([path for path in reads if path.name == "config.yaml"],
                                     [self.config_paths["orchestrator_root"]])

    def test_task_id_without_dag_id_does_not_filter_scheduler_parse(self):
        self._manifest(True)
        full, _, _ = self._parse()
        dags, _, built = self._parse(task_id="transform")
        self.assertCountEqual(built, self.config_paths)
        self.assertEqual({key: _snapshot(dag) for key, dag in dags.items()},
                         {key: _snapshot(dag) for key, dag in full.items()})

    def test_rollback_switch_restores_yaml_scan_without_changing_target_dag(self):
        self._manifest(True)
        full, _, _ = self._parse()
        with patch.dict(os.environ, {"FRAMEWORK_DISABLE_SELECTIVE_PARSING": "1"}):
            dags, reads, built = self._parse("analytics_vine", "publish")
        self._assert_only_target(dags, built, "analytics_vine")
        self.assertEqual(_snapshot(dags["analytics_vine"]), _snapshot(full["analytics_vine"]))
        self.assertCountEqual([path for path in reads if path.name == "config.yaml"],
                              self.config_paths.values())
        self.assertEqual({path for path in reads if path.suffix == ".sql"},
                         {self.sql_paths[name] for name in ("extract", "transform", "audit")})

    def test_legacy_whitespace_vine_id_has_identical_full_and_target_dag(self):
        # Legacy factories normalize IDs; execution-time selection must do the
        # same. This standalone Vine avoids changing existing Root lookup rules.
        self._write_config("vine", "whitespace-id-folder", {
            "vine": {"vine_id": " \tstandalone_vine \n"},
            "grapes": [
                {"grape_id": "first", "type": "bigquery_procedure", "procedure": "reporting.first"},
                {"grape_id": "last", "type": "bigquery_procedure", "procedure": "reporting.last",
                 "depends_on": ["first"]},
            ],
        })
        self._manifest(False)
        full, _, _ = self._parse()
        self.assertIn("standalone_vine", full)
        for task_id in (None, "first", "last"):
            with self.subTest(task_id=task_id):
                dags, _, built = self._parse("standalone_vine", task_id)
                self._assert_only_target(dags, built, "standalone_vine")
                self.assertEqual(_snapshot(dags["standalone_vine"]), _snapshot(full["standalone_vine"]))
                self.assertEqual(set(dags["standalone_vine"].task_ids),
                                 {"vine_start", "resolve_business_date", "first", "last", "vine_end"})
                self.assertEqual(dags["standalone_vine"].task_dict["first"].downstream_task_ids, {"last"})

    def test_user_dag_ids_cannot_overwrite_parser_control_globals(self):
        colliding_ids = ("TARGET_DAG_ID", "PARSE_STARTED_AT", "LOGGER", "catalog", "register_dag")
        for dag_id in colliding_ids:
            self._write_config("vine", f"collision-{dag_id}", {
                "vine": {"vine_id": dag_id},
                "grapes": [{"grape_id": "run", "type": "bigquery_procedure", "procedure": "reporting.run"}],
            })
        for manifest in (False, True):
            self._manifest(manifest)
            full, _, built = self._parse()
            self.assertEqual(set(full), set(self.config_paths))
            self.assertCountEqual(built, self.config_paths)
            for dag_id in colliding_ids:
                with self.subTest(manifest=manifest, dag_id=dag_id):
                    dags, _, built = self._parse(dag_id, "run")
                    self._assert_only_target(dags, built, dag_id)
                    self.assertEqual(_snapshot(dags[dag_id]), _snapshot(full[dag_id]))

    def test_invalid_target_fails_loudly_instead_of_registering_zero_dags(self):
        for label, dag_id, data, expected_error in self._invalid_configurations():
            path = self.config_paths[dag_id]
            original = path.read_bytes()
            try:
                path.write_text(yaml.safe_dump(data), encoding="utf-8")
                for manifest in (False, True):
                    with self.subTest(case=label, manifest=manifest):
                        self._manifest(manifest)
                        with self.assertLogs(level="ERROR") as logged:
                            with self.assertRaisesRegex(RuntimeError, "No DAGs were registered") as caught:
                                self._parse(dag_id, "resolve_business_date")
                        self.assertIn(expected_error, str(caught.exception))
                        self.assertIn(expected_error, "\n".join(logged.output))
            finally:
                path.write_bytes(original)

    def test_full_parse_isolates_factory_failures_and_keeps_healthy_dags(self):
        for label, dag_id, data, expected_error in self._invalid_configurations():
            path = self.config_paths[dag_id]
            original = path.read_bytes()
            try:
                path.write_text(yaml.safe_dump(data), encoding="utf-8")
                for manifest in (False, True):
                    with self.subTest(case=label, manifest=manifest):
                        self._manifest(manifest)
                        with self.assertLogs(level="ERROR") as logged:
                            dags, _, _ = self._parse(registration_error_count=1)
                        self.assertEqual(set(dags), set(self.config_paths) - {dag_id})
                        self.assertIn(expected_error, "\n".join(logged.output))
            finally:
                path.write_bytes(original)


if __name__ == "__main__":
    unittest.main()
