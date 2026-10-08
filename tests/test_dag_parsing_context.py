"""Regression tests for dags/main.py DAG parse selection without Composer dependencies.

Run: python -m unittest discover -s tests -v
"""
from __future__ import annotations

import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

DAG_MAIN = Path(__file__).resolve().parents[1] / "dags" / "main.py"


def execute_dag_file(target=None, vines=("vine_one", "vine_two"), roots=("root_one", "root_two"), failing=()):
    """Execute the actual entry point with stubbed Airflow/framework dependencies."""
    created = []
    log_records = []
    loader_calls = []

    class DAG:
        def __init__(self, dag_id):
            self.dag_id = dag_id

    class Config:
        def __init__(self, dag_id):
            self.id = dag_id
            self.source_path = Path(f"{dag_id}/config.yaml")

    class Catalog:
        def __init__(self):
            self.vines = [Config(dag_id) for dag_id in vines]
            self.roots = [Config(dag_id) for dag_id in roots]
            self.vine_ids = set(vines)

    class Loader:
        def __init__(self, directory):
            self.directory = directory

        def load(self):
            loader_calls.append(self.directory)
            return Catalog()

    class VineFactory:
        def __init__(self, config):
            self.config = config

        def create(self):
            created.append(("vine", self.config.id))
            if self.config.id in failing:
                raise ValueError("simulated vine failure")
            return DAG(self.config.id)

    class RootFactory:
        def __init__(self, config, available_vine_ids):
            self.config = config
            assert available_vine_ids == set(vines)

        def create(self):
            created.append(("root", self.config.id))
            if self.config.id in failing:
                raise ValueError("simulated root failure")
            return DAG(self.config.id)

    class Logger:
        def info(self, message, *args, **kwargs):
            log_records.append(("info", message, kwargs.get("extra", {})))

        def exception(self, message, *args, **kwargs):
            log_records.append(("exception", message, kwargs.get("extra", {})))

    def mod(name, **attributes):
        m = types.ModuleType(name)
        for key, value in attributes.items():
            setattr(m, key, value)
        return m

    modules = {
        "airflow": mod("airflow"),
        "airflow.models": mod("airflow.models"),
        "airflow.models.dag": mod("airflow.models.dag", DAG=DAG),
        "airflow.sdk": mod("airflow.sdk", get_parsing_context=lambda: types.SimpleNamespace(dag_id=target)),
        "framework": mod("framework"),
        "framework.config_loader": mod("framework.config_loader", PipelineCatalogLoader=Loader),
        "framework.logger": mod("framework.logger", get_logger=lambda _: Logger()),
        "framework.root_factory": mod("framework.root_factory", RootFactory=RootFactory),
        "framework.vine_factory": mod("framework.vine_factory", VineFactory=VineFactory),
    }
    modules["airflow"].__path__ = []
    modules["airflow.models"].__path__ = []
    modules["framework"].__path__ = []

    with patch.dict(sys.modules, modules):
        namespace = runpy.run_path(str(DAG_MAIN))
    return namespace, created, log_records, loader_calls


class DagParsingContextTest(unittest.TestCase):
    def test_full_parse_registers_all_vines_and_roots(self):
        namespace, created, logs, loaders = execute_dag_file()
        self.assertEqual(
            created, [("vine", "vine_one"), ("vine", "vine_two"),
                      ("root", "root_one"), ("root", "root_two")]
        )
        self.assertEqual(set(namespace["registered_dag_ids"]),
                         {"vine_one", "vine_two", "root_one", "root_two"})
        self.assertEqual(len(loaders), 1)
        self.assertIn("total_registration", self._stages(logs))

    def test_task_parse_creates_only_requested_vine(self):
        namespace, created, logs, loaders = execute_dag_file(target="vine_two")
        self.assertEqual(created, [("vine", "vine_two")])
        self.assertEqual(namespace["registered_dag_ids"], {"vine_two"})
        self.assertEqual(len(loaders), 1)  # catalog still fully loaded by design
        self.assertEqual(self._event(logs, "vine_stage")["selected_count"], 1)
        self.assertEqual(self._event(logs, "root_stage")["selected_count"], 0)
        self.assertEqual(self._event(logs, "catalog_load")["vine_count"], 2)

    def test_task_parse_creates_only_requested_root(self):
        namespace, created, logs, _ = execute_dag_file(target="root_one")
        self.assertEqual(created, [("root", "root_one")])
        self.assertEqual(namespace["registered_dag_ids"], {"root_one"})
        self.assertEqual(self._event(logs, "vine_stage")["selected_count"], 0)
        self.assertEqual(self._event(logs, "root_stage")["selected_count"], 1)

    def test_failed_dag_does_not_hide_unrelated_dags(self):
        namespace, created, logs, _ = execute_dag_file(failing={"vine_one"})
        self.assertEqual(len(created), 4)
        self.assertEqual(len(namespace["registration_errors"]), 1)
        self.assertEqual(namespace["registered_dag_ids"],
                         {"vine_two", "root_one", "root_two"})
        self.assertTrue(any(event == "dag_registration_failed"
                            for _, _, fields in logs
                            if (event := fields.get("event"))))

    def test_unknown_target_fails_instead_of_silent_empty_parse(self):
        with self.assertRaisesRegex(RuntimeError, "No DAGs were registered"):
            execute_dag_file(target="unknown_dag")

    def test_timing_logs_include_stages_and_nonnegative_durations(self):
        _, _, logs, _ = execute_dag_file(target="vine_one")
        for stage in ("catalog_load", "vine_create", "vine_stage", "root_stage", "total_registration"):
            event = self._event(logs, stage)
            self.assertGreaterEqual(event["duration_ms"], 0)
            self.assertEqual(event["target_dag_id"], "vine_one")

    @staticmethod
    def _stages(logs):
        return [data["stage"] for _, _, data in logs if data.get("event") == "dag_parse_timing"]

    @staticmethod
    def _event(logs, stage):
        return next(fields for _, _, fields in logs if fields.get("stage") == stage)


if __name__ == "__main__":
    unittest.main()
