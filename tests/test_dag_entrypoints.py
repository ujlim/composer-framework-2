"""Exercise both actual entrypoint files with real YAML/factories and offline Airflow doubles."""

from __future__ import annotations

import importlib
from pathlib import Path
import runpy
import shutil
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

import yaml

from test_runtime_options import DAG, Operator, _runtime_modules


DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


class EntryPointTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dags = Path(tmp.name)
        self.pipelines = self.dags / "pipelines"
        (self.pipelines / "roots").mkdir(parents=True)
        (self.pipelines / "vines").mkdir()
        for name in ("main_roots.py", "main_vines.py"):
            shutil.copyfile(DAGS_DIR / name, self.dags / name)

        module_patch = patch.dict(sys.modules, {})
        module_patch.start()
        self.addCleanup(module_patch.stop)
        for name in list(sys.modules):
            if name == "framework" or name.startswith("framework."):
                del sys.modules[name]
        sys.modules.update(_runtime_modules())

        self.target = None
        self._module("airflow.models.dag").DAG = DAG
        self._module("airflow.models").BaseOperator = Operator
        self._module("airflow.sdk").get_parsing_context = self._context
        self._module("airflow.hooks.base").BaseHook = object
        self._module("airflow.providers.google.cloud.hooks.gcs").GCSHook = object

    @staticmethod
    def _module(name):
        return sys.modules.setdefault(name, ModuleType(name))

    def _context(self):
        return SimpleNamespace(dag_id=self.target)

    def _write(self, kind, folder, data):
        path = self.pipelines / f"{kind}s" / folder / "config.yaml"
        path.parent.mkdir()
        path.write_text(yaml.safe_dump(data), encoding="utf-8")
        return path

    def root(self, ident, vines, folder=None):
        return self._write(
            "root",
            folder or ident,
            {
                "root": {"root_id": ident, "schedule": "0 1 * * *", "catchup": True},
                "vines": vines,
                "defaults": {"deferrable": False, "poke_interval_seconds": 95},
            },
        )

    def vine(self, ident, folder=None, broken=False):
        path = self._write(
            "vine",
            folder or ident,
            {
                "vine": {"vine_id": ident},
                "runtime": {"project_id": "offline-test", "location": "asia-northeast3"},
                "grapes": [] if broken else [
                    {"grape_id": "first", "type": "bigquery_sql", "sql_file": "sample.sql"},
                    {
                        "grape_id": "second",
                        "type": "bigquery_sql",
                        "sql_file": "sample.sql",
                        "depends_on": ["first"],
                    },
                ],
            },
        )

        (path.parent / "sample.sql").write_text("SELECT 1;\n", encoding="utf-8")
        return path

    def run_entry(self, kind):
        result = runpy.run_path(str(self.dags / f"main_{kind}s.py"))
        return {dag.dag_id: dag for dag in result.values() if isinstance(dag, DAG)}

    def run_with_factory_counts(self, target):
        self.target = target
        root_module = importlib.import_module("framework.root_factory")
        vine_module = importlib.import_module("framework.vine_factory")
        with (
            patch.object(root_module, "RootFactory", wraps=root_module.RootFactory) as roots,
            patch.object(vine_module, "VineFactory", wraps=vine_module.VineFactory) as vines,
        ):
            root_dags, vine_dags = self.run_entry("root"), self.run_entry("vine")
            return root_dags, vine_dags, (roots.call_count, vines.call_count)

    def setup_two_kinds(self):
        # YAML folder names are not required to match IDs.
        self.vine("vine-a", folder="not-the-vine-id")
        self.vine("vine-b")
        self.root("root-a", [{"vine_id": "vine-a"}, {"vine_id": "vine-b", "depends_on": ["vine-a"]}])
        self.root("root-b", [{"vine_id": "vine-b"}])

    def test_discovery_keeps_ids_schedules_dependencies_and_options(self):
        self.setup_two_kinds()
        # Prove the modules themselves do not import the other factory.
        roots = self.run_entry("root")
        self.assertNotIn("framework.vine_factory", sys.modules)
        del sys.modules["framework.root_factory"]
        vines = self.run_entry("vine")
        self.assertNotIn("framework.root_factory", sys.modules)
        self.assertEqual(set(roots), {"root-a", "root-b"})
        self.assertEqual(set(vines), {"vine-a", "vine-b"})
        root = roots["root-a"]
        self.assertEqual(root.schedule, "0 1 * * *")
        self.assertTrue(root.catchup)
        trig_a, trig_b = root.task_dict["trigger_vine-a"], root.task_dict["trigger_vine-b"]
        self.assertEqual(trig_a.trigger_dag_id, "vine-a")
        self.assertEqual(trig_b.upstream_task_ids, {"trigger_vine-a"})
        self.assertEqual(trig_a.poke_interval, 95)
        self.assertFalse(trig_a.deferrable)
        self.assertIsNone(vines["vine-a"].schedule)
        self.assertEqual(vines["vine-a"].task_dict["second"].upstream_task_ids, {"first"})

    def test_worker_context_builds_only_requested_dag_in_its_file(self):
        self.setup_two_kinds()
        for target, roots, vines, counts in [
            ("root-b", {"root-b"}, set(), (1, 0)),
            ("vine-a", set(), {"vine-a"}, (0, 1)),
            ("absent", {"root-a", "root-b"}, {"vine-a", "vine-b"}, (2, 2)),
        ]:
            with self.subTest(target=target):
                actual_roots, actual_vines, actual_counts = self.run_with_factory_counts(target)
                self.assertEqual((set(actual_roots), set(actual_vines), actual_counts), (roots, vines, counts))

    def test_cross_kind_collision_rejected_before_either_builds_even_after_trim(self):
        self.vine("same-id")
        self.root(" same-id ", [{"vine_id": "same-id"}], folder="root-folder")
        modules = [
            importlib.import_module("framework.root_factory"),
            importlib.import_module("framework.vine_factory"),
        ]
        for target in (None, "same-id"):
            self.target = target
            for kind, name, module in zip(("root", "vine"), ("RootFactory", "VineFactory"), modules):
                with (
                    self.subTest(target=target, kind=kind),
                    patch.object(module, name) as factory,
                    self.assertLogs("vine_framework", level="ERROR"),
                    self.assertRaisesRegex(ValueError, "Duplicate DAG ID across Root and Vine 'same-id'"),
                ):
                    self.run_entry(kind)
                factory.assert_not_called()

    def test_same_kind_duplicate_keeps_catalog_failure(self):
        self.vine("duplicated", folder="one")
        self.vine("duplicated", folder="two")
        for kind in ("root", "vine"):
            with self.subTest(kind=kind), self.assertLogs(level="ERROR"):
                with self.assertRaisesRegex(ValueError, "Duplicate vine ID 'duplicated'"):
                    self.run_entry(kind)

    def test_factory_error_isolated_and_empty_root_is_allowed(self):
        self.vine("vine-good")
        self.vine("vine-broken", broken=True)
        self.assertEqual(self.run_entry("root"), {})
        with self.assertLogs("vine_framework", level="ERROR"):
            self.assertEqual(set(self.run_entry("vine")), {"vine-good"})
        self.target = "vine-broken"
        with self.assertLogs("vine_framework", level="ERROR"):
            with self.assertRaisesRegex(RuntimeError, "No DAGs were registered"):
                self.run_entry("vine")

    def test_empty_catalog_fails_and_dag_id_does_not_replace_module_metadata(self):
        with self.assertRaisesRegex(RuntimeError, "No pipeline configurations were discovered"):
            self.run_entry("vine")
        self.vine("__file__")
        result = runpy.run_path(str(self.dags / "main_vines.py"))
        self.assertEqual(result["__file__"], str(self.dags / "main_vines.py"))
        self.assertEqual(result["_pipeline_dag___file__"].dag_id, "__file__")

    def test_1029_pipeline_discovery_and_worker_boundary(self):
        # Matches the reported catalog size, not its production definitions.
        # Load the entire catalog in each entrypoint but construct only owned DAGs.
        for index in range(1000):
            self.vine(f"vine-{index:04}")
        for index in range(29):
            self.root(f"root-{index:03}", [{"vine_id": f"vine-{index:04}"}])
        with patch.object(yaml, "load", wraps=yaml.load) as yaml_reads:
            roots, vines, counts = self.run_with_factory_counts(None)
        self.assertEqual(counts, (29, 1000))
        self.assertEqual(len(set(roots) | set(vines)), 1029)
        self.assertEqual(yaml_reads.call_count, 2 * 1029)

        # On task startup Airflow imports the owning file, not both files.
        self.target = "root-017"
        with patch.object(yaml, "load", wraps=yaml.load) as yaml_reads:
            self.assertEqual(set(self.run_entry("root")), {"root-017"})
        self.assertEqual(yaml_reads.call_count, 1029)

    def test_old_main_is_not_a_third_entrypoint(self):
        self.assertFalse((DAGS_DIR / "main.py").exists())

    def test_legacy_parsing_context_import_fallback(self):
        self.vine("vine-a")
        self.vine("vine-b")
        self.target = "vine-b"
        del sys.modules["airflow.sdk"].get_parsing_context
        self._module("airflow.utils.dag_parsing_context").get_parsing_context = self._context
        self.assertEqual(set(self.run_entry("vine")), {"vine-b"})


if __name__ == "__main__":
    unittest.main()
