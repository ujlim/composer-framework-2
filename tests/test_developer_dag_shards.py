# Author: LIM UI JIN
# Created: 2026-11-25
"""Run the shipped entry files with a real YAML loader and Airflow/factory doubles."""

from collections import Counter
import importlib
from pathlib import Path
import runpy
import shutil
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import yaml


DAGS = Path(__file__).resolve().parents[1] / "dags"
ENTRY_NAMES = ["main_roots.py", *(f"main_vines_{i:02}.py" for i in range(8))]


class Dag:
    def __init__(self, dag_id):
        self.dag_id = dag_id


class DeveloperDagShardTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dags = Path(tmp.name)
        (self.dags / "pipelines").mkdir()
        for name in ENTRY_NAMES:
            shutil.copyfile(DAGS / name, self.dags / name)
        self.target = None
        self.reads = []
        self.builds = []
        self.references = {}
        test = self

        class VineFactory:
            def __init__(self, config):
                self.config = config

            def create(self):
                dag_id = self.config.data["vine"]["vine_id"].strip()
                test.builds.append(dag_id)
                return Dag(dag_id)

        class RootFactory:
            def __init__(self, config, vine_ids):
                self.config = config
                self.vine_ids = vine_ids

            def create(self):
                dag_id = self.config.data["root"]["root_id"].strip()
                references = {v["vine_id"] for v in self.config.data.get("vines", [])}
                if not references <= self.vine_ids:
                    raise ValueError("Missing referenced Vine")
                test.builds.append(dag_id)
                test.references[dag_id] = references
                return Dag(dag_id)

        modules = patch.dict(sys.modules)
        modules.start()
        self.addCleanup(modules.stop)
        for name in list(sys.modules):
            if name == "framework" or name.startswith("framework."):
                del sys.modules[name]
        path = patch.object(sys, "path", [str(DAGS), *sys.path])
        path.start()
        self.addCleanup(path.stop)
        for name, attrs in {
            "airflow": {},
            "airflow.models": {},
            "airflow.models.dag": {"DAG": Dag},
            "airflow.sdk": {"get_parsing_context": lambda: types.SimpleNamespace(dag_id=self.target)},
            "framework.root_factory": {"RootFactory": RootFactory},
            "framework.vine_factory": {"VineFactory": VineFactory},
        }.items():
            mod = types.ModuleType(name)
            mod.__dict__.update(attrs)
            sys.modules[name] = mod
        self.entry = importlib.import_module("framework.dag_entrypoints")
        real_read = self.entry.PipelineCatalogLoader.read

        def tracked(path, kind):
            self.reads.append((kind, path.parent.name))
            return real_read(path, kind)

        reader = patch.object(self.entry.PipelineCatalogLoader, "read", staticmethod(tracked))
        reader.start()
        self.addCleanup(reader.stop)

    def write(self, kind, folder, dag_id, refs=()):
        path = self.dags / "pipelines" / f"{kind}s" / folder / "config.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {kind: {f"{kind}_id": dag_id}}
        if refs:
            data["vines"] = [{"vine_id": name} for name in refs]
        path.write_text(yaml.safe_dump(data), encoding="utf-8")
        return path

    def execute(self, filename):
        ns = runpy.run_path(str(self.dags / filename))
        return {k for k, v in ns.items() if isinstance(v, Dag)}

    def vine_file(self, folder):
        return f"main_vines_{self.entry.vine_shard(folder):02}.py"

    def test_full_discovery_reads_each_vine_twice_total_and_each_root_once(self):
        vines = {f"v{i}" for i in range(1000)}
        roots = {f"r{i}" for i in range(29)}
        for name in vines:
            self.write("vine", name, name)
        for name in roots:
            self.write("root", name, name, ("v0",))
        seen = set()
        for filename in ENTRY_NAMES:
            actual = self.execute(filename)
            self.assertTrue(seen.isdisjoint(actual))
            seen.update(actual)
        self.assertEqual(seen, roots | vines)
        self.assertEqual(len(self.reads), 2029)
        self.assertEqual(Counter(self.builds), Counter(seen))

    def test_targeted_vine_loads_only_its_yaml(self):
        for name in [f"v{i}" for i in range(50)]:
            self.write("vine", name, name)
        self.target = "v17"
        self.assertEqual(self.execute(self.vine_file(self.target)), {self.target})
        self.assertEqual(self.reads, [("vine", self.target)])

    def test_legacy_folder_keeps_id_and_targeted_worker(self):
        legacy, dag_id = "sales_daily", "sample_business_daily_vine"
        self.assertNotEqual(self.vine_file(legacy), self.vine_file(dag_id))
        self.write("vine", legacy, dag_id)
        self.write("root", "old_root_folder", "my_root", (dag_id,))
        self.assertEqual(self.execute("main_roots.py"), {"my_root"})
        self.assertEqual(self.references["my_root"], {dag_id})
        self.target = dag_id
        self.assertEqual(self.execute(self.vine_file(legacy)), {dag_id})
        self.target = "my_root"
        self.assertEqual(self.execute("main_roots.py"), {"my_root"})

    def test_duplicate_ids_do_not_interrupt_other_dags(self):
        self.write("vine", "sales_daily", "same")
        self.write("vine", "sales_monthly", "same")
        self.write("vine", "another", "another")
        self.write("root", "same", "same", ("another",))
        self.assertEqual(self.execute("main_roots.py"), {"same"})
        for folder in ("sales_daily", "sales_monthly", "another"):
            self.assertTrue(self.execute(self.vine_file(folder)))

    def test_empty_shard_and_unknown_worker_target_do_not_build_everything(self):
        self.write("vine", "sales_daily", "sales_daily")
        unused = (self.entry.vine_shard("sales_daily") + 1) % 8
        self.assertEqual(self.execute(f"main_vines_{unused:02}.py"), set())
        self.target = "not_found"
        self.assertEqual(self.execute(self.vine_file("sales_daily")), set())
        self.assertEqual(self.builds, [])

    def test_bad_yaml_does_not_stop_other_shards_or_roots(self):
        bad = self.write("vine", "sales_daily", "sales_daily")
        bad.write_text("vine: [", encoding="utf-8")
        self.write("vine", "sales_monthly", "sales_monthly")
        self.write("root", "root", "root", ("sales_monthly",))
        with self.assertLogs():
            self.assertEqual(self.execute("main_roots.py"), {"root"})
            with self.assertRaisesRegex(RuntimeError, "nonempty Vine shard"):
                self.execute(self.vine_file("sales_daily"))
        self.assertEqual(self.execute(self.vine_file("sales_monthly")), {"sales_monthly"})
