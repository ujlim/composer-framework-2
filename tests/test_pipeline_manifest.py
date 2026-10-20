"""Offline catalog/manifest safety tests (only PyYAML is required)."""
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

DAGS = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS))
from framework.config_loader import PipelineCatalogLoader
from framework.pipeline_manifest import MANIFEST_NAME, build_manifest, write_manifest, validate_manifest
from framework.parsing_context import get_target_dag_id


class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.put("roots/different_folder/config.yaml", "root:\n  root_id: actual_root\nvines:\n  - vine_id: actual_vine\n")
        self.vine = self.put("vines/not_the_id/config.yaml", "vine:\n  vine_id: actual_vine\ngrapes: []\n")
        self.loader = PipelineCatalogLoader(self.base)

    def put(self, relative, text):
        path = self.base / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def change_index(self, change):
        path = self.base / MANIFEST_NAME
        value = json.loads(path.read_text())
        change(value)
        path.write_text(json.dumps(value))

    def test_full_and_target_catalogs(self):
        write_manifest(self.base)
        full = self.loader.load()
        root = self.loader.load("actual_root")
        vine = self.loader.load("actual_vine")
        self.assertEqual((len(full.roots), len(full.vines)), (1, 1))
        self.assertEqual((len(root.roots), len(root.vines)), (1, 0))
        self.assertEqual((len(vine.roots), len(vine.vines)), (0, 1))
        self.assertEqual(root.vine_ids, {"actual_vine"})
        self.assertEqual(root.roots[0].data, full.roots[0].data)
        self.assertEqual(root.total_count, 2)
        self.assertEqual(root.parse_mode, "manifest_target")

    def test_target_reads_only_selected_yaml(self):
        write_manifest(self.base)
        original = Path.read_bytes
        reads = []
        def spy(path):
            reads.append(path)
            return original(path)
        with patch.object(Path, "read_bytes", spy):
            self.loader.load("actual_root")
        self.assertEqual(reads, [self.root])

    def test_large_catalog_reads_one_yaml_without_runtime_writes(self):
        for number in range(734):
            self.put(f"vines/batch_{number}/config.yaml", f"vine: {{vine_id: batch_{number}}}\n")
        write_manifest(self.base)
        original = Path.read_bytes
        reads = []
        def spy(path):
            reads.append(path)
            return original(path)
        with patch.object(Path, "read_bytes", spy), patch.object(
            Path, "write_text", side_effect=AssertionError("Runtime write")
        ):
            catalog = self.loader.load("actual_root")
        self.assertEqual(catalog.total_count, 736)
        self.assertEqual(reads, [self.root])

    def test_missing_manifest_explicit_fallback(self):
        with self.assertLogs("framework.config_loader", level="WARNING") as logs:
            catalog = self.loader.load("actual_root")
        self.assertEqual(catalog.parse_mode, "full_fallback")
        self.assertEqual(len(catalog.vines), 1)
        self.assertIn("missing manifest", logs.output[0])

    def test_disable_switch_bypasses_broken_index(self):
        self.put(MANIFEST_NAME, "not json")
        with patch.dict(os.environ, {"FRAMEWORK_DISABLE_SELECTIVE_PARSING": "1"}):
            self.assertEqual(self.loader.load("actual_root").parse_mode, "full_fallback")

    def test_unknown_target_fails(self):
        write_manifest(self.base)
        with self.assertRaisesRegex(ValueError, "Requested DAG"):
            self.loader.load("no_such_dag")

    def test_selected_stale_digest_fails(self):
        write_manifest(self.base)
        self.root.write_text(self.root.read_text() + "# changed\n")
        with self.assertRaisesRegex(ValueError, "Stale DAG manifest digest"):
            self.loader.load("actual_root")

    def test_full_preflight_detects_nonselected_content_changes(self):
        write_manifest(self.base)
        self.vine.write_text(self.vine.read_text() + "# changed\n")
        self.loader.load("actual_root")  # Intentional immutable-release contract.
        with self.assertRaisesRegex(ValueError, "digest"):
            validate_manifest(self.base)
        with self.assertRaisesRegex(ValueError, "digest"):
            self.loader.load()

    def test_added_deleted_inventory_fails(self):
        write_manifest(self.base)
        new = self.put("vines/new/config.yaml", "vine: {vine_id: new}\n")
        with self.assertRaisesRegex(ValueError, "inventory"):
            self.loader.load("actual_root")
        new.unlink()
        self.vine.unlink()
        with self.assertRaisesRegex(ValueError, "inventory"):
            self.loader.load("actual_root")

    def test_manifest_id_mismatch_fails_even_with_matching_digest(self):
        write_manifest(self.base)
        self.change_index(lambda v: v["entries"][0].update(dag_id="wrong"))
        with self.assertRaisesRegex(ValueError, "ID mismatch"):
            self.loader.load("wrong")

    def test_duplicate_ids_cross_kinds_rejected(self):
        self.vine.write_text("vine: {vine_id: actual_root}\n")
        with self.assertRaisesRegex(ValueError, "Duplicate DAG ID"):
            build_manifest(self.base)
        with self.assertRaisesRegex(ValueError, "Duplicate DAG ID"):
            self.loader.load()

    def test_duplicate_manifest_paths_ids_and_invalid_schema(self):
        for change in (
            lambda v: v["entries"].append(v["entries"][0].copy()),
            lambda v: v.update(schema_version=999),
            lambda v: v["entries"][0].update(sha256="bad"),
            lambda v: v["entries"][0].update(path="../escape/config.yaml"),
            lambda v: v["entries"][0].update(path="roots/../config.yaml"),
            lambda v: v["entries"][0].update(path="roots//different_folder/config.yaml"),
        ):
            with self.subTest(change=change):
                write_manifest(self.base)
                self.change_index(change)
                with self.assertRaises(ValueError):
                    self.loader.load("actual_root")

    def test_invalid_yaml_and_ambiguous_sections_cannot_build(self):
        for text in ("[", "root: {root_id: x}\nvine: {vine_id: y}", "root: {root_id: ' x '}"):
            self.root.write_text(text)
            with self.assertRaises(Exception):
                build_manifest(self.base)

    def test_outside_symlink_rejected(self):
        write_manifest(self.base)
        with tempfile.TemporaryDirectory() as other:
            outside = Path(other) / "config.yaml"
            outside.write_text(self.root.read_text())
            self.root.unlink()
            self.root.symlink_to(outside)
            with self.assertRaisesRegex(ValueError, "escapes release"):
                self.loader.load("actual_root")


class ContextTests(unittest.TestCase):
    def test_airflow_3_and_2_public_imports(self):
        for module_name in ("airflow.sdk", "airflow.utils.dag_parsing_context"):
            modules = {name: types.ModuleType(name) for name in (
                "airflow", "airflow.sdk", "airflow.utils", "airflow.utils.dag_parsing_context")}
            modules[module_name].get_parsing_context = lambda: types.SimpleNamespace(dag_id="target", task_id="one")
            with patch.dict(sys.modules, modules):
                self.assertEqual(get_target_dag_id(), "target")

    def test_unsupported_context_fallback_and_runtime_error(self):
        modules = {name: types.ModuleType(name) for name in (
            "airflow", "airflow.sdk", "airflow.utils", "airflow.utils.dag_parsing_context")}
        with patch.dict(sys.modules, modules), self.assertLogs("framework.parsing_context", level="WARNING"):
            self.assertIsNone(get_target_dag_id())
        def fail():
            raise RuntimeError("context failure")
        modules["airflow.sdk"].get_parsing_context = fail
        with patch.dict(sys.modules, modules), self.assertRaisesRegex(RuntimeError, "context failure"):
            get_target_dag_id()


if __name__ == "__main__":
    unittest.main()
