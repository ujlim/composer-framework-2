"""Offline validation and DAG registration isolation regression tests.

Run: python -m unittest discover -s tests -p test_dag_validation.py -v

No GCP services or running Airflow environment are required. Main registration
is exercised with lightweight factory/DAG stubs; real provider compatibility
must still be checked in Composer staging.
"""
from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


REPO = Path(__file__).resolve().parents[1]
DAGS = REPO / "dags"
sys.path.insert(0, str(DAGS))
from framework.config_loader import PipelineCatalogLoader  # noqa: E402


def write_config(base: Path, kind: str, folder: str, dag_id: str, *, raw=None):
    path = base / f"{kind}s" / folder / "config.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    content = raw if raw is not None else f"{kind}:\n  {kind}_id: {dag_id}\n"
    path.write_text(content, encoding="utf-8")
    return path


class CatalogIsolationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)

    def load(self):
        return PipelineCatalogLoader(self.base).load()

    def test_valid_root_and_vine(self):
        write_config(self.base, "vine", "valid_vine", "valid_vine")
        write_config(self.base, "root", "valid_root", "valid_root")
        result = self.load()
        self.assertEqual([c.id for c in result.vines], ["valid_vine"])
        self.assertEqual([c.id for c in result.roots], ["valid_root"])
        self.assertEqual(result.errors, [])

    def test_duplicate_vines_both_rejected_good_survives(self):
        first = write_config(self.base, "vine", "one", "shared")
        second = write_config(self.base, "vine", "two", "shared")
        write_config(self.base, "vine", "good", "good")
        result = self.load()
        self.assertEqual([c.id for c in result.vines], ["good"])
        conflicts = [e for e in result.errors if e.error_code == "DUPLICATE_DAG_ID"]
        self.assertEqual(len(conflicts), 2)
        self.assertEqual({e.config_path for e in conflicts}, {first, second})
        self.assertEqual(
            len([e for e in result.errors if e.error_code == "FOLDER_ID_MISMATCH"]),
            2,
        )

    def test_duplicate_roots_both_rejected(self):
        write_config(self.base, "root", "a", "same")
        write_config(self.base, "root", "b", "same")
        result = self.load()
        self.assertEqual(result.roots, [])
        self.assertEqual(
            len([e for e in result.errors if e.error_code == "DUPLICATE_DAG_ID"]),
            2,
        )

    def test_duplicate_root_and_vine_both_rejected(self):
        root = write_config(self.base, "root", "shared", "shared")
        vine = write_config(self.base, "vine", "shared", "shared")
        write_config(self.base, "vine", "good", "good")
        result = self.load()
        self.assertEqual([c.id for c in result.vines], ["good"])
        self.assertEqual(result.roots, [])
        conflicts = [e for e in result.errors if e.error_code == "DUPLICATE_DAG_ID"]
        self.assertEqual({e.config_path for e in conflicts}, {root, vine})
        for issue in conflicts:
            self.assertEqual(set(issue.related_paths), {root, vine})
            self.assertEqual(issue.dag_id, "shared")

    def test_mismatch_claiming_valid_id_quarantines_both_on_collision(self):
        write_config(self.base, "vine", "shared", "shared")
        write_config(self.base, "vine", "another_folder", "shared")
        result = self.load()
        self.assertEqual(result.vines, [])
        self.assertIn("DUPLICATE_DAG_ID", {x.error_code for x in result.errors})
        self.assertIn("FOLDER_ID_MISMATCH", {x.error_code for x in result.errors})

    def test_folder_id_mismatch_is_case_sensitive(self):
        write_config(self.base, "vine", "Alpha", "alpha")
        write_config(self.base, "root", "folder", "root_id")
        result = self.load()
        self.assertEqual(result.vines, [])
        self.assertEqual(result.roots, [])
        self.assertEqual(
            [x.error_code for x in result.errors],
            ["FOLDER_ID_MISMATCH", "FOLDER_ID_MISMATCH"],
        )

    def test_bad_id_and_missing_top_section_are_isolated(self):
        write_config(self.base, "vine", "bad", "hello world")
        write_config(self.base, "vine", "missing", "", raw="unexpected: value\n")
        write_config(self.base, "vine", "good", "good")
        result = self.load()
        self.assertEqual([c.id for c in result.vines], ["good"])
        self.assertEqual(
            {x.error_code for x in result.errors},
            {"INVALID_DAG_ID", "INVALID_CONFIG_STRUCTURE"},
        )

    def test_leading_or_trailing_id_whitespace_is_invalid(self):
        write_config(self.base, "vine", "a", "", raw=(
            "vine:\n  vine_id: ' a'\n"
        ))
        write_config(self.base, "vine", "good", "good")
        result = self.load()
        self.assertEqual([c.id for c in result.vines], ["good"])
        self.assertEqual([x.error_code for x in result.errors], ["INVALID_DAG_ID"])

    def test_both_sections_and_non_dict_are_rejected(self):
        write_config(
            self.base, "vine", "both", "",
            raw="vine:\n  vine_id: both\nroot:\n  root_id: other\n",
        )
        write_config(self.base, "vine", "list", "", raw="- not-a-mapping\n")
        result = self.load()
        self.assertEqual(result.vines, [])
        self.assertEqual(
            {x.error_code for x in result.errors}, {"INVALID_CONFIG_STRUCTURE"}
        )

    def test_parse_error_and_missing_config_are_isolated(self):
        write_config(self.base, "vine", "broken", "", raw="vine: [unclosed\n")
        (self.base / "vines" / "no_config").mkdir(parents=True)
        write_config(self.base, "vine", "good", "good")
        result = self.load()
        self.assertEqual([c.id for c in result.vines], ["good"])
        self.assertEqual(
            {x.error_code for x in result.errors},
            {"YAML_PARSE_ERROR", "MISSING_CONFIG_FILE"},
        )

    def test_missing_groups_are_empty(self):
        result = self.load()
        self.assertEqual((result.roots, result.vines, result.errors), ([], [], []))


class MainIsolationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dags_dir = Path(self.tmp.name) / "dags"
        self.dags_dir.mkdir()
        self.main = self.dags_dir / "main.py"
        self.main.write_text((DAGS / "main.py").read_text(encoding="utf-8"), encoding="utf-8")

    def write(self, kind, folder, dag_id, *, raw=None):
        return write_config(
            self.dags_dir / "pipelines", kind, folder, dag_id, raw=raw
        )

    def execute(self):
        class FakeDAG:
            def __init__(self, dag_id):
                self.dag_id = dag_id

        class UnregisteredVineError(ValueError):
            def __init__(self, root_id, vine_id):
                self.root_id = root_id
                self.vine_id = vine_id
                super().__init__(f"Root '{root_id}' references unregistered Vine '{vine_id}'")

        class FakeVineFactory:
            def __init__(self, config):
                self.config = config

            def create(self):
                if self.config.data["vine"].get("force_fail"):
                    raise ValueError("synthetic Vine factory error")
                if self.config.data["vine"].get("force_invalid"):
                    return object()
                return FakeDAG(self.config.id)

        class FakeRootFactory:
            def __init__(self, config, available_vine_ids):
                self.config = config
                self.available_vine_ids = available_vine_ids

            def create(self):
                if self.config.data["root"].get("force_fail"):
                    raise ValueError("synthetic Root factory error")
                for item in self.config.data.get("vines", []):
                    if item["vine_id"] not in self.available_vine_ids:
                        raise UnregisteredVineError(self.config.id, item["vine_id"])
                return FakeDAG(self.config.id)

        def package(name):
            module = types.ModuleType(name)
            module.__path__ = []
            return module

        modules = {
            "airflow": package("airflow"),
            "airflow.models": package("airflow.models"),
            "airflow.models.dag": types.ModuleType("airflow.models.dag"),
            "framework.root_factory": types.ModuleType("framework.root_factory"),
            "framework.vine_factory": types.ModuleType("framework.vine_factory"),
        }
        modules["airflow.models.dag"].DAG = FakeDAG
        modules["framework.root_factory"].RootFactory = FakeRootFactory
        modules["framework.root_factory"].UnregisteredVineError = UnregisteredVineError
        modules["framework.vine_factory"].VineFactory = FakeVineFactory
        # The real loader and JSON event logger remain in use.
        scope = {"__file__": str(self.main), "__name__": "_dag_registration_test_"}
        with patch.dict(sys.modules, modules):
            exec(compile(self.main.read_text(encoding="utf-8"), str(self.main), "exec"), scope)
        return scope

    def test_good_dags_continue_when_other_yaml_is_invalid(self):
        self.write("vine", "valid", "valid")
        self.write("vine", "broken", "", raw="vine: [broken\n")
        self.write("root", "root_ok", "root_ok", raw=(
            "root:\n  root_id: root_ok\nvines:\n  - vine_id: valid\n"
        ))
        result = self.execute()
        self.assertEqual(result["registered_dag_ids"], {"root_ok", "valid"})
        self.assertEqual(
            {item["error_code"] for item in result["registration_errors"]},
            {"YAML_PARSE_ERROR"},
        )

    def test_failed_vine_blocks_only_dependent_root(self):
        self.write("vine", "failed", "failed", raw=(
            "vine:\n  vine_id: failed\n  force_fail: true\n"
        ))
        self.write("vine", "good", "good")
        self.write("root", "dependent", "dependent", raw=(
            "root:\n  root_id: dependent\nvines:\n  - vine_id: failed\n"
        ))
        self.write("root", "independent", "independent", raw=(
            "root:\n  root_id: independent\nvines:\n  - vine_id: good\n"
        ))
        result = self.execute()
        self.assertEqual(result["registered_dag_ids"], {"good", "independent"})
        self.assertEqual(
            {item["error_code"] for item in result["registration_errors"]},
            {"DAG_FACTORY_ERROR", "ROOT_VINE_NOT_REGISTERED"},
        )

    def test_invalid_factory_object_is_isolated(self):
        self.write("vine", "bad", "bad", raw=(
            "vine:\n  vine_id: bad\n  force_invalid: true\n"
        ))
        self.write("vine", "good", "good")
        result = self.execute()
        self.assertEqual(result["registered_dag_ids"], {"good"})
        self.assertEqual(
            [e["error_code"] for e in result["registration_errors"]],
            ["INVALID_DAG_OBJECT"],
        )

    def test_root_factory_error_does_not_block_independent_root(self):
        self.write("vine", "good", "good")
        self.write("root", "failed_root", "failed_root", raw=(
            "root:\n  root_id: failed_root\n  force_fail: true\n"
        ))
        self.write("root", "good_root", "good_root", raw=(
            "root:\n  root_id: good_root\nvines:\n  - vine_id: good\n"
        ))
        result = self.execute()
        self.assertEqual(
            result["registered_dag_ids"], {"good", "good_root"}
        )
        self.assertEqual(
            [e["error_code"] for e in result["registration_errors"]],
            ["DAG_FACTORY_ERROR"],
        )

    def test_json_diagnostics_include_error_code_and_yaml_path(self):
        self.write("vine", "wrong", "actual")
        self.write("vine", "good", "good")
        with self.assertLogs("vine_framework.events", level="INFO") as captured:
            result = self.execute()
        self.assertEqual(result["registered_dag_ids"], {"good"})
        self.assertTrue(any(
            '"event": "dag_config_rejected"' in line
            and '"error_code": "FOLDER_ID_MISMATCH"' in line
            and 'wrong/config.yaml' in line
            for line in captured.output
        ))

    def test_global_symbol_collision_does_not_overwrite_logger(self):
        self.write("vine", "LOGGER", "LOGGER")
        self.write("vine", "another", "another")
        result = self.execute()
        self.assertEqual(result["registered_dag_ids"], {"LOGGER", "another"})
        self.assertFalse(hasattr(result["LOGGER"], "dag_id"))

    def test_duplicate_root_and_vine_do_not_hide_good_vine(self):
        self.write("vine", "shared", "shared")
        self.write("root", "shared", "shared")
        self.write("vine", "good", "good")
        result = self.execute()
        self.assertEqual(result["registered_dag_ids"], {"good"})
        self.assertEqual(
            len([e for e in result["registration_errors"]
                 if e["error_code"] == "DUPLICATE_DAG_ID"]), 2,
        )

    def test_all_invalid_configs_trigger_import_error(self):
        self.write("vine", "wrong_folder", "actual_id")
        with self.assertRaisesRegex(RuntimeError, "No DAGs registered"):
            self.execute()

    def test_empty_catalog_triggers_import_error(self):
        (self.dags_dir / "pipelines").mkdir()
        with self.assertRaisesRegex(RuntimeError, "No pipeline configurations"):
            self.execute()


if __name__ == "__main__":
    unittest.main()
