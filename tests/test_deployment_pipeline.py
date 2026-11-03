"""Offline tests: no cloud credentials, Airflow installation or network needed."""
from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools" / "deployment"
sys.path.insert(0, str(TOOLS))
import candidate
import full_parse
import publish


class FakeStore:
    def __init__(self):
        self.objects = {}
        self.sequence = 0
        self.mutations = 0
        self.fail_at = None
        self.lost_response_at = None

    def read(self, name, generation=None):
        obj = self.objects.get(name)
        if generation is not None and (obj is None or obj["generation"] != generation):
            raise ValueError("Generation precondition failed")
        return copy.deepcopy(obj)

    def list(self, prefix):
        return {name: self.read(name) for name in sorted(self.objects) if name.startswith(prefix)}

    def mutate(self):
        self.mutations += 1
        if self.mutations == self.fail_at:
            raise OSError("Injected failure before mutation")

    def write(self, name, data, generation, metadata=None, properties=None):
        self.mutate()
        current = self.objects.get(name, {}).get("generation", 0)
        if generation != current:
            raise ValueError("Generation precondition failed")
        self.sequence += 1
        self.objects[name] = {"generation": self.sequence, "data": data, "metadata": metadata or {}, "properties": properties or {}}
        if self.mutations == self.lost_response_at:
            raise OSError("Response lost after successful mutation")
        return self.sequence

    def delete(self, name, generation):
        self.mutate()
        if self.objects[name]["generation"] != generation:
            raise ValueError("Generation precondition failed")
        del self.objects[name]
        if self.mutations == self.lost_response_at:
            raise OSError("Response lost after successful deletion")


def make_candidate(root, files):
    root.mkdir(exist_ok=True)
    for name, data in files.items():
        path = root / "dags" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    manifest = {"schema": 1, "sources": [{"name": "test", "commit": "a" * 40}], "files": candidate.inventory(root / "dags")}
    manifest["release"] = candidate.digest(candidate.canonical(manifest))
    (root / "manifest.json").write_bytes(candidate.canonical(manifest))
    (root / "validation.json").write_bytes(candidate.canonical({"release": manifest["release"], "full_parse": True,
                                                              "image": "test@sha256:" + "a" * 64}))
    return manifest


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "candidate"
        self.live, self.control = FakeStore(), FakeStore()
        self.p = publish.Publisher(self.live, self.control, "gs://composer-bucket/dags", "env/")
        self.before = {"main.py": b"old-main", "framework/module.py": b"old-module", "pipelines/removed.txt": b"removed"}
        for name, data in self.before.items():
            self.live.write("dags/" + name, data, 0)
        # Unrelated environment bucket content must never be read/changed.
        self.live.write("plugins/unrelated.py", b"unrelated", 0)
        files = publish.hashes(self.p.snapshot())
        self.state = self.p.initialize(candidate.digest(candidate.canonical(files)))
        self.manifest = make_candidate(self.root, {"main.py": b"new-main", "framework/module.py": b"new-module", "pipelines/new.txt": b"new"})
        self.live.mutations = self.control.mutations = 0

    def operation_id(self):
        return json.loads(self.control.read("env/lock.json")["data"])["operation"]

    def assert_before(self):
        self.assertEqual({name: obj["data"] for name, obj in self.p.snapshot().items()}, self.before)
        self.assertEqual(self.live.read("plugins/unrelated.py")["data"], b"unrelated")

    def test_auto_apply_finalizes_and_next_push_can_publish(self):
        with patch.object(publish.subprocess, "check_output", return_value=("a" * 40 + "\trefs/heads/dev\n").encode()):
            result = self.p.auto_apply(self.root, "dev", "a" * 40, Path("."), wait_seconds=0)
            self.assertEqual(result["status"], "published-storage-verified")
            self.assertEqual(result["composer_health"], "not-verified")
            self.assertIsNone(self.control.read("env/lock.json"))
            self.p.auto_apply(self.root, "dev", "a" * 40, Path("."), wait_seconds=0)
            self.assertIsNone(self.control.read("env/lock.json"))

    def test_auto_apply_rejects_outdated_branch(self):
        with patch.object(publish.subprocess, "check_output", return_value=("b" * 40 + "\trefs/heads/prod\n").encode()):
            with self.assertRaisesRegex(ValueError, "Branch advanced"):
                self.p.auto_apply(self.root, "prod", "a" * 40, Path("."), wait_seconds=0)
        self.assertEqual(self.live.mutations, 0)

    def test_auto_apply_waits_for_lock_then_refreshes_state(self):
        op = self.p.apply(self.root, self.state["release"])["operation"]
        with patch.object(publish.subprocess, "check_output", return_value=("a" * 40 + "\trefs/heads/dev\n").encode()), \
             patch.object(publish.time, "sleep", side_effect=lambda _: self.p.accept(op)):
            self.p.auto_apply(self.root, "dev", "a" * 40, Path("."), wait_seconds=30)
        self.assertIsNone(self.control.read("env/lock.json"))

    def test_branch_advancing_during_acquire_blocks_old_candidate(self):
        original_acquire = self.p.acquire
        latest = "a" * 40

        def acquire_then_advance(operation):
            nonlocal latest
            result = original_acquire(operation)
            latest = "b" * 40
            return result

        with patch.object(self.p, "acquire", side_effect=acquire_then_advance), \
             patch.object(publish.subprocess, "check_output", side_effect=lambda *a, **k: (latest + "\trefs/heads/dev\n").encode()):
            with self.assertRaisesRegex(ValueError, "Branch advanced"):
                self.p.auto_apply(self.root, "dev", "a" * 40, Path("."), wait_seconds=30)
        self.assertEqual(self.live.mutations, 0)
        self.assertIsNone(self.control.read("env/lock.json"))

    def test_newer_release_between_tip_check_and_state_read_never_rolls_back(self):
        other = publish.Publisher(self.live, self.control, self.p.target, self.p.control_prefix)
        newer = Path(self.tmp.name) / "newer"
        newer_manifest = make_candidate(newer, {"main.py": b"newer-main", "framework/module.py": b"newer"})
        original_state = self.p.state
        latest, raced = "a" * 40, False

        def state_after_newer_release():
            nonlocal latest, raced
            if not raced:
                raced = True
                other.apply(newer, self.state["release"], finalize_storage=True)
                latest = "b" * 40
            return original_state()

        with patch.object(self.p, "state", side_effect=state_after_newer_release), \
             patch.object(publish.subprocess, "check_output", side_effect=lambda *a, **k: (latest + "\trefs/heads/dev\n").encode()):
            with self.assertRaisesRegex(ValueError, "Branch advanced"):
                self.p.auto_apply(self.root, "dev", "a" * 40, Path("."), wait_seconds=30)
        self.assertEqual(original_state()[0]["release"], newer_manifest["release"])
        self.assertEqual(self.live.read("dags/main.py")["data"], b"newer-main")
        self.assertIsNone(self.control.read("env/lock.json"))

    def test_completed_competitor_between_baseline_and_lock_retries(self):
        other = publish.Publisher(self.live, self.control, self.p.target, self.p.control_prefix)
        original_acquire = self.p.acquire
        raced = False

        def acquire_after_competitor(operation):
            nonlocal raced
            if not raced:
                raced = True
                other.apply(self.root, self.state["release"], finalize_storage=True)
            return original_acquire(operation)

        with patch.object(self.p, "acquire", side_effect=acquire_after_competitor), \
             patch.object(publish.subprocess, "check_output", return_value=("a" * 40 + "\trefs/heads/dev\n").encode()), \
             patch.object(publish.time, "sleep"):
            self.p.auto_apply(self.root, "dev", "a" * 40, Path("."), wait_seconds=30)
        self.assertTrue(raced)
        self.assertIsNone(self.control.read("env/lock.json"))

    def test_stale_baseline_releases_own_pre_mutation_lock(self):
        with self.assertRaises(publish.StaleBaseline):
            self.p.apply(self.root, "stale")
        self.assertIsNone(self.control.read("env/lock.json"))
        self.assertEqual(self.live.mutations, 0)

    def test_auto_apply_never_steals_failed_lock(self):
        self.p.acquire("b" * 32)
        with patch.object(publish.subprocess, "check_output", return_value=("a" * 40 + "\trefs/heads/dev\n").encode()):
            with self.assertRaisesRegex(TimeoutError, "never steal"):
                self.p.auto_apply(self.root, "dev", "a" * 40, Path("."), wait_seconds=0)
        self.assertEqual(self.operation_id(), "b" * 32)
        self.assertEqual(self.live.mutations, 0)

    def test_apply_waits_for_health_acceptance(self):
        result = self.p.apply(self.root, self.state["release"])
        self.assertEqual(self.p.state()[0], self.state)
        self.assertIsNotNone(self.control.read("env/lock.json"))
        self.p.accept(result["operation"])
        self.assertEqual(self.p.state()[0]["release"], self.manifest["release"])
        self.assertIsNone(self.control.read("env/lock.json"))

    def test_raw_archive_preserves_custom_and_content_properties(self):
        obj = self.live.read("dags/main.py")
        self.live.write("dags/main.py", obj["data"], obj["generation"], {"owner": "team"},
                        {"content_type": "text/x-python", "content_encoding": "gzip", "cache_control": "no-cache"})
        op = self.p.apply(self.root, self.state["release"])["operation"]
        self.p.recover(op)
        restored = self.live.read("dags/main.py")
        self.assertEqual(restored["metadata"]["owner"], "team")
        self.assertEqual(restored["properties"]["content_encoding"], "gzip")

    def test_plan_read_only_and_lists_deletions(self):
        plan = self.p.plan(self.manifest, self.state["release"])
        self.assertEqual(plan["delete"], ["pipelines/removed.txt"])
        self.assertEqual(self.live.mutations + self.control.mutations, 0)

    def test_drift_and_untracked_files_block(self):
        for name in ("dags/main.py", "dags/unmanaged.py"):
            with self.subTest(name=name):
                old = self.live.read(name)
                generation = old["generation"] if old else 0
                self.live.write(name, b"unknown", generation)
                with self.assertRaisesRegex(ValueError, "drifted"):
                    self.p.plan(self.manifest, self.state["release"])
                if old:
                    self.live.write(name, old["data"], self.live.read(name)["generation"])
                else:
                    self.live.delete(name, self.live.read(name)["generation"])

    def test_wrong_expected_release_and_target_block(self):
        with self.assertRaisesRegex(ValueError, "Accepted release changed"):
            self.p.plan(self.manifest, "stale")
        other = publish.Publisher(self.live, self.control, "gs://another-bucket/dags", "env/")
        with self.assertRaisesRegex(ValueError, "another Composer"):
            other.state()

    def test_two_publishers_do_not_share_lock(self):
        self.p.acquire("a" * 32)
        with self.assertRaisesRegex(ValueError, "precondition"):
            self.p.apply(self.root, self.state["release"])
        self.assertEqual(self.live.mutations, 0)
        self.assertEqual(self.operation_id(), "a" * 32)

    def test_backup_failure_has_zero_live_writes(self):
        self.control.fail_at = 2  # lock succeeds, durable archive fails
        with self.assertRaises(OSError):
            self.p.apply(self.root, self.state["release"])
        self.assertEqual(self.live.mutations, 0)
        self.assert_before()
        op = self.operation_id()
        self.control.fail_at = None
        self.p.release_unstarted(op)

    def test_fail_before_each_mutation_can_recover(self):
        # 3 writes + 1 stale file deletion. Every prefix state is recoverable.
        for point in range(1, 5):
            with self.subTest(point=point):
                self.live.fail_at = self.live.mutations + point
                with self.assertRaises(OSError):
                    self.p.apply(self.root, self.state["release"])
                op = self.operation_id()
                self.live.fail_at = None
                self.p.recover(op)
                self.assert_before()
                self.p.release_recovered(op)

    def test_lost_response_after_each_mutation_can_recover(self):
        for point in range(1, 5):
            with self.subTest(point=point):
                self.live.lost_response_at = self.live.mutations + point
                with self.assertRaises(OSError):
                    self.p.apply(self.root, self.state["release"])
                op = self.operation_id()
                self.live.lost_response_at = None
                self.p.recover(op)
                self.assert_before()
                self.p.release_recovered(op)

    def test_recovery_interruption_is_resumable(self):
        result = self.p.apply(self.root, self.state["release"])
        self.live.lost_response_at = self.live.mutations + 2
        with self.assertRaises(OSError):
            self.p.recover(result["operation"])
        self.live.lost_response_at = None
        self.p.recover(result["operation"])
        self.assert_before()

    def test_unknown_concurrent_edit_blocks_all_recovery_writes(self):
        op = self.p.apply(self.root, self.state["release"])["operation"]
        obj = self.live.read("dags/main.py")
        self.live.write("dags/main.py", b"foreign", obj["generation"])
        count = self.live.mutations
        with self.assertRaisesRegex(ValueError, "Unknown live edit"):
            self.p.recover(op)
        self.assertEqual(self.live.mutations, count)

    def test_missing_changed_file_blocks_recovery(self):
        op = self.p.apply(self.root, self.state["release"])["operation"]
        self.live.delete("dags/main.py", self.live.read("dags/main.py")["generation"])
        with self.assertRaisesRegex(ValueError, "Unexpected missing"):
            self.p.recover(op)

    def test_archive_corruption_blocks_recovery(self):
        op = self.p.apply(self.root, self.state["release"])["operation"]
        obj = self.control.read("env/operations/" + op + ".json")
        value = json.loads(obj["data"])
        value["before"]["main.py"]["sha256"] = "bad"
        self.control.write("env/operations/" + op + ".json", candidate.canonical(value), obj["generation"])
        with self.assertRaisesRegex(ValueError, "content mismatch"):
            self.p.recover(op)

    def test_accept_lost_response_can_finish_unlock(self):
        op = self.p.apply(self.root, self.state["release"])["operation"]
        self.control.lost_response_at = self.control.mutations + 1
        with self.assertRaises(OSError):
            self.p.accept(op)
        self.control.lost_response_at = None
        self.p.accept(op)
        self.assertIsNone(self.control.read("env/lock.json"))

    def test_accepted_release_requires_new_rollback(self):
        op = self.p.apply(self.root, self.state["release"])["operation"]
        # Simulate accept state write success, before lock release.
        self.control.lost_response_at = self.control.mutations + 1
        with self.assertRaises(OSError):
            self.p.accept(op)
        with self.assertRaisesRegex(ValueError, "accepted or state changed"):
            self.p.recover(op)

    def test_modified_candidate_and_stale_validation_block(self):
        (self.root / "dags/main.py").write_text("tampered")
        with self.assertRaisesRegex(ValueError, "bytes changed"):
            self.p.apply(self.root, self.state["release"])
        self.assertEqual(self.live.mutations, 0)

    def test_bootstrap_requires_exact_reviewed_inventory(self):
        self.control.objects.clear()
        with self.assertRaisesRegex(ValueError, "Inventory changed"):
            self.p.initialize("stale")
        self.assertEqual(self.live.mutations, 0)

    def test_plan_requires_bootstrap(self):
        self.control.objects.clear()
        with self.assertRaisesRegex(ValueError, "Initialize"):
            self.p.plan(self.manifest, "none")


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def repo(self, name, files):
        root = self.root / name
        root.mkdir()
        subprocess.run(["git", "init", "-q", str(root)], check=True)
        for filename, data in files.items():
            path = root / "dags" / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(data)
        subprocess.run(["git", "-C", str(root), "add", "."], check=True)
        subprocess.run(["git", "-C", str(root), "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "test"], check=True)
        sha = candidate.git(root, "rev-parse", "HEAD").decode().strip()
        return {"name": name, "repository": name, "commit": sha, "dags": "dags"}

    def assemble(self, sources):
        lock = self.root / "sources.json"
        lock.write_text(json.dumps({"sources": sources}))
        return candidate.assemble(lock, self.root / "out")

    def test_multiple_repositories_form_one_candidate(self):
        a = self.repo("framework", {"main.py": "pass", "framework/lib.py": "pass"})
        b = self.repo("business", {"pipelines/vines/vine/config.yaml": "vine:\n  vine_id: vine\n"})
        manifest = self.assemble([a, b])
        report = candidate.validate(self.root / "out")
        self.assertEqual(len(manifest["sources"]), 2)
        self.assertEqual(len(report["entries"]), 1)

    def test_same_path_conflict_never_silently_overlays(self):
        a = self.repo("a", {"main.py": "pass", "framework/lib.py": "pass"})
        b = self.repo("b", {"main.py": "pass"})
        with self.assertRaisesRegex(ValueError, "ownership overlap"):
            self.assemble([a, b])

    def test_dirty_worktree_not_in_candidate(self):
        source = self.repo("a", {"main.py": "pass", "framework/lib.py": "pass"})
        (self.root / "a/dags/main.py").write_text("DIRTY")
        self.assemble([source])
        self.assertEqual((self.root / "out/dags/main.py").read_text(), "pass")

    def test_branch_names_not_accepted_as_pins(self):
        source = self.repo("a", {"main.py": "pass"})
        source["commit"] = "main"
        with self.assertRaisesRegex(ValueError, "exact 40-character"):
            self.assemble([source])

    def test_airflowignore_and_unmanaged_entrypoint_rejected(self):
        for index, name in enumerate((".airflowignore", "extra.py")):
            source = self.repo(str(index), {"main.py": "pass", "framework/lib.py": "pass", name: "pass"})
            with self.assertRaises(ValueError):
                self.assemble([source])

    def test_duplicate_id_across_sources_blocks_candidate(self):
        a = self.repo("a", {"main.py": "pass", "framework/lib.py": "pass", "pipelines/vines/a/config.yaml": "vine:\n  vine_id: same\n"})
        b = self.repo("b", {"pipelines/vines/b/config.yaml": "vine:\n  vine_id: same\n"})
        self.assemble([a, b])
        with self.assertRaisesRegex(ValueError, "Duplicate DAG ID"):
            candidate.validate(self.root / "out")

    def test_yaml_duplicate_equivalent_keys_and_hidden_paths_block(self):
        for index, (name, yaml_text) in enumerate([
            ("pipelines/vines/a/config.yaml", "vine:\n  vine_id: a\noptions:\n  1: a\n  true: b\n"),
            ("pipelines/vines/a/config.yaml", "vine:\n  vine_id: a\n  vine_id: b\n"),
            ("pipelines/vines/nested/a/config.yaml", "vine:\n  vine_id: a\n"),
            ("pipelines/vines/a/config.yml", "vine:\n  vine_id: a\n"),
        ]):
            with self.subTest(name=name, yaml_text=yaml_text):
                folder = self.root / str(index)
                make_candidate(folder, {"main.py": b"pass", name: yaml_text.encode()})
                with self.assertRaisesRegex(ValueError, "validation failed"):
                    candidate.validate(folder)

    def test_path_traversal_and_symlinks(self):
        for name in ("../escape", "/absolute", "a//b", "a/./b", "a\\b", "a\nb"):
            with self.assertRaises(ValueError):
                candidate.safe_path(name)
        (self.root / "link").symlink_to("/tmp")
        with self.assertRaisesRegex(ValueError, "Symlink"):
            candidate.inventory(self.root)

    def test_full_parse_cannot_hide_partial_failures(self):
        baseline = {"target_dag_id": None, "registration_errors": [], "registered_dag_ids": {"a", "b"},
                    "selected_pipeline_count": 2, "configured_pipeline_count": 2}
        full_parse.check_result(baseline, {"a", "b"})
        for field, value in (("target_dag_id", "a"), ("registration_errors", ["bad"]),
                             ("registered_dag_ids", {"a"}), ("selected_pipeline_count", 1),
                             ("configured_pipeline_count", 1)):
            with self.subTest(field=field), self.assertRaises(ValueError):
                full_parse.check_result({**baseline, field: value}, {"a", "b"})

    def test_trigger_templates_separate_dev_and_prod(self):
        import yaml
        config = yaml.safe_load((ROOT / ".cloudbuild/triggers.example.yaml").read_text())
        triggers = config["triggers"]
        self.assertEqual({item["includedGitRefs"] for item in triggers}, {"^dev$", "^prod$"})
        self.assertEqual(len({item["serviceAccount"] for item in triggers}), 2)
        self.assertEqual(len({item["substitutions"]["_TARGET"] for item in triggers}), 2)
        for item in triggers:
            self.assertTrue(item["disabled"])
            self.assertEqual(item["substitutions"]["_EXECUTE"], "false")
        build = yaml.safe_load((ROOT / ".cloudbuild/deploy.yaml").read_text())
        self.assertEqual(build["substitutions"]["_EXECUTE"], "false")
        self.assertNotIn("accept", build["steps"][-1]["args"][-1])


if __name__ == "__main__":
    unittest.main()
