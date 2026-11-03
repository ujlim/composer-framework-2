#!/usr/bin/env python3
"""Plan, publish, accept or recover a Composer DAG candidate.

Cloud mutations require --execute and exact --confirm-prefix. No recursive rsync,
no automatic lock expiry, no DAG execution, no pause/unpause or IAM changes.
"""
from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path
import re
import sys
import time
import subprocess
import uuid

from candidate import canonical, digest, safe_path, verify


class DeploymentBusy(ValueError):
    pass


class StaleBaseline(ValueError):
    pass


class GCSStore:
    """Small generation-guarded adapter. Import cloud dependencies only on use."""
    def __init__(self, bucket):
        from google.cloud import storage
        self.bucket = storage.Client().bucket(bucket)

    def read(self, name, generation=None):
        blob = self.bucket.blob(name, generation=generation)
        from google.api_core.exceptions import NotFound
        try:
            blob.reload()
        except NotFound:
            if generation is not None:
                raise
            return None
        generation = int(blob.generation)
        data = blob.download_as_bytes(if_generation_match=generation, raw_download=True)
        properties = {key: getattr(blob, key) for key in ("content_type", "content_encoding", "cache_control", "content_disposition", "content_language")}
        return {"generation": generation, "data": data, "metadata": blob.metadata or {}, "properties": properties}

    def list(self, prefix):
        return {blob.name: self.read(blob.name, int(blob.generation))
                for blob in self.bucket.client.list_blobs(self.bucket, prefix=prefix)}

    def write(self, name, data, generation, metadata=None, properties=None):
        blob = self.bucket.blob(name)
        blob.metadata = metadata or {}
        properties = properties or {}
        for key in ("content_encoding", "cache_control", "content_disposition", "content_language"):
            setattr(blob, key, properties.get(key))
        blob.upload_from_string(data, if_generation_match=generation,
                                content_type=properties.get("content_type") or "application/octet-stream")
        return int(blob.generation)

    def delete(self, name, generation):
        self.bucket.blob(name).delete(if_generation_match=generation)


def parse_uri(uri, dags=False):
    match = re.fullmatch(r"gs://([a-z0-9][a-z0-9._-]+)/([^?#]+)", uri)
    if not match:
        raise ValueError("Use gs://bucket/nonempty-prefix without a trailing slash")
    bucket, prefix = match.groups()
    safe_path(prefix)
    if dags and prefix != "dags":
        raise ValueError("This implementation owns exactly the environment's dags/ prefix")
    return bucket, prefix + "/"


def hashes(objects):
    return {name: digest(obj["data"]) for name, obj in sorted(objects.items())}


def encode(objects):
    return {name: {"generation": obj["generation"], "metadata": obj["metadata"], "properties": obj.get("properties", {}),
                   "data": base64.b64encode(obj["data"]).decode(), "sha256": digest(obj["data"])}
            for name, obj in objects.items()}


def decode(objects):
    result = {}
    for name, obj in objects.items():
        safe_path(name)
        data = base64.b64decode(obj["data"], validate=True)
        if digest(data) != obj["sha256"]:
            raise ValueError("Operation backup content mismatch")
        result[name] = {"generation": obj["generation"], "metadata": obj["metadata"], "properties": obj.get("properties", {}), "data": data}
    return result


class Publisher:
    def __init__(self, live, control, target, control_prefix):
        self.live, self.control, self.target = live, control, target
        self.prefix = parse_uri(target, dags=True)[1]
        self.control_prefix = control_prefix
        self.lock_key = control_prefix + "lock.json"
        self.state_key = control_prefix + "state.json"

    def snapshot(self):
        result = {}
        for name, obj in self.live.list(self.prefix).items():
            relative = name[len(self.prefix):]
            safe_path(relative)
            result[relative] = obj
        return result

    def state(self):
        obj = self.control.read(self.state_key)
        if not obj:
            return None, 0
        value = json.loads(obj["data"])
        if value["target"] != self.target:
            raise ValueError("Control state belongs to another Composer bucket")
        return value, obj["generation"]

    def acquire(self, operation):
        try:
            return self.control.write(self.lock_key, canonical({"target": self.target, "operation": operation}), 0)
        except Exception:
            lock = self.control.read(self.lock_key)
            if lock and json.loads(lock["data"]).get("operation") != operation:
                raise DeploymentBusy("Generation precondition: deployment lock busy") from None
            # If our lock exists, the create may have succeeded with a lost
            # response. Do not steal/delete it or pretend no operation happened.
            raise

    def lock(self, operation):
        if not re.fullmatch(r"[0-9a-f]{32}|init-[0-9a-f]{32}", operation):
            raise ValueError("Invalid operation ID")
        lock = self.control.read(self.lock_key)
        if not lock or json.loads(lock["data"]) != {"target": self.target, "operation": operation}:
            raise ValueError("Lock is missing or belongs to another operation/target")
        return lock["generation"]

    def unlock(self, operation):
        self.control.delete(self.lock_key, self.lock(operation))

    def plan(self, manifest, expected_release):
        state, generation = self.state()
        if not state:
            raise ValueError("Initialize a reviewed live inventory first (init)")
        if state["release"] != expected_release:
            raise StaleBaseline("Accepted release changed; re-plan against the current release")
        current = self.snapshot()
        if hashes(current) != state["files"]:
            raise ValueError("Live DAG inventory/content drifted from accepted state; refusing overwrite")
        files = manifest["files"]
        return {"target": self.target, "before_release": state["release"], "release": manifest["release"],
                "add": sorted(set(files) - set(current)), "delete": sorted(set(current) - set(files)),
                "update": sorted(name for name in files.keys() & current.keys()
                                 if files[name] != digest(current[name]["data"])),
                "state_generation": generation}

    def initialize(self, expected_inventory):
        operation = "init-" + uuid.uuid4().hex
        self.acquire(operation)
        try:
            if self.state()[0] is not None:
                raise ValueError("State already exists")
            files = hashes(self.snapshot())
            actual = digest(canonical(files))
            if actual != expected_inventory:
                raise ValueError(f"Inventory changed; current digest={actual}")
            state = {"schema": 1, "target": self.target, "release": "bootstrap-" + actual, "files": files}
            self.control.write(self.state_key, canonical(state), 0)
            return state
        finally:
            # init never changes live DAGs. A failed state write can be inspected
            # with inventory; no operation archive is needed for bootstrap.
            self.unlock(operation)

    def apply(self, root, expected_release, finalize_storage=False, preflight=None):
        manifest = verify(root)
        validation = json.loads((root / "validation.json").read_text())
        if validation.get("release") != manifest["release"] or validation.get("full_parse") is not True:
            raise ValueError("Missing/stale full Airflow validation")
        image = validation.get("image", "")
        if not re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", image):
            raise ValueError("Validation image must be pinned by sha256 digest")
        operation = uuid.uuid4().hex
        print(f"OPERATION={operation}", flush=True)
        self.acquire(operation)
        # Plan only AFTER acquiring the lock, so another compliant writer's
        # partial upload cannot be mistaken for out-of-band inventory drift.
        try:
            plan = self.plan(manifest, expected_release)
            before = self.snapshot()
            state, state_generation = self.state()
            if state_generation != plan["state_generation"] or hashes(before) != state["files"]:
                raise StaleBaseline("Live/state changed during locked planning")
            if preflight is not None:
                preflight()  # Branch freshness while holding the environment lock.
        except Exception:
            # This entire block is read-only: no archive or live write was even
            # attempted. Safe to release our exact generation and retry a stale
            # baseline. Acquisition timeout and later failures retain the lock.
            self.unlock(operation)
            raise
        # After this point retain the lock on failure. Never guess that a
        # timed-out archive/upload failed; recover/accept inspect actual state.
        after = {name: {"generation": 0, "metadata": {}, "data": (root / "dags" / name).read_bytes()}
                 for name in manifest["files"]}
        if hashes(after) != manifest["files"]:
            raise ValueError("Candidate changed immediately before publish")
        record = {"schema": 1, "target": self.target, "operation": operation, "plan": plan,
                  "before": encode(before), "after": encode(after), "manifest": manifest, "validation": validation}
        key = self.control_prefix + "operations/" + operation + ".json"
        self.control.write(key, canonical(record), 0)
        # Read back the durable archive before touching live objects.
        stored = json.loads(self.control.read(key)["data"])
        if stored != record:
            raise ValueError("Operation archive verification failed")
        if self.snapshot() != before:
            raise ValueError("Live generations changed during backup")
        self.lock(operation)
        for name in sorted(after, key=lambda item: (item == "main.py", item)):
            if name in before and before[name]["data"] == after[name]["data"]:
                continue
            self.live.write(self.prefix + name, after[name]["data"], before.get(name, {}).get("generation", 0),
                            {"composer-deploy-operation": operation})
        for name in sorted(before.keys() - after.keys()):
            self.live.delete(self.prefix + name, before[name]["generation"])
        if hashes(self.snapshot()) != manifest["files"]:
            raise ValueError("Post-write inventory verification failed; lock retained")
        if finalize_storage:
            self.accept(operation)
            return {"operation": operation, "release": manifest["release"], "status": "published-storage-verified",
                    "composer_health": "not-verified"}
        return {"operation": operation, "release": manifest["release"], "status": "awaiting-finalization"}

    def auto_apply(self, root, branch, commit, repository, wait_seconds=900, poll_seconds=10):
        if branch not in {"dev", "prod"} or not re.fullmatch(r"[0-9a-f]{40}", commit):
            raise ValueError("auto-apply requires branch dev/prod and exact commit SHA")
        if wait_seconds < 0 or poll_seconds <= 0:
            raise ValueError("Invalid lock wait settings")
        def assert_current_branch():
            remote = subprocess.check_output(["git", "-C", str(repository), "ls-remote", "origin", "refs/heads/" + branch],
                                             timeout=60).decode().split()
            if len(remote) != 2 or remote[0] != commit or remote[1] != "refs/heads/" + branch:
                raise ValueError("Branch advanced or cannot be verified; rebuild the latest commit")

        deadline = time.monotonic() + wait_seconds
        while True:
            assert_current_branch()
            if not self.control.read(self.lock_key):
                state, _ = self.state()
                if not state:
                    raise ValueError("Initialize the reviewed live inventory first")
                try:
                    return self.apply(root, state["release"], finalize_storage=True, preflight=assert_current_branch)
                except (DeploymentBusy, StaleBaseline):
                    pass  # Known competing writer, with no live mutation by us.
            if time.monotonic() >= deadline:
                raise TimeoutError("Environment lock wait expired. Inspect prior build; never steal its lock")
            time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))

    def operation(self, operation):
        if not re.fullmatch(r"[0-9a-f]{32}", operation):
            raise ValueError("Invalid operation ID")
        self.lock(operation)
        obj = self.control.read(self.control_prefix + "operations/" + operation + ".json")
        if not obj:
            raise ValueError("No durable operation archive. Verify build is stopped and live state unchanged; see lock recovery guide")
        record = json.loads(obj["data"])
        if record["target"] != self.target or record["operation"] != operation:
            raise ValueError("Operation target mismatch")
        return record

    def accept(self, operation):
        record = self.operation(operation)
        state, generation = self.state()
        if hashes(self.snapshot()) != record["manifest"]["files"]:
            raise ValueError("Published objects differ from candidate")
        expected = {"schema": 1, "target": self.target, "release": record["manifest"]["release"],
                    "files": record["manifest"]["files"], "operation": operation}
        if state != expected:
            if generation != record["plan"]["state_generation"]:
                raise ValueError("Accepted release changed; do not overwrite state")
            self.control.write(self.state_key, canonical(expected), generation)
        # Idempotent if the state write succeeded but its response/unlock failed.
        self.unlock(operation)
        return expected

    def recover(self, operation):
        record = self.operation(operation)
        state, generation = self.state()
        if generation != record["plan"]["state_generation"]:
            raise ValueError("Release was accepted or state changed. Use a NEW reviewed rollback deployment")
        before, after, current = decode(record["before"]), decode(record["after"]), self.snapshot()
        if hashes(before) != state["files"] or hashes(after) != record["manifest"]["files"]:
            raise ValueError("Archive does not match accepted/candidate manifest")
        if set(current) - (set(before) | set(after)):
            raise ValueError("Unknown live paths; recovery blocked before mutations")
        # Preflight ALL files before writing any. Original generation or bytes
        # stamped by this operation only. Unknown concurrent edits are never lost.
        for name in before.keys() | after.keys():
            obj, old, new = current.get(name), before.get(name), after.get(name)
            if obj is None:
                if old is not None and new is not None:
                    raise ValueError(f"Unexpected missing object: {name}")
                continue
            if old and obj["generation"] == old["generation"] and obj["data"] == old["data"]:
                continue
            known_bytes = [entry["data"] for entry in (old, new) if entry]
            if obj["metadata"].get("composer-deploy-operation") != operation or obj["data"] not in known_bytes:
                raise ValueError(f"Unknown live edit blocks recovery: {name}")
        for name in sorted(before, key=lambda item: (item == "main.py", item)):
            obj = current.get(name)
            if obj and obj["data"] == before[name]["data"]:
                continue
            self.live.write(self.prefix + name, before[name]["data"], obj["generation"] if obj else 0,
                            {**before[name]["metadata"], "composer-deploy-operation": operation}, before[name].get("properties"))
        for name in sorted(current.keys() - before.keys()):
            self.live.delete(self.prefix + name, current[name]["generation"])
        if hashes(self.snapshot()) != state["files"]:
            raise ValueError("Recovery verification failed; lock retained")
        # Caller still verifies Composer propagation; keep lock until explicit
        # release-recovered, so no new publisher races this verification.
        return {"operation": operation, "status": "recovered-awaiting-composer-verification"}

    def release_unstarted(self, operation):
        self.lock(operation)
        if self.control.read(self.control_prefix + "operations/" + operation + ".json"):
            raise ValueError("Durable operation exists; use recover or accept")
        state, _ = self.state()
        if not state or hashes(self.snapshot()) != state["files"]:
            raise ValueError("Live state changed; manual investigation required")
        self.unlock(operation)
        return {"operation": operation, "status": "unstarted-lock-released"}

    def release_recovered(self, operation):
        record = self.operation(operation)
        state, generation = self.state()
        if generation != record["plan"]["state_generation"] or hashes(self.snapshot()) != state["files"]:
            raise ValueError("Recovery is not complete")
        self.unlock(operation)
        return {"operation": operation, "status": "recovered"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["inventory", "init", "plan", "apply", "auto-apply", "accept", "recover", "release-recovered", "release-unstarted"])
    parser.add_argument("--target", required=True, help="Exact config.dagGcsPrefix, gs://BUCKET/dags")
    parser.add_argument("--control", required=True, help="Dedicated external gs://BUCKET/environment-prefix")
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--expected-release")
    parser.add_argument("--expected-inventory")
    parser.add_argument("--operation")
    parser.add_argument("--branch", choices=["dev", "prod"])
    parser.add_argument("--commit")
    parser.add_argument("--git-repository", type=Path, default=Path("."))
    parser.add_argument("--wait-seconds", type=int, default=900)
    parser.add_argument("--finalize-storage", action="store_true", help="Finalize storage-verified release without claiming Composer health")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm-prefix")
    parser.add_argument("--writer-stopped", action="store_true", help="Operator confirmed originating build/process has stopped")
    args = parser.parse_args()
    try:
        bucket, prefix = parse_uri(args.target, dags=True)
        control_bucket, control_prefix = parse_uri(args.control)
        if bucket == control_bucket:
            raise ValueError("Control bucket must be separate from Composer bucket")
        if args.command not in {"inventory", "plan"}:
            if not args.execute or args.confirm_prefix != args.target:
                raise ValueError("Mutation requires --execute --confirm-prefix matching --target exactly")
        if args.command in {"recover", "release-recovered", "release-unstarted"} and not args.writer_stopped:
            raise ValueError("Confirm original writer is stopped with --writer-stopped; never steal a live lock")
        publisher = Publisher(GCSStore(bucket), GCSStore(control_bucket), args.target, control_prefix)
        if args.command == "inventory":
            files = hashes(publisher.snapshot())
            result = {"target": args.target, "files": files, "inventory_sha256": digest(canonical(files)), "state": publisher.state()[0]}
        elif args.command == "init":
            if not args.expected_inventory:
                raise ValueError("init requires reviewed --expected-inventory")
            result = publisher.initialize(args.expected_inventory)
        elif args.command == "auto-apply":
            if args.candidate is None or not args.branch or not args.commit:
                raise ValueError("auto-apply requires candidate, branch and commit")
            result = publisher.auto_apply(args.candidate, args.branch, args.commit, args.git_repository, args.wait_seconds)
        elif args.command in {"plan", "apply"}:
            if args.candidate is None or not args.expected_release:
                raise ValueError("plan/apply requires --candidate and --expected-release")
            result = (publisher.plan(verify(args.candidate), args.expected_release) if args.command == "plan"
                      else publisher.apply(args.candidate, args.expected_release, args.finalize_storage))
        else:
            if not args.operation:
                raise ValueError("Operation ID required")
            result = getattr(publisher, args.command.replace("-", "_"))(args.operation)
        print(json.dumps(result, indent=2))
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
