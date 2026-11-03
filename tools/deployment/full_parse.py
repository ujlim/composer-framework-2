#!/usr/bin/env python3
"""Require all framework DAGs and all tasks to build in the target Airflow image.

Run in a fresh process with no production secrets or data-plane permissions.
Does not execute tasks, contact Composer, or use private Airflow context env vars.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import runpy
import sys

from candidate import canonical, validate, verify

sys.dont_write_bytecode = True


def check_result(namespace, expected):
    if namespace.get("target_dag_id") is not None:
        raise ValueError("A targeted parse is not a deployment validation")
    if namespace.get("registration_errors"):
        raise ValueError(f"Factory/catalog errors: {namespace['registration_errors']}")
    actual = namespace.get("registered_dag_ids", set())
    if actual != expected:
        raise ValueError(f"DAG set mismatch: missing={sorted(expected - actual)}, extra={sorted(actual - expected)}")
    if namespace.get("configured_pipeline_count") != len(expected):
        raise ValueError("Runtime config count differs from strict catalog")
    if namespace.get("selected_pipeline_count") != len(expected):
        raise ValueError("Not every pipeline was constructed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--image", required=True, help="Verified Composer-compatible validation image digest")
    args = parser.parse_args()
    root = args.candidate.resolve()
    # A failed rerun must never leave an earlier success report behind.
    report_file = root / "validation.json"
    report_file.unlink(missing_ok=True)
    report = validate(root)
    expected = {entry["dag_id"] for entry in report["entries"]}
    sys.path.insert(0, str(root / "dags"))
    from framework import parsing_context
    # This public project helper is overridden only in this CI process, never in
    # deployed files. Worker context can therefore never reduce validation scope.
    parsing_context.get_target_dag_id = lambda: None
    namespace = runpy.run_path(str(root / "dags" / "main.py"))
    check_result(namespace, expected)
    from airflow.models.dagbag import DagBag
    bag = DagBag(dag_folder=str(root / "dags"), include_examples=False, safe_mode=False)
    if bag.import_errors:
        raise ValueError(f"DagBag import errors: {bag.import_errors}")
    if set(bag.dags) != expected:
        raise ValueError("DagBag differs from validated catalog; unmanaged/hidden DAGs are not supported")
    # DagBag validates cycles on collection; preserve a reviewable task/dependency snapshot.
    graphs = {dag_id: {task.task_id: sorted(task.downstream_task_ids) for task in dag.tasks}
              for dag_id, dag in sorted(bag.dags.items())}
    manifest = verify(root)  # Imports must not have modified deployable bytes.
    result = {"schema": 1, "release": manifest["release"], "image": args.image,
              "full_parse": True, "dag_ids": sorted(expected), "graphs": graphs}
    report_file.write_bytes(canonical(result))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
