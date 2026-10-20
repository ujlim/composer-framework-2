# Selective worker DAG parsing

## What changes

`main.py` uses Airflow's public parsing context. A DAG processor (`dag_id=None`)
continues to build **all** DAGs. A worker requesting a DAG builds that **entire**
Root or Vine with the same factories, tasks, dependencies, parameters, retries,
pools, trigger targets and callbacks. `task_id` never prunes tasks, including
retry and deferred-task resume imports. DAG objects are exposed under generated
module-global keys, so IDs such as `TARGET_DAG_ID` cannot overwrite parser state;
the DAG IDs presented to Airflow do not change.

A deployment-built `pipelines/.dag-manifest.json` indexes the IDs **inside YAML**,
not directory names. For example, `roots/minimal_daily/config.yaml` maps to
`minimal_daily_root`. A targeted parse reads one indexed YAML, not all YAMLs, and
never invokes factories or reads SQL for other DAGs. Root validation still uses
the complete catalog of configured Vine IDs, not only the one selected DAG and
not a newly inferred set of successfully built DAGs.

The index is read once; its schema, IDs, paths, duplicate IDs across both kinds,
and configuration-file inventory are validated. The selected YAML's SHA-256,
section and ID must agree with the index. No runtime manifest writes, cloud API
calls, database queries or deployment downloads are introduced. Inventory
listing still scales with the number of configuration files; the feature does
not claim constant-time total imports or remove Python/provider import costs.

## Airflow compatibility

The public API is imported from `airflow.sdk` on Airflow 3 and from
`airflow.utils.dag_parsing_context` on Airflow 2.4+. If neither exists, a warning
is logged and full parsing is used. Runtime errors from an available API are
not swallowed. Composer image version and Apache Airflow version are different;
verify the actual Airflow and provider versions in the target Composer image.
This repository's existing provider imports must also be supported there.

## Build and validation

Requirements for the index tool: Python and PyYAML. It does not import Airflow.
From the repository root, on a **staged copy of the complete release**:

```sh
python tools/dag-parsing/build_manifest.py STAGED_RELEASE/dags/pipelines
python tools/dag-parsing/build_manifest.py STAGED_RELEASE/dags/pipelines --check
python -m unittest discover -s tests -v
```

The builder is strict about YAML shape/IDs and duplicates; unlike legacy discovery
it will not silently skip invalid YAML. `--check` verifies every config digest.
Neither command validates operator/provider behavior, SQL semantics or all
factory/dependency rules. Before promotion, run a full `DagBag` import using the
actual Composer-equivalent Airflow/provider environment with no target context.
Require zero import errors, inspect `registration_errors` (partial factory
failures are intentionally isolated), and compare expected DAG IDs/counts,
tasks, edges, params, pools/retries and trigger targets against the current
release. A root pointing to an unknown vine and missing/cyclic dependencies must
still fail existing factory validation. The full manifest path validates all
YAML hashes; it does not skip invalid entries to conceal an inconsistent release.

Generate the index **after** the final YAML changes, and package the framework,
entry point, all YAML, SQL and other pipeline assets together. Do not edit an
indexed release in place. Keep the previous complete release for rollback.

## Deployment consistency is mandatory

A worker deliberately does **not** hash non-target YAML or SQL. It therefore
cannot detect a content-only change to another DAG's YAML (or a change to SQL)
in a live directory. Full preflight checking and full DAG-processor parsing
validate the complete YAML inventory; selected workers are not a substitute.
The manifest is an integrity/selection index, not a cryptographic trust boundary.

Deploy an immutable versioned release with a platform-supported atomic promotion
mechanism, so a parser's resolved entry-point directory and its framework/assets
refer to the same release. Resolve any Python path/bundle configuration to that
same release. Test that mechanism on your actual Composer version before use.
This change does not install or assume an atomic Composer deployment mechanism.
In particular, `gsutil rsync`, copying files one at a time, and uploading the
manifest last are **not atomic** and are not a safe live rollout of the fast path.
If your deployment cannot provide a coherent immutable snapshot, keep the
manifest absent (or set the disable switch) and use the fallback until you have a
maintenance/quiescence process that prevents parsing during mixed-file updates.
Pausing scheduled DAGs alone is insufficient: existing workers, retries,
triggerer resumes and the DAG processor may still parse. Drain or stop those
consumers through your supported operational procedure before replacing files.

Never enable this path solely to meet a clock-time SLA without validating both
correctness and the deployment process.

## Failure behavior and rollback

- No manifest: warning plus **full YAML catalog load**, with only the requested
  factory invoked. This is backward-compatible fallback, not the YAML fast path.
- `FRAMEWORK_DISABLE_SELECTIVE_PARSING=1`: ignore the manifest and use the same
  full-catalog fallback, including if a malformed manifest needs bypassing.
  Set it in all relevant parser/worker environments through normal deployment.
- Valid manifest, full context: validate/load all entries and build all DAGs.
- Unknown target, stale selected hash/ID, added/deleted config, invalid manifest
  schema/path or duplicates: fail clearly; do not silently use the wrong DAG.
- Selected factory/dependency error: report it and fail the target import.
- Full parse: existing factory-level isolation remains; partial failures are
  logged, and zero built DAGs still produces an import error.

For rollback, atomically restore the previous complete release or use a
quiesced deployment process. The disable switch/removing the manifest restores
full YAML loading but is **not** a remedy for incoherent YAML/SQL/code deployment.
No factory behavior, user YAML schema, retry semantics or task execution logic
is changed by this feature.

## Measurement and acceptance

The `dag_registration_completed` log includes `parse_mode`, `target_dag_id`,
`selected_dag_count`, `total_pipeline_count` and `parse_elapsed_ms`, also printed
in the message for log formatters that omit extra fields. Modes are
`manifest_target`, `manifest_full`, `full_fallback`, and `full`. Elapsed time
starts before Airflow/framework imports in the entry point but excludes process
startup and code executed before entering this file. A failed catalog load emits
`pipeline_catalog_load_failed`; a failed factory emits `dag_registration_failed`.
Do not treat a fast failure as a performance improvement.

In staging, measure full DAG-processor imports and targeted Root/Vine imports
separately with cold and warm workers, retries and deferred resumes. Collect
p50/p95/max parse latency, YAML/SQL reads, import-error count, DAG/task/edge
parity, and end-to-end task startup/execution times using the same image,
resources and representative configuration inventory. Offline tests assert the
I/O boundary and DAG shape but are **not a production performance benchmark**.
No reduction from a reported 449-second worker average or assurance of an
08:00 SLA can be made until measured under the actual workload.

Public API references: [Airflow dynamic DAG generation](https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html)
and [Airflow 2.4 parsing context](https://airflow.apache.org/docs/apache-airflow/2.4.2/_api/airflow/utils/dag_parsing_context/index.html).
