# SFTP connection order and runtime option updates

These changes are independent of selective DAG parsing and are applied to both
`main` and `feature/26.1020.selective-dag-parsing`. They do not deploy themselves.

## SFTP CSV merge

When `--merge-filename` is set, the runner completes GCS downloads, local CSV
merge and row-count verification **before** opening SSH/SFTP. The connection is
therefore not held idle throughout merge preparation. Data and FIN uploads still
use the same order and upload mode. Source deletion still happens only after
successful transfer, and non-merge transfers retain their existing order.

Temporary merged files are cleaned up even when connection establishment fails.
Established SFTP and SSH clients are closed in nested cleanup blocks so a failure
to close one does not prevent attempting the other. This does not change network
timeouts, implement automatic reconnects or guarantee a transfer cannot fail.
Regression tests cover merge/validation failures, connect/upload/cleanup failures,
non-merge behavior and upload-before-source-deletion ordering.

## BigQuery `durable`

Set a YAML boolean in `grape_defaults.durable`, with an optional per-grape
`options.durable` override. The framework default is `false`.

- Providers whose `BigQueryInsertJobOperator` constructor explicitly supports
  `durable` receive the resolved boolean, including the default `false`.
- Older providers without that parameter receive no extra keyword when the
  option is absent or `false`, preserving import compatibility. A generic
  `**kwargs` parameter alone does not establish support.
- Explicit `true` on an unsupported provider fails with an actionable error,
  rather than silently pretending to enable crash recovery.
- Strings such as `"false"`, numbers and null are rejected; use YAML `false` or
  `true`. Existing retry, `force_rerun` and `deferrable` settings are unchanged.

Current upstream documentation requires Airflow 3.3+ for durable state-store
recovery. The synchronous path can use it; `deferrable=true` takes precedence,
so `durable` has no effect in that mode. Supporting the keyword is not proof that
state-store recovery is available in the deployed runtime. See the
[official BigQuery operator guide](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html#durable-execution).

Check the actual Airflow and `apache-airflow-providers-google` versions and
operator signature in the Composer environment before deployment. A Composer
image label is not an Apache Airflow version. Run a real DAG import and, if using
synchronous durable recovery, a staging retry/recovery exercise. Offline tests
verify forwarding and legacy compatibility, not provider runtime recovery.

## Root and FIN polling defaults

Root `TriggerDagRunOperator` and FIN `GCSObjectExistenceSensor` now default to
120 seconds when `poke_interval_seconds` is absent. Previously the defaults were
30 and 60 seconds respectively.

Explicit configuration is preserved:
- Root: per-Vine `options.poke_interval_seconds` overrides Root `defaults`.
- FIN: per-grape `options.poke_interval_seconds` overrides `grape_defaults`.

This reduces default polling frequency but can increase completion/FIN-detection
latency. An event arriving just after a poll may wait roughly one polling interval
before the next check, subject to the provider's execution mode and scheduling.
It is not an end-to-end latency guarantee. Keep explicit shorter intervals for
latency-sensitive dependencies, and compare triggerer load, detection delay and
end-to-end batch completion in staging. Existing sample YAML with explicit 30/60
second settings continues to use those values; samples are not rewritten.

## Validation and rollback

Run `python -m unittest discover -s tests -v` in either branch. Tests exercise
real framework factories/helpers with offline Airflow/provider/client doubles.
No live SFTP, BigQuery, GCS or Composer changes are made by these tests.

Before release, import all DAGs with the exact target image/providers and check
for import errors. Test representative SFTP merge and non-merge transfers,
BigQuery settings and Root/FIN timing in a non-production environment. Roll back
by restoring the prior coherent code release, or explicitly set the old polling
values and `durable=false` where appropriate. The selective-parsing branch also
has its separate manifest consistency and rollback requirements.
