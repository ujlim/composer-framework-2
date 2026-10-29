# Optional Dataflow worker machine type and JDBC fetch size

Base: main commit `56d8b9a876967f278853ac672c441c7360f4fd86`, verified 2026-10-29.
Branch: `feature/26.1029.dataflow-machine-fetch-size`.

## Settings and compatibility

| YAML location | Accepted value | Unset / null | Effect |
|---|---|---|---|
| `runtime.dataflow.machine_type` | Non-empty string, surrounding whitespace trimmed | Omit Flex environment.machineType | Worker machine type for JDBC jobs in this Vine |
| `grapes[].source.fetch_size` | Integer 1..32767, booleans and strings rejected | Omit Flex parameter; ReadFromJdbc receives None | Per-source JDBC fetch-size hint |

Example fragment: `docs/examples/dataflow-machine-fetch-size.yaml`.
It is NOT a complete replacement Vine: merge the fields into the existing configuration.
Machine type syntax/region availability/quota are validated by Dataflow, not this framework.
The int16 upper bound follows the pinned Beam 2.75.0 cross-language JDBC schema.

No global high-memory default is introduced. Worker counts, SQL, partitioning,
credentials, retries, and BigQuery FILE_LOADS are unchanged. Machine type affects
worker VMs, not the launcher. Fetch size is not a maximum total memory limit,
a partition count, or a BigQuery commit batch size. It does not guarantee OOM resolution.
FILE_LOADS stages files and submits load jobs; delayed table visibility alone does
not establish that all rows are held in RAM. No streaming-write change is included.
New diagnostics log only the selected machine/fetch settings, not launch bodies or DB credentials.

## Deployment (not executed by this change)

1. Compare the source files against the actual deployed Composer executor and image source.
   This branch is based on main, NOT a verified copy of the production deployment.
   Preserve locally deployed ADC/error diagnostics and other customizations by merging;
   blindly replacing whole files could lose those changes. Existing main code is preserved.
2. Build a new image and a new template path. The existing scripts default to `:v1`
   and a shared template path, so explicitly override BOTH; do not overwrite production.

   ```bash
   export PROJECT_ID='YOUR_PROJECT'
   export REGION='asia-northeast3'
   export REPOSITORY='YOUR_ARTIFACT_REGISTRY_REPOSITORY'
   export SYSTEM_BUCKET='YOUR_SYSTEM_BUCKET'
   export IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/jdbc-export:20261029-machine-fetch"
   export TEMPLATE_PATH="gs://${SYSTEM_BUCKET}/templates/jdbc-export/20261029-machine-fetch/template.json"
   bash tools/dataflow/jdbc-export/script/build-image.sh
   bash tools/dataflow/jdbc-export/script/build-template.sh
   ```

3. Deploy the modified Composer `dags/framework/dataflow_executor.py` after merging
   deployment-specific changes. The image must contain the modified `docker/main.py`;
   rebuild the Flex template with the modified `docker/metadata.json`.
4. Point only a test Vine's `runtime.dataflow.template_gcs_path` at the NEW template.
   Use a separate disposable BigQuery destination. Test machine_type=n2-highmem-2
   and fetch_size=1000, ideally changing one factor at a time against the baseline.
5. Launch a NEW job. Check executor settings log, launcher fetch-size log, and actual
   Dataflow worker machine type. Verify row counts, data types, errors, memory and
   throughput. Keep SQL and workers unchanged for a controlled comparison.
   The log proves configured values, not that a driver honored the hint or that OOM is solved.
6. Rollback future jobs by restoring the previous template path and omitting the two
   settings. Running jobs do not change. Do not indiscriminately redeploy old whole files.

Do not rerun WRITE_APPEND blindly after partial work; use staging/run identifiers,
validation and a deliberate publish/deduplication strategy. WRITE_TRUNCATE in the
example is for an isolated disposable test table only.

## Verification

Run `python -m unittest discover -s tests -v` (offline doubles; no cloud resources).
Added tests cover unset/null compatibility, independent and combined settings,
JDBC-to-GCS and JDBC-to-BigQuery forwarding, invalid inputs including booleans,
1/32767 boundaries, CLI parsing, metadata optionality and unchanged FILE_LOADS.
These tests do not execute Beam Java expansion, Oracle JDBC, real Airflow,
Dataflow service validation or 8-million-row ingestion. Real integration/performance
checks and actual image/template deployment remain required.
