# Optional Dataflow worker machine type (2026-10-30)

Only `runtime.dataflow.machine_type` support is selected from the earlier machine/fetch proposal.
This change does not modify JDBC reads, fetch size, BigQuery FILE_LOADS, worker counts,
launch credentials or error/progress diagnostics.

Merge this field into the selected existing Vine; do not replace the whole configuration:

```yaml
runtime:
  dataflow:
    # Preserve existing project/region/template/network/service-account settings.
    machine_type: n2-highmem-2
```

- Optional non-empty string; surrounding whitespace is trimmed. Non-string values
  including booleans, and empty/whitespace-only strings, are rejected.
- Missing or YAML null omits `launchParameter.environment.machineType` entirely,
  retaining the service's prior default. No global high-memory default is forced.
- Applies to new JDBC jobs in this Vine. Not a `grapes[].options` setting.
- Worker machine type only, not launcher machine type or worker count. Existing jobs
  do not change. Machine availability, quota and service validation remain required.
- A higher-memory machine may help, but does not establish the OOM cause or guarantee
  successful ingestion. Keep SQL and other settings unchanged for comparison; use a
  separate test destination and verify actual workers in the newly launched job.

## Deployment / rollback

Compare the actual deployed Composer executor with this source and preserve any local
customizations before applying. This launch-environment-only change does not itself
require rebuilding the JDBC image or template. Other pending launcher changes may
have their own image/template requirements. No GCP deployment is performed here.
Remove/null the YAML field to restore the previous default for future jobs; do not
replace unrelated production code with older complete files.

## Fetch feature withdrawn

As requested on 2026-10-30, the proposed YAML `source.fetch_size` feature is withdrawn.
The historical branch `feature/26.1029.dataflow-machine-fetch-size` and its HTML/source
package are obsolete for deployment: **do not merge or deploy that branch wholesale**.
Its history is retained, not deleted or rewritten. Only the optional machine-type
change is selected for main and the progress-logs branch. This note does not claim
which JDBC fetch default is effective in an uninspected deployment.

## Verification

`python -m unittest discover -s tests -v`

Offline tests verify unset/null compatibility, exact request delta, validation,
per-Vine isolation, GCS/BigQuery forwarding and existing environment preservation.
Real Airflow/Dataflow validation, image behavior and OOM performance testing are separate.
