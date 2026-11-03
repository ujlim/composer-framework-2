#!/usr/bin/env bash
# Read-only evidence collection. Successful CLI exit is NOT a health verdict.
set -euo pipefail
if [[ $# -ne 4 ]]; then
  echo 'Usage: bash inspect_composer.sh PROJECT LOCATION ENVIRONMENT OUTPUT_DIRECTORY' >&2
  exit 2
fi
project=$1; location=$2; environment=$3; output=$4
mkdir -p "$output"
gcloud composer environments describe "$environment" --project "$project" --location "$location" \
  --format=json > "$output/environment.json"
gcloud composer environments run "$environment" --project "$project" --location "$location" \
  dags list-import-errors > "$output/import-errors.txt"
gcloud composer environments run "$environment" --project "$project" --location "$location" \
  dags list -- --output=json > "$output/dags.txt"
echo "Evidence saved in $output. Review import errors, DAG IDs, task graphs and worker sync before accept."
