#!/bin/bash
set -euo pipefail
PROJECT_ID="${PROJECT_ID:-gcp-prod-edp-sa-udw}"
REGION="${REGION:-asia-northeast3}"
SYSTEM_BUCKET="${SYSTEM_BUCKET:-bucket-prod-sa-udw-dataflow-system-1}"
REPOSITORY="${REPOSITORY:-repo-prod-sa-udw-dataflow-tmplt-1}"
IMAGE="${IMAGE:-${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/jdbc-export:v1}"
TEMPLATE_PATH="${TEMPLATE_PATH:-gs://${SYSTEM_BUCKET}/templates/jdbc-export/template.json}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
METADATA="$(cd "${SCRIPT_DIR}/../docker" && pwd)/metadata.json"
gcloud dataflow flex-template build "${TEMPLATE_PATH}" --project="${PROJECT_ID}" --image="${IMAGE}" --sdk-language=PYTHON --metadata-file="${METADATA}"
echo "TEMPLATE_PATH=${TEMPLATE_PATH}"
