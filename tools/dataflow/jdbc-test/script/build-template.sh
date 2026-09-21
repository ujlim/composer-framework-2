#!/bin/bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-gcp-prod-edp-sa-udw}"
REGION="${REGION:-asia-northeast3}"
REPOSITORY="${REPOSITORY:-repo-prod-sa-udw-dataflow-tmplt-1}"
IMAGE_TAG="${IMAGE_TAG:-v2}"
SYSTEM_BUCKET="${SYSTEM_BUCKET:-bucket-prod-sa-udw-dataflow-system-1}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/jdbc-test:${IMAGE_TAG}"
TEMPLATE_PATH="gs://${SYSTEM_BUCKET}/templates/jdbc/template.json"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../docker" && pwd)"

gcloud dataflow flex-template build "${TEMPLATE_PATH}" \
  --project="${PROJECT_ID}" \
  --image="${IMAGE}" \
  --sdk-language=PYTHON \
  --metadata-file="${DOCKER_DIR}/metadata.json"

echo "TEMPLATE_PATH=${TEMPLATE_PATH}"
