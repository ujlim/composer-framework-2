#!/bin/bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-gcp-prod-edp-sa-udw}"
REGION="${REGION:-asia-northeast3}"
REPOSITORY="${REPOSITORY:-repo-prod-sa-udw-dataflow-tmplt-1}"
IMAGE_TAG="${IMAGE_TAG:-v2}"
BUILD_LOG_BUCKET="${BUILD_LOG_BUCKET:-bucket-prod-sa-udw-cloudbuild-logs}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/jdbc-test:${IMAGE_TAG}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../docker" && pwd)"

echo "IMAGE=${IMAGE}"
cd "${DOCKER_DIR}"

gcloud builds submit . \
  --project="${PROJECT_ID}" \
  --tag="${IMAGE}" \
  --gcs-log-dir="gs://${BUILD_LOG_BUCKET}/logs"
