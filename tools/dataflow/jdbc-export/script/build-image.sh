#!/bin/bash
set -euo pipefail
PROJECT_ID="${PROJECT_ID:-gcp-prod-edp-sa-udw}"
REGION="${REGION:-asia-northeast3}"
REPOSITORY="${REPOSITORY:-repo-prod-sa-udw-dataflow-tmplt-1}"
IMAGE="${IMAGE:-${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/jdbc-export:v1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/../docker" && pwd)"
gcloud builds submit "${DOCKER_DIR}" --project="${PROJECT_ID}" --tag="${IMAGE}"
echo "IMAGE=${IMAGE}"
