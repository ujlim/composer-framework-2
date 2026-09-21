#!/bin/bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-gcp-prod-edp-sa-udw}"
REGION="${REGION:-asia-northeast3}"
REPOSITORY="${REPOSITORY:-repo-prod-sa-udw-dataflow-tmplt-1}"
MGNT_SA="${MGNT_SA:-sa-prod-sa-udw-mgnt-user@${PROJECT_ID}.iam.gserviceaccount.com}"
IMAGE_TAG="${IMAGE_TAG:-v2}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/jdbc-test:${IMAGE_TAG}"
DOCKER_CONFIG_DIR="${DOCKER_CONFIG_DIR:-/tmp/docker-mgnt}"

rm -rf "${DOCKER_CONFIG_DIR}"
mkdir -p "${DOCKER_CONFIG_DIR}"
echo '{}' > "${DOCKER_CONFIG_DIR}/config.json"

gcloud auth print-access-token \
  --impersonate-service-account="${MGNT_SA}" \
| docker --config "${DOCKER_CONFIG_DIR}" login \
    -u oauth2accesstoken \
    --password-stdin \
    "${REGION}-docker.pkg.dev"

docker --config "${DOCKER_CONFIG_DIR}" pull "${IMAGE}"
