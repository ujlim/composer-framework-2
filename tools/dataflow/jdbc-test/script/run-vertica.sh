#!/bin/bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-gcp-prod-edp-sa-udw}"
REGION="${REGION:-asia-northeast3}"
HOST_PROJECT_ID="${HOST_PROJECT_ID:-lgulpuls-edp-hub-prod-vpchost}"
SUBNET="${SUBNET:-subnet-prod-hub-int-sa-ubi-dataflow-1}"
SYSTEM_BUCKET="${SYSTEM_BUCKET:-bucket-prod-sa-udw-dataflow-system-1}"
DF_WORKER_SA="${DF_WORKER_SA:-sa-prod-sa-udw-dataflow-worker@${PROJECT_ID}.iam.gserviceaccount.com}"
TEMPLATE_PATH="${TEMPLATE_PATH:-gs://${SYSTEM_BUCKET}/templates/jdbc-test/template.json}"
SUBNET_URL="https://www.googleapis.com/compute/v1/projects/${HOST_PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET}"

VERTICA_HOST="${VERTICA_HOST:?set VERTICA_HOST}"
VERTICA_PORT="${VERTICA_PORT:-5433}"
VERTICA_DB="${VERTICA_DB:?set VERTICA_DB}"
VERTICA_USER="${VERTICA_USER:?set VERTICA_USER}"
VERTICA_PASSWORD="${VERTICA_PASSWORD:?set VERTICA_PASSWORD}"
VERTICA_JAR="${VERTICA_JAR:-gs://${SYSTEM_BUCKET}/drivers/vertica/vertica-jdbc-25.3.0-0.jar}"

if [[ ! "${VERTICA_JAR}" =~ ^gs://[^/]+/.+\.jar$ ]]; then
  echo "ERROR: invalid VERTICA_JAR: ${VERTICA_JAR}" >&2
  echo "Expected: gs://<bucket>/drivers/vertica/<driver>.jar" >&2
  exit 1
fi

echo "Checking Vertica JDBC driver: ${VERTICA_JAR}"
gcloud storage ls "${VERTICA_JAR}" >/dev/null || {
  echo "ERROR: JDBC driver not found or not readable: ${VERTICA_JAR}" >&2
  exit 1
}

JOB_NAME="jdbc-test-vertica-$(date +%Y%m%d-%H%M%S)"

echo "JOB_NAME=${JOB_NAME}"
echo "TEMPLATE_PATH=${TEMPLATE_PATH}"
echo "VERTICA_JAR=${VERTICA_JAR}"

gcloud dataflow flex-template run "${JOB_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --template-file-gcs-location="${TEMPLATE_PATH}" \
  --service-account-email="${DF_WORKER_SA}" \
  --subnetwork="${SUBNET_URL}" \
  --disable-public-ips \
  --staging-location="gs://${SYSTEM_BUCKET}/staging" \
  --temp-location="gs://${SYSTEM_BUCKET}/temp" \
  --parameters db_type=vertica,connection_url="jdbc:vertica://${VERTICA_HOST}:${VERTICA_PORT}/${VERTICA_DB}",username="${VERTICA_USER}",password="${VERTICA_PASSWORD}",driver_jars="${VERTICA_JAR}"

echo "Submitted: ${JOB_NAME}"
