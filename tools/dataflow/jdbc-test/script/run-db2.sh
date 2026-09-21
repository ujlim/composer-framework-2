#!/bin/bash
set -euo pipefail
echo "Set PROJECT_ID/REGION/HOST_PROJECT_ID/SUBNET/SYSTEM_BUCKET/DF_WORKER_SA and DB2_HOST, DB2_PORT, DB2_DB, DB2_USER, DB2_PASSWORD, DB2_JAR."
echo "Use the same flex-template run pattern as run-vertica.sh with db_type=db2 and jdbc:db2://HOST:PORT/DB."
