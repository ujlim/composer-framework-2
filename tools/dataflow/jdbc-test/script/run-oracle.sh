#!/bin/bash
set -euo pipefail
echo "Set PROJECT_ID/REGION/HOST_PROJECT_ID/SUBNET/SYSTEM_BUCKET/DF_WORKER_SA and ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, ORACLE_USER, ORACLE_PASSWORD, ORACLE_JAR."
echo "Use the same flex-template run pattern as run-vertica.sh with db_type=oracle and jdbc:oracle:thin:@//HOST:PORT/SERVICE."
