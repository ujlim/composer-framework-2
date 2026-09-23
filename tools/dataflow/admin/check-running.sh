#!/bin/bash
set -u

PROJECT_ID="${PROJECT_ID:-gcp-prod-edp-sa-udw}"
REGION="${REGION:-asia-northeast3}"
LIST_COUNT="${LIST_COUNT:-10}"
FETCH_COUNT="${FETCH_COUNT:-100}"
LOG_LIMIT="${LOG_LIMIT:-1000}"

echo
echo "============================================================"
echo " Dataflow Job Monitor"
echo "============================================================"
echo " Project : ${PROJECT_ID}"
echo " Region  : ${REGION}"
echo "============================================================"
echo

mapfile -t JOBS < <(
  gcloud dataflow jobs list \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --status=all \
    --limit="${FETCH_COUNT}" \
    --format="csv[no-heading](JOB_ID,NAME,STATE,CREATION_TIME)" \
  | sort -t',' -k4,4r \
  | head -n "${LIST_COUNT}" \
  | tr ',' '\t'
)

if [ ${#JOBS[@]} -eq 0 ]; then
  echo "ERROR: Dataflow Job이 없습니다."
  exit 1
fi

printf "%-4s %-48s %-22s %-25s\n" "NO" "JOB NAME" "STATE" "CREATE TIME"
for i in "${!JOBS[@]}"; do
  IFS=$'\t' read -r ID NAME STATE CREATE_TIME <<< "${JOBS[$i]}"
  printf "%-4s %-48s %-22s %-25s\n" "$((i+1))" "${NAME}" "${STATE}" "${CREATE_TIME}"
done

echo
read -rp "조회할 Job 번호 [1-${#JOBS[@]}] : " SELECTED

if ! [[ "${SELECTED}" =~ ^[0-9]+$ ]] || (( SELECTED < 1 || SELECTED > ${#JOBS[@]} )); then
  echo "ERROR: 올바른 번호를 입력하십시오."
  exit 1
fi

INDEX=$((SELECTED - 1))
IFS=$'\t' read -r JOB_ID JOB_NAME JOB_STATE CREATE_TIME <<< "${JOBS[$INDEX]}"

echo
echo "JOB_NAME=${JOB_NAME}"
echo "JOB_ID=${JOB_ID}"
echo "STATE=${JOB_STATE}"
echo

echo "===== [1] Job Status ====="
gcloud dataflow jobs describe "${JOB_ID}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --format="yaml(id,name,type,currentState,createTime,startTime,currentStateTime)"

echo
echo "===== [2] Job Messages ====="
gcloud logging read \
  "resource.type=\"dataflow_step\" AND resource.labels.job_id=\"${JOB_ID}\" AND logName:\"job-message\"" \
  --project="${PROJECT_ID}" --order=asc --limit="${LOG_LIMIT}" \
  --format="table(timestamp,severity,textPayload)"

echo
echo "===== [3] ERROR / WARNING ====="
gcloud logging read \
  "resource.type=\"dataflow_step\" AND resource.labels.job_id=\"${JOB_ID}\" AND severity>=WARNING" \
  --project="${PROJECT_ID}" --order=asc --limit="${LOG_LIMIT}" \
  --format="table(timestamp,severity,logName,textPayload)"

echo
echo "===== [4] Launcher Logs ====="
gcloud logging read \
  "resource.type=\"dataflow_step\" AND resource.labels.job_id=\"${JOB_ID}\" AND logName:\"launcher\"" \
  --project="${PROJECT_ID}" --order=asc --limit="${LOG_LIMIT}" \
  --format="value(timestamp,severity,textPayload,jsonPayload.message)"

echo
echo "===== [5] Launcher ERROR Detail ====="
gcloud logging read \
  "resource.type=\"dataflow_step\" AND resource.labels.job_id=\"${JOB_ID}\" AND logName:\"launcher\" AND severity>=ERROR" \
  --project="${PROJECT_ID}" --order=asc --limit="${LOG_LIMIT}" \
  --format="value(timestamp,severity,textPayload,jsonPayload.message)"

echo
echo "===== [6] Python / Beam / JDBC ====="
gcloud logging read \
  "resource.type=\"dataflow_step\" AND resource.labels.job_id=\"${JOB_ID}\" AND (textPayload:\"Traceback\" OR textPayload:\"Exception\" OR textPayload:\"Error\" OR textPayload:\"JDBC\" OR textPayload:\"Jdbc\" OR textPayload:\"Vertica\" OR jsonPayload.message:\"Traceback\" OR jsonPayload.message:\"Exception\")" \
  --project="${PROJECT_ID}" --order=asc --limit="${LOG_LIMIT}" \
  --format="value(timestamp,severity,logName,textPayload,jsonPayload.message)"

echo
echo "===== [6-1] Exception Detail ====="
gcloud logging read \
  "resource.type=\"dataflow_step\" AND resource.labels.job_id=\"${JOB_ID}\" AND logName:\"launcher\"" \
  --project="${PROJECT_ID}" --order=asc --limit=2000 \
  --format="value(textPayload,jsonPayload.message)" \
  | grep -A 20 -B 20 -E 'Traceback|ValueError|TypeError|RuntimeError|Exception|ERROR|Error' || true

echo
echo "===== [7] ALL Job Logs ====="
gcloud logging read \
  "resource.type=\"dataflow_step\" AND resource.labels.job_id=\"${JOB_ID}\"" \
  --project="${PROJECT_ID}" --order=asc --limit="${LOG_LIMIT}" \
  --format="value(timestamp,severity,logName,textPayload,jsonPayload.message)"


echo
echo "===== [8] JDBC QUERY RESULT ====="
gcloud logging read \
  "resource.type=\"dataflow_step\" AND resource.labels.job_id=\"${JOB_ID}\" AND (textPayload:\"JDBC QUERY RESULT\" OR jsonPayload.message:\"JDBC QUERY RESULT\")" \
  --project="${PROJECT_ID}" --order=asc --limit="${LOG_LIMIT}" \
  --format="value(timestamp,textPayload,jsonPayload.message)"
