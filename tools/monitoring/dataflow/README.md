# Dataflow JDBC 진단 대시보드 생성 방법

대상 저장소 브랜치: `feature/26.1030.dataflow-progress-logs`.
프로젝트 기본값 `gcp-prod-edp-sa-udw`, 리전 `asia-northeast3`.
이 폴더는 **JSON 템플릿 + 로컬 렌더/검사 + 사용자가 실행할 생성/갱신 명령**만 제공합니다.
GCP 리소스, 새 custom/log-based metric, API 활성화, IAM/서비스계정 키를 만들지 않습니다.

## 1. 먼저 실제 Job ID / 이름 확인

Dataflow 콘솔에서 실패한 실행의 Job ID와 Job name을 복사합니다. 서로 다른 값입니다.
실패 시각을 포함하도록 대시보드 시간 범위를 설정하세요.
Monitoring `dataflow_job` 리소스에는 project_id/job_name/region만 있고 **job_id 리소스 라벨은 없습니다**.
대부분 Job 지표의 실행 ID는 `metric.labels.job_id`입니다. worker memory는
`dataflow_worker`의 `resource.labels.job_id`로 제한합니다.
워커 location이 리전/존 중 무엇인지 임의 가정하지 않으므로 worker panel은 project+job_id+job_name으로 제한합니다.

## 2. 로컬 렌더와 검사 (GCP 호출 없음)

저장소 루트에서 실행합니다. Python 표준 라이브러리만 사용합니다.

```bash
export PROJECT_ID='gcp-prod-edp-sa-udw'
export REGION='asia-northeast3'
export JOB_ID='실제_JOB_ID로_교체'
export JOB_NAME='실제-job-name으로-교체'
python tools/monitoring/dataflow/render_dashboard.py \
  --project-id "$PROJECT_ID" --region "$REGION" \
  --job-id "$JOB_ID" --job-name "$JOB_NAME" \
  --output /tmp/dataflow-dashboard.json
python tools/monitoring/dataflow/render_dashboard.py --check /tmp/dataflow-dashboard.json
```

한글 예시 placeholder를 그대로 실행하면 입력 검증에서 거부됩니다. 실제 식별자를 넣으세요.
검사는 JSON/지원 schema subset/좌표겹침/필터 scope/지표 allowlist/치환/안전 집계를 확인합니다.
**Google 서버의 전체 schema 검증이나 실제 시계열 존재 검증은 아닙니다.**

## 3. 사용자 승인 하에 Google API 검증 후 생성

아래 명령은 사용자가 대상 프로젝트를 확인한 후 실행합니다. 이 커밋에서 실행하지 않았습니다.
기존 gcloud 로그인 계정을 사용하세요. 서비스계정 JSON 키를 만들 필요가 없습니다.

```bash
# API에 검증 요청만 보냄: 대시보드 저장 안 함
# 서비스 사용/권한이 준비돼 있어야 하며 자동으로 enable/IAM을 변경하지 마세요.
gcloud monitoring dashboards create \
  --project="$PROJECT_ID" --config-from-file=/tmp/dataflow-dashboard.json --validate-only

# 실제 대시보드 생성 (이 명령부터 GCP 변경)
gcloud monitoring dashboards create \
  --project="$PROJECT_ID" --config-from-file=/tmp/dataflow-dashboard.json --format=json \
  > /tmp/dataflow-dashboard-created.json
```

반환된 `name`/대시보드 ID를 기록하고 Monitoring → Dashboards에서 엽니다.
`create`를 반복하면 중복 대시보드가 생깁니다. 재실행 전 결과를 확인하고 기존 것은 update를 사용하세요.
필요 권한은 대시보드 생성/수정(`monitoring.dashboards.create/update`, 예: monitoring.dashboardEditor),
지표 조회(예: monitoring.viewer), 로그 조회(예: logging.viewer)입니다.
조직 정책에 따라 추가 제한이 있을 수 있으며 이 문서는 권한을 자동 부여하지 않습니다.

## 4. 기존 대시보드 갱신 — 최신 etag 사용

```bash
export DASHBOARD_ID='생성된-dashboard-ID'
gcloud monitoring dashboards describe "$DASHBOARD_ID" --project="$PROJECT_ID" --format=json \
  > /tmp/dataflow-dashboard-existing.json
# 응답 name에 project number가 사용될 수 있으므로 read-only 조회로 확인
export PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
python tools/monitoring/dataflow/render_dashboard.py \
  --project-id "$PROJECT_ID" --project-number "$PROJECT_NUMBER" --region "$REGION" \
  --job-id "$JOB_ID" --job-name "$JOB_NAME" \
  --existing-dashboard /tmp/dataflow-dashboard-existing.json \
  --output /tmp/dataflow-dashboard-update.json
# 여기서부터 기존 대시보드 전체 구성을 템플릿으로 교체함: 사용자 편집이 있으면 먼저 비교/백업
# 최신 name/etag가 포함되어 동시 수정 충돌을 방지함
gcloud monitoring dashboards update "$DASHBOARD_ID" --project="$PROJECT_ID" \
  --config-from-file=/tmp/dataflow-dashboard-update.json
```

etag 충돌이면 최신 내용을 다시 조회하고 사람의 수정과 비교 병합하세요. etag를 삭제해 우회하지 마세요.
이전 JSON을 백업했다면 동일하게 최신 etag로 검토 후 복구할 수 있습니다.
Job별 필터는 생성 시 고정되므로 다른 Job은 다시 렌더 후 새 대시보드 또는 기존 대시보드 갱신이 필요합니다.

## 패널 의미와 한계

| 패널 | 실제 의미 / 주의 |
|---|---|
| current_num_vcpus | 현재 vCPU 수. 워커 수 아님. 임의 나눗셈 없음 |
| is_failed | 1이면 실패. 공식 descriptor에 job_id가 없어 job_name+region+project 기준. 이름 재사용 시 실행 시간 범위 필수. 시작 전 실패는 기록 안 될 수 있음 |
| worker memory bytes_used / container_limit | 각각 실제 컨테이너 사용 바이트 / 한도. worker/container별 선 유지. GB-seconds나 총할당량으로 메모리% 계산하지 않음 |
| element_count / estimated_byte_count | PCollection별 누적 요소/추정바이트. 여러 단계 합산하면 중복. 원천 행수/저장파일 크기/적재행수 아님 |
| user_counter | 기존 native User Counter 지표. 실제 코드 namespace=jdbc_export_progress, counter=post_jdbc_output_rows. export metric_name 문자열 형식을 추측하지 않고 모든 counter를 보여주며 실제 label 확인 후 좁힐 수 있음 |
| total_shuffle_data_processed | 누적 처리한 shuffle 바이트. 현재 중간 저장소 점유량 아님 |
| Composer logs | `Dataflow progress`와 Job ID를 모든 payload에서 검색: textPayload/jsonPayload 포맷 차이를 피함. Composer 로그가 다른 프로젝트에 있으면 해당 logsPanel.resourceNames를 검증된 로그 프로젝트로 수정하고 조회권한 확인 |
| worker I/O logs | 실제 GCS파일/LOAD 제출/COPY 완료참조. LOAD 제출은 성공 아님. current_state는unknown. BQ 확정적재행수는 미지원 |
| worker errors | 해당 dataflow_step Job ID의 ERROR/OOM 문구. 로그만으로 자동 원인 확정 안 함 |

native timeSeriesFilter chart는 GAUGE의 각 시리즈를 60초 구간 `ALIGN_MAX`로 표시하고 시리즈 간 합산하지 않습니다.
메모리 비율 패널만 아래 설명의 PromQL로 동일 컨테이너 관측값을 나눕니다.
해당 구간 최대 관측값이지 정확한 순간값이 아닙니다. 누적 gauge를 시간 SUM/RATE로 바꾸지 않았습니다.
**데이터 없음 = unknown, 0/정상/성공 아님**. native 지표는 보통60초 sampling 후 최대420초 정도 지연될 수 있습니다.
로그도 비동기이며 60초 이내 최신성이나 단계별 모든 이벤트 표시를 보장하지 않습니다.

## 배포 의존성과 실제 확인

native 지표는 기존 Dataflow가 제공할 때만 표시됩니다. 새 진행로그는 Composer executor 배포가,
post-JDBC counter/worker I/O 로그는 신규 launcher 이미지·템플릿 배포가 필요합니다.
이 브랜치 코드를 커밋한 것만으로 운영에 적용되지는 않습니다. 새 이미지태그/템플릿경로로 테스트 후 적용하세요.
대시보드가 비면 Job ID/name/project/시간 범위/권한/지표제공/배포 여부를 먼저 확인합니다.
Monitoring Metrics Explorer에서 실제 `metric.labels.metric_name`, ptransform, pcollection을 확인하세요.
원천 JDBC 내부읽기 ≠ Reparallelize 후단출력 ≠ BigQuery 확정적재입니다. 진행률/전체800만 대비%는 만들지 않았습니다.
확정적재건수는 향후 검증된 적재완료 카운터/검증쿼리가 별도로 필요하며 이 변경에서는 구현하지 않습니다.

운영 검증 체크: Google `--validate-only` 결과, 실제 생성 성공, 선택Job 필터,
메모리 시리즈/진행로그 노출, LOAD 제출과 COPY 완료 구분, 데이터없음 의미를 점검하세요.
이 커밋의 offline tests는 서버 schema/API권한/실데이터 렌더를 검증하지 않습니다.

## 공식 근거 (2026-10-30 조회)

- 지표/metric labels: https://docs.cloud.google.com/monitoring/api/metrics_gcp_d_h#dataflow
- monitored resources: https://docs.cloud.google.com/monitoring/api/resources#tag_dataflow_job
- Dashboard v1 schema: https://docs.cloud.google.com/monitoring/api/ref_v3/rest/v1/projects.dashboards
- create/validate-only: https://docs.cloud.google.com/sdk/gcloud/reference/monitoring/dashboards/create
- update/etag: https://docs.cloud.google.com/sdk/gcloud/reference/monitoring/dashboards/update
- Logging resource: https://docs.cloud.google.com/logging/docs/api/v2/resource-list
- 권한: https://docs.cloud.google.com/monitoring/access-control


## OOM 보완 패널 (2026-10-30 수정)

- `worker/memory/total_limit`: 워커 전체 RAM **한도**. container_limit·사용량과 구분합니다.
- 관측 메모리 비율: `100 * bytes_used / (container_limit > 0)`를
  `on(project_id,location,job_name,job_id,worker_type,worker_id,container)`로 일치시킵니다.
  분모의 `> 0`은 bool 없이 양수인 실제 한도 값을 유지합니다. 분모0/음수/결측 또는 짝없는 시리즈는
  결과가 없고 0%로 채우지 않습니다. `total_limit`으로 컨테이너 비율을 나누거나 워커간 평균을 내지 않습니다.
  PromQL instant selector는 lookback 안의 직전 표본을 쓸 수 있어 동일 시각·실시간/순간peak 보장은 없습니다.
  100% 초과 관측도 clamp하지 않으므로 시각 차이·지표 이상을 점검할 수 있습니다.
- INFO `Successfully wrote OOM event`는 탐지기의 **기록 marker 후보**이며 kernel kill 확정이 아닙니다.
- Java heap / GC overhead·thrashing, cgroup·Killed process, SDK disconnect를 별도 로그 패널로 구분합니다.
  SDK disconnect·exit137만으로 OOM이라고 단정하지 마십시오. 부정문·반복 로그도 후보로 표시될 수 있어
  자동 이벤트 수·장애 수·OOM 발생률을 만들지 않았습니다.
- 80% 지속/90% 접근 같은 값은 검토를 시작할 **예시 운영 기준**일 뿐 OOM 판정값이 아닙니다.
  실제 한도와 일시적 peak, workload baseline으로 조정해야 하며 경보 정책은 생성하지 않습니다.

### 커널 로그: 확인한 worker VM에만 한정

기본 템플릿은 GCE kernel 조회를 하지 않고 안내문만 표시합니다. Dataflow Job의 worker 이름을
Compute Engine의 **해당 VM**과 대조하여 프로젝트/zone/숫자instance ID/Job 소유관계를 확인하세요.
`gce_instance`에는 `job_id` resource label이 없습니다. 아래 옵션은 자동 검증이 아니라
사용자가 확인한 상관관계를 입력하는 것입니다. 잘못된 VM을 넣으면 다른 VM의 로그를 보게 됩니다.

```bash
# 선택적 읽기 전용 조회: Dataflow Job에서 확인한 정확한 worker 이름과 zone 사용
export WORKER_NAME='확인된-worker-vm-name'
export WORKER_ZONE='asia-northeast3-a'
gcloud compute instances describe "$WORKER_NAME" --zone="$WORKER_ZONE" --project="$PROJECT_ID" \
  --format='json(id,name,zone,labels)'
# 응답과 Dataflow worker 목록을 사람이 대조한 뒤 실제 numeric id만 입력
export WORKER_INSTANCE_ID='확인된_숫자_ID'
python tools/monitoring/dataflow/render_dashboard.py \
  --project-id "$PROJECT_ID" --region "$REGION" --job-id "$JOB_ID" --job-name "$JOB_NAME" \
  --worker-instance-id "$WORKER_INSTANCE_ID" --worker-zone "$WORKER_ZONE" \
  --confirmed-worker-job-id "$JOB_ID" --output /tmp/dataflow-dashboard.json
```

이 경우에만 project+zone+instance_id로 엄격 제한한 `gce_instance` kernel 후보 패널이 생성됩니다.
Dashboard 시간 범위를 해당 실패 실행으로 제한하세요. 한 VM 표본이며 전체워커 통합판정이 아닙니다.
커널 로그가 기존 수집 설정으로 전송되지 않았다면 빈 결과입니다. 이 변경은 Ops Agent 설치나
새 로그수집/권한 설정을 하지 않습니다. 전체 프로젝트의 Killed process를 무제한 검색하지 않습니다.

### 아직 직접 계측하지 않는 항목

JVM heap 사용량/GC pause 수치, 프로세스 RSS/peak, cgroup memory.events·oom_kill 카운터는
새 에이전트/런타임 계측 없이 자동 수집한다고 주장하지 않습니다. 기존 로그 후보만 조회합니다.
분자·분모 관측값/Job·worker·stage 진행/실제 예외·커널 원문을 같은 시각에 대조한 뒤 원인을 판단하세요.
이 수정은 대시보드 JSON과 Composer 실패진단 코드 변경입니다. 기존 진행 launcher가 이미 배포됐다면
이번 OOM 수정만을 위해 이미지 재빌드할 필요는 없으나 미배포인 기존 진행계측에는 여전히 새 이미지가 필요합니다.

PromQL 공식 근거:
- https://docs.cloud.google.com/monitoring/promql#querying_cloud_monitoring_metrics_by_using_promql
- https://prometheus.io/docs/prometheus/latest/querying/operators/
- https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness
- https://docs.cloud.google.com/dataflow/docs/guides/troubleshoot-oom


### Composer 로그 가독성 (2026-10-30, 임의진)

기본 한국어 요약도 `Dataflow progress`와 Job ID를 포함하므로 기존 Composer logs 패널은 그대로 조회됩니다.
긴 snapshot JSON은 Vine `grape_defaults.progress_log_details: true` 또는 개별 `grapes[].options`에서
선택 출력합니다(기본false, boolean만). 패널은 JSON필드나 카운터를 파싱하지 않으며 native metric 차트는 변경 없습니다.
기본 요약의 후단 카운터는 원천 읽기/BigQuery 확정적재 수가 아닙니다. 이 표시 변경만으로 이미지 재빌드가 필요하지 않습니다.
