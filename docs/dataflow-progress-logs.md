# Dataflow JDBC 진행 관측 로그 (2026-10-30)

기준: `feature/26.1023.dataflow-error-logs` / `d136c25ca763626015b88c71a563d74f96da1b43`.
진행로그는 원인 파악용입니다. 10/29 machine/fetch 브랜치를 통째로 합치지 않았습니다.
JDBC query/fetch/output_parallelization 및 BigQuery FILE_LOADS는 그대로입니다.
추가로 runtime.dataflow.machine_type만 선택 지원합니다: 명시한 Vine의 새 Job에만 적용하고
미지정/null이면 machineType 필드를 생략하여 기존 동작을 유지합니다. fetch_size 설정 기능은 포함하지 않습니다.
머신 유형 적용/폐기된 이전 예제 주의는 docs/dataflow-machine-type.md를 참조하십시오.
실행 배포본과 원문이 다를 수 있으므로 배포 시 비교 병합하여 추가 진단을 보존하십시오.

## 무엇이 기록되는가

- Composer: dag/run/task/job와 observer PID, 설정된 temp location, 예상 bq_load 부모 prefix,
  실행 단계의 실제 API state/start/end/경과, 이전 관측 state와 변화 여부.
- stage 이름: Job description의 executionPipelineStage.id와 stageId **정확 일치**로만 매핑.
  componentTransform.userName 및 prerequisiteStage를 기록; 불명확하면 unknown/null.
- stage/job 지표: element/byte/post-JDBC 관련 scalar 일부를 origin/name/context/kind/cumulative/
  tentative/updateTime과 함께 보존합니다. 단위는 검증 없이 추측하지 않고 unknown.
  attempted(tentative)와 committed는 별개 관측이며 합산하지 않습니다.
  stage와 job 응답은 겹칠 수 있으므로 서로 합산하지 마십시오. 전체 행수 추정은 하지 않습니다.
- 워커: 실행 중 stage 하나의 최대 5 worker / worker당 10 work-item 표본.
  workerName/taskId/attemptId/state만 기록. 역사적 항목도 있으므로 활성 워커 수가 아닙니다.
- 메모리: 현재 사용량은 이 API가 보장하지 않으므로 **unknown**. TotalMemoryUsage 같은
  GB-seconds 할당 지표를 RAM 사용량으로 바꾸지 않습니다.
- launcher/worker: `jdbc_export_progress/post_jdbc_output_rows` Beam counter는
  Java ReadFromJdbc **후단**으로 나온 행수입니다. 원천 Oracle cursor 읽기 수나 BQ 커밋 수가 아닙니다.
  기본 Reparallelize의 완료 대기 때문에 0/미변화여도 Oracle이 읽지 않았다는 뜻이 아닙니다.
- `DATAFLOW_IO_PROGRESS`: 실제 FILE_LOADS 출력에서 확인된 GCS 닫힌 파일 URI/크기와
  load 제출 참조 및 copy 완료 참조를 기록합니다. load의 `SUBMITTED_REFERENCE_OBSERVED`는
  ONGOING_JOBS 출력으로 실제 현재 상태/성공 여부는 **unknown**입니다.
  copy의 `SUCCESS_OUTPUT_OBSERVED`만 Beam이 성공 대기 후 낸 출력의 관측입니다.
  추가 BigQuery jobs.get 조회가 아니므로 실행 중 최신 BQ job 상태는 제공하지 않습니다.
  출력 처리 스케줄 때문에 로그가 지연될 수도 있습니다.

없거나 접근 불가한 값은 unknown/null/빈 관측으로 유지하며 0으로 채우지 않습니다.
실제 API가 0을 반환한 경우에만 scalar=0입니다. 중복 metric identity는 한 snapshot 안에서 제거하되
전체 작업의 정확히 한 번 처리/적재 성공을 증명하지 않습니다. unchanged는 정체/장애/재시도 판정이 아닙니다.

## 주기·오버헤드·안전

```yaml
grape_defaults:
  progress_log_interval_seconds: 60  # 실제 정수, 최소60초. 각 grape options로 override 가능.
# 기존 poll_interval_seconds는 작업 종료상태 확인 주기이며 별개입니다.
```

- 매 관측 최대 12초 / 요청당 최대 3초, 응답 최대512KiB, 로그 이벤트 최종 UTF-8 최대48KB. ExecutionDetails 최대2페이지/
  50stage, metric 최대40/source(stage 로그는4개), worker detail 한 stage/한 페이지로 제한합니다.
  제한 시 partial/sample 표기를 확인하십시오. 매행 로그나 bucket 전체 스캔/BQ jobs.list는 없습니다.
- Job description은 처음 한번만 조회합니다. 초기 graph 미준비/권한없음이면 이름이 unknown일 수 있으나
  stage ID는 그대로 기록합니다. 이 경우 콘솔 Execution details에서 이름을 대조하십시오.
- FILE_LOADS 출력 로그는 DoFn 인스턴스당60초 표본입니다. suppression count는 직전 로그 이후
  생략된 출력 수일 뿐, 누락 행수나 전체 파일 수가 아닙니다. 워커 수에 따라 총 로그량이 늘 수 있습니다.
  번들 재실행으로 같은 이벤트가 중복될 수 있습니다. PID/host는 컨테이너 프로세스 식별이고 worker_id는 unknown입니다.
- 관측 API/로그 실패는 진행 Job을 실패시키지 않습니다. 기존 작업상태 조회 오류도 unknown으로
  기록하고 최소60초 뒤 재확인합니다. 지속 권한/통신장애면 기존 Airflow execution timeout까지
  기다릴 수 있으며, timeout이 원격 Dataflow Job을 자동취소한다는 보장은 없습니다.
- 종료 상태에서 final_snapshot을 남깁니다. 관측 주기 안이면 cached=true와 age를 기록하며
  이전 관측이 현재 마지막 순간 상태라는 뜻은 아닙니다. 실패 시 기존 오류 진단도 그대로 실행합니다.
- 기존 ADC/서비스 계정으로 읽기 전용 API만 호출. IAM 변경 없음.
  dataflow.metrics.get 권한 부재는 unavailable로 기록합니다. SQL/행값/인증 payload를 신규로그에 넣지 않습니다.

## 배포와 검증 (이 변경에서 실제 배포하지 않음)

1. Composer에 수정 executor와 신규 dataflow_progress.py를 함께 비교 병합 배포.
   기존 dataflow_diagnostics.py는 유지해야 합니다.
2. launcher main.py 변경은 새 이미지 태그 + 새 Flex template 경로로 빌드하고 테스트 Vine만 참조.
   기존 build scripts 기본 :v1/공용 template 경로를 그대로 실행하여 덮어쓰지 마십시오.
   기존 진단 파일을 통째로 오래된 버전으로 되돌리지 마십시오.
3. 새 Job에서 `Dataflow progress`(Composer)와 `DATAFLOW_IO_PROGRESS`(Dataflow worker 로그)를
   같은 job_id/resource.labels.job_id 또는 job_name으로 연결합니다.
4. 800만행 시험 전에 작은 테스트 작업으로 stage 이름/진행/state 관측, API권한 없음의
   비치명 동작, GCS파일/BQ제출·copy완료참조 로그, 원천읽기와후단카운터 차이를 확인합니다.
5. 실패 원인 분석은 마지막 snapshot, 원래 오류 진단, worker OOM/Java heap/container exit 로그를
   함께 보십시오. 실패 Job ID와 발생 시각 없이 원인이나 중간저장량을 단정하지 않습니다.

별도 Cloud Monitoring에서 실제 메모리는 `dataflow.googleapis.com/worker/memory/bytes_used`,
`worker/memory/container_limit`, `worker/memory/total_limit` 지표를 확인하십시오.
이 코드가 Monitoring API를 추가로 호출하거나 IAM을 늘리지는 않습니다.

## 검증 범위

`python -m unittest discover -s tests -v`

mock/API/clock 기반 offline regression tests입니다. 실제 Oracle, Beam Java expansion,
Dataflow API의 운영 metric 제공 여부, OOM 재현·해결, 이미지빌드·운영배포는 검증하지 않았습니다.
읽기/적재방식 변경이나 메모리 문제 해결을 약속하는 패치가 아닙니다.

## 공식 근거

- https://docs.cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs/getExecutionDetails
- https://docs.cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs/getMetrics
- https://docs.cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs.stages/getExecutionDetails
- https://docs.cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#ExecutionStageSummary
- https://docs.cloud.google.com/dataflow/docs/reference/rest/v1b3/MetricUpdate
- https://docs.cloud.google.com/monitoring/api/metrics_gcp_d_h#dataflow
- https://github.com/apache/beam/blob/v2.75.0/sdks/python/apache_beam/runners/dataflow/dataflow_metrics.py
- https://github.com/apache/beam/blob/v2.75.0/sdks/python/apache_beam/io/gcp/bigquery_file_loads.py
