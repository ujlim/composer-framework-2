# Dataflow 실패 로그를 Airflow 태스크 로그에서 확인하기

## 적용 범위

`DataflowJdbcOperator`의 Flex Template 실행 실패 또는 실행 후 실패 상태에서만 Cloud Logging 로그를 조회합니다. 이 실패 진단 모듈은 성공 작업에는 조회하지 않습니다. 별도 진행로그 observer는 실행 중 주기적으로 조회합니다. 배치의 성공/실패 판정 자체는 변경하지 않습니다.

기존 두 항목과 OOM 후보 전용 항목을 API로 조회합니다. `gcloud` 설치는 필요 없습니다.

- **[7] ALL Job Logs (bounded)**: 해당 Dataflow job ID의 `dataflow_step` 로그. severity 제한 없이 INFO도 포함합니다.
- **[8] JDBC QUERY RESULT**: 동일 범위에서 `textPayload` 또는 `jsonPayload.message`에 `JDBC QUERY RESULT`가 있는 로그만 조회합니다. 실제로 존재하는 결과만 출력합니다. 추출/적재 템플릿이 결과 행을 기록하지 않으면 이 항목은 비어 있을 수 있습니다.

[7]/[8]은 `timestamp asc`로 조회하며 timestamp, severity, logName, textPayload 또는 jsonPayload.message를 Airflow 태스크 로거에 출력합니다. [8] 결과가 [7]에도 있어도 별도 항목에 다시 표시합니다. 같은 항목의 동일 로그는 중복 제거합니다. [6]/[6-1] 및 별도 Job Messages API 조회는 추가하지 않습니다.

- **[9] OOM GC SDK signal candidates**: 같은 Job의 `Successfully wrote OOM event` INFO marker,
  Java heap/GC pressure, cgroup kill, SDK disconnect 후보를 severity 제한 없이 최신순(`timestamp desc`) 조회합니다.
  원문 맥락만 표시하며 커널 OOM 확정·고유 이벤트 수·재시도 수로 계산하지 않습니다.
  부정문(예: not OOM)이나 반복 시도도 검색에 걸릴 수 있습니다. GCE 전역 로그 조회는 하지 않습니다.

## 안전한 조회 범위와 한계

- project/region/job ID를 정확히 제한합니다. 시간 범위는 이 Operator의 launch 직전 30초부터 실패 처리 시각 +15초까지입니다.
- job ID를 얻기 전 실패하면 이름만으로 다른 작업 로그를 조회하지 않습니다. Dataflow 확인 링크와 “job ID 없음” 안내를 남깁니다. 이 경우 컨테이너 로그가 자동 복사된다고 보장하지 않습니다.
- 항목당 최대 2페이지 × 100개, 출력 본문 약 42.7 KiB, 한 로그 본문 최대 8 KiB입니다. 세 항목 합계 본문 128 KiB에 머리말/링크가 추가됩니다. API 응답은 요청당 256 KiB 스트리밍 상한입니다.
- 총 20초의 best-effort 조회 예산과 항목별 약 6.7초 예산, 요청별 최대 5초 타임아웃을 사용합니다. 인증 갱신/네트워크 라이브러리 특성상 엄밀한 20초 wall-clock 중단 보장은 아닙니다. 자동 API 재시도나 로그 반영 대기용 sleep은 하지 않습니다.
- 페이지/바이트/시간 상한, 로그 수집 지연, 제외 설정, 권한 부족으로 일부가 빠질 수 있습니다. “ALL”은 전체 종류를 조회한다는 뜻이지 전체 실행 로그가 무제한으로 복사된다는 뜻이 아닙니다. 오래되거나 로그가 많은 작업은 ASC 앞부분만 포함될 수 있어 마지막 오류가 잘릴 수 있습니다. [9]는 최신순 후보로 이를 보완하지만 여전히 완전한 목록은 아닙니다.
- 각 항목의 조회 링크, 가져온 건수, 다음 페이지 여부 및 출력 상한 도달 여부를 남깁니다. 링크에서 전체 기간과 로그를 확인하세요. 비어 있다고 해서 오류가 없다는 뜻은 아닙니다.
- 진단 조회 실패는 원래 Dataflow 실패 상태를 덮어쓰지 않습니다. launch API 오류는 원본 응답/요청 URI를 출력하지 않고 오류 유형·HTTP 상태·마스킹한 오류 메시지를 포함한 RuntimeError로 전달합니다. 따라서 launch 예외의 Python 타입은 바뀔 수 있습니다.

## 권한과 실행 주체

이 변경은 기존 Dataflow launch/poll의 ADC 실행 주체를 유지합니다. 같은 ADC 자격증명을 로그 조회에도 사용합니다. 보통 Composer 실행 SA이지만 실제 ADC 설정을 확인해야 합니다. `service_account_email`의 Dataflow worker SA나 GCS 업로드용 `impersonation_chain`으로 자동 전환하지 않습니다.

로그 조회 주체에 대상 프로젝트의 `logging.logEntries.list` 권한(예: 적절히 범위를 제한한 Logs Viewer)이 필요합니다. 별도 로그 버킷/뷰 접근 제한이 있으면 실제 라우팅과 뷰 권한을 검토해야 합니다. 로그를 읽을 수 없으면 오류 유형만 안내하고 원래 실패 처리를 계속합니다. 이 브랜치는 IAM/권한을 변경하지 않습니다.

로그 내용을 Composer 태스크 로그로 복사하므로 Composer 로그 열람자에게도 해당 내용이 보일 수 있습니다. 알려진 Connection 비밀번호·로그인·JDBC URL·SQL 원문 및 일반적인 password/token/인증 헤더 패턴을 마스킹합니다. 모든 업무 데이터나 임의 비밀 표현을 완벽히 탐지하는 DLP 기능은 아닙니다. 특히 JDBC QUERY RESULT는 업무 행을 포함할 수 있으므로 운영에서 결과 행 자체를 로그에 기록하지 않는 정책을 권장합니다.

## 배포 및 검증

함께 배포할 파일:
- `dags/framework/dataflow_executor.py`
- `dags/framework/dataflow_diagnostics.py`
- 로그 강화 executor를 쓰는 경우 `dags/framework/dataflow_progress.py`도 함께 유지

Dataflow 이미지나 템플릿 재빌드는 필요하지 않습니다. Composer 측 코드의 변경입니다. 테스트는 모의 API를 이용하며 실제 Composer/Dataflow/IAM 검증을 대신하지 않습니다.

1. 비운영 작업으로 의도적인 SQL 오류를 실행합니다.
2. Airflow 실패 로그에서 [7], [8], [9], 정확한 job 링크와 조회 제한 정보를 확인합니다.
3. 원래 작업 실패가 유지되는지, Cloud Logging 원문과 시간/로그 이름이 대응되는지 확인합니다.
4. 권한 없는 진단 환경에서도 원래 실패를 덮어쓰지 않는지 확인합니다.
5. 정상 작업에는 진단 로그 API가 추가로 호출되지 않는지 확인합니다.

공식 문서:
- https://docs.cloud.google.com/logging/docs/reference/v2/rest/v2/entries/list
- https://docs.cloud.google.com/dataflow/docs/guides/troubleshoot-templates
- https://google-auth.readthedocs.io/en/latest/reference/google.auth.transport.requests.html
