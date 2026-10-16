# 3. GCS 파일 처리, SFTP Runner, FIN 대기

[목차](README.md) · [이전: SQL](02-sql.md) · [다음: Dataflow](04-dataflow.md)

이 문서는 `gcs_copy_object`, `gcs_delete_object`, `gcs_move_prefix`, `gcs_merge_csv`, `gcs_to_sftp_runner`, `gcs_fin_file_sensor`의 실제 구현을 기준으로 설정 방법과 운영상 주의사항을 설명합니다. 예시의 `REPLACE_WITH_...` 값은 사용 환경에 맞게 변경해야 합니다.

## 1. 공통 규칙과 인증 경계

- 각 작업은 Vine의 `grapes` 목록에 작성합니다. `grape_id`는 같은 Vine 안에서 유일해야 하고, `depends_on`에는 같은 Vine의 선행 `grape_id`를 지정합니다.
- 실행 옵션은 `grape_defaults`와 작업별 `options`를 재귀 병합합니다. 같은 키는 작업별 값이 우선합니다. 아래 기본값은 두 곳 모두 해당 키를 지정하지 않았을 때의 코드 기본값입니다.
- 참/거짓은 따옴표 없는 YAML `true` / `false`로 작성합니다. 구현에서 `bool(...)`로 변환하므로 문자열 `"false"`도 참으로 처리될 수 있습니다.
- GCS의 `bucket`에는 버킷 이름만, `object` / `prefix`에는 버킷 내부 이름만 입력합니다. `gs://...` 전체 URI를 넣지 않습니다.
- Prefix는 폴더가 아닌 문자열 접두사입니다. `inbound/2026-10-11/`처럼 끝의 `/`까지 지정해야 인접 이름까지 포함하는 실수를 줄일 수 있습니다.
- 날짜 템플릿은 `{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}` 사용을 권장합니다. Root 실행과 Vine 직접 실행 모두에서 해결된 업무일자를 사용합니다. Vine을 직접 실행할 때는 `business_date`를 지정합니다.
- GCS 작업과 FIN 센서는 Airflow의 Google Cloud 연결을 사용합니다. SFTP 작업은 Composer → Runner SSH 연결과 Runner → GCS/SFTP 인증을 따로 사용합니다.

### 1.1 작업 및 Google Cloud 공통 필드

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 작업 | `grapes[].grape_id` | O | 없음 | 문자열. Airflow Task ID. 영문, 숫자, `_`, `-`, `.`만 허용합니다. | `copy_daily_file` |
| 작업 | `grapes[].type` | O | 없음 | 문자열. 이 문서에 설명된 작업 타입 중 하나입니다. | `gcs_copy_object` |
| 의존성 | `grapes[].depends_on` | X | `[]` | 문자열 목록. 같은 Vine의 선행 작업 ID. 목록에서 이름만 앞에 둔다고 순차 실행되지 않습니다. | `[copy_daily_file]` |
| 실행 기본값 | `grape_defaults` | X | `{}` | 매핑. 각 작업의 `options`에 재귀 병합되는 공통 실행 옵션입니다. | `retries: 0` |
| 실행 옵션 | `grapes[].options` | X | `{}` | 매핑. 이 작업의 옵션으로 공통값을 덮어씁니다. | `overwrite: false` |
| GCP 연결 | `runtime.gcp_conn_id` | X | `google_cloud_default` | 문자열. GCS 4종과 FIN 센서가 사용하는 Airflow Connection ID. SFTP Runner에는 전달되지 않습니다. | `google_cloud_default` |
| GCP 권한 위임 | `runtime.impersonation_chain` | X | `null` | 문자열 또는 문자열 목록 또는 null. GCS 4종과 FIN 센서의 서비스 계정 impersonation 설정. 각 단계에 필요한 권한이 있어야 합니다. SFTP Runner에는 전달되지 않습니다. | `pipeline-sa@REPLACE_WITH_PROJECT.iam.gserviceaccount.com` |
| 실행 제한 | `options.retries` | X | GCS/SFTP `1`, FIN `0` | 정수. 최초 실패 후 재시도 횟수. 파일 생성·삭제 작업은 일부 반영 후 실패할 수 있어 초기 검증에서는 `0`을 권장합니다. | `0` |
| 재시도 간격 | `options.retry_delay_seconds` | X | GCS/SFTP `300`, FIN `60` | 정수, 초. 재시도 대기 시간입니다. | `60` |
| 실행 제한 | `options.execution_timeout_seconds` | X | GCS/SFTP `7200`; FIN은 §7 참조 | 정수, 초. GCS는 PythonOperator 실행 제한, SFTP는 SSH 명령 제한과 Airflow 실행 제한에 동일하게 적용합니다. | `7200` |
| 리소스 | `options.pool` | X | 미지정 (`None`) | 문자열 또는 null. Airflow pool 이름. 미지정 시 Airflow의 기본 처리에 따릅니다. | `default_pool` |
| 우선순위 | `options.priority_weight` | X | `1` | 정수. Airflow 스케줄링 우선순위 가중치입니다. | `1` |

`runtime.project_id`, `runtime.location`, `runtime.labels`는 이 문서의 GCS 4종 및 FIN 센서 구현에서 읽지 않습니다. SFTP Runner는 `runtime` 전체를 전달받지 않습니다. 해당 키를 써도 이 작업의 GCP 프로젝트·인증·SFTP 접속 대상을 바꾸지 못합니다.

## 2. 단일 객체 복사: `gcs_copy_object`

한 객체를 복사하고 원본과 목적지의 크기 및 CRC32C가 같은지 검사합니다. 원본 삭제는 하지 않습니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 타입 | `type` | O | `gcs_copy_object` | 문자열. 단일 객체 복사 작업입니다. | `gcs_copy_object` |
| 원본 | `source.bucket` | O | 없음 | 비어 있지 않은 문자열. 원본 GCS 버킷 이름입니다. | `REPLACE_WITH_SOURCE_BUCKET` |
| 원본 | `source.object` | O | 없음 | 비어 있지 않은 문자열. 정확한 객체 이름. glob 패턴은 지원하지 않습니다. 앞의 `/`는 제거됩니다. | `input/2026-10-11/result.csv` |
| 목적지 | `destination.bucket` | O | 없음 | 비어 있지 않은 문자열. 목적지 버킷입니다. 원본과 같아도 객체 이름은 분리합니다. | `REPLACE_WITH_ARCHIVE_BUCKET` |
| 목적지 | `destination.object` | O | 없음 | 비어 있지 않은 문자열. 목적지의 완전한 객체 이름. 폴더명만 지정하지 않습니다. 앞의 `/`는 제거됩니다. | `archive/2026-10-11/result.csv` |
| 덮어쓰기 | `options.overwrite` | X | `false` | 불리언. 목적지 존재 시 기본적으로 실패. `true`는 교체를 허용합니다. | `false` |

### 안전한 전체 예시

```yaml
version: 1
vine:
  vine_id: example_gcs_copy
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 10800
business_date:
  type: daily
runtime:
  gcp_conn_id: google_cloud_default
  impersonation_chain: null
grape_defaults:
  retries: 0
  execution_timeout_seconds: 7200
grapes:
  - grape_id: copy_daily_result
    type: gcs_copy_object
    source:
      bucket: REPLACE_WITH_SOURCE_BUCKET
      object: "input/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/result.csv"
    destination:
      bucket: REPLACE_WITH_ARCHIVE_BUCKET
      object: "archive/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/result.csv"
    options:
      overwrite: false
```

실행 순서는 원본 존재 확인 → 목적지 충돌 확인 → 복사 → 목적지 재조회 → 크기/CRC32C 검증입니다. 검증 실패 시 이미 복사된 목적지는 자동으로 지우거나 되돌리지 않습니다. 재시도 시 `overwrite: false` 때문에 기존 목적지와 충돌할 수 있으므로 실행 결과를 확인한 뒤 복구합니다. 원본과 목적지를 완전히 같은 객체로 지정하지 않습니다.

## 3. 객체 삭제: `gcs_delete_object`

정확한 객체 하나 또는 glob 패턴과 일치하는 객체들을 삭제합니다. 별도의 dry-run 옵션이나 복사·백업 단계는 없습니다. 예시는 테스트용 정리 경로를 사용하며, 실제 삭제 대상과 버킷의 복구 정책을 먼저 확인해야 합니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 타입 | `type` | O | `gcs_delete_object` | 문자열. 객체 삭제 작업입니다. | `gcs_delete_object` |
| 원본 | `source.bucket` | O | 없음 | 비어 있지 않은 문자열. 삭제할 객체가 있는 버킷입니다. | `REPLACE_WITH_TEST_BUCKET` |
| 단일 삭제 | `source.object` | 조건부 | 없음 | 문자열. 정확한 객체 이름. `source.pattern`과 둘 중 정확히 하나만 지정합니다. 앞의 `/`는 제거됩니다. | `cleanup-test/2026-10-11/obsolete.csv` |
| 패턴 삭제 | `source.pattern` | 조건부 | 없음 | 문자열. 전체 객체 이름에 적용하는 대소문자 구분 glob. `*`, `?`, `[...]`를 사용할 수 있습니다. `source.object`와 동시 지정할 수 없습니다. | `cleanup-test/2026-10-11/part-*.csv` |
| 누락 허용 | `options.ignore_if_missing` | X | `false` | 불리언. 일치 객체가 없으면 기본적으로 실패합니다. `true`는 아무 것도 삭제하지 않고 정상 반환합니다. Airflow Task를 SKIPPED로 바꾸는 것은 아닙니다. | `true` |

### 날짜가 고정된 테스트 경로의 패턴 삭제 예시

```yaml
version: 1
vine:
  vine_id: example_gcs_delete_test_files
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
business_date:
  type: daily
runtime:
  gcp_conn_id: google_cloud_default
grape_defaults:
  retries: 0
  execution_timeout_seconds: 1800
grapes:
  - grape_id: delete_test_parts
    type: gcs_delete_object
    source:
      bucket: REPLACE_WITH_TEST_BUCKET
      pattern: "cleanup-test/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/part-*.csv"
    options:
      ignore_if_missing: true
```

### 삭제 범위와 실패 처리

- `source.object`는 존재 여부를 확인한 뒤 삭제합니다. `source.pattern`은 첫 wildcard 이전 경로의 디렉터리 prefix로 목록을 조회한 후 `fnmatch.fnmatchcase`로 전체 객체 이름을 검사합니다.
- `*`는 이 구현에서 `/`까지 매칭할 수 있습니다. `cleanup-test/날짜/*.csv`가 하위 경로의 CSV까지 포함할 수 있으므로 “직속 파일만”으로 이해하지 않습니다.
- 패턴 조회에서는 `/`로 끝나는 디렉터리 마커를 제외합니다. 단일 `source.object` 삭제에는 같은 제외 규칙이 없습니다.
- 패턴은 사전순으로 정렬하여 순차 삭제합니다. 중간 삭제 실패 시 이전 삭제는 롤백하지 않습니다. `ignore_if_missing`은 최초 조회의 일치 건수 0을 허용하는 옵션이며, 조회 이후 다른 프로세스가 삭제한 객체에서 발생하는 모든 오류를 무시하는 옵션은 아닙니다.
- 넓은 패턴을 막는 안전장치가 없습니다. 버킷 전체에 걸친 `*`나 공용 prefix를 사용하지 않습니다.
- 복사 후 삭제를 의도하면 삭제 작업의 `depends_on: [복사_작업_ID]`를 명시합니다. 같은 목록에 배치하는 것만으로는 순서가 보장되지 않습니다.

## 4. Prefix 복사/이동: `gcs_move_prefix`

Prefix 아래의 모든 비디렉터리 객체를 복사하고 각 객체의 크기/CRC32C를 검사합니다. 전체 복사·검증이 완료된 다음 원본 삭제를 시작합니다.

**`delete_source`의 코드 기본값은 `true`입니다.** 최초 적용이나 보관용 복사는 반드시 `false`를 명시하고, 실제 이동이 필요한 경우에만 `true`로 변경합니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 타입 | `type` | O | `gcs_move_prefix` | 문자열. Prefix 단위 복사/이동 작업입니다. | `gcs_move_prefix` |
| 원본 | `source.bucket` | O | 없음 | 문자열. 원본 버킷입니다. | `REPLACE_WITH_SOURCE_BUCKET` |
| 원본 | `source.prefix` | O | 비어 있지 않은 날짜별 경로 권장 | 문자열. 앞의 `/`는 제거됩니다. 빈 prefix는 버킷 전체 이동 방지를 위해 거부합니다. glob이 아니며, `source.pattern`은 지원하지 않습니다. | `input/2026-10-11/` |
| 목적지 | `destination.bucket` | O | 없음 | 문자열. 목적지 버킷입니다. | `REPLACE_WITH_ARCHIVE_BUCKET` |
| 목적지 | `destination.prefix` | X | `""`; 분리된 경로 권장 | 문자열. 앞의 `/`는 제거됩니다. 비우면 목적지 버킷 루트 아래에 상대경로를 생성합니다. | `archive/2026-10-11/` |
| 원본 삭제 | `options.delete_source` | X | 기본 `true`; 초기 검증 `false` 권장 | 불리언. 모든 복사와 검증이 성공한 후 복사한 원본들을 삭제합니다. | `false` |
| 빈 입력 | `options.allow_empty` | X | `false` | 불리언. 입력 목록이 비어 있으면 기본 실패. `true`는 복사·삭제 건수 0으로 정상 반환합니다. | `false` |
| 덮어쓰기 | `options.overwrite` | X | `false` | 불리언. 목적지 객체가 이미 있으면 기본 실패. 교체가 의도된 경우에만 `true`를 사용합니다. | `false` |
| 경로 보존 | `options.preserve_relative_path` | X | `true` | 불리언. `true`면 source prefix를 제외한 상대경로를 보존. `false`면 각 객체의 마지막 파일명만 사용합니다. | `true` |

### 원본을 보존하는 전체 예시

```yaml
version: 1
vine:
  vine_id: example_gcs_prefix_archive_copy
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 10800
business_date:
  type: daily
runtime:
  gcp_conn_id: google_cloud_default
grape_defaults:
  retries: 0
  execution_timeout_seconds: 7200
grapes:
  - grape_id: copy_daily_prefix
    type: gcs_move_prefix
    source:
      bucket: REPLACE_WITH_SOURCE_BUCKET
      prefix: "input/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/"
    destination:
      bucket: REPLACE_WITH_ARCHIVE_BUCKET
      prefix: "archive/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/"
    options:
      delete_source: false
      allow_empty: false
      overwrite: false
      preserve_relative_path: true
```

### 경로와 원본 삭제의 주의사항

- 예를 들어 원본 `input/2026-10-11/region-a/result.csv`는 경로 보존 시 `archive/2026-10-11/region-a/result.csv`가 됩니다. 경로 보존을 끄면 `archive/2026-10-11/result.csv`가 되어 다른 하위 경로의 동명 파일과 충돌할 수 있습니다.
- 원본과 목적지가 같은 객체가 되는 설정, 목적지 prefix가 원본 범위 안에 들어가는 설정을 피합니다. 구현이 이런 겹침을 자동 거부하지 않습니다. 특히 자기 자신을 복사한 뒤 `delete_source: true`로 삭제하는 설정은 위험합니다.
- 복사 중간 실패 시 원본 삭제 단계는 시작하지 않지만, 이미 만들어진 목적지는 남습니다. 삭제 단계의 중간 실패는 일부 원본만 삭제된 상태를 남길 수 있습니다.
- 원본 객체가 prefix 자체와 정확히 같아서 상대경로가 비어 있으면 그 객체는 복사에서 제외됩니다. `/`로 끝나는 디렉터리 마커도 제외합니다.
- 동시 쓰기나 generation 조건부 삭제에 대한 제어는 없습니다. 전송 중 원본을 변경하지 않는 운영 계약이 필요합니다.

## 5. GCS 안에서 CSV 병합: `gcs_merge_csv`

같은 버킷의 여러 CSV 객체를 Airflow 워커의 임시 디스크로 다운로드하여 CSV 레코드 단위로 병합하고, 결과를 같은 버킷에 업로드합니다. SFTP 전송은 하지 않습니다.

**목적지 버킷은 항상 `source.bucket`입니다. `destination.bucket`을 써도 읽지 않습니다.** 다른 버킷에 결과가 필요하면 병합 후 `gcs_copy_object` 작업을 연결합니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 타입 | `type` | O | `gcs_merge_csv` | 문자열. GCS CSV 병합 작업입니다. | `gcs_merge_csv` |
| 원본 | `source.bucket` | O | 없음 | 비어 있지 않은 문자열. 입력과 결과가 함께 위치하는 버킷입니다. | `REPLACE_WITH_EXPORT_BUCKET` |
| 원본 | `source.prefix` | O | 비어 있지 않은 전용 입력 경로 | 비어 있지 않은 문자열. 빈 값 또는 `/`만인 값은 거부합니다. 앞의 `/`는 제거됩니다. | `shards/2026-10-11/` |
| 필터 | `source.pattern` | X | `*.csv` | 비어 있지 않은 문자열. Prefix를 제거한 상대경로 또는 전체 객체 이름 중 하나라도 일치하면 포함하는 대소문자 구분 glob입니다. | `part-*.csv` |
| 목적지 | `destination.object` | O | 입력 prefix 외부 경로 권장 | 비어 있지 않은 문자열. 같은 버킷 안의 완전한 결과 객체 이름입니다. | `merged/2026-10-11/result.csv` |
| CSV 헤더 | `options.csv_header` | X | `true` | 불리언. 각 입력 CSV의 첫 레코드를 헤더로 간주하고 첫 번째로 만나는 헤더 한 번만 출력합니다. 데이터 행수에서 헤더를 제외합니다. | `true` |
| CSV 인코딩 | `options.csv_encoding` | X | `utf-8` | 비어 있지 않은 문자열. 입력 읽기와 결과 쓰기에 함께 사용합니다. | `utf-8` |
| CSV 구분자 | `options.csv_delimiter` | X | `","` | 입력·출력에 같은 한 글자 문자열을 사용합니다. 파이프, 탭, 세미콜론, 공백 등 지원. CR/LF/NUL/큰따옴표, 빈 문자열, 여러 글자, 비문자열은 거부합니다. | `"\|"` |
| 원본 삭제 | `options.delete_source` | X | `false` | 불리언. 데이터 업로드 및 요청된 FIN 생성이 완료된 다음 선택된 입력 객체만 삭제합니다. | `false` |
| 빈 입력 | `options.allow_empty` | X | `false` | 불리언. 매칭 입력이 없으면 기본 실패. `true`는 결과 CSV/FIN 생성 없이 건수 0으로 정상 반환합니다. | `false` |
| 덮어쓰기 | `options.overwrite` | X | `false` | 불리언. 결과 CSV와 요청된 FIN 객체의 교체 허용 여부입니다. | `false` |
| 행수 FIN | `options.fin_filename` | X | 없음 | 문자열 또는 null. 같은 버킷 안의 완전한 FIN 객체 이름. 내용은 병합 결과의 데이터 행수와 줄바꿈입니다. | `merged/2026-10-11/result_rows.fin` |
| 파일수 FIN | `options.file_count_fin_filename` | X | 없음 | 문자열 또는 null. 같은 버킷 안의 완전한 FIN 객체 이름. 최종 데이터 객체 수 `1`과 줄바꿈을 기록합니다. 입력 shard 개수가 아닙니다. | `merged/2026-10-11/result_files.fin` |

### 원본 보존 + 두 종류 FIN 생성 전체 예시

```yaml
version: 1
vine:
  vine_id: example_gcs_merge_with_fin
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 10800
business_date:
  type: daily
runtime:
  gcp_conn_id: google_cloud_default
  impersonation_chain: null
grape_defaults:
  retries: 0
  execution_timeout_seconds: 7200
grapes:
  - grape_id: merge_daily_csv
    type: gcs_merge_csv
    source:
      bucket: REPLACE_WITH_EXPORT_BUCKET
      prefix: "shards/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/"
      pattern: "part-*.csv"
    destination:
      object: "merged/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/result.csv"
    options:
      csv_header: true
      csv_encoding: utf-8
      csv_delimiter: ","
      delete_source: false
      allow_empty: false
      overwrite: false
      fin_filename: "merged/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/result_rows.fin"
      file_count_fin_filename: "merged/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/result_files.fin"
```

### 병합·검증·FIN·삭제의 순서

1. Prefix 목록에서 `/`로 끝나는 이름과 `destination.object` 자체를 제외하고 glob 조건을 적용합니다. 선택된 이름을 사전순 정렬합니다.
2. 파일들을 임시 디스크에 다운로드하고 Python `csv.reader` / `csv.writer`로 처리합니다. 기본 쉼표 CSV이며, 출력 줄바꿈은 `\n`입니다. 구분자는 `options.csv_delimiter`로 설정하며, quote dialect를 YAML로 바꾸는 옵션은 없습니다.
3. `csv_header: true`이면 각 파일의 첫 레코드를 제거하되 첫 헤더 한 번만 출력합니다. 입력별 헤더 내용·열 수·스키마의 동일 여부는 검사하지 않습니다.
4. 입력 데이터 레코드 수의 합과 병합 파일을 다시 읽은 데이터 레코드 수가 같은지 검사합니다. 인용부호 안의 줄바꿈은 CSV 레코드 기준으로 처리합니다.
5. 병합 결과를 업로드하고 GCS 객체가 존재하는지 확인합니다. 이 작업 자체가 업로드 후 원본 대비 크기/CRC32C를 비교하는 것은 아닙니다.
6. 요청한 FIN을 `text/plain`, 내용 `정수\n`으로 순서대로 생성합니다. 행수 FIN → 파일수 FIN 순서입니다.
7. `delete_source: true`인 경우 선택했던 입력 객체들을 삭제합니다.

### 운영 한계 및 재실행

- 임시 디스크에 병합 파일과 다운로드한 shard 파일들을 함께 보관합니다. 큰 입력은 Airflow 워커 디스크 용량·실행시간을 먼저 산정합니다. 분산 처리나 GCS compose 방식이 아닙니다.
- 기존 목적지 데이터 충돌은 업로드 전에 확인하지만, 기존 FIN 충돌은 데이터 업로드 후 해당 FIN 생성 시 확인합니다. 따라서 FIN 충돌로 실패해도 결과 CSV가 이미 생성되었을 수 있습니다.
- 결과 CSV, 행수 FIN, 파일수 FIN은 서로 다른 이름을 사용합니다. 입력 경로와도 분리합니다. 이름 충돌을 포괄적으로 검증하지 않으며, 특히 `overwrite: true`이면 CSV를 FIN 내용으로 덮어쓸 수도 있습니다.
- 파일들이 비어 있거나 헤더만 있어도 “객체 목록 없음”과 다릅니다. 입력이 존재하면 데이터 행수 `0`인 결과와 FIN을 생성할 수 있습니다.
- `*`는 `/`도 매칭할 수 있고, 패턴은 상대 이름과 전체 이름 모두에 적용합니다. 정확한 shard 이름 규칙으로 필터를 제한합니다.
- 실패 시 결과 CSV/FIN/이미 삭제한 원본의 롤백은 없습니다. `delete_source: true` 이후 동일 업무일자 재실행은 입력 부재로 실패하거나, `allow_empty: true`이면 아무 결과도 재검증하지 않고 정상 반환할 수 있습니다.
- 입력을 생성하는 작업이 끝난 뒤 병합하도록 의존성을 설정하고, 병합 중 같은 prefix에 파일을 추가하거나 교체하지 않습니다.

## 6. Runner VM 경유 SFTP 전송: `gcs_to_sftp_runner`

Composer가 `SSHOperator`로 Runner VM에 접속하여 Python 프로그램을 실행합니다. Runner 프로그램이 자신의 Google 인증으로 GCS 데이터를 읽고, 별도의 SFTP 인증으로 목적지 서버에 전송합니다.

### 6.1 세 가지 인증을 구분하기

1. **Composer → Runner VM:** YAML `runner.ssh_conn_id`가 가리키는 Airflow SSH Connection을 사용합니다. 접속 대상은 최종 SFTP 서버가 아니라 Runner VM입니다. Connection의 호스트·로그인 사용자·SSH 인증을 구성하고 Runner 계정으로 기본 실행 경로를 실행할 수 있어야 합니다. Runner 계정의 `~/.ssh/authorized_keys`는 이 인바운드 SSH 인증에 쓰입니다.
2. **Runner → Google Cloud:** Runner VM/Application Default Credentials(ADC)를 사용합니다. Runner 설정의 `runner.service_account`가 있으면 그 서비스 계정으로 impersonation합니다. 원래 ADC 주체는 대상 서비스 계정에 대한 토큰 생성 권한이 필요하며, 실제 사용되는 주체는 GCS 객체 조회·목록 권한 및 필요한 Secret Manager 접근 권한을 가져야 합니다. 원본 삭제를 켜면 객체 삭제 권한도 필요합니다.
3. **Runner → SFTP 목적지:** `destination.target`으로 Runner `targets.json`의 항목을 선택합니다. 그 항목의 `username`, `auth_type`, Secret Manager 비밀번호 참조 또는 개인키 파일로 인증합니다. `known_hosts`로 최종 SFTP 서버의 호스트 키를 확인합니다. Composer → Runner용 SSH 키와 목적지용 비밀번호/개인키는 다른 인증입니다.

Vine `runtime.gcp_conn_id` 또는 `runtime.impersonation_chain`을 수정해도 Runner의 Google 인증은 바뀌지 않습니다. Runner 서비스 계정과 설정 파일을 별도로 관리해야 합니다. Composer에 호환되는 `apache-airflow-providers-ssh`가 설치되어 있어야 하며, 없으면 작업 생성 시 ImportError가 발생합니다.

### 6.2 전송 YAML 필드

아래 공통 옵션들은 §1의 `retries`, `retry_delay_seconds`, `execution_timeout_seconds`, `pool`, `priority_weight`와 함께 사용합니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 타입 | `type` | O | `gcs_to_sftp_runner` | 문자열. Runner VM에서 GCS → SFTP 전송을 실행합니다. | `gcs_to_sftp_runner` |
| Runner 연결 | `runner.ssh_conn_id` | O | 없음 | 비어 있지 않은 문자열. Composer → Runner용 Airflow SSH Connection ID입니다. | `conn-sftp-runner` |
| 실행 명령 | `runner.command` | X | `/engn/sftp-runner/venv/bin/python /engn/sftp-runner/app/gcs_to_sftp.py` | 문자열. Runner에서 실행할 명령 접두사. 프레임워크가 뒤에 CLI 인자를 붙입니다. 신뢰할 수 있는 관리 설정만 사용합니다. | `/engn/sftp-runner/venv/bin/python /engn/sftp-runner/app/gcs_to_sftp.py` |
| 원본 | `source.bucket` | O | 없음 | 비어 있지 않은 문자열. Runner가 읽을 GCS 버킷입니다. | `REPLACE_WITH_OUTBOUND_BUCKET` |
| 원본 | `source.prefix` | O | 날짜별 전용 입력 prefix 권장 | 비어 있지 않은 문자열. Runner에서 앞의 `/`를 제거합니다. 제거 후 비어 있으면 전체 버킷 전송을 거부합니다. | `shards/2026-10-11/` |
| 목적지 | `destination.target` | O | 없음 | 비어 있지 않은 문자열. Runner 설정의 `targets` 하위 키와 정확히 일치해야 합니다. | `partner-test` |
| 목적지 | `destination.remote_dir` | O | 명시적 전용 디렉터리 권장 | 비어 있지 않은 문자열. SFTP 서버의 출력 디렉터리. 하위 디렉터리를 생성하므로 쓰기·디렉터리 생성 권한이 필요합니다. | `/data/inbound/daily/2026-10-11` |
| 빈 입력 | `options.allow_empty` | X | `false` | 불리언. 입력 객체가 없으면 실패. `true`는 SFTP 접속이나 FIN 생성 없이 종료 코드 0으로 끝납니다. | `false` |
| 원본 삭제 | `options.delete_source` | X | `false` | 불리언. 모든 데이터 전송 및 병합 모드의 요청된 FIN 전송이 끝난 후 GCS 원본을 삭제합니다. | `false` |
| 덮어쓰기 | `options.overwrite` | X | `false` | 불리언. 최종 원격 파일이 있으면 실패. `true`면 교체를 허용합니다. 데이터와 FIN 모두에 적용됩니다. | `false` |
| SSH 연결 제한 | `options.conn_timeout_seconds` | X | `30` | 정수, 초. Composer → Runner SSH 연결 timeout. Runner → SFTP의 timeout과 별개입니다. | `30` |
| SSH 실행 제한 | `options.execution_timeout_seconds` | X | `7200` | 정수, 초. SSH 명령 `cmd_timeout`과 Airflow `execution_timeout`에 모두 적용합니다. `get_pty`는 코드에서 `false` 고정입니다. | `7200` |

**일반 전송 모드에는 `source.pattern` 필터가 없습니다.** Prefix 아래의 모든 비디렉터리 객체를 전송하며, prefix 이후 상대경로를 보존합니다. `preserve_relative_path`, `destination.filename`, CSV/FIN 옵션으로 일반 전송의 파일명을 바꿀 수 없습니다.

### 6.3 병합 및 FIN 옵션: 권장 형식

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 병합 활성화 | `options.merge_filename` | 병합 시 필수 또는 별칭 사용 | 없음 | 비어 있지 않은 문자열. 지정하면 CSV 병합 모드로 실행합니다. `destination.remote_dir` 아래 최종 파일명. 경로 없는 파일명을 권장합니다. | `result_2026-10-11.csv` |
| CSV 헤더 | `options.csv_header` | X | 단축 형식은 기본 `false`; 명시 권장 | 불리언. `merge_filename`으로 직접 켜는 형식에서는 생략하면 `--csv-header`를 붙이지 않아 헤더 없는 CSV로 처리합니다. 헤더가 있으면 반드시 `true`를 지정합니다. | `true` |
| CSV 인코딩 | `options.csv_encoding` | X | `utf-8` | 비어 있지 않은 문자열. 병합 입력 및 출력 인코딩입니다. FIN은 별도로 UTF-8입니다. | `utf-8` |
| CSV 구분자 | `options.csv_delimiter` | X | `","` | 입력·출력에 같은 한 글자 문자열을 사용합니다. 파이프, 탭, 세미콜론, 공백 등 지원. CR/LF/NUL/큰따옴표, 빈 문자열, 여러 글자, 비문자열은 거부합니다. | `"\|"` |
| 업로드 방식 | `options.upload_mode` | X | `atomic` | `atomic` 또는 `direct`. Direct는 remove/rename 없이 최종 경로에 기록하며 원자성을 보장하지 않습니다. | `direct` |
| 행수 FIN | `options.fin_filename` | X | 없음 | 비어 있지 않은 문자열 또는 null. 병합 모드에서만 사용합니다. 내용은 헤더를 제외한 병합 데이터 행수와 줄바꿈이며, SFTP 목적지에 생성합니다. | `result_rows_2026-10-11.fin` |
| 파일수 FIN | `options.file_count_fin_filename` | X | 없음 | 비어 있지 않은 문자열 또는 null. 병합 모드에서만 사용합니다. 내용은 최종 데이터 파일수 `1`과 줄바꿈입니다. | `result_files_2026-10-11.fin` |
| 검증 | `options.verification.row_count` | X | `true`, 비활성화 불가 | 불리언. 병합 결과 행수 검증은 필수입니다. 병합 모드에서 `false`로 지정하면 작업 생성 시 오류입니다. | `true` |

- `options.merge_filename`이 없고 아래의 별칭도 병합 파일명으로 해석되지 않으면 일반 전송 모드입니다. FIN 옵션만 넣어도 병합이나 FIN 생성이 자동 활성화되지 않습니다.
- 병합 모드도 prefix 아래의 **모든** 비디렉터리 객체를 CSV로 읽습니다. `.csv` 필터나 확장자 검사가 없으므로 FIN, ZIP, 로그, 이전 병합 결과를 같은 입력 prefix에 두지 않습니다.
- 행수 FIN과 파일수 FIN은 독립적으로 선택할 수 있습니다. 모두 생략, 행수만, 파일수만, 두 종류 모두를 지원합니다.

### 6.4 지원되는 별칭과 우선순위

기존 설정과의 호환을 위한 중첩 형식입니다. 신규 설정은 `merge_filename`, `csv_header`, `csv_encoding`, `fin_filename`을 직접 명시하는 쪽이 기본값을 이해하기 쉽습니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 병합 별칭 | `options.merge.enabled` | X | `false` | 불리언. 유효 옵션의 `merge_filename`이 null/미지정일 때만 적용합니다. `true`이면 별칭 파일명으로 병합 설정을 시도합니다. | `true` |
| 병합 별칭 | `destination.filename` | 별칭 병합 시 조건부 | 없음 | 문자열. `merge.enabled: true`일 때 `options.merge.filename`보다 우선하는 병합 파일명입니다. 단독으로는 병합을 활성화하지 않습니다. | `result.csv` |
| 병합 별칭 | `options.merge.filename` | 별칭 병합 시 조건부 | 없음 | 문자열. `destination.filename`이 없거나 빈 값일 때 쓰는 병합 파일명입니다. 직접 `merge_filename`이 있으면 사용하지 않습니다. | `result.csv` |
| 헤더 별칭 | `options.merge.header` | X | 별칭 병합 경로에서 `true` | 불리언. 별칭 병합을 해석하는 경로에서 유효 `csv_header`가 null/미지정일 때만 사용합니다. | `true` |
| 인코딩 별칭 | `options.merge.encoding` | X | `utf-8` | 문자열. 별칭 병합 경로에서 유효 옵션에 `csv_encoding` 키 자체가 없을 때만 사용합니다. 공통 기본값에 키가 있으면 그 값이 우선합니다. | `utf-8` |
| FIN 별칭 | `options.fin.enabled` | X | `false` | 불리언. 병합이 활성화되어 있고 유효 `fin_filename`이 null/미지정일 때만 적용합니다. | `true` |
| FIN 별칭 | `options.fin.filename` | 별칭 FIN 사용 시 조건부 | 없음 | 문자열. `fin.enabled: true`일 때 사용할 행수 FIN 파일명. 파일명을 생략하면 FIN은 생성되지 않습니다. | `result_rows.fin` |

우선순위는 다음과 같습니다.

1. `grape_defaults`와 `options`를 먼저 재귀 병합합니다.
2. 유효 `merge_filename`이 null이 아니면 직접 형식을 사용합니다. 별칭의 `merge.enabled: false`로도 이미 지정된 직접 파일명을 끌 수 없습니다.
3. 직접 파일명이 null/미지정이고 `merge.enabled: true`이면 `destination.filename` → `merge.filename` 순서로 파일명을 선택합니다. 둘 다 없으면 병합 명령이 생성되지 않고 일반 전송으로 남을 수 있으므로 파일명을 반드시 지정합니다.
4. `csv_header`는 위 별칭 해석 경로에서만 `merge.header` 및 기본 `true`로 보완합니다. 따라서 직접 형식의 헤더 기본값과 다릅니다.
5. 병합이 활성화된 경우에만 `fin_filename` → (`fin.enabled: true`인 경우 `fin.filename`) 순으로 행수 FIN 파일명을 결정합니다. 파일수 FIN에는 중첩 별칭이 없습니다.

### 6.5 일반 전송 전체 예시

입력의 하위 경로를 보존하며 원본은 남깁니다. CSV 외 파일도 전송할 수 있습니다.

```yaml
version: 1
vine:
  vine_id: example_sftp_transfer
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 10800
business_date:
  type: daily
grape_defaults:
  retries: 0
  retry_delay_seconds: 60
  execution_timeout_seconds: 7200
grapes:
  - grape_id: send_daily_files
    type: gcs_to_sftp_runner
    runner:
      ssh_conn_id: conn-sftp-runner
    source:
      bucket: REPLACE_WITH_OUTBOUND_BUCKET
      prefix: "ready/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/"
    destination:
      target: partner-test
      remote_dir: "/data/inbound/daily/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}"
    options:
      allow_empty: false
      delete_source: false
      overwrite: false
      conn_timeout_seconds: 30
```

### 6.6 병합 + 행수/파일수 FIN 전체 예시

```yaml
version: 1
vine:
  vine_id: example_sftp_merge_with_fin
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 10800
business_date:
  type: daily
grape_defaults:
  retries: 0
  retry_delay_seconds: 60
  execution_timeout_seconds: 7200
grapes:
  - grape_id: merge_and_send_daily_csv
    type: gcs_to_sftp_runner
    runner:
      ssh_conn_id: conn-sftp-runner
    source:
      bucket: REPLACE_WITH_OUTBOUND_BUCKET
      prefix: "csv-shards/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/"
    destination:
      target: partner-test
      remote_dir: "/data/inbound/daily/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}"
    options:
      merge_filename: "result_{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}.csv"
      csv_header: true
      csv_encoding: utf-8
      csv_delimiter: ","
      fin_filename: "result_rows_{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}.fin"
      file_count_fin_filename: "result_files_{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}.fin"
      verification:
        row_count: true
      allow_empty: false
      delete_source: false
      overwrite: false
```

위 예시는 두 FIN을 모두 생성합니다.

- 행수 FIN만 필요하면 `file_count_fin_filename`을 제거합니다.
- 파일수 FIN만 필요하면 `fin_filename`을 제거합니다.
- FIN이 필요하지 않으면 두 키 모두 제거합니다.

### 6.7 Runner 설정 파일: YAML 외부 설정

다음 설정은 Vine YAML이 아니라 **Runner VM의 JSON 파일**입니다. 표의 `JSON:` 및 `환경변수:` 표시는 YAML에 넣는 키가 아니라는 뜻입니다.

기본 파일 경로는 `/engn/sftp-runner/conf/targets.json`이며, Runner 프로세스의 `SFTP_RUNNER_CONFIG` 환경변수로 경로를 변경할 수 있습니다. 애플리케이션·설정은 `/engn`, 임시 데이터는 `/data`, 로그는 `/logs` 아래가 기본입니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 설정 파일 | 환경변수: `SFTP_RUNNER_CONFIG` | X | `/engn/sftp-runner/conf/targets.json` | 문자열. Runner 프로그램이 시작할 때 읽는 설정 파일 경로. Vine 필드가 아닙니다. | `/engn/sftp-runner/conf/targets.json` |
| GCP 프로젝트 | JSON: `runner.project_id` | 조건부 | ADC 탐지값; 비밀번호 인증 시 명시 권장 | 문자열. GCS 클라이언트 프로젝트. 없으면 ADC 탐지 프로젝트를 사용합니다. 단, Secret Manager 프로젝트의 fallback은 이 명시값만 사용합니다. | `REPLACE_WITH_PROJECT` |
| GCP 실행 주체 | JSON: `runner.service_account` | X | 미지정 시 VM/ADC 직접 사용 | 문자열. 지정 시 Runner의 원래 ADC에서 이 서비스 계정으로 impersonation합니다. Vine의 `runtime.impersonation_chain`과 다릅니다. | `sftp-runner@REPLACE_WITH_PROJECT.iam.gserviceaccount.com` |
| 토큰 수명 | JSON: `runner.impersonation_lifetime_seconds` | X | `3600` | 정수, 초. `service_account`를 지정한 경우 300 이상 3600 이하만 허용합니다. 미지정 직접 ADC 경로에서는 이 옵션을 사용하지 않습니다. | `3600` |
| 임시 경로 | JSON: `runner.temp_dir` | X | `/data/sftp-runner/tmp` | 문자열. 임시 다운로드·병합 파일 경로. Runner 계정에 생성/쓰기/삭제 권한과 충분한 공간이 필요합니다. | `/data/sftp-runner/tmp` |
| 로그 경로 | JSON: `runner.log_dir` | X | `/logs/sftp-runner` | 문자열. `sftp-runner.log`를 만들 디렉터리. 파일 및 표준 출력으로 로그를 남깁니다. | `/logs/sftp-runner` |
| 호스트 검증 | JSON: `runner.known_hosts` | X | `/engn/sftp-runner/conf/known_hosts` | 문자열. SFTP 목적지 호스트 키 파일. 비밀번호 인증에서도 필요합니다. 알 수 없는 호스트 키는 RejectPolicy로 거부합니다. | `/engn/sftp-runner/conf/known_hosts` |
| 대상 이름 | JSON: `targets.<이름>` | O | 없음 | 매핑. YAML `destination.target`으로 선택하는 대상 설정입니다. | `targets.partner-test` |
| SFTP 호스트 | JSON: `targets.<이름>.host` | O | 없음 | 문자열. Runner에서 접근 가능한 SFTP 서버 주소입니다. | `sftp.partner.example` |
| SFTP 포트 | JSON: `targets.<이름>.port` | X | `22` | 정수. SFTP SSH 포트입니다. | `22` |
| SFTP 계정 | JSON: `targets.<이름>.username` | O | 없음 | 문자열. 최종 SFTP 서버의 로그인 계정입니다. | `batch_user` |
| SFTP 인증 방식 | JSON: `targets.<이름>.auth_type` | X | `password` | 문자열. 대소문자를 소문자로 변환한 뒤 `password` 또는 `private_key`만 허용합니다. | `password` |
| 비밀번호 참조 | JSON: `targets.<이름>.password_secret` | 비밀번호 인증 시 필수 | 없음 | 문자열. Secret Manager secret ID만 입력합니다. 값은 `versions/latest`에서 UTF-8로 읽습니다. JSON/YAML에 비밀번호 자체를 저장하지 않습니다. | `partner-sftp-password` |
| Secret 프로젝트 | JSON: `targets.<이름>.secret_project_id` | 조건부 | `runner.project_id` | 문자열. 비밀번호 secret의 프로젝트. 이 키와 `runner.project_id`가 모두 없으면 ADC가 프로젝트를 탐지했어도 비밀번호 인증은 실패합니다. | `REPLACE_WITH_SECRET_PROJECT` |
| 개인키 경로 | JSON: `targets.<이름>.key_file` | 개인키 인증 시 필수 | 없음 | 문자열. Runner 로컬 개인키 파일 경로. `auth_type: private_key`에서만 사용합니다. SSH agent/기본 키 검색은 꺼져 있습니다. | `/engn/sftp-runner/conf/keys/partner-test` |
| TCP 접속 제한 | JSON: `targets.<이름>.connect_timeout_seconds` | X | `30` | 정수, 초. Runner → SFTP 연결 timeout입니다. | `30` |
| SSH 배너 제한 | JSON: `targets.<이름>.banner_timeout_seconds` | X | `30` | 정수, 초. SFTP 대상의 SSH 배너 대기 제한입니다. | `30` |
| SSH 인증 제한 | JSON: `targets.<이름>.auth_timeout_seconds` | X | `30` | 정수, 초. SFTP 대상의 인증 대기 제한입니다. | `30` |
| SFTP 접속 재시도 | JSON: `targets.<이름>.connect_max_attempts` | X | `3` | 정수, 1 이상. SSH 접속 및 SFTP subsystem 열기를 포함한 총 시도 횟수입니다. Airflow retries와 별개입니다. | `3` |
| SFTP 접속 재시도 간격 | JSON: `targets.<이름>.connect_retry_seconds` | X | `5` | 정수, 초, 0 이상. SFTP 접속 시도 사이의 대기 시간입니다. | `5` |

비밀번호 방식의 설정 예시:

```json
{
  "runner": {
    "project_id": "REPLACE_WITH_PROJECT",
    "service_account": "sftp-runner@REPLACE_WITH_PROJECT.iam.gserviceaccount.com",
    "impersonation_lifetime_seconds": 3600,
    "temp_dir": "/data/sftp-runner/tmp",
    "log_dir": "/logs/sftp-runner",
    "known_hosts": "/engn/sftp-runner/conf/known_hosts"
  },
  "targets": {
    "partner-test": {
      "host": "sftp.partner.example",
      "port": 22,
      "username": "batch_user",
      "auth_type": "password",
      "secret_project_id": "REPLACE_WITH_SECRET_PROJECT",
      "password_secret": "partner-sftp-password",
      "connect_timeout_seconds": 30,
      "banner_timeout_seconds": 30,
      "auth_timeout_seconds": 30,
      "connect_max_attempts": 3,
      "connect_retry_seconds": 5
    }
  }
}
```

개인키 방식은 해당 target에서 `auth_type`을 `private_key`로 바꾸고 `key_file`을 지정합니다. 이 경로에서 `password_secret`은 사용하지 않습니다. 현재 구현에는 키 암호(passphrase)를 별도로 전달하는 설정이 없으므로 암호화된 개인키 사용 가능 여부를 배포 전에 확인합니다. 검증된 호스트 키를 `known_hosts`에 등록하고 네트워크·방화벽·SFTP subsystem 설정을 점검합니다. 호스트 키 검증을 우회하는 옵션은 제공하지 않습니다.

### 6.8 전송 순서, 검증 및 실패 시 상태

**일반 모드**

1. GCS prefix의 비디렉터리 객체를 조회합니다.
2. 대상 설정을 찾고 Runner → SFTP 접속을 엽니다.
3. 각 객체를 하나씩 Runner 임시 파일로 다운로드합니다.
4. 기본 `atomic` 모드에서는 원격 `최종경로.uploading`에 업로드하고 크기를 비교한 뒤 최종 파일명으로 rename합니다. `direct` 모드는 최종 경로에 바로 기록하고 크기를 확인합니다. 임시 로컬 파일은 각 전송 후 제거합니다.
5. 모든 객체 전송이 성공한 뒤 `delete_source: true`이면 전송 완료된 원본들을 삭제합니다.

**병합 모드**

1. 조회한 GCS 객체를 이름 사전순으로 병합합니다. 각 shard는 처리 후 임시 파일을 삭제합니다.
2. 각 입력의 CSV 데이터 행수 합과 로컬 병합 파일의 데이터 행수를 비교합니다. 헤더 옵션은 각 입력에 동일하게 적용하며 헤더 일치·스키마는 검증하지 않습니다.
3. 병합 데이터 한 개를 선택한 업로드 방식으로 전송하고 크기를 확인합니다. 기본 `atomic` 모드는 `.uploading`에서 최종 이름으로 바꾸며, `direct`는 최종 경로에 바로 기록합니다.
4. 요청한 행수 FIN과 파일수 FIN을 같은 방식으로 각각 전송합니다. FIN은 각각 UTF-8 `정수\n` 내용입니다.
5. 모든 요청된 전송이 끝난 뒤 원본 삭제를 수행합니다.

공통 운영 주의사항:

- 원격 업로드 검증은 **크기 비교**입니다. 원격 파일을 다시 읽어 CRC32C/해시를 비교하거나 원격 CSV 행수를 재검증하지 않습니다. 병합 행수 검증은 Runner 로컬에서 수행합니다.
- `atomic` 모드의 `.uploading` 파일은 기존에 있으면 먼저 삭제합니다. `overwrite: true`는 최종 파일도 지운 뒤 rename하므로 파일 교체 전 구간의 원자성을 보장하지 않습니다. 데이터+두 FIN 전체를 하나의 트랜잭션으로 처리하는 것도 아닙니다.
- 여러 실행이 같은 최종 이름/`.uploading` 이름을 공유하면 경합할 수 있습니다. 날짜별 경로·파일명을 분리하고 동시 실행을 통제합니다. 한 Vine의 `max_active_runs: 1`만으로 다른 Vine과의 충돌까지 막지는 못합니다.
- 데이터 업로드 후 FIN 충돌이나 전송 실패가 발생하면 데이터는 남습니다. 원격 파일, FIN, 이미 삭제한 GCS 원본의 자동 롤백은 없습니다. 실패한 `.uploading`이 남을 수 있습니다.
- Runner 접속 재시도는 SSH/SFTP 연결 단계용입니다. Secret 조회나 `known_hosts` 파일 로드 오류, 전송 중간 실패를 모두 재시도하는 일반 재시도 기능이 아닙니다. Airflow의 `retries`는 전체 Runner 명령을 다시 실행하므로 기존 최종 파일 충돌을 고려합니다.
- 일반 전송에서는 source prefix 자체와 같은 이름의 객체로 상대경로가 비어 있으면 해당 객체를 건너뜁니다. 병합 모드는 조회된 모든 객체를 포함합니다.
- `remote_dir`에는 `/data/inbound`처럼 루트가 아닌 명시적 경로를 권장합니다. 현재 경로 결합은 끝의 `/`를 모두 제거하므로 `remote_dir: "/"`는 의도와 달리 상대경로를 만들 수 있습니다.
- 병합/FIN 파일명은 서로 다른 단순 파일명을 사용합니다. 절대경로나 `..`가 포함되면 지정한 `remote_dir` 바깥으로 경로가 해석될 수 있으며 구현이 이를 거부하지 않습니다. 일반 전송의 GCS 상대 객체 이름도 안전한 경로 규칙으로 관리합니다.
- Runner 로컬 디스크에 병합 결과와 현재 처리 중인 shard를 저장할 공간이 필요합니다. 로그 파일 회전은 이 프로그램에서 구현하지 않으므로 별도 운영 정책을 적용합니다.
- Airflow timeout/취소 발생 시 원격 프로세스와 생성 파일의 실제 상태를 확인한 후 재실행합니다. 코드에 별도의 작업 ID 기반 원격 취소·복구·중복 실행 잠금 프로토콜은 없습니다.
- SFTP에서 만든 FIN은 **SFTP 서버의 파일**입니다. 아래의 `gcs_fin_file_sensor`로 직접 감지할 수 없습니다. GCS FIN이 필요하면 별도의 명시적 업로드 절차를 설계해야 합니다.

## 7. GCS FIN 파일 대기: `gcs_fin_file_sensor`

`GCSObjectExistenceSensor`로 GCS의 정확한 객체 이름이 존재할 때까지 대기합니다. 이름이 `.fin`으로 끝날 필요는 없고 `_FIN`, `_SUCCESS` 등 약속한 객체 이름을 사용할 수 있습니다.

**FIN 내용, 행수, 파일수, 생성 시각 또는 데이터 파일의 무결성을 확인하는 센서가 아닙니다.** 기존 FIN이 남아 있으면 즉시 통과할 수 있습니다. 업무일자별 객체 이름과 FIN 생성·보관 정책을 함께 설계합니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 타입 | `type` | O | `gcs_fin_file_sensor` | 문자열. GCS 객체 존재 여부 대기 센서입니다. | `gcs_fin_file_sensor` |
| 대상 | `source.bucket` | O | 없음 | 비어 있지 않은 문자열. FIN 객체의 GCS 버킷 이름. 다른 GCS 작업과 달리 이 executor는 앞뒤 공백을 제거한 값을 전달하지 않으므로 깨끗한 이름을 입력합니다. | `REPLACE_WITH_INBOUND_BUCKET` |
| 대상 | `source.object` | O | 날짜별 정확한 객체 이름 | 비어 있지 않은 문자열. 감지할 객체 이름. `source.pattern`, prefix 목록, SFTP 경로는 지원하지 않습니다. 앞의 `/`를 자동 제거하지 않습니다. | `ready/2026-10-11/_FIN` |
| 대기 제한 | `options.timeout_seconds` | X | `execution_timeout_seconds` → `21600` | 정수, 초. 센서의 `timeout`과 Airflow `execution_timeout`에 같은 값을 적용합니다. 이 키가 있으면 `execution_timeout_seconds`보다 우선합니다. | `21600` |
| 대기 제한 대체값 | `options.execution_timeout_seconds` | X | `21600` | 정수, 초. 유효 `timeout_seconds`가 없을 때만 사용하는 대체값. `grape_defaults`에 설정한 값도 이 우선순위에 참여합니다. | `21600` |
| 확인 간격 | `options.poke_interval_seconds` | X | `60` | 정수, 초. 센서 확인 간격으로 전달합니다. 정확한 polling/deferral 동작은 설치한 Google provider 버전에 따릅니다. | `60` |
| 센서 모드 | `options.mode` | X | `reschedule` | 문자열. Airflow 센서 mode. 일반적으로 `reschedule` 또는 `poke`. 별도 백엔드 동작은 `deferrable` 설정도 함께 확인합니다. | `reschedule` |
| 비동기 대기 | `options.deferrable` | X | `true` | 불리언. Deferrable 센서 사용 여부. 이를 지원하는 provider 및 동작 중인 Triggerer가 필요합니다. | `true` |
| 실패 처리 | `options.soft_fail` | X | `false` | 불리언. 센서 실패/timeout의 skip 처리는 Airflow/provider 구현에 따릅니다. `true`는 정상 감지로 간주하는 설정이 아니며, downstream이 기본 성공 의존성을 사용하면 건너뛰어질 수 있습니다. | `false` |
| 재시도 | `options.retries` | X | `0` | 정수. 센서 Task 실패 후 재시도 횟수입니다. 공통 `grape_defaults.retries`가 있으면 그 값이 우선합니다. | `0` |
| 재시도 간격 | `options.retry_delay_seconds` | X | `60` | 정수, 초. 센서 재시도 간격입니다. | `60` |
| 리소스 | `options.pool` | X | 미지정 (`None`) | 문자열 또는 null. Airflow pool 이름입니다. | `default_pool` |
| 우선순위 | `options.priority_weight` | X | `1` | 정수. Airflow 스케줄링 우선순위 가중치입니다. | `1` |

GCP 연결과 impersonation은 §1의 `runtime.gcp_conn_id`, `runtime.impersonation_chain`을 사용합니다. `destination`, 원본 삭제, FIN 내용 비교 등의 추가 옵션은 읽지 않습니다.

### FIN 감지 후 데이터 복사 전체 예시

```yaml
version: 1
vine:
  vine_id: example_wait_fin_then_copy
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 32400
business_date:
  type: daily
runtime:
  gcp_conn_id: google_cloud_default
  impersonation_chain: null
grape_defaults:
  retries: 0
  retry_delay_seconds: 60
  pool: default_pool
  priority_weight: 1
grapes:
  - grape_id: wait_daily_fin
    type: gcs_fin_file_sensor
    source:
      bucket: REPLACE_WITH_INBOUND_BUCKET
      object: "ready/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/_FIN"
    options:
      timeout_seconds: 21600
      poke_interval_seconds: 60
      mode: reschedule
      deferrable: true
      soft_fail: false

  - grape_id: copy_ready_data
    type: gcs_copy_object
    depends_on: [wait_daily_fin]
    source:
      bucket: REPLACE_WITH_INBOUND_BUCKET
      object: "ready/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/result.csv"
    destination:
      bucket: REPLACE_WITH_ARCHIVE_BUCKET
      object: "archive/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/result.csv"
    options:
      execution_timeout_seconds: 7200
      overwrite: false
```

### 운영 계약과 시간 제한

- 생산자는 데이터 쓰기를 모두 마친 후 FIN을 마지막으로 생성해야 합니다. 센서는 이 규칙을 검증하지 않습니다.
- 업무일자별 FIN을 사용하더라도 같은 업무일자 재처리 시 기존 FIN을 어떻게 다룰지 결정해야 합니다. 센서는 FIN을 소비하거나 삭제하지 않습니다.
- `timeout_seconds`가 명시되면 공통 `execution_timeout_seconds`보다 우선합니다. 예를 들어 공통값이 7,200초이고 센서 `timeout_seconds`가 없으면 센서 기본 21,600초가 아니라 7,200초로 구성됩니다.
- 센서 6시간 + 후속 처리 시간을 고려하여 Vine의 `dagrun_timeout_seconds`를 정합니다. Root에서 호출한다면 Root의 TriggerDagRun 대기 timeout, Root DAG timeout도 함께 충분히 확보해야 합니다.
- FIN을 기다리는 Vine 완료 후 다른 Vine을 실행하려면 Root의 Vine 간 `depends_on`과 완료 대기 설정을 함께 구성합니다. 저장소의 [Root FIN 연계 예시](../../dags/pipelines/roots/root_fin_sensor_daily/config.yaml)는 `wait_for_completion: true`로 센서 Vine 완료를 기다린 뒤 두 downstream Vine을 호출합니다.
- `deferrable: true`와 `mode: reschedule`을 동시에 전달하지만 둘을 독립적인 누적 절약 옵션으로 해석하지 않습니다. Provider/Triggerer 환경에 맞게 실제 대기·복귀 동작을 테스트합니다.

## 8. 배포 전 점검과 복구 기준

1. 날짜 prefix와 최종 경로를 실제 렌더링한 업무일자로 확인합니다. 공용 경로, 너무 넓은 glob, 원본/목적지 겹침을 제거합니다.
2. `gcs_move_prefix`에는 초기 검증 시 `delete_source: false`를 명시합니다. GCS merge/SFTP도 원본 보존부터 검증합니다.
3. 데이터를 쓰는 작업, 병합/전송, 원본 정리 사이에 `depends_on`을 지정합니다. 특히 같은 CSV 경로를 삭제하는 작업과 병합 작업을 병렬로 두지 않습니다.
4. GCS 4종/센서의 Airflow GCP 인증과 Runner의 ADC/impersonation, 목적지 SFTP 인증을 각각 검증합니다. 연결 ID나 서비스 계정 이름만 YAML에 적는다고 자격증명·IAM 권한이 자동 생성되지는 않습니다.
5. `overwrite: false`, `retries: 0`, 테스트 버킷/테스트 SFTP 경로로 최초 검증합니다. 재시도 및 덮어쓰기를 활성화하려면 부분 완료와 재전송에 대한 소비자 측 처리 규칙을 정합니다.
6. CSV delimiter·인코딩·헤더 유무가 모든 shard에서 같은지 확인합니다. 두 병합 구현 모두 CSV 스키마 일치 검증이 없습니다.
7. 두 FIN과 데이터 파일명이 겹치지 않도록 하고, 병합 입력 prefix와 출력을 분리합니다. FIN의 `1`은 입력 파일 수가 아니라 최종 병합 데이터 파일 수입니다.
8. 입력 없음 정상 종료가 필요한지 명확히 합니다. `allow_empty: true`나 `ignore_if_missing: true`는 후속 데이터/FIN의 존재 또는 무결성을 보증하지 않습니다.
9. 실패 후에는 Task 상태만 보고 다시 실행하지 않습니다. 이미 생성된 목적지 CSV/FIN, `.uploading`, 남은 원본, 삭제 완료 여부를 확인하고 해당 상태에 맞춰 복구합니다.
10. 병합 임시 디스크, Runner 로그 경로, 네트워크 경로, 호스트 키, SFTP 디렉터리 권한, Airflow/provider/Triggerer 구성과 timeout의 전체 관계를 확인합니다.

### 구현 및 예시 출처

이 문서는 아래 구현을 기준으로 작성했습니다. 라이브러리별 세부 동작은 배포 환경의 Airflow/provider 버전과 함께 검증합니다.

- [GCS executor: 복사·삭제·prefix 이동·CSV 병합](../../dags/framework/gcs_executor.py)
- [SFTP Runner 명령 생성 및 별칭 처리](../../dags/framework/sftp_runner_executor.py)
- [Runner 실제 전송 프로그램](../../tools/sftp-runner/gcs_to_sftp.py)
- [Runner target 설정 예시](../../tools/sftp-runner/targets.example.json)
- [GCS FIN 센서 executor](../../dags/framework/fin_file_sensor_executor.py)
- [Vine DAG 생성과 의존성 연결](../../dags/framework/vine_factory.py)
- [옵션 재귀 병합 및 ID/그래프 검증](../../dags/framework/utils.py)
- [GCS 작업 샘플](../../dags/pipelines/vines/ujlim_gcs_operator_sample/config.yaml), [원본 보존 병합 샘플](../../dags/pipelines/vines/ujlim_gcs_merge_keep_source_sample/config.yaml), [원본 삭제 병합 샘플](../../dags/pipelines/vines/ujlim_gcs_merge_delete_source_sample/config.yaml)
- [SFTP 일반 전송 샘플](../../dags/pipelines/vines/ujlim_sftp_runner_sample_daily/config.yaml), [FIN 없는 병합](../../dags/pipelines/vines/ujlim_sftp_merge_no_fin_sample/config.yaml), [행수 FIN](../../dags/pipelines/vines/ujlim_sftp_merge_row_fin_sample/config.yaml), [파일수 FIN](../../dags/pipelines/vines/ujlim_sftp_merge_file_count_fin_sample/config.yaml), [두 FIN](../../dags/pipelines/vines/ujlim_sftp_merge_both_fin_sample/config.yaml)
- [FIN 센서 Vine](../../dags/pipelines/vines/vine_fin_sensor/config.yaml), [FIN 연계 Root](../../dags/pipelines/roots/root_fin_sensor_daily/config.yaml)

저장소 예시는 일부 실제 환경용 값, `overwrite: true`, `delete_source: true` 등을 포함합니다. 그대로 실행하기 전에 이 문서의 안전한 예시와 비교하고 경로·권한·삭제 정책을 확인합니다. 특히 GCS 통합 샘플의 서로 독립적인 작업들은 `depends_on`이 없으면 병렬로 실행될 수 있습니다.


## CSV 구분자 설정 및 배포 순서

`gcs_merge_csv`와 `gcs_to_sftp_runner` 병합의 `options.csv_delimiter`는 같은 의미이며,
생략 시 기존 쉼표 처리를 유지합니다. 모든 입력 파일의 구분자는 같아야 합니다.
병합의 읽기·쓰기, 헤더 처리, 행수 검증 및 row-count FIN에 같은 구분자가 적용됩니다.
따옴표로 감싼 구분자와 필드 안 개행은 Python CSV 처리 규칙으로 보존됩니다.
헤더와 인코딩 옵션의 기존 기본값은 바뀌지 않습니다.

각 예시는 독립적인 options 설정입니다.

```yaml
# 쉼표
options:
  csv_delimiter: ","
```

```yaml
# 파이프
options:
  csv_delimiter: "|"
```

```yaml
# 캐럿
options:
  csv_delimiter: "^"
```

```yaml
# SOH (ASCII 0x01): 큰따옴표 안의 역슬래시 이스케이프
options:
  csv_delimiter: "\x01"
```

SOH는 눈에 보이는 `\x01` 네 글자가 아니라 실제 제어문자 한 글자입니다.
YAML 큰따옴표의 `"\x01"` 또는 `"\u0001"`은 로딩 시 SOH로 변환됩니다.
작은따옴표의 `'\x01'`, 이중 역슬래시의 `"\\x01"`, 실제 원화 기호를 쓴 `"₩x01"`은
여러 글자 문자열이므로 거부됩니다. 일부 한글 환경에서 역슬래시가 원화 모양으로 보일 수 있지만,
설정 파일에는 역슬래시(U+005C)를 사용해야 합니다. 실제 원화 기호(U+20A9)로 바꾸지 마세요.
프레임워크가 문자열 이스케이프를 다시 해석하지 않으므로 YAML 로딩 후 값이 한 글자인지 확인합니다.
Executor가 이 값을 shell 인자 하나로 인용하고 Runner CLI가 같은 문자를 받아 CSV 읽기·쓰기에 사용합니다.
Runner를 직접 실행할 때도 실제 SOH를 전달해야 합니다. Bash에서는 `--csv-delimiter=$'\x01'`을 사용할 수 있으며,
`--csv-delimiter='\x01'`은 네 글자를 전달하므로 거부됩니다.

```yaml
# 탭: 반드시 YAML 큰따옴표의 이스케이프를 사용합니다.
options:
  csv_delimiter: "\t"
```

작은따옴표의 `'\t'`는 두 글자이므로 거부됩니다. 탭이나 공백을 자동으로 trim하지 않습니다.
세미콜론은 `csv_delimiter: ";"`로 지정합니다.
큰따옴표(`"`)는 고정 CSV quote character와 충돌하여 데이터가 손상될 수 있으므로 구분자로 사용할 수 없습니다.
실행 가능한 구조의 예시는
[CSV 구분자 샘플](../../dags/pipelines/vines/ujlim_sftp_csv_delimiter_sample/config.yaml)을 참고하세요.
샘플 경로·연결·대상은 실행 전에 환경에 맞게 바꾸고 실제 파일의 구분자도 맞춰야 합니다.

BigQuery `EXPORT DATA`로 CSV를 만드는 경우 SQL의 `field_delimiter`를 별도로 동일하게 설정해야 합니다.
예를 들어 파이프는 `field_delimiter='|'`입니다. 프레임워크는 SQL을 자동 수정하거나 구분자를 자동 감지하지 않습니다.
앞단에 `gcs_merge_csv`가 있다면 해당 옵션도 같은 값으로 맞추세요.
SFTP 일반 전송(병합 없음)은 파일 바이트를 그대로 전달하며 구분자를 변환하지 않습니다.

Runner VM의 `gcs_to_sftp.py`를 먼저 갱신한 뒤 Composer executor/utils 및 YAML을 배포하세요.
신규 executor는 기본 쉼표를 포함해 `--csv-delimiter`를 전달하므로 구형 Runner는 인자를 거부합니다.
신규 Runner는 인자 생략 시 쉼표를 사용하여 구형 executor와 호환됩니다.
기본 `options.upload_mode: atomic`은 임시 파일 업로드·크기 검증 후 rename 방식입니다.
`options.upload_mode: direct`는 최종 파일명에 직접 기록하며 remove/rename을 호출하지 않습니다.
`overwrite: false`이면 exclusive create로 기존 파일을 덮어쓰지 않고, `true`이면 기존 내용을 잘라내고 다시 씁니다.
Direct 전송은 원자적이지 않아 전송 중이거나 실패한 파일이 노출될 수 있습니다. 수신 측은 데이터 업로드 후 생성되는 FIN을 확인하세요.
이 옵션 역시 신규 Runner를 먼저 배포해야 합니다.

로컬 회귀 테스트(외부 연결 없음, PyYAML 필요):

```sh
python -m unittest discover -s tests -v
```

쉼표·캐럿·파이프·SOH·탭·세미콜론을 대상으로 병합/행수/FIN을 확인합니다.
YAML → executor → 로컬 shell 인자 → Runner CLI 및 `gcs_merge_csv` 경로에서
SOH 한 글자가 유지되는지, 필드 안 구분자·따옴표·개행이 보존되는지 검증합니다.
GCS/SSH/SFTP는 테스트 대역을 사용하며 실제 서비스에는 연결하지 않습니다.
