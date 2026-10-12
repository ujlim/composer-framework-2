# 2. SQL 실행과 PostgreSQL CSV Export

[목차](README.md) · [공통 YAML](01-yaml-common.md) · [다음: 스토리지](03-storage-sftp.md)

이 문서는 현재 `BigQueryExecutor`와 `PostgresExecutor`가 실제로 읽는 YAML 키와 실행 동작을 설명합니다. SQL 파일은 각 Vine의 `config.yaml`이 있는 디렉터리를 기준으로 배치합니다. 예제의 `example-project`, `example-export-bucket`, Connection ID, dataset 및 procedure 이름은 배포 환경에 맞게 바꾸세요. 비밀번호나 키 파일 내용을 YAML에 쓰지 않습니다.

## 지원 유형과 선택 기준

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| BigQuery SQL | `grapes[].type` | O | `bigquery_sql` | 문자열. SQL 파일을 BigQuery 쿼리 작업으로 실행합니다. | `bigquery_sql` |
| BigQuery Procedure | `grapes[].type` | O | `bigquery_procedure` | 문자열. 기존 BigQuery stored procedure 호출문을 생성합니다. | `bigquery_procedure` |
| PostgreSQL SQL | `grapes[].type` | O | `postgres_sql` | 문자열. Composer worker에서 PostgreSQL에 직접 접속해 SQL을 실행합니다. | `postgres_sql` |
| PostgreSQL Export | `grapes[].type` | O | `postgres_to_gcs` | 문자열. 행을 반환하는 PostgreSQL 쿼리를 실행하고 CSV 하나를 GCS에 업로드합니다. | `postgres_to_gcs` |

`postgres_to_gcs`는 Dataflow JDBC Export가 아닙니다. Python DB 드라이버와 Composer worker의 네트워크·메모리·임시 디스크를 사용합니다. 대용량 데이터의 처리는 별도의 JDBC/Dataflow 유형과 비교해 선택하세요.

## 공통 작성 규칙

- Grape 고유값은 `grape_id`입니다. 영문자, 숫자, `.`, `_`, `-`만 사용할 수 있으며 같은 Vine 내에서 중복될 수 없습니다.
- 선행 작업은 `depends_on`에 같은 Vine의 Grape ID 목록으로 씁니다. 생략하면 `resolve_business_date` 다음에 실행됩니다. 없는 ID, 자기 참조, 순환 의존성은 허용되지 않습니다.
- 실행 옵션은 `grape_defaults`를 먼저 읽고 각 `grapes[].options`로 재귀 병합합니다. 같은 키는 Grape 값이 우선합니다. `null`은 기본값으로 되돌리는 특별한 값이 아닙니다.
- 파일 경로는 Vine 디렉터리 내부의 실제 파일이어야 합니다. 바깥으로 나가는 `../` 경로는 거부됩니다. SQL은 UTF-8로 읽으며 DAG 구성 시 파일이 존재해야 합니다.
- YAML 불리언은 `true`/`false`로 씁니다. `"false"`처럼 문자열로 쓰면 Python의 `bool(...)` 변환에서 참으로 처리될 수 있습니다.
- 수동 Vine 실행에는 `business_date`를 입력합니다. SQL에서 Airflow 실행일을 업무기준일이라고 가정하지 말고 `resolve_business_date` XCom을 사용하세요.
- SQL, 템플릿 및 결과가 Airflow의 렌더링 화면, 로그, XCom에 노출될 수 있으므로 인증정보를 SQL 파라미터로 전달하지 않습니다.

## BigQuery: SQL 및 Stored Procedure

### 공통 YAML 파라미터

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 식별자 | `grapes[].grape_id` | O | 없음 | 문자열. Airflow task ID와 기본 BigQuery job label 생성에 사용합니다. | `check_business_date` |
| 유형 | `grapes[].type` | O | 없음 | 문자열. `bigquery_sql` 또는 `bigquery_procedure`입니다. 별칭은 없습니다. | `bigquery_sql` |
| 의존성 | `grapes[].depends_on` | X | `[]` | 문자열 목록. 같은 Vine 내 선행 Grape ID입니다. | `[check_business_date]` |
| 실행 프로젝트 | `runtime.project_id` | X | `None` 전달 / 명시 권장 | 문자열. BigQuery job 실행 프로젝트입니다. 생략 시 provider와 Google 연결의 프로젝트 해석에 의존합니다. 테이블/프로시저가 속한 프로젝트와 별개일 수 있습니다. | `example-project` |
| 실행 위치 | `runtime.location` | X | `asia-northeast3` | 문자열. BigQuery job 위치입니다. 접근할 dataset과 맞춰 지정합니다. | `asia-northeast3` |
| Google 연결 | `runtime.gcp_conn_id` | X | `google_cloud_default` | 문자열. BigQueryInsertJobOperator에 전달할 Airflow Google Connection ID입니다. | `google_cloud_default` |
| 실행 계정 | `runtime.impersonation_chain` | X | `null` | 문자열 또는 문자열 목록. BigQuery operator에 전달할 서비스 계정 또는 impersonation chain입니다. 관련 IAM은 환경에서 미리 구성합니다. | `bq-runner@example-project.iam.gserviceaccount.com` |
| Job label | `runtime.labels` | X | `{}` + 프레임워크 기본 label | 매핑. 키는 문자열 변환만 하고, 값은 소문자·허용 문자·길이 규칙으로 정규화합니다. 같은 기본 label 키를 지정하면 덮어씁니다. | `{environment: dev, workload: daily_report}` |
| SQL 바인딩 | `grapes[].parameters` | X | `{}` | 매핑. 이름별 `{type, value}` 구조의 scalar 파라미터입니다. 목록이나 명시적 `null`은 허용되지 않습니다. | 아래 예제 참고 |
| 파라미터 타입 | `grapes[].parameters.<name>.type` | X | `STRING` | 문자열. 대문자로 변환한 뒤 지원 목록과 비교합니다. 값으로 타입을 추론하지 않습니다. | `DATE` |
| 파라미터 값 | `grapes[].parameters.<name>.value` | 조건부 | 없음 | scalar 값 또는 Jinja 문자열. 키 자체가 필수이며 SQL NULL은 `value: null`로 지정합니다. 불리언은 `"true"`/`"false"`, 그 외 null이 아닌 값은 문자열로 인코딩됩니다. | `"{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}"` |
| 쿼리 우선순위 | `grapes[].options.priority` | X | `INTERACTIVE` | 문자열. 대문자로 바꿔 BigQuery query config에 전달합니다. 통상 `INTERACTIVE` 또는 `BATCH`를 사용하며 executor가 허용값을 자체 검사하지는 않습니다. | `BATCH` |
| 지연 대기 | `grapes[].options.deferrable` | X | `true` | 불리언. BigQuery operator의 deferrable 설정입니다. Airflow/Composer 환경의 triggerer 및 provider 지원을 확인하세요. | `true` |
| 강제 재실행 | `grapes[].options.force_rerun` | X | `false` | 불리언. BigQuery operator의 job ID/재실행 처리에 전달됩니다. 동일 작업의 중복 실행·재연결 동작은 설치된 provider 설정도 확인합니다. | `false` |
| 재시도 | `grapes[].options.retries` | X | `1` | 정수. Airflow task 재시도 횟수입니다. | `0` |
| 재시도 간격 | `grapes[].options.retry_delay_seconds` | X | `300` | 정수, 초. Airflow 재시도 대기시간입니다. | `60` |
| Task 제한 시간 | `grapes[].options.execution_timeout_seconds` | X | `7200` | 정수, 초. Airflow task의 실행 제한시간입니다. | `1800` |
| Pool | `grapes[].options.pool` | X | 미지정(`None`) | 문자열. 기존 Airflow pool 이름입니다. | `default_pool` |
| 우선순위 가중치 | `grapes[].options.priority_weight` | X | `1` | 정수. Airflow scheduler의 task 우선순위로, BigQuery의 `priority`와 별개입니다. | `1` |

모든 `options.*`는 `grape_defaults`에도 같은 키로 설정할 수 있습니다. 기본값 적용 순서는 **executor 기본값 → `grape_defaults` → Grape `options`**입니다. 예를 들어 저장소 `minimal_bq_vine` 샘플은 `force_rerun: true`를 명시하지만 executor 기본값은 `false`입니다. 예제에서 명시한 값을 프레임워크 기본값과 혼동하지 마세요.

BigQuery에 실제 전달하는 query config는 `query`, `useLegacySql: false`, `priority`이며 파라미터가 있을 때만 `parameterMode: NAMED`와 `queryParameters`가 추가됩니다. Legacy SQL은 지원하지 않습니다.

### 지원 타입, 별칭 및 값 인코딩

허용되는 `type`은 다음과 같습니다.

- 문자열/바이트: `STRING`, `BYTES`
- 정수/실수: `INT64`, `INTEGER`, `FLOAT64`, `FLOAT`
- 고정 정밀도: `NUMERIC`, `BIGNUMERIC`
- 불리언: `BOOL`, `BOOLEAN`
- 날짜/시간: `TIMESTAMP`, `DATE`, `TIME`, `DATETIME`
- 기타 scalar: `GEOGRAPHY`, `JSON`

`INTEGER`, `FLOAT`, `BOOLEAN`도 코드의 허용 목록에 있지만, executor가 이를 각각 `INT64`, `FLOAT64`, `BOOL`로 치환하지는 않습니다. 입력을 대문자로 만든 그대로 BigQuery에 전달합니다. 새 YAML에는 `INT64`, `FLOAT64`, `BOOL`처럼 일관된 타입명을 권장합니다.

`ARRAY`와 `STRUCT`는 지원하지 않습니다. 이름은 비어 있지 않은 문자열이어야 하며, 실제 SQL에 쓸 수 있는 단순한 이름(예: `business_date`, `region_code`)을 사용하세요. 이름의 SQL 식별자 문법은 프레임워크가 완전히 검증하지 않습니다.

```yaml
parameters:
  business_date:
    type: DATE
    value: "2026-01-01"
  row_limit:
    type: INT64
    value: 10
  enabled:
    type: BOOL
    value: true
  optional_text:
    type: STRING
    value: null
  metadata_json:
    type: JSON
    value: '{"source":"example","version":1}'
```

타입의 업무상 유효값까지 executor가 검사하지는 않습니다. 잘못된 날짜·숫자·JSON 표현은 BigQuery에서 실패할 수 있습니다. JSON을 사용할 때 YAML 매핑을 `value`로 넣으면 Python의 `str(dict)` 형식이 되어 유효한 JSON이 아닐 수 있으므로 JSON 문자열을 명시하세요. 목록을 `value`로 넣는 것도 ARRAY 바인딩이 되지 않습니다.

### Jinja 렌더링과 BigQuery 바인딩

DAG 구성 시 YAML의 파라미터를 BigQuery API 형식으로 만들고 SQL 파일 본문을 읽습니다. 이후 BigQueryInsertJobOperator가 실행 전에 `configuration`의 SQL과 파라미터 값에 포함된 Jinja를 렌더링합니다. SQL 본문의 `@business_date`는 이 Jinja 단계가 아니라 BigQuery named parameter로 바인딩됩니다.

```yaml
parameters:
  business_date:
    type: DATE
    value: "{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}"
```

```sql
SELECT @business_date AS business_date;
```

값에 SQL 문자열 조합 대신 `@name`을 사용하고 placeholder를 따옴표로 감싸지 않습니다. 테이블·컬럼·프로시저 이름은 값 파라미터로 대체할 수 없습니다. `type`, `priority`, 숫자/불리언 옵션 등은 DAG 구성 때 검사·변환하므로 Jinja 대신 정적 설정으로 씁니다.

### Label 규칙과 권한

기본 job label은 `framework: root-vine-grape`, `vine: <vine_id>`, `grape: <grape_id>`입니다. 자동 label 및 사용자 지정 label의 **값**은 소문자로 변환하고 `[a-z0-9_-]` 이외 문자를 `_`로 바꾼 뒤 최대 63자로 자릅니다. 빈 결과는 `unknown`입니다. 사용자 label의 **키**는 이 정규화 대상이 아니므로 BigQuery에서 허용하는 키를 직접 작성해야 합니다. `runtime.labels`에서 `framework`, `vine`, `grape`를 지정하면 기본 label을 덮어씁니다.

Label 값은 DAG 구성 단계에서 정규화되므로 여기에 `{{ ... }}`를 넣어 런타임 템플릿이 그대로 보존될 것으로 기대하지 마세요. 이스케이프된 실행정보 대신 정적인 운영·환경 분류를 쓰세요.

BigQuery job 생성 프로젝트에 대한 실행 권한, 읽거나 쓸 dataset/table 및 procedure에 대한 필요한 권한을 사전에 구성합니다. IAM 역할·dataset·table·procedure를 이 YAML이 자동으로 생성하거나 권한을 부여하지 않습니다. SQL을 읽기 쿼리에서 DML/DDL로 바꾸거나 procedure 내부 구현을 바꾸면 필요한 권한과 데이터 변경 범위도 달라집니다.

## `bigquery_sql`

### 전용 파라미터

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| SQL 파일 | `grapes[].sql_file` | O | 없음 | 문자열. Vine 디렉터리 내부 UTF-8 SQL 파일입니다. DAG 구성 시 읽습니다. 인라인 `sql` 키는 지원하지 않습니다. | `sql/010_select.sql` |

`bigquery_sql`의 `procedure` 키는 사용되지 않습니다. 반환 결과를 PostgreSQL처럼 `fetch`/`max_fetch_rows`로 XCom에 담는 기능도 없습니다. job 실행 및 provider의 기본 XCom 동작을 사용하며, 이 프레임워크가 조회 결과행을 따로 반환하지는 않습니다.

### 전체 예제: SQL 파일과 named parameter

파일 구성:

```text
pipelines/vines/example_bigquery_sql/
├── config.yaml
└── sql/
    └── 010_select.sql
```

`config.yaml`:

```yaml
version: 1

vine:
  vine_id: example_bigquery_sql
  description: Read-only BigQuery SQL and parameter check
  owner: data-engineering
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 3600
  tags: [example, bigquery]

business_date:
  type: daily

runtime:
  project_id: example-project
  location: asia-northeast3
  gcp_conn_id: google_cloud_default
  impersonation_chain: null
  labels:
    environment: dev

grape_defaults:
  retries: 0
  retry_delay_seconds: 60
  execution_timeout_seconds: 1800
  pool: default_pool
  priority_weight: 1
  deferrable: true
  force_rerun: false
  priority: INTERACTIVE

grapes:
  - grape_id: select_business_date
    type: bigquery_sql
    sql_file: sql/010_select.sql
    parameters:
      business_date:
        type: DATE
        value: "{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}"
      region_code:
        type: STRING
        value: KR
```

`sql/010_select.sql`:

```sql
SELECT
  @business_date AS business_date,
  @region_code AS region_code,
  CURRENT_TIMESTAMP() AS executed_at;
```

이 예제는 사용자 테이블에 쓰지 않는 작은 읽기 쿼리입니다. 실제 운영 테이블을 조회하도록 변경할 때에는 쿼리 비용과 실행 위치를 확인하세요.

## `bigquery_procedure`

### 전용 파라미터와 인수 순서

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 프로시저 | `grapes[].procedure` | O | 완전 수식 이름 권장 | 문자열. `dataset.procedure` 또는 `project.dataset.procedure`만 지원합니다. 각 부분에 영문자·숫자·`_`·`-`만 허용됩니다. 백틱과 괄호를 넣지 않습니다. Jinja는 허용 문자 검사에 실패합니다. | `example-project.framework_demo.sp_echo_date` |
| 인수 | `grapes[].parameters` | X | `{}` | 매핑. YAML에서 적은 순서대로 `CALL`의 위치 인수를 생성합니다. 키 이름이 procedure 선언의 인수 이름과 일치한다고 자동 정렬되지 않습니다. | 아래 예제 참고 |

예를 들어 YAML이 아래 순서이면:

```yaml
parameters:
  business_date:
    type: DATE
    value: "2026-01-01"
  region_code:
    type: STRING
    value: KR
```

생성 SQL은 다음과 같이 procedure 전체를 백틱으로 감싼 형태입니다.

```sql
CALL `project.dataset.procedure`(@business_date, @region_code);
```

 procedure 선언이 `(p_date DATE, p_region STRING)` 순서여야 하며, YAML에서 두 키를 바꾸면 인수 순서도 바뀝니다. 빈 `parameters`이면 인수 없는 `CALL ...();`를 생성합니다. OUT/INOUT 변수를 별도로 선언·수집하는 기능은 없으므로 이 자동 생성 형태에서는 IN scalar 인수 중심으로 사용하세요. 더 복잡한 호출 스크립트는 `bigquery_sql`의 SQL 파일에 개발자가 직접 작성할 수 있습니다.

`bigquery_procedure`는 procedure를 **호출만** 합니다. 생성·수정·존재 검사나 procedure SQL 파일 자동 실행은 하지 않습니다. 이 유형에 `sql_file`을 써도 사용되지 않습니다.

### 전체 예제: 기존 읽기 전용 Procedure 호출

아래 procedure는 예제의 선행 조건입니다. 관리자가 별도의 테스트 dataset을 미리 준비하고, 새 이름으로 한 번 생성하세요. 기존 객체를 대체하는 `CREATE OR REPLACE`는 사용하지 않았습니다.

```sql
CREATE PROCEDURE `example-project.framework_demo.sp_echo_date`(
  p_date DATE,
  p_region STRING
)
BEGIN
  SELECT p_date AS business_date, p_region AS region_code;
END;
```

`pipelines/vines/example_bigquery_procedure/config.yaml`:

```yaml
version: 1

vine:
  vine_id: example_bigquery_procedure
  description: Call a pre-created read-only BigQuery procedure
  owner: data-engineering
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 3600
  tags: [example, bigquery, procedure]

business_date:
  type: daily

runtime:
  project_id: example-project
  location: asia-northeast3
  gcp_conn_id: google_cloud_default
  impersonation_chain: null
  labels:
    environment: dev

grape_defaults:
  retries: 0
  retry_delay_seconds: 60
  execution_timeout_seconds: 1800
  pool: default_pool
  priority_weight: 1
  deferrable: true
  force_rerun: false
  priority: INTERACTIVE

grapes:
  - grape_id: call_echo_date
    type: bigquery_procedure
    procedure: example-project.framework_demo.sp_echo_date
    parameters:
      business_date:
        type: DATE
        value: "{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}"
      region_code:
        type: STRING
        value: KR
```

이 Vine에는 SQL 파일이 필요하지 않습니다. 예제 procedure와 job의 location을 맞추고 호출 계정의 권한을 확인합니다. 운영 procedure가 DML을 수행한다면 `CALL` 자체는 단순해 보여도 데이터가 변경될 수 있으므로 재시도 및 재실행 정책을 procedure 설계와 함께 검토하세요.

### BigQuery의 미지원 설정과 운영 확인

다음은 현재 executor가 YAML에서 읽지 않는 항목입니다. BigQuery 서비스/API에 기능이 존재해도 여기에 임의의 키를 추가하면 동작하지 않습니다.

- `destination_table`, `destinationTable`, `write_disposition`, `create_disposition`, `schema_file`, `maximum_bytes_billed` 및 임의의 raw `configuration`
- `useLegacySql` 변경, positional query parameter, ARRAY/STRUCT 파라미터
- 별도의 `job_id`, `reattach_states`, 쿼리 결과 fetch 옵션
- SQL 결과의 자동 CSV Export 또는 schema 파일 생성

필요한 DDL/DML, 테이블 생성, Export SQL 등은 개발자가 관리하는 SQL 파일에 명시할 수 있지만, 서비스 권한·비용·파괴적 동작의 검토는 별도로 해야 합니다. 위 미지원 옵션이 작동한다고 가정하지 마세요.

오류를 점검할 때는 SQL 파일 존재 여부, named parameter 이름/타입, procedure의 실제 존재와 인수 순서, 실행 프로젝트 권한, dataset location, triggerer/provider 구성 순으로 확인합니다. `force_rerun: false`만으로 모든 수동 재실행이 자동 성공하거나 부작용이 없다고 보장할 수 없습니다.


## PostgreSQL: 연결과 SQL 파라미터

### 공통 YAML 파라미터

아래 표는 `postgres_sql`과 `postgres_to_gcs`에 공통으로 적용됩니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 식별자 | `grapes[].grape_id` | O | 없음 | 문자열. Airflow task ID로 사용합니다. | `query_postgres` |
| 유형 | `grapes[].type` | O | 없음 | 문자열. `postgres_sql` 또는 `postgres_to_gcs`를 정확히 지정합니다. 별칭은 없습니다. | `postgres_sql` |
| 의존성 | `grapes[].depends_on` | X | `[]` | 문자열 목록. 선행 Grape ID입니다. | `[prepare_data]` |
| SQL 파일 | `grapes[].sql_file` | O | 없음 | 문자열. Vine 디렉터리 기준 UTF-8 SQL 파일 경로입니다. 인라인 `sql` 키는 지원하지 않습니다. | `sql/010_query.sql` |
| DB 연결 | `grapes[].connection_id` | 조건부 | `runtime.postgres_connection_id` | 문자열. task 값이 비어 있지 않으면 runtime 값보다 우선합니다. 어느 쪽에도 유효한 문자열이 없으면 DAG 구성에 실패합니다. | `pg_reporting_readonly` |
| 기본 DB 연결 | `runtime.postgres_connection_id` | 조건부 | 없음 | 문자열. Grape `connection_id`를 생략했을 때 사용할 Airflow Connection ID입니다. | `pg_reporting_readonly` |
| SQL 바인딩 | `grapes[].parameters` | X | `{}` | 매핑. 키는 SQL의 `%(이름)s`에 대응하며 값은 DB 드라이버에 전달됩니다. BigQuery의 `{type, value}` 구조를 사용하지 않습니다. 명시적 `null`과 목록은 허용되지 않습니다. | `{region: KR}` |
| Google 연결 | `runtime.gcp_conn_id` | X | `google_cloud_default` | 문자열. Export의 GCSHook에 전달됩니다. 현재 비밀번호 Secret Manager 조회에는 이 값이 적용되지 않습니다. | `google_cloud_default` |
| GCS 실행 계정 | `runtime.impersonation_chain` | X | `null` | 문자열 또는 문자열 목록. `postgres_to_gcs`의 GCSHook에만 전달됩니다. PostgreSQL 로그인이나 비밀번호 Secret 조회 계정을 바꾸지 않습니다. | `exporter@example-project.iam.gserviceaccount.com` |
| 접속 제한 시간 | `grapes[].options.connect_timeout_seconds` | X | `30` | 정수, 초. DB 드라이버 `connect_timeout`입니다. SQL 실행시간 제한이 아니며 executor 자체 최소값 검증은 없습니다. | `30` |
| 재시도 | `grapes[].options.retries` | X | `1` | 정수. Airflow task 재시도 횟수입니다. 재실행 시 SQL 전체가 다시 실행됩니다. | `0` |
| 재시도 간격 | `grapes[].options.retry_delay_seconds` | X | `300` | 정수, 초. Airflow 재시도 대기시간입니다. | `60` |
| Task 제한 시간 | `grapes[].options.execution_timeout_seconds` | X | `7200` | 정수, 초. SQL/Export 전체 Airflow task의 제한시간입니다. 서버의 `statement_timeout`을 설정하지는 않습니다. | `1800` |
| Pool | `grapes[].options.pool` | X | 미지정(`None`) | 문자열. Airflow pool입니다. executor가 자체 pool을 생성하지 않습니다. 운영에서는 존재하는 pool을 명시하세요. | `default_pool` |
| 우선순위 | `grapes[].options.priority_weight` | X | `1` | 정수. Airflow task 우선순위 가중치입니다. | `1` |

위 `options.*` 키는 동일한 이름으로 `grape_defaults`에 넣을 수 있습니다. PostgreSQL executor는 `runtime.project_id`, `runtime.location`, `runtime.labels`, `options.deferrable`, `options.force_rerun`을 사용하지 않습니다.

### Airflow Connection 구성

executor는 `BaseHook.get_connection(connection_id)`로 Connection을 찾습니다. Airflow UI 또는 환경에 구성된 secrets backend가 반환한 Connection을 사용하며, Connection을 자동으로 생성하지 않습니다.

아래 `extra.*`는 Vine YAML이 아닌 **Airflow Connection의 Extra JSON 필드**입니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 연결 참조 | `grapes[].connection_id` 또는 `runtime.postgres_connection_id` | O | 기존 Connection ID | 문자열. Connection의 `host`, `schema`, `login`, `port`, `extra`를 조회할 ID입니다. Connection type은 executor에서 검사하지 않지만 PostgreSQL Connection 사용을 권장합니다. | `pg_reporting_readonly` |
| Connection Host | 해당 없음: Connection `host` | O | 없음 | 문자열. Composer worker에서 접근할 DB 호스트입니다. | `postgres.internal.example` |
| Connection Port | 해당 없음: Connection `port` | X | `5432` | 정수. 값이 없거나 0이면 5432를 사용합니다. | `5432` |
| Connection Schema | 해당 없음: Connection `schema` | O | 없음 | 문자열. PostgreSQL의 데이터베이스 이름(`dbname`)으로 전달합니다. SQL schema/search_path가 아닙니다. | `reporting` |
| Connection Login | 해당 없음: Connection `login` | O | 최소 권한 계정 | 문자열. PostgreSQL 로그인 사용자입니다. | `report_reader` |
| 비밀번호 Secret | 해당 없음: Connection `extra.password_secret` | O | Secret Manager 리소스 | 문자열. `projects/{project}/secrets/{secret}` 또는 `/versions/{version}`까지 지정합니다. 버전 생략 시 `latest`입니다. | `projects/example-project/secrets/pg-reporting-password/versions/latest` |
| SSL 모드 | 해당 없음: Connection `extra.sslmode` | X | 드라이버 기본값 / 환경에 맞는 TLS 설정 권장 | 문자열. 값이 비어 있지 않으면 그대로 DB 드라이버로 전달합니다. | `verify-full` |
| CA 인증서 | 해당 없음: Connection `extra.sslrootcert` | X | 없음 | 문자열. DB 드라이버가 읽을 worker 로컬 파일 경로입니다. 인증서를 다운로드하거나 배포하지 않습니다. | `/path/to/root.crt` |
| 클라이언트 인증서 | 해당 없음: Connection `extra.sslcert` | X | 없음 | 문자열. 필요한 경우 worker 로컬 인증서 파일 경로입니다. | `/path/to/client.crt` |
| 클라이언트 키 | 해당 없음: Connection `extra.sslkey` | X | 없음 | 문자열. 필요한 경우 worker 로컬 키 파일 경로입니다. 키 내용을 YAML에 쓰지 않습니다. | `/path/to/client.key` |
| 애플리케이션 이름 | 해당 없음: Connection `extra.application_name` | X | 없음 | 문자열. 드라이버에 전달할 접속 애플리케이션 이름입니다. | `composer_report_export` |

현재 구현의 보안·접속 동작은 다음과 같습니다.

1. `extra.password_secret`는 반드시 필요합니다. Connection의 일반 `password` 필드를 대신 사용하는 fallback은 없습니다. `encrypted_credentials`, KMS 복호화 설정 또는 YAML 평문 비밀번호도 이 executor가 지원하는 방식이 아닙니다.
2. Secret은 `SecretManagerServiceClient()`의 기본 자격증명으로 조회합니다. 일반적인 Composer 환경에서는 Composer worker의 관리/실행 서비스 계정이 해당 Secret 버전을 읽을 권한이 필요합니다. 코드가 `gcp_conn_id`나 `impersonation_chain`을 이 클라이언트에 전달하지 않으므로 GCS용 impersonation 권한만 주어서는 충분하지 않습니다.
3. DB에는 위에서 얻은 DB 사용자·비밀번호로 접속합니다. GCP 서비스 계정이 DB 사용자로 자동 매핑되는 IAM DB 인증이나 Cloud SQL Proxy/Connector 설치 기능은 없습니다. worker에서 DB 호스트·포트에 도달할 수 있어야 합니다.
4. 드라이버는 `psycopg` import를 먼저 시도하고, 패키지가 없을 때만 `psycopg2`로 넘어갑니다. `psycopg`의 실제 접속 실패를 `psycopg2`로 재시도하지 않습니다. Composer에 적합한 PostgreSQL 드라이버와 Google Secret Manager 클라이언트가 설치되어 있어야 합니다.
5. 최소 권한 DB 계정과 Secret 접근권한을 사용하세요. Export에는 추가로 선택한 GCS 계정의 대상 객체 쓰기 권한이 필요합니다. impersonation을 사용하면 호출 계정 및 체인 중간 계정에 필요한 권한도 환경에서 미리 구성해야 합니다.

> 기존 `ujlim_pg_operator_sample/README.md`의 KMS 및 `encrypted_credentials` 설명은 현재 코드와 일치하지 않습니다. 새 구성에는 위 Connection + `extra.password_secret` 방식을 사용하세요.

### Jinja 렌더링과 DB 바인딩의 차이

PythonOperator의 `op_kwargs`에 SQL 본문과 `parameters`가 전달되므로, Airflow가 task 실행 전에 그 안의 Jinja를 렌더링합니다. 그다음 `cursor.execute(sql, parameters or None)`로 DB 드라이버에 전달합니다.

```yaml
parameters:
  business_date: "{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}"
```

```sql
SELECT CAST(%(business_date)s AS date) AS business_date;
```

업무기준일 문자열은 먼저 Airflow가 계산하고, `%(... )s` 자리에는 PostgreSQL 드라이버가 값을 바인딩합니다. SQL에서는 정확히 `%(business_date)s`로 쓰고 따옴표로 감싸지 않습니다. BigQuery의 `@business_date`나 JDBC의 다른 placeholder 문법과 혼용하지 마세요.

- 동적 **값**은 `parameters`를 사용합니다. Jinja로 값이나 사용자 입력을 SQL 문자열에 직접 이어 붙이는 방식은 피합니다.
- 테이블명·컬럼명 같은 **식별자**는 값 파라미터로 대체할 수 없습니다. 개발자가 검토한 고정 SQL을 권장하며, 동적 식별자에 외부 입력을 그대로 넣지 않습니다.
- `parameters`의 값이 자동으로 `DATE`로 변환된다고 가정하지 않습니다. 예제처럼 DB SQL에서 명시적으로 `CAST`하거나 드라이버가 지원하는 형식을 사용합니다.
- 옵션 값은 DAG 구성 때 `int(...)`/`bool(...)`로 변환되는 것이 있으므로 `retries`, timeout 등 숫자·불리언 설정을 Jinja로 작성하지 않습니다.

## `postgres_sql`

### 전용 파라미터

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 트랜잭션 | `grapes[].options.autocommit` | X | `false` | 불리언. false이면 성공 시 commit, 예외 시 rollback을 수행합니다. true이면 드라이버의 autocommit을 사용합니다. | `false` |
| 결과 조회 | `grapes[].options.fetch` | X | `none` | 문자열. `none`, `one`, `all`을 지원하며 소문자로 정규화합니다. `none`은 결과행을 반환하지 않습니다. | `all` |
| 결과 상한 | `grapes[].options.max_fetch_rows` | X | `100` | 1 이상의 정수. `fetch: all`일 때 한 번의 `fetchmany`로 읽을 최대 행 수입니다. `all`도 전체 결과를 무제한 반환하지 않습니다. `none`/`one`에서도 값 검증은 수행됩니다. | `10` |

`fetch: one`은 첫 행이 있으면 한 행만 반환합니다. DDL/DML처럼 `cursor.description`이 없는 문장은 fetch 옵션을 주어도 `columns`와 `rows`가 비어 있습니다. `rowcount`는 DB 드라이버가 보고하는 값이며, 반환한 행 수와 같다는 보장은 없습니다.

반환 dict는 PythonOperator의 일반 반환값입니다. 환경의 기본 XCom 동작을 사용하면 다음 정보가 XCom에 저장됩니다.

```json
{
  "rowcount": 1,
  "columns": ["business_date"],
  "rows": [["2026-01-01"]],
  "fetch_mode": "all"
}
```

위 JSON은 구조 예시입니다. 실제 셀 값은 DB 드라이버의 Python 자료형으로 반환될 수 있으므로 날짜·Decimal·바이너리 등은 XCom 직렬화와 downstream 사용을 확인해야 합니다. 민감정보나 대량 데이터를 XCom으로 반환하지 말고, 필요한 컬럼과 작은 행 수만 조회하세요.

### 전체 예제: 업무기준일 확인용 읽기 SQL

파일 구성:

```text
pipelines/vines/example_postgres_query/
├── config.yaml
└── sql/
    └── 010_query.sql
```

`config.yaml`:

```yaml
version: 1

vine:
  vine_id: example_postgres_query
  description: Read-only PostgreSQL connectivity and business-date check
  owner: data-engineering
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 3600
  tags: [example, postgres]

business_date:
  type: daily

runtime:
  gcp_conn_id: google_cloud_default
  postgres_connection_id: pg_reporting_readonly

grape_defaults:
  retries: 0
  retry_delay_seconds: 60
  execution_timeout_seconds: 1800
  connect_timeout_seconds: 30
  pool: default_pool
  priority_weight: 1

grapes:
  - grape_id: query_postgres
    type: postgres_sql
    sql_file: sql/010_query.sql
    parameters:
      business_date: "{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}"
    options:
      autocommit: false
      fetch: all
      max_fetch_rows: 10
```

`sql/010_query.sql`:

```sql
SELECT
    CAST(%(business_date)s AS date) AS business_date,
    current_database() AS database_name,
    current_timestamp AS executed_at;
```

이 예제는 데이터 변경 없이 접속과 바인딩을 확인합니다. Connection과 password Secret은 미리 준비해야 합니다. 운영 DML로 바꿀 경우 재시도·수동 재실행을 고려해 업무키, 중복 방지 또는 멱등한 SQL을 설계하세요. Airflow 재시도만으로 exactly-once 처리가 보장되지는 않습니다.

## `postgres_to_gcs`

### 전용 파라미터

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| 목적지 | `grapes[].destination` | O | 없음 | 매핑. `bucket`과 `object`가 런타임에 필요합니다. 다른 저장 형식이나 schema 설정은 읽지 않습니다. | 아래 예제 참고 |
| 버킷 | `grapes[].destination.bucket` | O | 없음 | 문자열. `gs://`를 제외한 GCS 버킷 이름입니다. 런타임에 비어 있으면 실패합니다. | `example-export-bucket` |
| 객체 | `grapes[].destination.object` | O | 실행별 고유 경로 권장 | 문자열. 버킷 내 객체명입니다. 선행 `/`는 제거합니다. 동일 객체명을 재사용하면 기존 객체를 대체할 수 있습니다. | `exports/{{ run_id }}/result.csv` |
| 읽기 묶음 | `grapes[].options.chunk_rows` | X | `5000` | 1 이상의 정수. 반복 `cursor.fetchmany`에 전달할 행 수입니다. GCS 객체 분할 수나 완전 스트리밍 보장이 아닙니다. | `5000` |
| CSV 헤더 | `grapes[].options.include_header` | X | `true` | 불리언. DB cursor의 컬럼명을 첫 행으로 씁니다. 결과행이 0개여도 true이면 헤더가 기록됩니다. | `true` |

Export에는 `options.autocommit`, `options.fetch`, `options.max_fetch_rows`가 적용되지 않습니다. `connect_timeout_seconds`, 재시도, timeout, pool 및 priority는 공통 표를 따릅니다.

### 출력 스키마와 CSV 규격

- 별도의 `schema_file`이나 컬럼 타입 선언은 없습니다. SQL SELECT 목록과 `cursor.description`이 헤더와 컬럼 순서를 결정합니다. 필요한 컬럼 순서와 안정적인 alias를 SQL에 명시하세요.
- 형식은 UTF-8 CSV, MIME type은 `text/csv`로 고정됩니다. Python `csv.writer` 기본 설정인 쉼표 구분, 필요한 값에 큰따옴표 quoting, CRLF 레코드 구분을 사용합니다. 구분자·quote 문자·인코딩·개행·압축을 바꾸는 YAML 옵션은 없습니다.
- Python CSV 변환상 SQL NULL은 빈 필드로 쓰여 빈 문자열과 구분되지 않을 수 있습니다. NULL의 별도 표현이 필요하면 SQL에서 명시적으로 처리하고 downstream 계약을 맞추세요.
- 날짜, 숫자 등은 드라이버가 반환한 값을 CSV writer가 문자열로 기록합니다. 수신 시스템에 고정 포맷이 필요하면 SQL의 `CAST`/format 함수를 사용하세요.
- `SELECT` 등 행을 반환하는 쿼리여야 합니다. 결과 스키마가 없는 SQL은 `postgres_to_gcs requires a SELECT/query that returns rows` 오류가 납니다. 쓰기 SQL + `RETURNING`처럼 데이터 변경을 동반하는 Export는 사용하지 마세요. 이 함수에는 명시적 commit이 없습니다.
- 출력은 단일 객체입니다. shard, gzip, Parquet, JSON, 별도 FIN 파일, row-count sidecar, BigQuery schema 생성은 지원하지 않습니다.

### 메모리·재시도·보안 제한

`chunk_rows`는 DB 결과를 읽는 묶음 크기만 제한합니다. 파일은 `SpooledTemporaryFile`에 쓰고 임시 저장 크기가 약 16 MiB를 넘으면 worker 디스크를 사용하지만, 마지막에 `tmp.read().encode("utf-8")`로 **전체 CSV를 메모리에 읽어** `GCSHook.upload(data=...)`합니다. 일반 DB cursor를 사용하며 서버 측 named cursor도 설정하지 않습니다. 따라서 DB 드라이버 버퍼, 임시 파일, 업로드용 문자열/bytes를 포함한 메모리·디스크 용량을 검토해야 합니다.

목적지 존재 여부 검사, generation precondition, no-overwrite 옵션 또는 업로드 후 checksum/건수 대조 기능은 executor에 없습니다. 동일한 업무일 경로로 재실행하면 기존 결과를 덮어쓸 수 있으므로 최초 검증에는 예제처럼 `run_id`별 경로를 쓰세요. 같은 task의 재시도는 같은 `run_id`를 사용합니다. 오류 발생 시 CSV 객체와 DB 처리의 원자성 또는 exactly-once를 보장하지 않습니다.

CSV를 Excel 등에서 직접 여는 경우, 문자열 셀에 포함된 수식형 입력에 대한 별도 무해화는 없습니다. 외부 입력을 내보내는 워크플로에서는 수신 도구에 맞는 처리 정책을 검토하세요.

성공 시 반환값 예시:

```json
{
  "row_count": 1,
  "destination": "gs://example-export-bucket/postgres/example/manual-run/sample.csv",
  "format": "csv"
}
```

`row_count`는 CSV 헤더를 제외한 데이터 행 수입니다. 실제 CSV 데이터 자체는 반환하지 않습니다.

### 전체 예제: 읽기 쿼리 결과를 실행별 객체로 Export

파일 구성:

```text
pipelines/vines/example_postgres_export/
├── config.yaml
└── sql/
    └── 010_export.sql
```

`config.yaml`:

```yaml
version: 1

vine:
  vine_id: example_postgres_export
  description: Export a small read-only PostgreSQL result to GCS
  owner: data-engineering
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 7200
  tags: [example, postgres, gcs]

business_date:
  type: daily

runtime:
  gcp_conn_id: google_cloud_default
  impersonation_chain: null
  postgres_connection_id: pg_reporting_readonly

grape_defaults:
  retries: 0
  retry_delay_seconds: 60
  execution_timeout_seconds: 3600
  connect_timeout_seconds: 30
  pool: default_pool
  priority_weight: 1

grapes:
  - grape_id: export_postgres
    type: postgres_to_gcs
    sql_file: sql/010_export.sql
    parameters:
      business_date: "{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}"
    destination:
      bucket: example-export-bucket
      object: "postgres/example/business_date={{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/run={{ run_id }}/sample.csv"
    options:
      chunk_rows: 5000
      include_header: true
```

`sql/010_export.sql`:

```sql
SELECT
    CAST(%(business_date)s AS date) AS business_date,
    current_timestamp AS exported_at,
    'example_postgres_export' AS source_name;
```

처음에는 이 작은 읽기 쿼리로 Secret 접근, DB 네트워크, SQL 렌더링, GCS 쓰기와 CSV 내용을 확인하세요. 이후 실제 SELECT로 바꾸고 데이터량에 맞춰 worker 용량과 실행 제한시간을 검토합니다.

## PostgreSQL 오류 점검

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
| --- | --- | --- | --- | --- | --- |
| SQL 파일 누락 | `grapes[].sql_file` | O | Vine 내부 실제 파일 | 파일은 DAG 구성 때 읽습니다. 배포 누락, 오타, 경로 이탈을 확인합니다. | `sql/010_query.sql` |
| Connection 누락 | `grapes[].connection_id` / `runtime.postgres_connection_id` | O | 존재하는 Connection ID | task 값 우선순위, secrets backend 설정 및 Connection 조회 가능 여부를 확인합니다. | `pg_reporting_readonly` |
| Secret 오류 | Connection `extra.password_secret` | O | 정규 리소스 경로 | 경로 형식·Secret 버전·빈 비밀번호·기본 자격증명의 Secret 접근권한을 확인합니다. | `projects/example-project/secrets/pg-reporting-password` |
| 접속 실패 | Connection `host`/`schema`/`login`/`port` | O | worker에서 접속 가능 | 네트워크, 방화벽, DNS, TLS 파일 경로, DB 사용자 및 실제 DB 이름을 확인합니다. | `schema: reporting` |
| 바인딩 오류 | `grapes[].parameters` | X | 매핑 | PostgreSQL은 `{name: value}` 및 `%(name)s`입니다. BigQuery 형식으로 작성하지 않습니다. | `{region: KR}` |
| 결과가 일부만 반환됨 | `grapes[].options.max_fetch_rows` | X | `100` | `fetch: all`도 최대 지정 행만 가져오는 구현입니다. 대량 결과를 XCom으로 옮기지 않습니다. | `10` |
| Export 메모리 부족 | `grapes[].options.chunk_rows` | X | `5000` | chunk를 줄여도 최종 전체 payload 메모리 사용은 없어지지 않습니다. 데이터량과 실행 방식을 재검토합니다. | 더 작은 쿼리로 검증 |
| 대상 파일 대체 | `grapes[].destination.object` | O | 고유 경로 권장 | 같은 객체로 재실행하면 덮어쓸 수 있습니다. `run_id` 등 실행 식별자를 넣고 보관 정책을 정합니다. | `exports/{{ run_id }}/sample.csv` |

## 구현 및 저장소 예제

- [BigQuery executor](../../dags/framework/bigquery_executor.py)
- [PostgreSQL executor](../../dags/framework/postgres_executor.py)
- [VineFactory 및 타입 분기](../../dags/framework/vine_factory.py)
- [파일 경로 검증 및 기본값 병합](../../dags/framework/utils.py)
- [BigQuery SQL/Procedure 샘플 YAML](../../dags/pipelines/vines/minimal_bq_vine/config.yaml)
- [PostgreSQL SQL 샘플 YAML](../../dags/pipelines/vines/ujlim_pg_operator_sample/config.yaml)
- [PostgreSQL Export 샘플 YAML](../../dags/pipelines/vines/ujlim_pg_to_gcs_sample/config.yaml)
