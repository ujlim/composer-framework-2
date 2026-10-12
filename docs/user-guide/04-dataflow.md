# 4. Dataflow JDBC YAML 가이드

[목차](README.md) · [이전: 스토리지](03-storage-sftp.md) · [다음: 운영](05-operations.md)

DB2·Oracle·Vertica를 JDBC로 읽어 GCS 또는 BigQuery에 적재하는 Grape를 설명합니다. 기준은 [Dataflow 실행기](../../dags/framework/dataflow_executor.py), [jdbc-export 코드](../../tools/dataflow/jdbc-export/docker/main.py), [템플릿 메타데이터](../../tools/dataflow/jdbc-export/docker/metadata.json)입니다. Root/Vine 공통 설정과 업무일자는 [공통 YAML](01-yaml-common.md), 배포·운영 점검은 [운영 가이드](05-operations.md)를 참고하세요.

> 이 기능은 저장소의 **사용자 정의 `jdbc-export` Flex Template**을 호출합니다. Google 제공 JDBC 템플릿이나 저장소의 `jdbc-test` 템플릿과 파라미터 계약이 다릅니다. 아래 기본값은 예제 YAML의 값이 아니라 **프레임워크 코드에서 생략 시 적용하는 값**입니다.

## 1. 지원 범위와 실행 흐름

- `jdbc_to_gcs`: JDBC → GCS. `json`, `csv`, `parquet`를 지원합니다.
- `jdbc_to_bigquery`: JDBC → BigQuery. `FILE_LOADS` 방식으로 적재합니다.
- 지원 DB는 `db2`, `oracle`, `vertica`입니다. PostgreSQL은 이 실행기의 대상이 아닙니다.
- 두 타입 모두 SQL 파일이 필수입니다. BigQuery 및 Parquet 출력은 JSON 스키마 파일도 필수입니다.
- `vine.schedule`은 생략하거나 `null`로 지정합니다. Root 호출 또는 수동 실행으로 시작하며, Vine 수동 실행에는 `business_date` 입력이 필요합니다.

실행 순서:

1. DAG 파싱 때 Vine 폴더 내부 SQL·스키마 파일을 읽고 필요한 설정을 검증합니다.
2. 실행 때 Airflow가 SQL, `destination`, `job_name_prefix`를 Jinja 렌더링합니다.
3. Airflow Connection에서 JDBC 접속 정보를 읽습니다.
4. 렌더링된 SQL을 `gs://<system_bucket>/sql/<dag_id>/<run_id>/<task_id>.sql`에 업로드합니다.
5. SQL 본문 대신 `query_uri`, Secret 참조, JDBC 정보 등을 Flex Template에 전달합니다.
6. 템플릿이 GCS의 SQL 및 Secret Manager의 비밀번호를 읽고 JDBC 데이터를 추출합니다.
7. GCS 또는 BigQuery에 쓰고, Airflow 작업은 Dataflow 종료 상태를 주기적으로 확인합니다.

## 2. 설정 위치와 덮어쓰기 규칙

```text
<vine 폴더>/
├── config.yaml
├── sql/010_export.sql
└── schema/output.json
```

- `runtime.dataflow`는 같은 Vine의 Dataflow Grape가 공유합니다. `grapes[].runtime` 등의 개별 덮어쓰기 경로는 없습니다.
- 옵션은 `grape_defaults` → `grapes[].options` 순서로 재귀 병합하며, 뒤의 값이 우선합니다. 리스트·스칼라는 통째로 교체합니다.
- 병합 결과에 옵션이 **없을 때**만 아래 표의 코드 기본값을 적용합니다. `null`은 기본값 복원을 뜻하지 않습니다. 정수 변환 옵션에 `null`을 쓰면 실패합니다.
- `source`, `destination`은 `grape_defaults`에서 상속하지 않습니다.
- `source.password_secret`이 비어 있지 않으면 Connection의 `extra.password_secret`보다 우선합니다.
- Connection의 `extra.jdbc_url`이 비어 있지 않으면 `host`·`port`·`schema`로 만든 URL보다 우선합니다.
- `source.db_type`과 GCS `destination.format`은 소문자로 변환합니다. 공백 제거는 하지 않습니다.
- 문서에 없는 키를 추가해도 Dataflow 옵션으로 자동 전달되지 않습니다. 실행기가 읽지 않는 키는 효과가 없으므로 오타를 별도로 점검하세요.

## 3. 공통 Grape 및 JDBC source

표의 `grapes[]`는 `grapes` 목록의 각 항목을 뜻합니다. 타입은 설명 열에 표시합니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| 식별 | `grapes[].grape_id` | O | 없음 | 문자열. Vine 안에서 고유한 Airflow task ID. 영문·숫자·`_`·`.`·`-`만 허용합니다. | `export_db2_parquet` |
| 유형 | `grapes[].type` | O | 없음 | 문자열. 정확히 `jdbc_to_gcs` 또는 `jdbc_to_bigquery`. 대소문자 별칭은 없습니다. | `jdbc_to_gcs` |
| 의존성 | `grapes[].depends_on` | X | `[]` | 문자열 목록. 같은 Vine의 상위 Grape ID. 누락 참조·자기 참조·순환을 검증합니다. | `[prepare_source]` |
| JDBC | `grapes[].source` | O | 없음 | 매핑. 아래 DB 접속 설정을 담습니다. | 아래 전체 예제 참고 |
| JDBC | `grapes[].source.db_type` | O | 없음 | 문자열. `db2`, `oracle`, `vertica`. 내부에서 소문자로 정규화하며 JDBC driver class는 고정 매핑합니다. | `db2` |
| JDBC | `grapes[].source.connection_id` | O | 없음 | 문자열. `BaseHook.get_connection()`으로 조회할 Airflow Connection ID. `conn_type` 자체는 검사하지 않습니다. | `conn-dataflow-db2-test` |
| JDBC | `grapes[].source.driver_jars` | O | 없음 / 검증한 JAR의 GCS URI | 문자열. Beam `ReadFromJdbc.driver_jars`에 그대로 전달합니다. 실행기는 URI 형식·존재·버전 호환성을 사전 검사하지 않습니다. 목록형 YAML 대신 문자열을 사용하세요. | `gs://REPLACE_SYSTEM_BUCKET/drivers/db2/db2jcc4.jar` |
| 비밀정보 | `grapes[].source.password_secret` | 조건부 | Connection `extra.password_secret` 권장 | 문자열. Secret Manager 리소스 이름. 이 값 또는 Connection extra에 참조가 필요합니다. **비밀번호 본문을 넣지 않습니다.** 버전이 없으면 템플릿에서 `/versions/latest`를 덧붙입니다. | `projects/REPLACE_SECRET_PROJECT/secrets/db2-read-password/versions/1` |
| SQL | `grapes[].sql_file` | O | 없음 | 문자열. Vine 디렉터리 기준 UTF-8 SQL 파일 경로. 파싱 시 파일을 읽으며 Vine 밖으로 나가는 경로와 없는 파일을 거부합니다. SQL 내용은 실행 시 Jinja 렌더링됩니다. | `sql/010_export.sql` |
| 목적지 | `grapes[].destination` | O | 없음 | 매핑. GCS와 BigQuery의 하위 필드는 다음 두 절을 참고하세요. | 아래 전체 예제 참고 |
| 옵션 | `grapes[].options` | X | `{}` | 매핑. 이 Grape에 한해 `grape_defaults`를 덮어씁니다. 지원 키는 옵션 표에 한정됩니다. | `{retries: 0}` |

### Airflow Connection 규칙

다음은 **YAML 키가 아닌 Airflow Connection 필드**입니다. Connection을 등록한 뒤 YAML에서는 `source.connection_id`로 참조합니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| Connection | 해당 없음: `login` | O | 없음 | 문자열. DB 사용자명이며 Dataflow `username` 파라미터로 전달됩니다. 최소 권한의 읽기 계정을 권장합니다. | `etl_reader` |
| Connection | 해당 없음: `host` | 조건부 | 없음 | 문자열. `extra.jdbc_url`이 없을 때 필수입니다. | `db2.example.internal` |
| Connection | 해당 없음: `schema` | 조건부 | 없음 | 문자열. `extra.jdbc_url`이 없을 때 필수. DB2/Vertica는 데이터베이스, Oracle 기본 URL에서는 service name으로 사용합니다. SQL 객체 schema와 혼동하지 마세요. | `SAMPLEDB` |
| Connection | 해당 없음: `port` | X | DB2 `50000`, Oracle `1521`, Vertica `5433` | 정수. 값이 없거나 false로 평가되면 DB별 포트를 사용합니다. | `50000` |
| Connection extra | 해당 없음: `extra.jdbc_url` | X | host/port/schema로 구성 | 문자열. 지정하면 생성 URL을 완전히 대체합니다. 비밀번호·토큰을 URL에 포함하지 마세요. | `jdbc:db2://db2.example.internal:50000/SAMPLEDB` |
| Connection extra | 해당 없음: `extra.password_secret` | 조건부 | 없음 / Secret 참조 저장 권장 | 문자열. YAML `source.password_secret`이 없을 때 사용합니다. 둘 다 없으면 실행 실패합니다. | `projects/REPLACE_SECRET_PROJECT/secrets/db2-read-password` |
| Connection | 해당 없음: `password` | 사용하지 않음 | 비워 두기 | 이 실행기는 `conn.password`를 읽지 않습니다. 여기에만 DB 암호를 저장하면 실행되지 않습니다. | 생략 |

기본 URL과 driver class:

- DB2: `jdbc:db2://<host>:<port>/<schema>`, `com.ibm.db2.jcc.DB2Driver`
- Oracle: `jdbc:oracle:thin:@//<host>:<port>/<schema>`, `oracle.jdbc.OracleDriver`
- Vertica: `jdbc:vertica://<host>:<port>/<schema>`, `com.vertica.jdbc.Driver`

Connection extra 예시:

```json
{
  "password_secret": "projects/REPLACE_SECRET_PROJECT/secrets/db2-read-password/versions/1"
}
```

`username`, `connection_url`, `password`, `driver_class_name`은 이 YAML의 source 별칭이 아닙니다. URL 사용자 정의는 Connection extra에서 하고, 암호는 Secret Manager에 보관합니다.

## 4. runtime 설정

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| GCP 접속 | `runtime.gcp_conn_id` | X | `google_cloud_default` | 문자열. **렌더링 SQL의 GCS 업로드**에 사용하는 Airflow Google Connection. 현재 Dataflow API 클라이언트에는 이 Connection의 자격 증명이 명시적으로 전달되지 않습니다. | `google_cloud_default` |
| GCP 접속 | `runtime.impersonation_chain` | X | 없음 | 문자열 또는 문자열 목록. SQL 업로드용 `GCSHook`에 전달합니다. 현재 Dataflow API 실행·조회에 동일한 impersonation이 적용된다고 가정하면 안 됩니다. | `sql-uploader@REPLACE_PROJECT.iam.gserviceaccount.com` |
| Dataflow | `runtime.dataflow` | O | 없음 | 매핑. 아래 실행 환경 설정입니다. | 아래 전체 예제 참고 |
| 프로젝트 | `runtime.dataflow.project_id` | O | 없음 | 문자열. Dataflow Job이 생성될 프로젝트. Secret·Shared VPC host·BigQuery 목적지 프로젝트와 다를 수 있습니다. | `REPLACE_DATAFLOW_PROJECT` |
| 리전 | `runtime.dataflow.region` | O | 없음 / 환경 표준 리전 | 문자열. Dataflow launch와 jobs.get 호출에 공통으로 사용합니다. | `asia-northeast3` |
| 템플릿 | `runtime.dataflow.template_gcs_path` | O | 없음 / `jdbc-export` spec | 문자열. `containerSpecGcsPath`로 전달할 Flex Template JSON의 GCS URI. 이 문서의 export 파라미터 계약을 구현한 spec이어야 합니다. | `gs://REPLACE_SYSTEM_BUCKET/templates/jdbc-export/template.json` |
| 시스템 버킷 | `runtime.dataflow.system_bucket` | O | 없음 / 버킷 이름만 권장 | 문자열. SQL 저장 버킷 및 기본 staging/temp 위치의 기준. 코드는 `gs://`와 양끝 `/`를 제거하지만 버킷 내 prefix를 지원하는 키는 아닙니다. | `REPLACE_SYSTEM_BUCKET` |
| 네트워크 | `runtime.dataflow.subnetwork` | O | 없음 / 전체 Compute API URI | 문자열. `environment.subnetwork`로 전달합니다. Shared VPC라면 실제 host project와 region을 확인하세요. | `https://www.googleapis.com/compute/v1/projects/REPLACE_HOST_PROJECT/regions/asia-northeast3/subnetworks/REPLACE_SUBNET` |
| 실행 계정 | `runtime.dataflow.service_account_email` | O | 없음 / 전용 Dataflow 실행 SA | 문자열. launch environment의 `serviceAccountEmail`. Airflow 작업 자체의 API 인증 계정을 바꾸는 설정이 아닙니다. | `dataflow-worker@REPLACE_PROJECT.iam.gserviceaccount.com` |
| staging | `runtime.dataflow.staging_location` | X | `gs://<system_bucket>/staging` | 문자열. Dataflow `stagingLocation`. 비어 있으면 기본값을 사용합니다. | `gs://REPLACE_SYSTEM_BUCKET/staging` |
| temp | `runtime.dataflow.temp_location` | X | `gs://<system_bucket>/temp` | 문자열. Dataflow `tempLocation`. 비어 있으면 기본값을 사용합니다. | `gs://REPLACE_SYSTEM_BUCKET/temp` |

### 인증·권한·private worker 확인

이 코드에는 서로 다른 인증 경로가 있습니다.

1. Airflow의 SQL 업로드: `GCSHook(gcp_conn_id=..., impersonation_chain=...)` 사용.
2. Airflow의 Dataflow launch 및 상태 조회: `googleapiclient.discovery.build("dataflow", "v1b3")`를 자격 증명 인자 없이 호출하므로 실행 환경의 ADC에 의존.
3. Flex Template의 SQL·Secret 읽기: `storage.Client()` 및 `SecretManagerServiceClient()`의 ADC 사용. 지정한 실행 SA와 실제 launcher/worker 로그의 ADC 신원을 확인해야 합니다.

따라서 Google Connection 하나만 바꾸어 모든 단계의 인증 주체가 바뀐다고 생각하지 마세요. 실제 주체별로 SQL 업로드·조회, 템플릿/JAR/이미지 조회, Dataflow 생성·조회, 지정 SA 사용, 목적지 쓰기 권한을 확인합니다. Secret은 **실제 Secret이 있는 프로젝트와 해당 Secret**에 대한 `secretmanager.versions.access` 권한이 필요하며, 일반적인 부여 역할은 `roles/secretmanager.secretAccessor`입니다. 조직 환경의 세부 IAM·Shared VPC 설정은 운영 담당자와 확인합니다.

- worker의 `ipConfiguration`은 **`WORKER_IP_PRIVATE` 고정**입니다. YAML로 public IP를 켜는 옵션은 없습니다.
- DB 경로·방화벽·DNS 및 GCS/Secret Manager/Artifact Registry 등 필요한 서비스 접근이 private 환경에서 가능해야 합니다.
- [export Dockerfile](../../tools/dataflow/jdbc-export/docker/Dockerfile)은 Java 17과 Python 의존성을 이미지 빌드 시 설치하며, 실행 시 재다운로드를 유발하는 `FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE`을 설정하지 않습니다. 이것만으로 expansion service/JAR 등 모든 런타임 네트워크 요구가 없어지는 것은 아닙니다.
- YAML·SQL·JDBC URL에 암호를 쓰지 마세요. SQL은 시스템 버킷에 저장되며 코드에 자동 삭제가 없습니다. SQL literal에 민감정보를 넣는 경우에도 저장·조회 권한과 보존 정책을 검토하세요.
- Secret resource, JDBC URL, 사용자명은 실행 파라미터·로그에서 보일 수 있습니다. Secret의 **값**을 launch parameter로 전달하지 않는다는 뜻이지, 모든 메타데이터가 비공개가 된다는 뜻은 아닙니다.

## 5. GCS destination

`type: jdbc_to_gcs`에서 사용합니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| 경로 | `grapes[].destination.path` | O | 없음 / 실행별 분리 prefix | 문자열. Beam 출력 파일의 **prefix**입니다. 고정된 단일 파일명이나 폴더만을 뜻하지 않습니다. Jinja 렌더링 대상입니다. | `gs://REPLACE_OUTPUT_BUCKET/jdbc/run={{ ts_nodash }}/part` |
| 포맷 | `grapes[].destination.format` | X | `json` | 문자열. `json`, `csv`, `parquet`만 지원하며 소문자로 정규화합니다. `jsonl`은 허용값이 아닙니다. | `parquet` |
| 스키마 | `grapes[].destination.schema_file` | Parquet 필수 | 없음 | 문자열. Vine 내부 JSON 파일 상대 경로. 파싱 때 JSON을 읽어 `parquet_schema` 문자열로 전달합니다. JSON/CSV에서는 읽지 않습니다. | `schema/output.json` |

출력 차이:

- `json`: 행마다 JSON 객체 1개를 쓰는 JSON Lines, 확장자는 `.jsonl`. `ensure_ascii=False`, `default=str`이므로 JSON 기본 타입이 아닌 값은 문자열화될 수 있습니다.
- `csv`: `csv.writer` 기본 구분자/인용 규칙, **헤더 없음**. 결과 Row의 딕셔너리 값 순서대로 출력합니다. 컬럼 순서를 SQL에서 고정하세요.
- `parquet`: 행을 dict로 바꾼 뒤 명시 스키마로 `WriteToParquet`에 전달하며 확장자는 `.parquet`입니다.
- `WriteToText` / `WriteToParquet`의 shard 설정을 노출하지 않습니다. 파일 수·shard 이름은 Beam에 맡기므로 `part`가 정확히 `part.csv` 하나로 생성된다고 가정하면 안 됩니다.
- `compression`, `num_shards`, `header`, `delimiter`, `encoding`, 단일 파일 병합 및 FIN 생성은 이 Dataflow YAML의 지원 옵션이 아닙니다.
- 같은 prefix로 재실행했을 때의 중복·파일 충돌을 이 실행기가 해결하지 않습니다. 실행별 prefix와 후속 정리/검증 방식을 설계하세요.

## 6. BigQuery destination

`type: jdbc_to_bigquery`에서 사용합니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| 테이블 | `grapes[].destination.table` | O | 없음 / 격리된 테스트 테이블 | 문자열. `PROJECT:DATASET.TABLE` 표기를 권장합니다. Jinja 렌더링 대상이며 실제 해석은 Beam에 위임합니다. | `REPLACE_BQ_PROJECT:jdbc_test.db2_export` |
| 스키마 | `grapes[].destination.schema_file` | O | 없음 | 문자열. Vine 내부 JSON 스키마 파일. JSON 파싱 후 문자열로 전달하고 템플릿이 다시 읽습니다. 기존 테이블이어도 이 필드는 필수입니다. | `schema/output.json` |
| 쓰기 방식 | `grapes[].destination.write_disposition` | X | `WRITE_APPEND` / 최초 테스트는 `WRITE_EMPTY` | 문자열. Beam `WriteToBigQuery`에 그대로 전달합니다. `WRITE_APPEND`는 추가, `WRITE_EMPTY`는 비어 있을 때 쓰기, `WRITE_TRUNCATE`는 기존 내용 교체이므로 주의하세요. 프레임워크에서 enum 사전 검증은 하지 않습니다. | `WRITE_EMPTY` |
| 생성 방식 | `grapes[].destination.create_disposition` | X | `CREATE_IF_NEEDED` | 문자열. Beam에 그대로 전달합니다. `CREATE_IF_NEEDED` 또는 `CREATE_NEVER` 등 유효성은 하위 라이브러리에서 검증됩니다. 데이터셋 자체를 만드는 설정은 아닙니다. | `CREATE_IF_NEEDED` |

- 적재 방식은 `WriteToBigQuery.Method.FILE_LOADS`로 고정입니다. Storage Write API/streaming inserts 선택은 YAML에 없습니다.
- `WRITE_APPEND`는 Airflow 재시도·재실행 시 중복을 만들 수 있습니다. 전체 새 job을 제출하므로 exactly-once나 deduplication이 보장되지 않습니다.
- `WRITE_TRUNCATE`는 실데이터를 대체할 수 있으므로 코드 기본값처럼 취급하지 마세요.
- `partitioning`, `clustering`, `schema_update_options`, `additional_bq_parameters`, load job의 별도 location 설정은 노출하지 않습니다. 필요하면 목적 테이블을 별도로 준비하거나 템플릿을 확장해야 합니다.

## 7. grape_defaults 및 options 전체 목록

아래 키는 `grape_defaults.<키>` 또는 `grapes[].options.<키>`에 지정합니다. Grape별 값이 우선합니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| 재시도 | `grape_defaults.retries` / `grapes[].options.retries` | X | `1` / 중복 위험이 있으면 `0` 검토 | 정수. Airflow 재시도 횟수. 재시도 때 Dataflow job을 새로 제출할 수 있습니다. | `0` |
| 재시도 간격 | `grape_defaults.retry_delay_seconds` / `grapes[].options.retry_delay_seconds` | X | `300` | 정수, 초. `timedelta`로 변환합니다. 저장소 샘플의 `60`은 예시 override입니다. | `60` |
| 실행 제한 | `grape_defaults.execution_timeout_seconds` / `grapes[].options.execution_timeout_seconds` | X | `21600` | 정수, 초. Airflow task timeout. Dataflow job의 자동 취소 timeout을 뜻하지 않습니다. Root/Vine timeout과 함께 조정하세요. | `21600` |
| 상태 조회 | `grape_defaults.poll_interval_seconds` / `grapes[].options.poll_interval_seconds` | X | `30` | 정수, 초. Dataflow 상태 조회 사이 `time.sleep()` 간격. 양수 사용을 권장하며 deferrable 센서가 아닙니다. | `30` |
| 작업명 | `grape_defaults.job_name_prefix` / `grapes[].options.job_name_prefix` | X | 해당 `grape_id` | 문자열. `<prefix>-<db_type>-<ts_nodash 변형>`을 만든 뒤 정규화합니다. Jinja 렌더링 대상. 영문자로 시작하는 짧은 소문자 이름을 권장합니다. | `db2-export-test` |
| 풀 | `grape_defaults.pool` / `grapes[].options.pool` | X | `None` 전달 / Airflow 환경 기본 적용 | 문자열. Airflow pool 이름입니다. 프레임워크가 pool을 생성하지 않습니다. | `dataflow_pool` |
| 우선순위 | `grape_defaults.priority_weight` / `grapes[].options.priority_weight` | X | `1` | 정수. Airflow scheduler 우선순위이며 Dataflow worker 성능 설정이 아닙니다. | `5` |

`job_name_prefix`의 결과는 소문자화되고 `[a-z0-9-]` 밖 문자는 `-`로 바뀌며, 연속 하이픈과 양끝 하이픈을 정리한 뒤 최대 55자로 자릅니다. 빈 결과는 `jdbc-export`가 됩니다. 앞쪽이 숫자인 이름을 문자로 보정하지 않고, 긴 prefix는 뒤쪽 timestamp를 잘라낼 수 있습니다. 짧은 영문 prefix를 쓰세요. 실행 이름에 Airflow `run_id`, task attempt 번호, 무작위 suffix를 넣지 않으므로 충돌 회피가 보장되지 않습니다.

`deferrable`, `wait_for_completion`, `trigger_rule`, `cancel_on_kill`, `num_workers`, `max_workers`, `machine_type`, `disk_size_gb`, `autoscaling_algorithm`, `experiments`, `labels`, `additional_experiments`, JDBC `fetch_size`·`partition_column`·`partitions`·`connection_properties`는 현재 이 실행기의 YAML 옵션으로 전달되지 않습니다. 임의로 적어도 활성화되지 않습니다.

## 8. schema/output.json과 타입

### 공통 작성 규칙

안전한 기본 형태는 `{"fields": [...]}`입니다. SQL 결과의 alias와 schema `name`이 정확히 일치해야 하며 대소문자도 확인합니다. 프레임워크는 JSON 구문을 확인하지만 컬럼과 결과 데이터의 호환성까지 검증하지 않습니다.

```json
{
  "fields": [
    {"name": "business_date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "current_ts", "type": "STRING", "mode": "NULLABLE"}
  ]
}
```

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| JSON schema | 해당 없음: `fields` | 안전한 사용에 필수 | 배열을 가진 최상위 객체 | 배열. Parquet 변환 함수는 객체의 `fields`를 읽습니다. **최상위 배열은 현재 `doc.get()` 호출에서 실패하므로 사용하지 않습니다.** | `{"fields": [...]}` |
| JSON schema | 해당 없음: `fields[].name` | O | SQL alias와 동일 | 문자열. Parquet 필드명 또는 BigQuery 컬럼명입니다. | `business_date` |
| JSON schema | 해당 없음: `fields[].type` | O | 실제 출력값과 맞는 타입 | 문자열. Parquet는 아래 고정 타입만 지원하고 대문자로 정규화합니다. BigQuery는 schema JSON을 Beam에 그대로 넘깁니다. | `STRING` |
| JSON schema | 해당 없음: `fields[].mode` | X | Parquet `NULLABLE` | 문자열. Parquet에서는 대문자화한 값이 `REQUIRED`일 때만 nullable=false. `REPEATED`를 배열로 변환하는 기능은 없습니다. | `NULLABLE` |

### Parquet 지원 타입과 별칭

- `STRING` → `pyarrow.string()`
- `BYTES` → `pyarrow.binary()`
- `BOOL`, `BOOLEAN` → `pyarrow.bool_()`
- `INT64`, `INTEGER` → `pyarrow.int64()`
- `FLOAT64`, `FLOAT` → `pyarrow.float64()`
- `DATE` → `pyarrow.date32()`
- `TIMESTAMP` → `pyarrow.timestamp("us")` (스키마에 timezone을 지정하지 않음)
- `NUMERIC` → `pyarrow.decimal128(38, 9)`
- `BIGNUMERIC` → `pyarrow.decimal256(76, 38)`

위의 타입 별칭만 명시적으로 구현되어 있습니다. `RECORD`/`STRUCT`, 중첩 필드, `REPEATED`, `DATETIME`, `TIME`, `JSON`, `GEOGRAPHY`, 사용자 지정 precision/scale은 Parquet 변환에서 지원하지 않습니다. 특히 `REPEATED`는 사전 거부되지 않고 단순 nullable 필드처럼 처리되므로 사용하지 마세요. 정의에 없는 타입은 템플릿 실행 중 `Unsupported parquet schema type` 오류가 납니다.

BigQuery는 이 Parquet 변환기를 사용하지 않습니다. `schema_file` JSON을 그대로 `WriteToBigQuery`에 전달하므로 BigQuery schema의 필드 구조를 따르되, JDBC Row가 실제로 해당 타입/중첩 구조를 제공하는지는 별도 검증해야 합니다. Parquet 지원 타입 목록을 BigQuery의 전체 지원 타입 목록으로 해석하지 마세요.

### DB 타입 정규화 주의

이 템플릿의 Parquet/BigQuery 경로는 Row → dict 변환만 하고 **값의 타입 변환을 하지 않습니다**. 스키마에 `DATE`라고 쓰는 것만으로 문자열이 Python date가 되지 않습니다. 특히 날짜/시간을 SQL에서 문자열로 바꾼 경우 Parquet에도 `STRING`으로 쓰거나, 별도 형 변환을 구현해야 합니다.

[기존 JDBC 가이드](../dataflow-jdbc-yaml-developer-guide.txt)는 DB2 native DATE/TIMESTAMP에서 JDBC `ERRORCODE=-4461`, `SQLSTATE=42815` 사례를 기록하고 문자열 정규화를 제안합니다. 아래 예제는 그 위험을 줄이기 위해 날짜/시간을 문자열로 추출하고 목적 스키마도 `STRING`으로 맞춥니다. 기존 [DB2 샘플 SQL](../../dags/pipelines/vines/ujlim_dataflow_db2_to_gcs_sample/sql/010_export_sample.sql)은 native DATE/TIMESTAMP를 사용하므로 모든 드라이버 조합에서 안전하다는 보장이 아닙니다.

운영 데이터에는 DB별 날짜/시간대, decimal precision/scale, NULL, 긴 문자열, LOB/BLOB, 비ASCII 문자를 포함한 소량 검증이 필요합니다. BigQuery DATE/TIMESTAMP 문자열 로딩을 이용하더라도 해당 형식이 실제 FILE_LOADS 경로에서 허용되는지 검증하고, Parquet에 같은 동작을 기대하지 마세요.

## 9. 전체 예제 A: DB2 → GCS Parquet

아래는 **비밀번호 없이**, 단일 행 읽기와 실행별 출력 prefix를 사용하는 예제입니다. `REPLACE_...` 값은 모두 실제 테스트 리소스로 바꾸세요. Airflow Connection `conn-dataflow-db2-test`의 login/host/schema 및 `extra.password_secret`를 먼저 준비합니다. 외부 리소스는 이 YAML이 생성하지 않습니다.

`config.yaml`:

```yaml
version: 1

vine:
  vine_id: example_db2_parquet
  description: DB2 single-row JDBC export test
  owner: data-engineering
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 25200
  tags: [example, dataflow, jdbc, db2]

business_date:
  type: daily

runtime:
  gcp_conn_id: google_cloud_default
  dataflow:
    project_id: REPLACE_DATAFLOW_PROJECT
    region: asia-northeast3
    system_bucket: REPLACE_SYSTEM_BUCKET
    template_gcs_path: gs://REPLACE_SYSTEM_BUCKET/templates/jdbc-export/template.json
    subnetwork: https://www.googleapis.com/compute/v1/projects/REPLACE_HOST_PROJECT/regions/asia-northeast3/subnetworks/REPLACE_SUBNET
    service_account_email: dataflow-worker@REPLACE_DATAFLOW_PROJECT.iam.gserviceaccount.com
    staging_location: gs://REPLACE_SYSTEM_BUCKET/staging
    temp_location: gs://REPLACE_SYSTEM_BUCKET/temp

grape_defaults:
  retries: 0
  retry_delay_seconds: 300
  execution_timeout_seconds: 21600
  poll_interval_seconds: 30
  pool: default_pool
  priority_weight: 1

grapes:
  - grape_id: export_db2_parquet
    type: jdbc_to_gcs
    depends_on: []
    source:
      db_type: db2
      connection_id: conn-dataflow-db2-test
      driver_jars: gs://REPLACE_SYSTEM_BUCKET/drivers/db2/db2jcc4.jar
    sql_file: sql/010_export.sql
    destination:
      path: "gs://REPLACE_OUTPUT_BUCKET/jdbc-test/db2/business_date={{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/run={{ ts_nodash }}/part"
      format: parquet
      schema_file: schema/output.json
    options:
      job_name_prefix: db2-parquet-test
```

`sql/010_export.sql`:

```sql
SELECT
  VARCHAR_FORMAT(
    DATE('{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}'),
    'YYYY-MM-DD'
  ) AS "business_date",
  VARCHAR_FORMAT(
    CURRENT TIMESTAMP,
    'YYYY-MM-DD HH24:MI:SS.FF6'
  ) AS "current_ts"
FROM SYSIBM.SYSDUMMY1
```

`schema/output.json`은 앞 절의 두 `STRING` 필드 예제를 그대로 사용합니다. 이를 통해 JDBC → Parquet 사이에서 별도 날짜 변환이 필요하지 않습니다. `ts_nodash`는 같은 logical date 재실행에 같을 수 있으므로, 이 경로 설계만으로 모든 재실행 충돌이 제거되는 것은 아닙니다.

### 같은 예제에서 JSON 또는 CSV 사용하기

전체 YAML의 `destination`을 아래 중 하나로 **교체**합니다. SQL은 그대로 사용할 수 있고 JSON/CSV에는 스키마 파일이 필요하지 않습니다.

```yaml
destination:
  path: "gs://REPLACE_OUTPUT_BUCKET/jdbc-test/db2-json/run={{ ts_nodash }}/part"
  format: json
```

```yaml
destination:
  path: "gs://REPLACE_OUTPUT_BUCKET/jdbc-test/db2-csv/run={{ ts_nodash }}/part"
  format: csv
```

## 10. 전체 예제 B: Oracle → BigQuery

별도 Vine 폴더에 아래 세 파일을 만듭니다. 신규 격리 테스트 테이블에 `WRITE_EMPTY`로 실행하여 기존 데이터에 조용히 추가하거나 덮어쓰지 않도록 했습니다. 재실행 때 테이블이 비어 있지 않으면 실패할 수 있으며 이는 이 테스트 설정의 의도입니다.

`config.yaml`:

```yaml
version: 1

vine:
  vine_id: example_oracle_bigquery
  description: Oracle single-row JDBC export test
  owner: data-engineering
  schedule: null
  timezone: Asia/Seoul
  start_date: "2026-01-01"
  max_active_runs: 1
  dagrun_timeout_seconds: 25200
  tags: [example, dataflow, jdbc, oracle]

business_date:
  type: daily

runtime:
  gcp_conn_id: google_cloud_default
  dataflow:
    project_id: REPLACE_DATAFLOW_PROJECT
    region: asia-northeast3
    system_bucket: REPLACE_SYSTEM_BUCKET
    template_gcs_path: gs://REPLACE_SYSTEM_BUCKET/templates/jdbc-export/template.json
    subnetwork: https://www.googleapis.com/compute/v1/projects/REPLACE_HOST_PROJECT/regions/asia-northeast3/subnetworks/REPLACE_SUBNET
    service_account_email: dataflow-worker@REPLACE_DATAFLOW_PROJECT.iam.gserviceaccount.com

grape_defaults:
  retries: 0
  retry_delay_seconds: 300
  execution_timeout_seconds: 21600
  poll_interval_seconds: 30
  pool: default_pool
  priority_weight: 1

grapes:
  - grape_id: export_oracle_bigquery
    type: jdbc_to_bigquery
    source:
      db_type: oracle
      connection_id: conn-dataflow-oracle-test
      driver_jars: gs://REPLACE_SYSTEM_BUCKET/drivers/oracle/ojdbc11.jar
      password_secret: projects/REPLACE_SECRET_PROJECT/secrets/oracle-read-password/versions/1
    sql_file: sql/010_export.sql
    destination:
      table: REPLACE_BQ_PROJECT:jdbc_test.oracle_export_test
      schema_file: schema/output.json
      write_disposition: WRITE_EMPTY
      create_disposition: CREATE_IF_NEEDED
    options:
      job_name_prefix: oracle-bq-test
```

`sql/010_export.sql`:

```sql
SELECT
  TO_CHAR(
    TO_DATE('{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}', 'YYYY-MM-DD'),
    'YYYY-MM-DD'
  ) AS "business_date",
  TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM') AS "current_ts"
FROM DUAL
```

`schema/output.json`:

```json
{
  "fields": [
    {"name": "business_date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "current_ts", "type": "STRING", "mode": "NULLABLE"}
  ]
}
```

Connection의 `login`은 여전히 필수이며, 이 예제는 Secret 참조를 YAML에 명시해 Connection extra보다 우선하는 경우입니다. 날짜/시각을 업무용 DATE/TIMESTAMP로 적재하려면 SQL 결과·스키마·시간대 정책을 함께 바꾸고 별도로 검증하세요.

Vertica를 사용할 때는 `source.db_type`, `connection_id`, `driver_jars`를 바꾸고 SQL을 Vertica 문법으로 작성합니다. 같은 `jdbc-export` 템플릿을 사용하며, driver class를 YAML에 추가하는 방식은 아닙니다.

## 11. 템플릿 파라미터 대응표

아래는 framework가 생성하는 **Flex Template 실행 파라미터**입니다. YAML 최상위에 직접 추가하는 필드가 아닙니다. 사용자 정의 템플릿을 교체·확장할 때 계약을 확인하는 용도입니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| export 입력 | `source.db_type` → `db_type` | 항상 전달 | `db2`/`oracle`/`vertica` | 문자열. argparse choices와 일치해야 합니다. | `db2` |
| export 입력 | Connection → `connection_url` | 항상 전달 | 생성 URL 또는 `extra.jdbc_url` | 문자열. 실제 JDBC URL입니다. | `jdbc:db2://db.example.internal:50000/SAMPLEDB` |
| export 입력 | Connection `login` → `username` | 항상 전달 | 없음 | 문자열. DB 사용자명입니다. | `etl_reader` |
| export 입력 | source/Connection → `password_secret` | 항상 전달 | 없음 | 문자열. Secret 참조만 전달합니다. | `projects/REPLACE_PROJECT/secrets/db-read-password` |
| export 입력 | `source.driver_jars` → `driver_jars` | 항상 전달 | 없음 | 문자열. JDBC JAR 위치입니다. | `gs://REPLACE_SYSTEM_BUCKET/drivers/db2/db2jcc4.jar` |
| export 입력 | 자동 생성 → `query_uri` | 항상 전달 | SQL 업로드 경로 | 문자열. `gs://` 시작 여부를 템플릿에서 확인합니다. | `gs://REPLACE_SYSTEM_BUCKET/sql/example/run/task.sql` |
| export 입력 | Grape type → `destination_type` | 항상 전달 | 타입에서 결정 | 문자열. `gcs` 또는 `bigquery`. | `gcs` |
| GCS 출력 | `destination.path` → `output_path` | GCS 필수 | 없음 | 문자열. 출력 prefix입니다. | `gs://REPLACE_OUTPUT_BUCKET/export/part` |
| GCS 출력 | `destination.format` → `output_format` | GCS에 전달 | `json` | 문자열. `json`/`csv`/`parquet`. | `parquet` |
| GCS 출력 | `destination.schema_file` 내용 → `parquet_schema` | Parquet 필수 | 없음 | JSON 문자열. 프레임워크가 파일을 읽어 생성합니다. | `{"fields":[...]}` |
| BigQuery 출력 | `destination.table` → `output_table` | BigQuery 필수 | 없음 | 문자열. 목적 테이블입니다. | `REPLACE_PROJECT:jdbc_test.output` |
| BigQuery 출력 | `destination.schema_file` 내용 → `bq_schema` | BigQuery 필수 | 없음 | JSON 문자열. 프레임워크가 파일을 읽어 생성합니다. | `{"fields":[...]}` |
| BigQuery 출력 | `destination.write_disposition` → `write_disposition` | BigQuery에 전달 | `WRITE_APPEND` | 문자열. 쓰기 정책입니다. | `WRITE_EMPTY` |
| BigQuery 출력 | `destination.create_disposition` → `create_disposition` | BigQuery에 전달 | `CREATE_IF_NEEDED` | 문자열. 테이블 생성 정책입니다. | `CREATE_NEVER` |

공식 템플릿의 camelCase 파라미터나 추가 Beam 옵션을 이 YAML에 적어도 이 대응표에 자동 합쳐지지 않습니다. `destination.schema`는 파일을 읽어 생성하는 내부 값이며, 이를 직접 적어 `schema_file` 요구를 대체할 수 없습니다.

## 12. jdbc-test는 별도 진단 도구

[jdbc-test 코드](../../tools/dataflow/jdbc-test/docker/main.py)는 DB별 고정된 현재시각 조회 SQL을 실행하고 결과를 로그에 남기는 별도 템플릿입니다. `jdbc_to_gcs`/`jdbc_to_bigquery`의 destination, SQL 파일, Secret 참조 계약을 구현하지 않으므로 `runtime.dataflow.template_gcs_path`를 `jdbc-test/template.json`으로 바꾸어 실행할 수 없습니다.

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| test CLI | 해당 없음: `--db_type` | O | 없음 | 문자열. `db2`/`oracle`/`vertica`. YAML Grape 옵션이 아닙니다. | `vertica` |
| test CLI | 해당 없음: `--connection_url` | O | 없음 | 문자열. 테스트 JDBC URL입니다. | `jdbc:vertica://db.example.internal:5433/TESTDB` |
| test CLI | 해당 없음: `--username` | O | 없음 | 문자열. DB 사용자명입니다. | `etl_reader` |
| test CLI | 해당 없음: `--password` | O | 없음 / 운영 사용 주의 | 문자열. **현재 구현은 평문 password 파라미터를 요구합니다.** export의 Secret Manager 처리가 없습니다. | 실제 값 예시 생략 |
| test CLI | 해당 없음: `--driver_jars` | O | 없음 | 문자열. JDBC JAR 위치입니다. | `gs://REPLACE_SYSTEM_BUCKET/drivers/vertica/driver.jar` |

- [`run-vertica.sh`](../../tools/dataflow/jdbc-test/script/run-vertica.sh)는 환경변수의 암호를 `--parameters ... password=...`로 전달합니다. shell 이력·프로세스 인자·실행 메타데이터 노출 위험이 있으므로 Secret 기반 export와 같은 보안 수준으로 취급하지 마세요. 실제 암호가 포함된 명령을 문서·로그·저장소에 남기지 않습니다.
- [`run-db2.sh`](../../tools/dataflow/jdbc-test/script/run-db2.sh)와 [`run-oracle.sh`](../../tools/dataflow/jdbc-test/script/run-oracle.sh)는 현재 안내 메시지만 출력하며 job을 제출하지 않습니다.
- [test Dockerfile](../../tools/dataflow/jdbc-test/docker/Dockerfile)은 export와 달리 `FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE`을 설정하므로 private launcher의 의존성 다운로드 경로를 별도로 점검해야 합니다.
- Secret 기반 연결 확인이 목적이라면 위처럼 `jdbc-export`와 소량 읽기 SQL·격리된 출력으로 검증하는 방식을 사용할 수 있습니다. 실행 자체는 Dataflow 비용과 목적지 쓰기를 발생시킵니다.

## 13. 완료 판정·재실행·제한

### 상태와 반환값

- 성공: `JOB_STATE_DONE`, `JOB_STATE_UPDATED`
- 종료했지만 실패로 처리: `JOB_STATE_FAILED`, `JOB_STATE_CANCELLED`, `JOB_STATE_DRAINED`
- 그 외 상태: 다음 poll까지 대기. launch 응답에 job ID가 없으면 즉시 실패합니다.
- 성공 반환값: `job_id`, `job_name`, `state`, `query_uri`. 기본 Airflow XCom 동작에서 후속 작업이 이 결과를 참조할 수 있습니다.
- row count, 출력 파일 목록, BigQuery 적재 행 수, JDBC checksum을 별도로 검증하거나 반환하지 않습니다. Job 성공 뒤 업무 데이터 검증을 추가하세요.

### 운영상 중요한 한계

1. **일반 동기 Operator**입니다. polling 동안 Airflow worker slot을 계속 사용합니다. `deferrable` 옵션이 없습니다.
2. **취소 전파를 구현하지 않았습니다.** Airflow timeout·작업 종료 시 Dataflow job이 자동 취소된다고 가정하지 마세요. Dataflow 상태와 비용을 별도로 확인합니다.
3. **재시도 시 기존 job에 다시 연결하지 않습니다.** SQL 업로드와 launch를 다시 수행합니다. 이미 실행 중인 job, 이름 충돌, GCS prefix 충돌, BigQuery 중복 적재가 가능하므로 재시도 전에 이전 job 상태를 확인합니다.
4. **JDBC 읽기 병렬화 옵션을 노출하지 않습니다.** `ReadFromJdbc`에 query와 접속 정보만 넘깁니다. 읽기 분할·fetch 크기·connection properties를 YAML로 조절할 수 없습니다.
5. **직접 SQL 검증을 하지 않습니다.** 문법·컬럼·읽기 전용 여부를 검사하지 않으므로 읽기 권한 DB 계정과 검증된 SELECT를 사용합니다.
6. **스키마는 자료형 변환기가 아닙니다.** 문자열 날짜와 Parquet DATE 같은 불일치를 스키마만 바꾸어 해결할 수 없습니다.
7. **빌드와 배포는 별도 작업입니다.** YAML은 이미지/템플릿/Connection/Secret/IAM/네트워크/BigQuery 데이터셋을 생성하지 않습니다. [build-image](../../tools/dataflow/jdbc-export/script/build-image.sh)와 [build-template](../../tools/dataflow/jdbc-export/script/build-template.sh)의 환경변수 및 기본 리소스를 확인하고 운영 담당 범위에서 수행합니다.

## 14. 실행 전 점검과 빠른 진단

- [ ] 모든 `REPLACE_...`를 테스트 리소스로 바꾸고 운영 table/bucket이 아닌지 확인
- [ ] `template_gcs_path`가 `jdbc-export` 계약의 템플릿인지 확인
- [ ] SQL·JSON 파일이 Vine 폴더 안에 존재하며 JSON이 올바른지 확인
- [ ] Connection login과 URL 또는 host/schema, DB별 port 확인
- [ ] Secret의 프로젝트·이름·버전과 실제 ADC 실행 주체의 권한 확인
- [ ] JDBC JAR의 존재·읽기 권한·Java/DB 버전 호환성 확인
- [ ] worker SA 및 Airflow ADC의 권한을 구분하여 확인
- [ ] private subnet의 DB 및 필요한 Google 서비스 연결 확인
- [ ] SQL alias와 schema 필드명·자료형·nullable 일치 확인
- [ ] Parquet는 지원 타입과 객체형 `fields` 스키마 사용
- [ ] `WRITE_APPEND` 중복 또는 `WRITE_TRUNCATE` 교체 영향 검토
- [ ] task timeout보다 상위 Vine/Root 제한이 짧지 않은지 확인
- [ ] 소량 실행 뒤 Dataflow 종료뿐 아니라 실제 파일/테이블 내용 확인

오류를 구분하면 확인 지점이 명확합니다.

- DAG 생성 시 필수 필드/SQL/schema 오류: YAML 위치와 파일 경로를 먼저 확인합니다.
- `secretmanager.versions.access` 403: 참조 프로젝트·Secret·버전·ADC 계정·권한을 확인합니다. 403만으로 원인을 하나로 단정하지 않습니다.
- launcher timeout: 템플릿/이미지, private 네트워크, 의존성 및 expansion service 로딩 로그를 확인합니다.
- DB2 `-4461` / `42815`: 기존 가이드에 기록된 날짜/시간 등 JDBC 타입 변환 가능성을 검토합니다.
- Parquet schema/Arrow 변환 오류: 최상위 `fields` 객체, 지원 타입, 실제 Row 값의 자료형을 확인합니다.
- BigQuery load 오류: dataset/table, FILE_LOADS 권한·위치, schema/SQL alias, 실제 값, 쓰기 정책을 확인합니다.
- Airflow 실패 뒤 Dataflow가 계속 실행: 자동 취소가 없다는 제한을 고려하여 이전 job을 점검하고 재실행 여부를 판단합니다.

## 15. 저장소 참고 샘플

기존 샘플은 구조 확인용이며, 환경 값·placeholder·SQL native 타입을 그대로 운영에 적용하지 마세요.

- [DB2 → GCS](../../dags/pipelines/vines/ujlim_dataflow_db2_to_gcs_sample/config.yaml), [DB2 → BigQuery](../../dags/pipelines/vines/ujlim_dataflow_db2_to_bq_sample/config.yaml)
- [Oracle → GCS](../../dags/pipelines/vines/ujlim_dataflow_oracle_to_gcs_sample/config.yaml), [Oracle → BigQuery](../../dags/pipelines/vines/ujlim_dataflow_oracle_to_bq_sample/config.yaml)
- [Vertica → GCS](../../dags/pipelines/vines/ujlim_dataflow_vertica_to_gcs_sample/config.yaml), [Vertica → BigQuery](../../dags/pipelines/vines/ujlim_dataflow_vertica_to_bq_sample/config.yaml)
- [6개 JDBC Vine 호출 Root](../../dags/pipelines/roots/ujlim_dataflow_jdbc_sample_daily/config.yaml)

[목차로](README.md) · [공통 YAML](01-yaml-common.md) · [운영 가이드](05-operations.md)
