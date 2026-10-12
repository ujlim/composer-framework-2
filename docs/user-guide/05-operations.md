# 5. 실행 준비와 운영

[목차](README.md) · [이전: Dataflow](04-dataflow.md)

## 배포 전 확인

이 저장소는 Composer 3 / Airflow 진입점 [dags/main.py](../../dags/main.py), 프레임워크 코드, YAML·SQL 샘플, 별도 실행 도구를 제공합니다. 이 브랜치에는 전체 Airflow 환경을 설치하는 잠금 파일·배포 자동화·자동 테스트 모음이 없습니다. 실제 Composer의 Python/Airflow/provider 버전에 맞춰 검증해야 합니다.

1. `main.py`, `framework/`, `pipelines/`를 같은 DAG 배포 구조로 준비합니다. 설정 파일만 올리고 실행기 코드를 누락하지 마세요.
2. 환경에 PyYAML, Pendulum과 사용 중인 Airflow standard/google/ssh provider, PostgreSQL 드라이버 `psycopg` 또는 fallback `psycopg2`, Google API·Secret Manager 라이브러리가 있는지 확인합니다. `VineFactory`가 실행기들을 import하므로 아직 쓰지 않는 type의 import 의존성도 DAG 발견에 영향을 줄 수 있습니다.
3. 모든 `root_id`/`vine_id`가 전역 고유하고 Grape ID/trigger ID가 자동 task ID와 충돌하지 않는지 확인합니다.
4. SQL·스키마 참조가 해당 Vine 폴더 안의 실제 파일을 가리키는지 확인합니다. 스케줄 없는 Vine만 단독 배포해 테스트할 수도 있습니다.
5. Airflow Connection, Pool, Triggerer, IAM, 네트워크, 버킷/BigQuery 리전, Runner와 Flex Template를 먼저 준비합니다. YAML이 이 자원을 생성하지는 않습니다.
6. 샘플의 프로젝트·버킷·SA·Connection·DB 주소를 환경별 값으로 교체합니다. 기존 샘플에는 실제 환경을 가정한 이름이 있으므로 그대로 실행하지 마세요.
7. Cloud Composer DAG import 오류, `dag_registered`, `dag_registration_failed` 로그를 확인합니다. Graph의 의존성과 Rendered Templates를 검토합니다.
8. 테스트 전용 출력 경로에서 수동 `business_date`를 지정해 결과·행 수·FIN·재실행을 검증하고, 마지막에 Root 스케줄을 활성화합니다.

`deferrable: true`는 BigQuery, Root trigger, FIN sensor에서 사용합니다. 관련 provider와 Triggerer가 없으면 설정만으로 동작하지 않습니다. Dataflow JDBC는 자체 polling operator이며 deferrable 옵션을 읽지 않습니다.

## 연결과 실행 주체를 구분하기

| 실행 경로 | 설정·주체 | 주의점 |
|---|---|---|
| BigQuery | `runtime.gcp_conn_id`, `runtime.impersonation_chain` | 프로젝트·리전은 runtime 설정. query 실행 및 읽기/쓰기 대상에 필요한 권한 확보 |
| GCS copy/delete/move/merge, FIN sensor | 같은 GCP Connection/impersonation | list/read/create/delete 권한은 동작별로 다름. delete_source 사용 시 삭제 권한도 필요 |
| PostgreSQL 연결 | Grape `connection_id` → `runtime.postgres_connection_id` | Airflow Connection의 host/schema/login/port 및 `extra.password_secret` (Connection password는 사용하지 않음) |
| PostgreSQL Secret Manager | Composer 프로세스의 ADC | `SecretManagerServiceClient()` 직접 사용. GCS Connection과 impersonation_chain으로 자동 전환되지 않음 |
| PostgreSQL→GCS 업로드 | runtime GCP Connection/impersonation | DB 인증과 GCS 인증이 서로 다름 |
| Dataflow SQL GCS 업로드 | runtime GCP Connection/impersonation | Dataflow job 제출 주체와 다를 수 있음 |
| Dataflow API 제출·조회 | Composer 프로세스의 ADC | 현재 `build('dataflow', 'v1b3')`에 hook 자격증명을 넘기지 않음 |
| Dataflow worker | `runtime.dataflow.service_account_email` | 실행 SA. Secret·JDBC JAR·SQL·출력·temp/staging 접근 및 DB 네트워크가 필요 |
| Composer→SFTP Runner | `runner.ssh_conn_id` | Runner VM에 SSH 접속하는 Connection. 최종 SFTP 계정이 아님 |
| Runner→Google | Runner ADC 또는 targets JSON의 `runner.service_account` | Runner 내부 GCS/Secret 접근. Vine runtime GCP 설정과 분리 |
| Runner→최종 SFTP | `destination.target`가 고른 targets JSON | SSH key/password Secret, host key, port 등 Runner 설정 사용 |
| 관리 shell 도구 | 도구 실행자의 gcloud/도구별 impersonation | Composer·Dataflow·Runner SA와 같은 주체라고 가정하지 말 것 |

권한은 실제 사용하는 자원·동작에 맞게 최소화합니다. SA를 지정한다고 해당 SA를 사용할 권한이 생기지는 않습니다. impersonation 허용, Dataflow 실행 SA 사용 권한, 네트워크 접근은 관리자가 별도로 준비해야 합니다. YAML에는 Secret **리소스 이름**만 기록하고 비밀번호·private key·토큰을 커밋하지 마세요. 런타임 Connection의 password를 Secret Manager로 바꿔도 모든 로그가 자동으로 비밀정보에 안전해지는 것은 아닙니다.

구체적인 필드와 인증 우선순위는 [SQL](02-sql.md), [SFTP](03-storage-sftp.md), [Dataflow](04-dataflow.md)를 참고하세요.

## 수동 실행과 재처리

- Root를 수동 실행할 때 Airflow Trigger 화면의 Business Date에 `YYYY-MM-DD`를 입력합니다. Root를 통해 호출된 Vine에는 context가 전달됩니다.
- Vine을 직접 실행할 때도 Business Date가 필요합니다. Root의 일반 parameters는 자동 제공되지 않으므로 필요한 값을 conf로 명시합니다.
- 같은 business_date라도 다른 run ID이면 별도 실행입니다. 프레임워크 전체에 공통 중복 방지·트랜잭션·rollback 기능이 있는 것은 아닙니다.
- 실패한 Root trigger를 재시도할 때 같은 생성 run ID가 충돌할 수 있습니다. `reset_dag_run`을 무조건 true로 바꾸기 전에 기존 Vine 상태와 이미 생성된 결과를 확인합니다.
- overwrite, append, source delete, DB DML, FIN 발행은 재시도 시 부작용이 다릅니다. 출력 경로와 SQL의 멱등성 정책을 먼저 정하세요.
- Airflow timeout/취소가 외부 작업을 모두 취소한다고 가정하지 마세요. 특히 Dataflow의 기존 job, Runner에서 진행 중인 원격 작업, 이미 업로드된 파일을 직접 확인해야 합니다.
- FIN 파일은 성공의 범위를 명확히 하세요. GCS FIN sensor는 **존재 여부만** 확인하며 내용·업무일자·행 수 일치를 검증하지 않습니다. 오래된 동일 이름 FIN이 남으면 즉시 통과할 수 있습니다.

## 로그와 장애 대응

[logger.py](../../dags/framework/logger.py)는 `vine_framework.*` logger로 JSON 이벤트를 기록합니다. task_success/task_failure에는 DAG/task/run/시도 수, 성공 시 duration, 실패 시 exception과 log_url이 포함됩니다. DAG 성공/실패 이벤트, 실행기별 완료 이벤트, main의 등록 로그도 확인하세요.

키 이름에 password/secret/token/private_key/credential이 포함된 dict 값은 마스킹하지만 **임의 문자열 안의 비밀, SQL 결과, 예외 메시지 전체를 정교하게 정제하는 기능은 아닙니다**. 민감한 쿼리 결과를 로그/XCom으로 출력하지 않도록 fetch·테스트 사용을 제한하세요.

| 증상 | 우선 확인 | 해석·다음 단계 |
|---|---|---|
| DAG가 안 보임 | main import, 탐색 경로, YAML 오류 로그 | 파일 단위 로드/Factory 오류는 다른 DAG와 격리되지만 catalog 중복 ID·import 오류 등은 전체를 막을 수 있음 |
| Root는 보이는데 Vine 실행 불가 | 해당 Vine 등록 로그와 DAG pause 상태 | catalog 존재 검사만 통과하고 Vine Factory 생성은 실패했을 수 있음 |
| business_date required | scheduled 여부, conf/params 값 | 수동 실행 날짜 입력. 비정기 run에 오늘 날짜 자동 사용 없음 |
| unknown ID / cycle | depends_on 이름과 ID 중복 | 목록 순서가 아니라 ID 그래프 수정 |
| file escapes / not found | sql_file/schema_file 상대 경로 | Vine 폴더 내부 실제 파일로 수정 |
| 권한 거부 | 표의 실제 호출 주체와 대상 자원 | runtime SA, Composer ADC, worker SA, Runner SA 혼동 여부 확인 |
| Dataflow 대기·timeout | job ID/currentState/launcher/worker 로그 | 기존 job이 살아 있는지 확인 후 재시도 판단 |
| FIN이 너무 빨리 통과 | bucket/object, 이전 파일 잔존 | 날짜별 객체명, 생성·정리 정책 확인 |
| overwrite/exists 실패 | 이전 실행의 완성·부분 출력 | 삭제부터 하지 말고 결과와 재실행 정책 확인 |
| CSV 병합 결과 이상 | 패턴/파일 정렬/헤더/인코딩 | 행 수 의미와 헤더 처리, 이종 스키마 혼합 여부 확인 |
| SFTP 접속 실패 | Composer→Runner SSH와 Runner→SFTP 구간 구분 | 각 Connection/Secret/known_hosts/네트워크를 별도로 확인 |

## 제공 운영 도구

다음은 YAML Grape가 아닌 별도 shell/Python 도구입니다. 설명은 현재 코드 기준이며 여기서는 실행하지 않습니다. 각 도구의 기본값에는 특정 운영 프로젝트/버킷이 들어 있으므로 **실행 전 환경변수를 명시하고 원문을 검토**하세요.

| 도구 | 입력·기본값 | 기능·주의사항 |
|---|---|---|
| [jdbc-export build-image.sh](../../tools/dataflow/jdbc-export/script/build-image.sh) | PROJECT_ID, REGION, REPOSITORY, IMAGE | Cloud Build로 jdbc-export 이미지를 빌드·게시. 비용·외부 상태 변경 발생 |
| [jdbc-export build-template.sh](../../tools/dataflow/jdbc-export/script/build-template.sh) | PROJECT_ID, REGION, SYSTEM_BUCKET, REPOSITORY, IMAGE, TEMPLATE_PATH | metadata.json 기반 Flex Template를 GCS에 기록. framework의 `runtime.dataflow.template_gcs_path`와 맞춰야 함 |
| [jdbc-test build-image.sh](../../tools/dataflow/jdbc-test/script/build-image.sh) | PROJECT_ID, REGION, REPOSITORY, IMAGE_TAG=`v2`, BUILD_LOG_BUCKET | JDBC 연결 진단용 이미지. export 이미지와 별도 |
| [jdbc-test build-template.sh](../../tools/dataflow/jdbc-test/script/build-template.sh) | PROJECT_ID, REGION, REPOSITORY, IMAGE_TAG, SYSTEM_BUCKET | 출력은 `templates/jdbc/template.json`; run-vertica 기본 경로 `templates/jdbc-test/template.json`과 다르므로 정합성 확인 |
| [jdbc-test run-vertica.sh](../../tools/dataflow/jdbc-test/script/run-vertica.sh) | VERTICA_HOST/DB/USER/PASSWORD 필수; PORT=`5433`; VERTICA_JAR 및 프로젝트/서브넷/SA/template env | JAR 확인 후 실제 Dataflow 테스트 제출. password를 template parameter로 전달하므로 운영 비밀 취급에 특히 주의. framework의 password_secret 방식과 다름 |
| [run-db2.sh](../../tools/dataflow/jdbc-test/script/run-db2.sh), [run-oracle.sh](../../tools/dataflow/jdbc-test/script/run-oracle.sh) | 안내문에 DB별 env 제시 | **현재 안내문만 출력**하며 실제 job을 제출하지 않음 |
| [check-running.sh](../../tools/dataflow/admin/check-running.sh) | PROJECT_ID, REGION, LIST_COUNT=`10`, FETCH_COUNT=`100`, LOG_LIMIT=`1000` | job 목록에서 대화형 번호 선택 후 상태·launcher·warning/error·JDBC 결과 로그 조회. 지속 polling 서비스가 아님 |
| [img-pull-impersonated.sh](../../tools/dataflow/admin/img-pull-impersonated.sh) | PROJECT_ID, REGION, REPOSITORY, MGNT_SA, IMAGE_TAG=`v2`, DOCKER_CONFIG_DIR=`/tmp/docker-mgnt` | SA impersonation token으로 Docker login 후 jdbc-test 이미지 pull. 지정 Docker config 폴더를 먼저 **재귀 삭제**하므로 안전한 전용 경로만 사용 |
| [backup-dags.sh](../../tools/mgnt/backup-dags.sh) | 인자1 COMPOSER_DAGS_URI, 인자2 BACKUP_DIR; env로 대체 가능; RETENTION_DAYS=`30` | GCS dags 다운로드→timestamp tgz→tar 무결성 확인. 기본 출력은 현재 폴더. 오래된 `dags-*.tgz`를 삭제하며 `RETENTION_DAYS=0`이면 정리 안 함 |
| [gcs_to_sftp.py](../../tools/sftp-runner/gcs_to_sftp.py) | CLI와 targets JSON | SSHOperator가 Runner VM에서 실행하는 별도 프로그램. [전체 설정](03-storage-sftp.md) 참고 |

백업 보관 정책은 소스 DAG 삭제와 다릅니다. 백업 도구는 로컬 보관 파일을 지울 수 있으므로 처음 실행할 때는 `RETENTION_DAYS=0`과 전용 BACKUP_DIR를 사용해 확인하는 편이 안전합니다.

## 검증 범위와 남은 환경 검증

이 사용자 가이드는 현재 브랜치의 12개 type 등록 경로, Factory·실행기·Runner·Flex Template·샘플·운영 스크립트를 대조해 작성했습니다. Markdown 링크와 YAML 구문 및 로컬 소스 기반 검증 결과는 커밋을 전달할 때 별도로 보고합니다.

실제 Composer DAG import/provider 호환성, GCP IAM, 네트워크/방화벽, DB 접근, Dataflow job, Runner SSH/SFTP, 대용량 처리, 과금·결과 데이터는 운영 환경 통합 테스트로 확인해야 합니다. 문서 예제 통과는 외부 서비스 실행 성공을 의미하지 않습니다.
