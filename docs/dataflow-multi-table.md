# 작은 Oracle 테이블 여러 개를 Dataflow Job 하나로 적재하기

작성자: 임의진 (LIM UI JIN) · 작성일: 2026-10-28 · 수정일: 2026-10-31

## 목적과 적용 범위

테이블마다 Dataflow job을 새로 띄우면 launcher/worker 준비 시간이 반복됩니다. **약 10분의 시작 대기가 반복되는 작은 테이블들**을 한 job의 독립 분기로 묶는 것이 이 기능의 목적입니다. 실제 시작 시간·비용 개선은 환경별 측정이 필요하며 보장하지 않습니다.

**이전에 OOM이 났다가 16GB로 늘린 뒤 800만 행을 약 3시간에 처리한 무거운 테이블들을 그대로 함께 묶는 해결책이 아닙니다.** 한 job에서 읽기·변환·파일 쓰기가 겹치면 worker 메모리, Oracle 세션, DB CPU/I/O와 네트워크 부하가 커질 수 있습니다. 먼저 작은 테이블 소수로 검증하고 무거운 테이블은 별도 job으로 유지하십시오. YAML의 테이블 순서는 순차 실행·동시성 제한이 아닙니다.

이 기능은 신규 `jdbc_tables_to_bigquery`에만 적용합니다. 기존 `jdbc_to_bigquery` / `jdbc_to_gcs` 설정과 단일 모드 흐름은 그대로 두며, 전역 worker 기본값을 변경하지 않습니다. fetch_size, SDK container image, 자료형 자동 변환, JDBC 분할 읽기는 이번 기능에 추가하지 않습니다.

```text
Vine: ujlim_dataflow_oracle_multi_to_bq
  resolve_business_date  ← 공통 업무일 1회 확정
      ↓
  export_three_oracle_tables  ← Grape 1개, Flex Template launch 1회
      ↓
  Dataflow Job 1개 / Beam Pipeline 1개
      ├─ EMP 읽기   → emp_stage   (FILE_LOADS)
      ├─ DEPT 읽기  → dept_stage  (FILE_LOADS)
      └─ SALES 읽기 → sales_stage (FILE_LOADS)
```

같은 Oracle connection 설정을 사용하지만 JDBC 세션 하나를 공유한다는 뜻은 아닙니다. 분기별 세션이 만들어질 수 있으며 실제 실행 순서·동시성은 runner가 결정합니다. BigQuery FILE_LOADS의 개별 load/copy job 개수와 Dataflow job 개수는 다릅니다.

## 예제 파일과 YAML

`dags/pipelines/vines/ujlim_dataflow_oracle_multi_to_bq/`에 `config.yaml`, SQL 3개, 스키마 JSON 3개가 있습니다. 핵심 설정 형태는 다음과 같습니다(실제 전체 값은 파일 참조).

```yaml
grapes:
  - grape_id: export_three_oracle_tables
    type: jdbc_tables_to_bigquery
    source:
      db_type: oracle
      connection_id: conn-dataflow-oracle
      driver_jars: gs://YOUR_SYSTEM_BUCKET/drivers/oracle/ojdbc11.jar
    tables:
      - id: emp
        sql_file: sql/emp.sql
        destination:
          table: "YOUR_PROJECT:YOUR_STAGE_DATASET.emp_stage"
          schema_file: schema/emp.json
          write_disposition: WRITE_TRUNCATE
          create_disposition: CREATE_IF_NEEDED
      # dept, sales도 같은 tables 목록에 추가. 별도 Grape를 만들지 않음.
```

공통 `source`와 `runtime.dataflow`는 한 번만 선언합니다. 각 table에는 고유 `id`와 SQL·스키마·목적지가 필요합니다. 여러 항목이 같은 목적지 테이블에 쓰는 구성은 허용하지 않습니다. 이 예제는 평면 BigQuery 스키마를 사용합니다. 중첩 RECORD/STRUCT나 Oracle 컬렉션 매핑을 제공하지 않습니다.

Composer가 SQL을 한 번 렌더링한 뒤 실행별 고유 GCS 경로에 SQL/schema를 올리고 `version: 1` manifest를 작성합니다. launcher에는 manifest URI 한 개를 전달합니다. manifest에는 table 식별자·query_uri·schema_uri·destination만 담고 DB 비밀번호는 담지 않습니다. 비밀번호는 기존 Secret Manager 경로를 유지합니다. 단, JDBC transform 구성 과정에서 비밀번호는 Beam Java expansion payload에 전달되므로 “manifest에 없다”가 “직렬화/메타데이터에 절대 없다”는 뜻은 아닙니다. IAM·로그·메타데이터 접근을 제한하십시오.

## 실행 전 반드시 교체·확인할 값

- `REPLACE_WITH_SHARED_VPC_HOST_PROJECT`, `REPLACE_WITH_STAGE_DATASET`를 교체합니다. project, region, system bucket, worker SA, connection ID, driver JAR는 기존 저장소 기본값을 유지한 예시이므로 실제 배포 환경과 대조합니다.
- `SAMPLE_EMP`, `SAMPLE_DEPT`, `SAMPLE_SALES`는 **설명용 가상 테이블명**입니다. 실제 업무 테이블이 존재한다고 가정하지 않습니다. DDL은 제공하지 않으며 실제 SELECT 권한이 있는 테이블/컬럼으로 수정해야 합니다.
- 예제 컬럼 전제:
  - EMP: EMP_ID, EMP_NAME, HIRED_AT(Oracle DATE), UPDATED_AT(Oracle DATE)
  - DEPT: DEPT_ID, DEPT_NAME, UPDATED_AT(Oracle DATE)
  - SALES: SALE_ID, SALE_AMOUNT(NUMBER(18,2)), SOLD_AT(TIMESTAMP WITH TIME ZONE), UPDATED_AT(Oracle DATE)
- UPDATED_AT는 업무 시간대의 벽시계 DATE라는 전제입니다. `[business_date, business_date + 1일)` 필터를 사용합니다. 원천이 UTC/TZ이면 필터를 해당 의미에 맞게 수정하십시오.
- ID는 STRING으로 투영합니다. DATE의 시간은 DATETIME 문자열로 보존하며 TSTZ는 SYS_EXTRACT_UTC 후 `FF6"Z"` 형태로 출력합니다. NULL에 `Z`만 붙지 않으며 원래 timezone/offset 자체는 보존하지 않습니다.
- SALE_AMOUNT의 `FM9999999999999990D00`은 **NUMBER(18,2) 범위 전용**입니다. 더 큰 정밀도나 2자리 초과 scale은 SQL 포맷과 NUMERIC/BIGNUMERIC 스키마를 함께 변경하고 경계값을 검증해야 합니다.
- BigQuery staging dataset을 미리 만들고 GCS temp 위치와 BigQuery 위치·권한을 확인합니다. 샘플 WRITE_TRUNCATE는 당일 결과로 staging 테이블 전체를 교체하므로 운영 누적 테이블에 그대로 쓰면 안 됩니다.

SQL 별칭은 스키마와 일치하는 소문자를 쌍따옴표로 지정합니다. SQL 끝에 세미콜론은 붙이지 않습니다. 스키마 JSON에는 주석 대신 description으로 작성 정보를 기록했습니다.

## 공통 BUSINESS_DATE와 실행

Airflow Manual Trigger의 파라미터 이름은 소문자 `business_date`입니다. 예: `2026-10-30`. 기존 `resolve_business_date`가 날짜를 검증·확정하고 세 SQL 모두 다음 값으로 렌더링됩니다.

```jinja
{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}
```

이는 세 분기의 공통 BUSINESS_DATE이며 launcher/worker가 현재 날짜로 재계산하지 않습니다. 같은 업무일을 쓴다고 같은 Oracle snapshot/SCN을 읽는 것은 아닙니다.

새 Vine은 `schedule: null`이고 자동 실행 Root를 추가하지 않습니다. 승인된 배포 후 DAG import 오류를 확인하고 Vine을 **한 번만** 수동 실행합니다. 테이블 수만큼 trigger하지 마십시오. job ID 하나와 Dataflow graph의 세 분기를 확인한 뒤 목적지별 건수·자료형·NULL을 검증합니다. 정기 실행은 검증 후 기존 Root에 이 Vine 한 개를 연결하는 별도 변경으로 진행합니다.

## 새 이미지/템플릿: 안전한 준비 절차

이 기능의 launcher 코드·helper·metadata가 포함된 **새 이미지와 새 Flex Template**이 필요합니다. 기존 progress 이미지가 있다는 사실만으로 새 기능이 배포되지는 않습니다. 반대로 기존 단일 테이블 Vine을 새 template로 일괄 전환하지 마십시오.

현재 코드 작업은 실제 Cloud Build, Artifact Registry 게시, GCS template 생성, Composer 배포 또는 Dataflow launch를 승인하거나 수행한 것이 아닙니다. 아래 명령은 기존 `build-progress-safe.py`의 **offline dry-run**이며 클라우드 읽기/쓰기를 하지 않습니다.

```bash
# legacy overrides를 쓰지 않음. 이 shell에서만 해제.
unset IMAGE TEMPLATE_PATH
python tools/dataflow/jdbc-export/script/build-progress-safe.py \
  --project gcp-prod-edp-sa-udw \
  --region asia-northeast3 \
  --repository repo-prod-sa-udw-dataflow-tmplt-1 \
  --bucket bucket-prod-sa-udw-dataflow-system-1 \
  --version 20261031-multi-v1
```

이 버전의 예정 목적지:

- 이미지: `asia-northeast3-docker.pkg.dev/gcp-prod-edp-sa-udw/repo-prod-sa-udw-dataflow-tmplt-1/jdbc-export-progress:20261031-multi-v1`
- 템플릿: `gs://bucket-prod-sa-udw-dataflow-system-1/templates/jdbc-export-progress/20261031-multi-v1.json`

이 이름을 신규 sample의 template_gcs_path에 사용했습니다. 실제 버전이 이미 존재하면 다른 새 버전을 정하고 sample 경로도 함께 바꿉니다. “dry-run 출력 성공”은 원격 목적지 부재·권한·빌드 성공 증명이 아닙니다.

실제 실행 전 별도 승인을 받고 [안전 빌드 안내](../tools/dataflow/jdbc-export/script/README-progress-safe.md)를 따르십시오. 실행 시에는 승인된 **이미 존재하는** GCS source staging prefix를 `--gcs-source-staging-dir`로 지정해야 하고 build region 및 로그 위치도 검토해야 합니다. `--execute` 없이는 게시하지 않습니다. script는 immutableTags인 승인된 repository를 요구하고 기존 tag/template 재사용을 거부하며 GCS 객체를 generation-match=0으로 생성합니다. IAM/정책/버킷을 자동 생성·변경하지 않습니다. 실패 시 무조건 재제출하지 말고 기존 build 및 목적지 상태부터 확인합니다. 기본 v1 경로를 덮어쓸 수 있는 legacy build-image.sh/build-template.sh를 이 기능 배포에 사용하지 마십시오.

VPC private IP, 기존 subnetwork·worker SA, staging/temp 및 선택 runtime 설정은 기존 경로를 유지합니다. Secret Manager, GCS SQL/schema/manifest/JAR 읽기, GCS temp 쓰기, BigQuery job/dataset 권한과 Oracle DNS/방화벽/라우팅을 실제 환경에서 검증하십시오. worker의 JDBC expansion dependency 접근 및 launcher image pull도 검증 대상입니다.

## 실패·재실행·관측

- 세 목적지 쓰기는 **원자적 트랜잭션이 아닙니다.** 한 분기 실패 시 다른 staging 테이블은 이미 갱신됐을 수 있습니다. run이 성공하고 세 테이블 검증이 끝나기 전 결과를 소비하지 마십시오.
- 공통 Oracle snapshot/SCN과 exactly-once를 보장하지 않습니다. SCN/Flashback이 필요한 일관성 요구는 별도 설계·권한·UNDO 보존 검증 대상입니다.
- **0행 입력 주의:** FILE_LOADS에서 파일/load job이 생성되지 않을 수 있으므로 WRITE_TRUNCATE만으로 기존 staging 테이블이 비워진다고 보장하지 않습니다. 이전 run의 데이터가 남을 수 있습니다. 업무일/run별 별도 staging 대상과 예상 건수 검증 등으로 빈 결과와 오래된 데이터를 구분하고, 비우기 정책이 필요하면 별도 승인된 단계로 설계하십시오.
- **WRITE_APPEND 재시도 주의:** 일부 분기가 이미 적재된 뒤 전체 job을 재실행하면 성공했던 분기의 데이터가 중복 적재될 수 있습니다. 자동 재시도 횟수를 올리기 전에 run별 staging/중복 제거 또는 재처리 정책을 정해야 합니다.
- 샘플만 retries=0으로 두어 부분 쓰기 후 자동으로 전체 job을 다시 띄우지 않습니다. 기존 기본값을 전역 변경하지 않습니다. 재실행 전 이전 원격 job의 terminal 상태와 목적지 상태를 확인해야 합니다.
- Airflow timeout/cancel이 원격 Dataflow job 자동 취소를 보장하지 않습니다. 같은 staging 목적지에 두 job이 동시에 쓰지 않도록 하십시오.
- production publish/merge는 세 staging의 완료·건수·품질 검증 이후 별도 승인된 단계로 분리합니다. 이 예제에는 production publish가 없습니다.
- 기존 [진행 관측 로그 안내](dataflow-progress-logs.md)를 함께 참고하십시오. 관측된 JDBC 후단 행수는 원천 읽기나 BigQuery 확정 적재 건수가 아닙니다. 여러 분기 시리즈의 카운터를 무조건 합산하거나 전체 진행률로 해석하지 마십시오. job/분기/목적지를 구분해 확인합니다.
- 새 manifest에는 비밀번호가 없더라도 SQL·테이블명 등 업무 메타데이터는 포함됩니다. system bucket 최소 권한·retention과 외부 JDBC/Beam 라이브러리 로그의 비밀정보 노출 위험을 함께 검토합니다.

## 검증과 운영 승인 체크리스트

로컬 mock/static 테스트는 단일 launch·단일 Pipeline·세 분기·경로 및 스키마 검증·기존 단일 모드 회귀를 확인합니다. 이는 실제 Oracle/GCP 실행 증명이 아닙니다. 현재 작업에서 실제 컨테이너 빌드, Java JDBC expansion, Oracle handshake, GCP IAM/VPC, BigQuery FILE_LOADS는 미검증입니다.

운영 반영 전 test dataset에서 다음을 확인하십시오.

1. 세 작은 테이블 각 0/1/여러 행, 한글·NULL·긴 문자열, DATE 시간과 TSTZ offset/NULL, NUMBER(18,2) 최대값·음수.
2. 한 테이블의 SELECT 권한/스키마 오류에 따른 job 실패와 부분 출력. 실패 run을 downstream이 소비하지 않는지 확인.
3. 동일 business_date 재실행 전 기존 job 종료 확인, staging 건수·값 재검증.
4. 원래 단일 모드의 기존 template 실행 회귀 확인.
5. 별도 job 대비 startup/전체 소요시간, peak worker 메모리, Oracle 세션·CPU/I/O, 동시성 관측. OOM/장시간 대형 테이블은 합치지 않은 상태로 먼저 비교.

준비 비용을 줄이는 편의 기능이지 무거운 추출의 메모리·처리량 문제를 자동 해결하는 기능이 아닙니다.
