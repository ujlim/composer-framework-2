# 1. YAML 작성과 공통 실행 규칙

[목차](README.md) · [다음: SQL](02-sql.md)

## 계층과 파일 위치

- **Root**: 스케줄을 소유하는 DAG. Vine DAG를 트리거하고 선택적으로 완료를 기다립니다.
- **Vine**: 스케줄 없는 DAG. 실제 Grape 작업과 Grape 간 의존성을 정의합니다.
- **Grape**: Vine 안의 Airflow task 하나. `grape_id`가 task ID가 됩니다.

[main.py](../../dags/main.py)는 `dags/pipelines/roots/*/config.yaml`, `dags/pipelines/vines/*/config.yaml`만 한 단계 깊이로 탐색합니다. `.yml`, 임의 파일명, 더 깊은 폴더는 탐색하지 않습니다. SQL·스키마 상대 경로는 **해당 Vine의 config.yaml 폴더**를 기준으로 하며 폴더 밖으로 나가는 경로는 차단합니다. 폴더명과 DAG ID가 같을 필요는 없지만 일치시키면 관리하기 쉽습니다.

YAML은 `yaml.safe_load`로 읽습니다. 날짜는 `"2026-10-01"`, Jinja는 `"{{ ... }}"`처럼 인용하고, boolean은 **따옴표 없는** `true`/`false`를 사용하세요. `"false"` 문자열은 Python `bool()` 처리 시 참이 될 수 있습니다. 정수 옵션은 코드에서 `int()`로 변환되지만 정상 정수를 쓰는 것이 안전합니다. 필드마다 `get`/`or` 처리 방식이 달라 `null`은 생략과 같지 않습니다. 선택 필드는 필요 없으면 생략하세요. 알 수 없는 키를 모두 거부하는 스키마 검증은 없으므로 오타가 무시될 수 있습니다.

## 최상위 구성

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| 공통 | `version` | X | — | 정수 관례값. 현재 로더·Factory는 읽거나 버전을 검증하지 않음 | `1` |
| Root | `root` | O | — | mapping. Root DAG 속성 | 아래 표 |
| Root | `vines` | O | — | 비어 있지 않은 list[mapping]. 호출 Vine 목록 | `[{vine_id: sample_vine}]` |
| Root | `defaults` | X | `{}` | mapping. Vine 트리거의 기본 옵션. Grape 기본값이 아님 | `{wait_for_completion: true}` |
| Root | `parameters` | X | `{}` | mapping. Vine dag_run.conf로 전달하는 업무값 | `{region: KR}` |
| 공통 | `business_date` | X | `{}` | mapping. Root/Vine별 업무일자 정책 | `{type: daily}` |
| Vine | `vine` | O | — | mapping. Vine DAG 속성 | 아래 표 |
| Vine | `runtime` | X | `{}` | mapping. 실행기별 GCP/DB/Dataflow 설정. Root에서 자동 상속되지 않음 | `{gcp_conn_id: google_cloud_default}` |
| Vine | `grape_defaults` | X | `{}` | mapping. 각 Grape `options`의 공통 기본값 | `{retries: 1}` |
| Vine | `grapes` | O | — | 비어 있지 않은 list[mapping] | 아래 예제 |

## Root DAG 속성

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| Root | `root.root_id` | O | — | string. 전역 DAG ID, 영문·숫자·`.`·`_`·`-`만 허용 | `orders_daily_root` |
| Root | `root.description` | X | `null` | string. DAG 설명 | `주문 일배치` |
| Root | `root.owner` | X | `batch-integration` | string. 기본 task owner | `data-team` |
| Root | `root.schedule` | X | `null` | cron string 등 Airflow schedule 값. `null`, 빈 문자열, `false`는 스케줄 없음 | `"0 2 * * *"` |
| Root | `root.timezone` | X | `Asia/Seoul` | string. 시작일 파싱 기준 시간대 | `Asia/Seoul` |
| Root | `root.start_date` | X | `2026-01-01` | 날짜 string. 스케줄 시작 기준; 최초 실행 시각 자체와 동일하지 않음 | `"2026-10-01"` |
| Root | `root.catchup` | X | `false` | boolean. 과거 스케줄 구간 보충 여부 | `false` |
| Root | `root.max_active_runs` | X | `1` | integer. 동시 Root run 제한 | `1` |
| Root | `root.dagrun_timeout_seconds` | X | `86400` | integer, 초. DAG run 제한 | `86400` |
| Root | `root.tags` | X | `[]` | list[string]. `root` 태그 자동 추가·중복 제거 | `[orders, daily]` |

## Root에서 Vine 호출

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| Vine 참조 | `vines[].vine_id` | O | — | string. catalog에 존재하는 Vine ID, Root 내 중복 불가 | `orders_extract` |
| Vine 참조 | `vines[].trigger_id` | X | `trigger_<vine_id>` | string. Root 내 실제 task ID. 자동 task와 다른 고유 ID 필요 | `trigger_orders` |
| 의존성 | `vines[].depends_on` | X | `[]` | list[string]. **trigger_id가 아닌 vine_id**로 선행 Vine 지정 | `[orders_extract]` |
| 호출 옵션 | `defaults.wait_for_completion` / `vines[].options.wait_for_completion` | X | `true` | boolean. Vine 완료까지 기다림. false이면 후행 작업은 업무 완료가 아니라 트리거 완료 후 진행 | `true` |
| 호출 옵션 | `defaults.poke_interval_seconds` / `vines[].options.poke_interval_seconds` | X | `30` | integer, 초. 완료 상태 조회 간격 | `30` |
| 호출 옵션 | `defaults.reset_dag_run` / `vines[].options.reset_dag_run` | X | `false` | boolean. 동일 run 재트리거 시 기존 run 초기화 여부. 재실행 영향 확인 필요 | `false` |
| 호출 옵션 | `defaults.deferrable` / `vines[].options.deferrable` | X | `true` | boolean. provider/Triggerer 지원 필요 | `true` |
| 호출 옵션 | `defaults.execution_timeout_seconds` / `vines[].options.execution_timeout_seconds` | X | `21600` | integer, 초. 트리거 task 제한. Root 전체 timeout과 별개 | `21600` |
| 업무값 | `parameters.<이름>` | X | — | 임의 YAML 값(목록 포함). mapping이면 `.value`만 전달; 임의 mapping 전체 전달이 아님 | `region: KR` |
| 업무값 | `parameters.<이름>.value` | X | `null` | mapping 형식일 때 전달 값. 생략하면 null 전달. Jinja 사용 가능 | `"{{ dag_run.conf.get('recovery', false) }}"` |

트리거 옵션은 `{**defaults, **vines[].options}`의 **얕은 덮어쓰기**입니다. Root에서 지원하는 옵션은 위 5개뿐이며 `retries`, `pool` 등을 넣어도 전달되지 않습니다. 모든 Vine에 같은 conf가 전달되며, Vine별 별도 `parameters`/`conf`는 구현되지 않았습니다. run ID는 `root__<root_id>__<Root dag_run.run_id>__<vine_id>`입니다.

## Vine DAG와 Grape 공통 속성

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| Vine | `vine.vine_id` | O | — | string. 전체 Root/Vine DAG ID와 중복 불가 | `orders_extract` |
| Vine | `vine.description` | X | `null` | string. DAG 설명 | `주문 추출` |
| Vine | `vine.owner` | X | `data-engineering` | string. task owner | `data-team` |
| Vine | `vine.schedule` | X | `null` | `null`, `""`, `false`만 허용. 실제 DAG는 항상 schedule=None | `null` |
| Vine | `vine.timezone` | X | `Asia/Seoul` | string. 시작일 파싱 시간대 | `Asia/Seoul` |
| Vine | `vine.start_date` | X | `2026-01-01` | 날짜 string | `"2026-10-01"` |
| Vine | `vine.max_active_runs` | X | `1` | integer. 동시 Vine run 제한 | `1` |
| Vine | `vine.dagrun_timeout_seconds` | X | `21600` | integer, 초 | `21600` |
| Vine | `vine.tags` | X | `[]` | list[string]. `vine` 태그 자동 추가 | `[orders]` |
| Grape | `grapes[].grape_id` | O | — | string. Vine 내 고유 task ID. ID 허용문자 규칙 적용 | `query_orders` |
| Grape | `grapes[].type` | O | — | string. [12개 등록 type](README.md#전체-기능-지도) 중 하나 | `bigquery_sql` |
| Grape | `grapes[].depends_on` | X | `[]` | list[string]. 같은 Vine 내 선행 grape_id | `[query_orders]` |
| Grape | `grapes[].options` | X | `{}` | mapping. grape_defaults를 재귀 병합한 후 덮어씀 | `{retries: 0}` |

Vine catchup은 **항상 false**이며 `vine.catchup`을 읽지 않습니다. Root와 Vine의 `start_date`, timeout, owner는 각각 독립적입니다. Root는 catalog에 Vine 설정이 있는지 확인하지만 해당 Vine DAG가 정상 등록됐는지까지 보장하지 않으므로 등록 오류 로그를 함께 확인해야 합니다.

### 공통 Grape 옵션의 정확한 위치

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| 재시도 | `grape_defaults.retries` / `grapes[].options.retries` | X | 일반 `1`; FIN 센서 `0` | integer. Airflow task 재시도 횟수 | `0` |
| 재시도 | `grape_defaults.retry_delay_seconds` / `grapes[].options.retry_delay_seconds` | X | 일반 `300`; FIN 센서 `60` | integer, 초. **SFTP도 기본 300**; 60은 별도 지정할 권장 예시일 뿐 | `300` |
| 제한 | `grape_defaults.execution_timeout_seconds` / `grapes[].options.execution_timeout_seconds` | X | 일반 `7200`; Dataflow `21600`; FIN 별도 설명 | integer, 초. FIN은 timeout_seconds 우선 | `7200` |
| Pool | `grape_defaults.pool` / `grapes[].options.pool` | X | `null` 전달 | string. Airflow Pool. 실제 pool이 존재해야 함 | `default_pool` |
| 우선순위 | `grape_defaults.priority_weight` / `grapes[].options.priority_weight` | X | `1` | integer. Airflow task priority | `1` |

**`pool`, `priority_weight`, `retries`를 Grape 바로 아래에 두면 읽지 않습니다.** 참조 화면의 위치를 그대로 옮기지 말고 `options` 또는 `grape_defaults` 아래에 작성하세요. 각 실행기는 자신이 지원하는 option만 읽습니다. 공통 `grape_defaults`에 센서 전용·SFTP 전용 옵션을 무분별하게 넣지 마세요.

### 의존성, 병렬 실행과 자동 task

선행 ID가 없는 작업들은 업무일자 계산 후 병렬로 실행될 수 있습니다. `depends_on` 배열은 모든 선행 작업에 의존하며 Airflow 기본 trigger rule을 사용합니다. 존재하지 않는 참조, 자기 참조, 중복 ID, 순환은 오류입니다. 목록 순서만으로 순차 실행되지 않습니다.

자동 task ID `root_start`, `root_end`, `vine_start`, `vine_end`, `resolve_business_date`는 사용자 ID로 피하세요. Root는 `root_start → resolve_business_date → 선행 없는 trigger → ... → root_end`, Vine은 `vine_start → resolve_business_date → 선행 없는 grape → ... → vine_end` 형태입니다. 마지막 end는 모든 말단 task에 연결됩니다.

## 업무일자 정책

| 구분 | YAML 파라미터 | 필수 | 기본값 / 권장값 | 설명 | 예시 |
|---|---|---|---|---|---|
| 공통 | `business_date.type` | X | `daily` | string. daily / monthly / yearly, 소문자 정규화 | `monthly` |
| Root | `business_date.source` | X | `data_interval_end` | string. scheduled 자동 계산에서 이 값만 지원 | `data_interval_end` |
| Root | `business_date.timezone` | X | `root.timezone` | string. data_interval_end를 변환할 시간대 | `Asia/Seoul` |
| Root | `business_date.offset_days` | X | `-1` | integer. 변환된 구간 종료일에 더할 일수 | `-1` |
| 공통 | `business_date.date_offsets` | X | `{}` | mapping. 업무 기준일에서 계산할 이름별 파생일 | 아래 예제 |
| 파생일 | `business_date.date_offsets.<이름>.years` | X | `0` | integer. 연도 이동 | `-1` |
| 파생일 | `business_date.date_offsets.<이름>.months` | X | `0` | integer. 월 이동 | `-1` |
| 파생일 | `business_date.date_offsets.<이름>.days` | X | `0` | integer. 일 이동 | `-7` |
| 파생일 | `business_date.date_offsets.<이름>.adjust` | X | `none` | string. 이동 후 none / month_start / month_end / year_start / year_end | `month_end` |

Vine은 `type`, `date_offsets`만 읽습니다. `source`, `timezone`, `offset_days`를 Vine에 넣어도 업무일자를 자동 산출하지 않습니다.

**Root 결정 순서:** `dag_run.conf.business_date`가 있으면 최우선 → scheduled run이면 구간 종료 시각을 정책 시간대로 바꾼 뒤 offset_days 적용 → 그 밖의 run은 `params.business_date` → 없으면 실패. UI 수동 실행에는 날짜를 입력하세요. scheduled가 아닌 backfill 등에도 자동 추정을 기대하지 마세요.

**Vine 결정 순서:** `dag_run.conf.business_date`와 conf의 period_type(없으면 Vine type) → 직접 수동 실행의 `params.business_date`와 Vine type → 없으면 실패. Root가 넘긴 `period_start`/`period_end`와 파생일을 그대로 XCom에 복사하지 않고, **Vine의 date_offsets로 다시 계산**합니다. Root에서만 정의한 파생일은 `dag_run.conf`에는 있어도 Vine의 resolve_business_date XCom에는 없습니다.

| 결과 키 | daily | monthly | yearly |
|---|---|---|---|
| `business_date` | 입력 기준일 그대로 | 입력 기준일 그대로 | 입력 기준일 그대로 |
| `period_type` | daily | monthly | yearly |
| `period_start` | 기준일 | 해당 월 1일 | 해당 연도 1월 1일 |
| `period_end` | 기준일 | 해당 월 말일 | 해당 연도 12월 31일 |

monthly/yearly는 기준일 자체를 월초·연초로 바꾸지 않습니다. 예를 들어 기준일 `2026-10-15`, monthly이면 기간은 `2026-10-01`~`2026-10-31`입니다. 파생일은 period_start가 아닌 business_date 기준이며 연·월·일을 이동한 뒤 adjust를 적용합니다.

```yaml
business_date:
  type: monthly
  date_offsets:
    previous_month_end:
      months: -1
      adjust: month_end
    previous_week:
      days: -7
```

파생일 이름은 비어 있지 않은 문자열이어야 하며 `business_date`, `period_type`, `period_start`, `period_end`는 예약 키입니다. Root parameter 이름과 충돌하면 오류입니다. 기본 4개 context 키는 parameters보다 프레임워크 값이 우선합니다. 이름은 단순 영문·숫자·밑줄로 유지하세요.

### Jinja 사용

업무일자 XCom은 dict입니다. `ti.xcom_pull(task_ids='resolve_business_date')['business_date']`처럼 접근합니다. SQL·객체명 등 각 실행기의 template field에서만 렌더링되며 YAML 전체를 일괄 렌더링하지 않습니다. ID, 연결 ID, 로컬 파일 경로, 숫자 옵션에 임의 Jinja를 넣지 마세요. 실행기별 필드 설명을 확인하세요.

```yaml
source:
  bucket: example-input-bucket
  prefix: "orders/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/"
```

Root의 일반 parameters는 Vine에서 `dag_run.conf.get('이름')`로 읽습니다. 일반 parameters가 자동으로 SQL `parameters`에 합쳐지거나 XCom context에 들어가지는 않습니다. Jinja로 SQL 문자열을 조립하는 것은 DB bind parameter와 다르므로 검증되지 않은 외부 입력을 SQL에 직접 삽입하지 마세요.

## 최소 Root/Vine 예제

다음 두 파일과 SQL을 준비합니다. 예제는 구문·구조 설명용이며 GCP Connection과 BigQuery 실행 권한이 필요합니다.

`dags/pipelines/roots/example_daily/config.yaml`:

```yaml
version: 1
root:
  root_id: example_daily_root
  schedule: "0 2 * * *"
  timezone: Asia/Seoul
  start_date: "2026-10-01"
  catchup: false
business_date:
  type: daily
  source: data_interval_end
  offset_days: -1
defaults:
  wait_for_completion: true
  deferrable: true
vines:
  - vine_id: example_daily_vine
```

`dags/pipelines/vines/example_daily/config.yaml`:

```yaml
version: 1
vine:
  vine_id: example_daily_vine
  timezone: Asia/Seoul
runtime:
  project_id: example-project
  location: asia-northeast3
  gcp_conn_id: google_cloud_default
grape_defaults:
  retries: 0
  execution_timeout_seconds: 600
grapes:
  - grape_id: show_business_date
    type: bigquery_sql
    sql_file: sql/show_date.sql
```

같은 Vine 폴더의 `sql/show_date.sql`:

```sql
SELECT DATE('{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}') AS business_date;
```

기존 전체 예제는 [Root 모음](../../dags/pipelines/roots), [Vine 모음](../../dags/pipelines/vines), [업무일자 샘플 설명](../../dags/pipelines/README-business-date-samples.md)을 참고하세요.

구현 근거: [config_loader.py](../../dags/framework/config_loader.py), [root_factory.py](../../dags/framework/root_factory.py), [vine_factory.py](../../dags/framework/vine_factory.py), [business_date.py](../../dags/framework/business_date.py), [utils.py](../../dags/framework/utils.py).
