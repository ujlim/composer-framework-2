# DAG 설정 검증·오류 격리 및 선택적 파싱 운영 가이드

- 적용 대상: `dags/main.py`, `dags/framework/config_loader.py`, `dags/framework/root_factory.py`
- 검사 대상: `pipelines/roots/<root_id>/config.yaml`, `pipelines/vines/<vine_id>/config.yaml`
- 오류 코드는 프레임워크가 정의한 값이며 **Airflow 자체 오류 코드가 아닙니다**.
- 이 문서의 로그는 *필드 예시*입니다. 시간, 실제 파일 절대경로, 메시지, 다중 오류 개수는 환경마다 달라집니다.
- 실험용 YAML은 **운영 Composer 버킷에 바로 배포하지 않습니다**. 우선 로컬 테스트 또는 전용 검증 환경을 사용합니다.

## 1. 무엇을 보장하는가

1. 정상 파일은 계속 DAG를 생성하고 오류 파일은 건너뜁니다.
2. 같은 `dag_id`를 주장하는 모든 설정(Root↔Root, Vine↔Vine, Root↔Vine)을 격리합니다. **먼저 읽힌 설정을 살리지 않습니다.**
3. `folder.name == root.root_id / vine.vine_id`를 **대소문자까지 정확히** 요구합니다. 잘못된 폴더가 유효 ID를 주장하는 경우 폴더 불일치와 ID 중복을 **동시에** 보고할 수 있습니다.
4. YAML 파싱/읽기 오류, 구조 오류, 누락된 `config.yaml`, 잘못된 ID도 다른 DAG를 중단시키지 않습니다.
5. Vine **Factory가 실제로 정상 등록된 경우에만** Root의 사용 가능 Vine 집합에 포함합니다. YAML 존재만으로 Root를 허용하지 않습니다.
6. 동일한 Python 모듈에 DAG를 동적 등록할 때 `LOGGER`나 `register_dag` 같은 전역 심볼 이름을 덮지 않습니다.
7. **등록된 DAG가 0개이면** Import Error를 발생시켜 전체 실패를 숨기지 않습니다. 일부 DAG만 실패했으면 나머지는 등록하고, 오류는 DAG Processor 로그에 별도로 남깁니다.

### 격리 범위의 한계

- 검증은 **이 `main.py`가 관리하는 YAML 카탈로그 내부**에 적용됩니다. `dags/`의 별도 Python DAG 파일, 다른 패키지/배포 경로와의 DAG ID 충돌까지 전역적으로 보장하지는 않습니다.
- `main.py`의 필수 모듈 import 실패, 프레임워크 코드 전체 문법 오류, 공통 파일 시스템 장애, Airflow/provider 호환성 문제는 **설정별로 격리할 수 없는 파일/환경 수준 오류**입니다.
- Catalog 자체를 스캔할 수 없거나 정상 DAG가 하나도 없으면 전체 Import Error를 의도적으로 냅니다.
- 로그만 남긴 격리 오류는 Airflow UI의 **DAG Import Errors** 목록에 반드시 보인다고 보장할 수 없습니다. DAG Processor 로그를 확인하세요.
- 이미 직전에 직렬화된 DAG가 UI에 남아 보일 수 있습니다. **UI에 남았다는 이유만으로 최신 파싱 성공으로 판단하지 마세요.** 파서 최신 실행의 `dag_registration_completed`와 등록 ID를 확인하세요.

## 2. 오류 코드별 개발자 로그 예시와 대응

아래 로그는 구조를 보여주는 **예시**입니다. 실제 JSON은 `vine_framework.events` 로거의 단일 이벤트 메시지로 기록됩니다. `error`, `config_path`, `related_paths` 필드에 원인이 들어갑니다.

### 2.1 Catalog 단계: `dag_config_rejected`

| error_code | 개발자가 받는 로그 예시(핵심 필드) | 원인 | 개발자 대응 |
|---|---|---|---|
| `DUPLICATE_DAG_ID` | `event=dag_config_rejected error_code=DUPLICATE_DAG_ID dag_id=shared related_paths=[...]` | 여러 YAML이 동일 ID 주장. Root/Vine 교차 중복 포함 | 로그의 `related_paths`에 표시된 **모든** YAML 확인. 각 DAG의 ID와 폴더를 함께 고치거나 불필요한 정의 제거. 재검증 시 중복 0건 확인 |
| `FOLDER_ID_MISMATCH` | `event=dag_config_rejected error_code=FOLDER_ID_MISMATCH dag_id=actual` | 예: `vines/wrong/config.yaml` 안의 `vine_id: actual` | 폴더를 `actual`로 바꾸거나 YAML ID를 `wrong`으로 바꿈. Root의 참조도 함께 수정 |
| `MISSING_CONFIG_FILE` | `event=dag_config_rejected error_code=MISSING_CONFIG_FILE config_path=.../config.yaml` | 파이프라인 하위 폴더에 `config.yaml` 누락 | 해당 폴더의 파일명/배포 누락을 복구하거나 사용하지 않는 폴더 삭제 |
| `YAML_PARSE_ERROR` | `event=dag_config_rejected error_code=YAML_PARSE_ERROR error="while parsing ..."` | 들여쓰기·괄호·따옴표 등 YAML 구문 오류 | 오류 메시지의 파일·줄 확인, YAML 문법 검사, 재배포 |
| `YAML_READ_ERROR` | `event=dag_config_rejected error_code=YAML_READ_ERROR error="..."` | 파일 읽기/인코딩 실패 | UTF-8 확인, GCS 파일 동기화 및 읽기 권한/파일 상태 점검 |
| `INVALID_CONFIG_STRUCTURE` | `event=dag_config_rejected error_code=INVALID_CONFIG_STRUCTURE error="..."` | 최상위 `root` / `vine` 섹션이 없거나 mapping이 아니거나 양쪽이 동시에 존재 | 파일의 배치 폴더에 맞는 최상위 섹션 하나만 정의하고 dictionary 형태 유지 |
| `INVALID_DAG_ID` | `event=dag_config_rejected error_code=INVALID_DAG_ID error="... unsupported characters"` | ID 공란/누락, 공백, 허용하지 않는 특수문자 등 | `[A-Za-z0-9_.-]` 문자만 사용하도록 ID를 수정하고 같은 이름의 폴더로 일치 |

**실제 JSON 로그 형식 예시:**

```json
{"event":"dag_config_rejected","timestamp":"<UTC timestamp>","error_code":"FOLDER_ID_MISMATCH","dag_kind":"vine","config_path":"<dags>/pipelines/vines/old_name/config.yaml","dag_id":"new_name","error":"Folder name 'old_name' must exactly match vine.vine_id 'new_name'.","related_paths":[]}
```

ID가 두 개의 파일에서 충돌하면 `DUPLICATE_DAG_ID` 이벤트가 **각 파일에 하나씩** 발생합니다. 어느 한쪽을 임의로 승자로 정하지 않습니다. 불일치와 중복이 겹치면 해당 파일에서 **두 종류의 이벤트**가 발생할 수 있습니다.

### 2.2 DAG 생성·등록 단계: `dag_registration_failed`

| error_code | 로그 예시(핵심 필드) | 원인 | 개발자 대응 |
|---|---|---|---|
| `DAG_FACTORY_ERROR` | `event=dag_registration_failed error_code=DAG_FACTORY_ERROR dag_id=job_vine error="..."` | Factory 검증 실패: 예를 들어 Grape type 미지원, 의존성 순환/중복, SQL 파일 경로 오류, Task ID 충돌 | `error`와 해당 YAML의 grapes/vines/SQL 파일을 확인하고 수정. Python traceback은 DAG Processor 로그 확인 |
| `ROOT_VINE_NOT_REGISTERED` | `event=dag_registration_failed error_code=ROOT_VINE_NOT_REGISTERED dag_id=job_root error="Root 'job_root' references Vine 'job_vine'..."` | Root가 없거나 **YAML이 있더라도 등록에 실패한** Vine을 참조 | 먼저 해당 Vine의 거부/Factory 오류를 해결. Root의 `vines[].vine_id` 오타 및 배포 누락 확인 |
| `INVALID_DAG_OBJECT` | `event=dag_registration_failed error_code=INVALID_DAG_OBJECT error="... expected airflow.models.DAG ..."` | Factory가 DAG가 아닌 객체나 빈 ID 객체를 반환 | 프레임워크 코드의 해당 Factory 수정·단위 테스트 |
| `DUPLICATE_DAG_ID` | `event=dag_registration_failed error_code=DUPLICATE_DAG_ID error="Duplicate DAG ID during registration..."` | Catalog 이후 Factory 동작상 ID 충돌이 생긴 경우를 위한 2차 방어 | Factory가 YAML의 검증된 ID를 그대로 사용하는지 확인 |

**실제 JSON 로그 형식 예시:**

```json
{"event":"dag_registration_failed","timestamp":"<UTC timestamp>","error_code":"ROOT_VINE_NOT_REGISTERED","dag_kind":"root","config_path":"<dags>/pipelines/roots/job_root/config.yaml","dag_id":"job_root","error":"Root 'job_root' references Vine 'job_vine' which is not registered; check rejected YAML / Vine factory diagnostics.","related_paths":[]}
```

### 2.3 성공·전체 실패 확인

| 상황 | 확인 로그 / 상태 | 개발자 대응 |
|---|---|---|
| 정상 DAG 등록 | `event=dag_registered dag_id=good_vine` | DAG 목록, Graph, 필요한 경우 테스트 실행 확인 |
| 전체 파싱 완료 | `event=dag_registration_completed registered_dag_count=N registration_error_count=M` | N이 예상 정상 DAG 수와 같은지, M이 예상 오류 수와 같은지 확인. 오류 1개 설정이 여러 이벤트를 만들 수 있으므로 M은 오류 YAML 수와 다를 수 있음 |
| 유효한 DAG 0개 + 오류 있음 | Airflow `RuntimeError: No DAGs registered; ... code=...` | Import Errors에서 요약을 읽고 상세 로그로 원인별 수정 |
| YAML 탐색 결과 0개 | Airflow `RuntimeError: No pipeline configurations were discovered ...` | 배포 경로, `pipelines/` 하위 구조, `config.yaml` 존재 확인 |
| `pipelines/` 자체 없음 | Airflow `FileNotFoundError: Pipelines directory does not exist ...` | `main.py`와 동일 DAG 배포 트리로 pipelines 업로드 |
| 공통 import 실패/코드 문법 오류 | Airflow Python Import Error (이 프레임워크의 JSON 코드 없음) | 문제 모듈, Airflow/provider/Python 버전, DAG 배포 내용 복구 |

### 로그 검색 팁

Cloud Logging에서는 Composer 환경의 DAG Processor 로그를 선택한 후 다음 문자열로 좁히세요. 프로젝트별 로그 이름과 resource 필드는 다를 수 있으므로 고정하지 않습니다.

```text
"dag_config_rejected"
"dag_registration_failed"
"DUPLICATE_DAG_ID"
"FOLDER_ID_MISMATCH"
"ROOT_VINE_NOT_REGISTERED"
"dag_registration_completed"
```

Airflow Import Errors 화면에 메시지가 없더라도 DAG Processor 로그를 확인합니다.

## 3. 테스트 방법 / 정상 판정

```bash
python -m unittest discover -s tests -p test_dag_validation.py -v
```

`tests/test_dag_validation.py`는 임시 디렉터리에 실제 YAML 파일을 만들어 Catalog를 시험하고, 가벼운 가짜 Airflow DAG/Factory를 사용해 **실제 `main.py` 등록 코드**를 별도로 실행합니다. 테스트는 Composer에 업로드하거나 BigQuery/DB에 접속하지 않습니다.

| 테스트 조합 | 합격 조건 |
|---|---|
| 정상 Vine+Root | 둘 다 등록, 오류 0 |
| Vine↔Vine, Root↔Root, Root↔Vine ID 중복 | 충돌 파일 **모두** 격리, 정상 DAG 보존 |
| 폴더명 불일치 및 ID 중복 동시 발생 | 두 오류 모두 기록, 충돌 ID 전체 격리 |
| 대소문자 불일치 | `FOLDER_ID_MISMATCH` 기록 |
| 잘못된 YAML / 누락된 config / 잘못된 섹션 / 부적합 ID | 문제 파일만 격리, 정상 DAG 등록 유지 |
| Vine Factory 오류 + 이 Vine을 참조하는 Root | Vine 오류 및 `ROOT_VINE_NOT_REGISTERED` 기록, 독립 Root 정상 |
| `LOGGER` 등 모듈 심볼과 동일한 DAG ID | Python 전역 오염 없이 DAG 등록 |
| 정상 DAG 0개 / 설정 0개 | 명시적 Import Error |

추가로 Composer **비운영 환경**에서 Parser 로그, Import Errors, 실제 등록 수, Root 트리거 대상을 점검합니다. 오프라인 단위 테스트만으로 provider/API 호환성은 보장하지 않습니다.

## 4. 기존 결함과 선택적 파싱의 한계를 명확히 구분

**격리 문제가 먼저 존재했습니다.** 기존 `config_loader.py`는 Root/Vine 그룹 내 중복을 발견하면 Catalog 전체 예외로 중단하고, 폴더명도 검사하지 않았습니다. 반면 개별 Factory 오류만 `main.py`의 try/except로 격리했습니다. 이번 수정은 이 **기존 검증·격리 결함을 보완**하는 것입니다.

**선택적 파싱은 별개의 성능 최적화**입니다. Apache Airflow 공식 설명에 따르면:

- **DAG File Processor(스케줄링/메타데이터 갱신)**: DAG 파일의 전체 DAG 목록을 탐색해야 하므로 보통 전체 DAG를 생성·검증합니다.
- **Task 실행 직전 파싱(Worker 측)**: 특정 DAG만 필요할 수 있으므로 `get_parsing_context().dag_id`를 기준으로 다른 DAG 객체 생성을 생략할 수 있습니다.
- 선택적 파싱은 DAG File Processor의 **모든 YAML 검증/전체 카탈로그 구성 비용을 없애는 기능이 아닙니다**. 이를 전부 생략하면 중복 ID, Root 참조 오류가 다른 DAG와 연계된 경우 발견이 늦어질 수 있습니다.
- 성능 최적화를 적용할 때는 **전체 Discovery/검증**과 **특정 DAG 객체 Factory 생성**을 분리해야 합니다. 선택된 DAG가 격리된 상태라면 정상 DAG를 대신 임의로 선택하지 말고 그 오류를 보존해야 합니다.

참고: [Apache Airflow Dynamic Dag Generation – Optimizing Dag parsing delays during execution](https://airflow.apache.org/docs/apache-airflow/3.2.1/howto/dynamic-dag-generation.html).

**이 브랜치의 이번 변경은 오류 격리·로그·테스트 보완입니다.** 현재 확인한 `dags/main.py`에는 `get_parsing_context()` 기반 선택적 Factory 생성이 구현되어 있지 않습니다. 따라서 **이번 커밋만으로 선택적 파싱 성능 개선까지 적용됐다고 판단하면 안 됩니다.** 선택적 파싱을 병합할 때 위 글로벌 검증을 먼저 수행한 뒤 Worker 문맥에서만 불필요한 Factory 생성을 건너뛰도록 통합 테스트가 필요합니다.

## 5. 운영 배포 권장 순서

1. 새 브랜치에서 오프라인 테스트 실행 및 변경 파일 리뷰.
2. 비운영 Composer에서 오류 없는 YAML 전체 등록 수 확인.
3. 의도적으로 중복/폴더 불일치/Factory 오류를 주입하여 정상 DAG 유지와 개별 이벤트 로그 확인.
4. 오류 주입 파일 제거 후 로그의 오류 건수 0, 정상 DAG 수 정상 복귀 확인.
5. 파싱 성능(전체 DAG Processor vs Worker 특정 DAG)과 메모리, 파싱 시간, Root/Vine Graph를 별도 측정한 뒤 운영 반영.

> **핵심:** 성능 개선은 선택적 파싱의 목적이고, 정상 DAG 보호는 오류 격리의 목적입니다. 어느 쪽도 다른 쪽을 자동으로 대신하지 않습니다.
