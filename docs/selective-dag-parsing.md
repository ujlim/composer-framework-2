# 선택적 DAG 생성·파싱 가이드

- 작성자: 임의진 (LIM UI JIN)
- 작성/수정: 2026-11-02
- 기준: `main` d1b14ef59bfd70fbba388a6a70c51c107cdec9e1
- 작업 브랜치: `feature/26.1102.selective-dag-parsing`

## 적용 범위

스케줄러/DAG File Processor의 전체 DAG 발견은 그대로 유지합니다. Airflow가 실행 대상으로 DAG ID를 전달할 때에만 해당 Root 또는 Vine의 DAG와 **전체 task graph**를 생성합니다. `task_id`로 task를 잘라내지 않으므로 파일 센서, dependency, callback, 템플릿, 업무일자 계산은 기존 factory 경로를 그대로 사용합니다.

모든 `pipelines/roots/*/config.yaml`, `pipelines/vines/*/config.yaml`은 매번 읽고 파싱합니다. 따라서 **변경된 YAML만 파싱하는 기능이 아니며**, YAML 읽기 비용 자체는 줄지 않습니다. 비대상 factory 생성과 그 안의 SQL/schema 파일 읽기·operator 구성을 생략하는 최적화입니다. 런타임 외부 API 호출이나 실제 task 수행을 생략하지 않습니다. 모듈 import 자체는 계속 수행됩니다. 영구 캐시/mtime 캐시/별도 manifest는 도입하지 않았습니다.

## 동작

| 상황 | 생성 범위 |
|---|---|
| parsing context `dag_id=None` | 모든 Vine → 모든 Root, 기존 순서 |
| 특정 Vine ID | 해당 Vine 전체 graph만 |
| 특정 Root ID | 해당 Root 전체 graph만. 전체 catalog의 Vine ID로 참조 검증 |
| 존재하지 않는 대상 ID | DAG 0개 + `dag_target_not_found` warning; 무관한 DAG는 생성하지 않음 |
| context API 미지원·오류·비문자열 또는 빈 ID | warning 후 전체 생성으로 fallback |
| 구성 자체 0개 | 기존처럼 Import Error |
| 선택한 factory 전부 실패 | Import Error |

ID는 YAML의 `vine.vine_id` / `root.root_id`입니다. 폴더 이름과 달라도 동작합니다. Root 의존성 확인에 필요한 전체 Vine ID 집합을 유지하고, 선택되지 않은 Vine 객체는 만들지 않습니다. 기존에도 Root 참조 검증은 catalog 기준이지 Vine factory 성공 보장이 아니었습니다. 비대상 DAG의 factory 오류는 worker targeted parse에서 확인하지 않으며 전체 scheduler parse에서 확인합니다.

동일 종류 내 중복 ID 검사에 더해 **Root와 Vine 사이의 중복 DAG ID도 catalog 단계에서 실패**시킵니다. 이전에는 등록 단계에서 일부가 먼저 등록될 수 있었지만 선택적 실행에서 충돌이 숨겨지지 않도록 fail-fast 처리했습니다. 배포 전 Root/Vine 전체 ID 중복을 해소해야 합니다. YAML 읽기/문법 오류는 기존처럼 로그 후 해당 구성을 건너뛰고, 나머지를 로드합니다. 기존 catalog의 ID 필드 오류 처리 방식은 바꾸지 않았습니다. ID 앞뒤 공백은 기존 factory와 동일하게 제거하여 선택/중복 검사에 사용합니다. `root`와 `vine` 섹션을 동시에 갖는 모호한 YAML은 로드 오류로 격리합니다.

## 호환성 및 로그

1. Airflow 3: `airflow.sdk.get_parsing_context`
2. SDK import가 불가능하면 Airflow 2.4+: `airflow.utils.dag_parsing_context.get_parsing_context`
3. 조회 실패 시 전체 생성. private 환경변수는 읽지 않습니다.

`dag_registration_completed` 로그에 `parsing_mode`, `target_dag_id`, `selected_pipeline_count`, `skipped_pipeline_count`가 추가됩니다. Airflow 버전마다 worker의 Python 재파싱 여부/시점이 다르므로 운영 성능은 실제 Composer 이미지와 실행 경로에서 확인해야 합니다. 이 변경만으로 Airflow 2/3 전체 provider 호환성이 새로 보장되는 것은 아닙니다.

공식 근거: [Airflow 2.4 동적 DAG 문서](https://airflow.apache.org/docs/apache-airflow/2.4.0/howto/dynamic-dag-generation.html), [Airflow 3.1.3 동적 DAG 문서](https://airflow.apache.org/docs/apache-airflow/3.1.3/howto/dynamic-dag-generation.html). 두 문서의 공개 API/전체 발견과 단일 대상 생성 구분을 적용했습니다.

## 배포 전 검증

```sh
python -m unittest discover -s tests
python -m compileall -q dags tests
```

오프라인 전체 246개 테스트와 compileall이 통과했습니다. 이 중 선택적 파싱 26개, 이름 검사 CLI 14개 회귀 테스트를 추가했습니다. 오프라인 검증은 Airflow API와 클라우드 연산을 mock 처리합니다. 3개 YAML fixture에서 전체 parse는 YAML 3회/팩토리 3회, Vine 대상 parse는 YAML 3회/팩토리 1회입니다. 이는 호출 수 측정이며 운영 시간·메모리 절감률 추정이 아닙니다. 실제 Composer smoke test와 import 오류 확인은 별도 필요합니다.

## 배포 (승인 후 운영자가 수행)

이번 작업은 feature 브랜치 코드 게시까지만이며 main merge/GCP 배포를 수행하지 않습니다.

1. 기존 배포본을 백업하고 전체 Root/Vine ID 중복 및 현재 import 오류를 확인합니다.
2. staging Composer의 동일 버전/provider 환경에 아래 네 파일을 반영합니다. YAML 변경은 없습니다.
   - `dags/framework/parsing_context.py` (신규)
   - `dags/framework/config_loader.py`
   - `dags/framework/models.py`
   - `dags/main.py`
3. 새 helper → models → loader → main 순으로 배포하거나 원자적 버전 배포를 사용합니다. 파일 업로드 중간 상태에서 import가 실패할 수 있으므로 업로드 완료 후 다시 확인합니다.
4. scheduler full parse의 DAG ID 목록과 task/dependency 목록을 기존 배포와 비교합니다. 누락과 신규 import error가 없어야 합니다.
5. 대표 Vine 실행, Root 실행/하위 Vine 트리거, 파일 FIN 센서 대기를 staging 데이터로 확인합니다. task graph는 전체와 동일해야 합니다.
6. worker가 해당 파일을 targeted parse하는 환경에서는 `targeted`, 선택 1개, 나머지 skip 로그를 확인합니다. full만 나타나면 해당 실행 경로에서 최적화 효과가 없을 수 있으며 강제로 private 환경변수를 주입하지 않습니다.
7. 위 검증과 승인 후 운영에 동일 절차로 반영합니다. 대규모 무분별 DAG trigger는 하지 않습니다.

## 롤백

백업한 `main.py`, `framework/config_loader.py`, `framework/models.py`를 함께 복구하고 신규 `framework/parsing_context.py`를 제거합니다(기존 main 복구 완료 후 제거). 신규 Python 파일만 제거하면 import 오류가 발생합니다. 다른 Dataflow/multi-table/worker logging 변경을 되돌리지 마세요. 원래 세 파일만 기준 main 커밋에서 복원하면 됩니다. scheduler 전체 DAG 목록/import 오류와 대표 실행을 재확인합니다. DB migration, IAM 변경, task 데이터 삭제는 없습니다.

## 전체 이름 유일성 검사·이름으로 폴더 찾기

신규 `tools/validate_pipeline_names.py`는 Airflow 없이 PyYAML만으로 전체 catalog를 새로 스캔하는 읽기 전용 관리 도구입니다. Root/Vine 전체 DAG ID, 전체 폴더 이름, 그리고 `DAG ID 또는 폴더 이름`이라는 합쳐진 이름 공간의 충돌까지 검사합니다. 이름은 대소문자를 구분합니다. ID 앞뒤 공백은 제거하되 lookup 입력은 정확히 일치해야 합니다.

```sh
# 전체 검사 (현재 저장소는 아래 기존 중복 때문에 exit 1이 정상)
python tools/validate_pipeline_names.py
# 기계 판독 보고서: entries에는 실제 config_path/folder_path 포함
python tools/validate_pipeline_names.py --json
# 전체 검사를 통과한 catalog에서만 이름 → 실제 config.yaml 경로 반환
python tools/validate_pipeline_names.py --name root_fin_sensor_daily
# 다른 배포 디렉터리 검사
python tools/validate_pipeline_names.py --pipelines-dir /path/to/pipelines --name my_dag
# 폴더명=ID 규칙까지 강제하려면 사용
python tools/validate_pipeline_names.py --require-id-folder-match
```

종료코드: 0=전체 검사/조회 성공, 1=검증 실패 또는 이름 없음, 2=인자 오류. ID≠폴더명은 기본 warning이며 strict 옵션에서는 error입니다. 오류가 하나라도 있으면 다른 이름이 유일하더라도 **모든 lookup을 차단**합니다. malformed YAML, 중복 YAML key, ID 형식/250자 제한, 두 종류 섹션 혼재, 디렉터리 밖을 가리키는 config 경로도 검사합니다. 전체 task schema/dependency/operator validation을 대체하지는 않습니다. 기존 runtime loader보다 엄격한 사전 검사입니다.

### 실제 저장소 검사 결과 (2026-11-02)

35개 설정에서 중복 DAG ID는 없지만, 다음 폴더명 네 개는 roots/vines 양쪽에서 사용됩니다.

- `sample_business_daily`
- `sample_business_monthly`
- `sample_business_yearly`
- `ujlim_sftp_runner_sample_daily`

ID/폴더 불일치 warning도 8건입니다. 따라서 현재 저장소에서 전체 검사는 실패하며 `--name` 조회도 차단됩니다. 이는 도구 고장이 아니라 기존 이름 모호성을 발견한 결과입니다. 기존 폴더/문서 참조를 임의 변경하지 않았습니다. 이름만으로 찾는 운영 규칙을 도입하려면 별도 검토 후 폴더명을 전역 유일하게 정리하고 도구를 다시 실행해야 합니다. `_root`/`_vine` 구분을 유지한 실제 DAG ID와 폴더명을 일치시키는 방법을 검토할 수 있으나 배포 경로/상대 SQL/schema 참조에 대한 검증이 선행되어야 합니다.

이 CLI의 mapping은 **runtime에서 사용하지 않습니다**. 현재 선택적 파싱은 YAML의 실제 ID를 읽어 결정하므로 폴더명 중복/불일치에 영향받지 않습니다. 이 도구 실행 결과를 영구 캐시로 사용하지 않으므로 stale index 문제는 없지만, 검사 직후 파일 변경까지 막는 원자적 snapshot은 아닙니다. 같은 immutable release에서 검사·배포하세요.
