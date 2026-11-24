# Composer DAG 진입점 분리: Root 1개 / Vine 8개

- 작성: LIM UI JIN
- 변경일: 2026-11-24
- 비교 기준: 단일 `dags/main.py`(`faab5bd2`) → Root/Vine 분리(`d4f5fc3`) → Vine 해시 샤드. 이 브랜치는 소스 변경이며 운영 배포나 Composer 설정 변경이 아닙니다.

## 변경 내용

| 구분 | 교체 전 | 교체 후 |
|---|---|---|
| 예정 실행과 Root → Vine 호출 | `dags/main.py` | `dags/main_roots.py` |
| 스케줄 없는 Vine 및 Grape 작업 | `dags/main.py` 또는 `dags/main_vines.py` | `dags/main_vines_00.py` ~ `dags/main_vines_07.py` |
| YAML 탐색, 모델 및 Factory | 공용 | 동일한 `PipelineCatalogLoader`, `RootFactory`, `VineFactory` 사용 |

DAG ID, 스케줄, catchup, 업무일자, 태스크 ID·의존성·콜백 및 YAML 경로/양식은 변경하지 않습니다. Root가 참조할 Vine 목록도 공용 카탈로그에서 가져옵니다. 단, Root와 Vine이 **같은 DAG ID**를 선언하면 Root 및 카탈로그를 읽는 Vine 진입점 모두 DAG를 등록하기 전에 두 YAML 경로가 표시되는 import 오류로 중단합니다. 별도 파일의 등록 순서에 따라 하나가 우연히 등록되는 동작을 허용하지 않습니다. 동일 종류의 기존 카탈로그 중복 검사와 유효한 YAML 사이의 개별 Factory 실패 격리는 유지합니다.

- 각 Vine은 정규화된 DAG ID의 UTF-8 **SHA-256 앞 64비트(big-endian)를 8로 나눈 나머지**에 해당하는 파일 하나가 소유합니다. 예: `vine-a`는 `main_vines_02.py`. YAML 폴더명·목록 순서·Python 해시 시드의 영향을 받지 않으며 Vine 추가로 기존 DAG가 이동하지 않습니다. 빈 샤드는 정상입니다. **8은 고정된 배포 계약**이므로 숫자 변경 시 기존 ID의 file location이 이동할 수 있어 별도 이관이 필요합니다.
- DAG processor는 Root 파일에서 Root만, 각 Vine 파일에서 자기 샤드만 생성합니다. Worker 파싱 컨텍스트에 알려진 Vine ID가 있으면 소유 파일에서만 해당 DAG를 만들고, 다른 샤드는 YAML 탐색/카탈로그/Factory import·생성을 건너뜁니다. 카탈로그에 없는 ID의 fallback은 그 ID의 해시 샤드 내에서만 수행합니다(Root는 기존과 동일).
- **전체 Discovery에서는 9개 진입점이 각각 전체 YAML 카탈로그를 로드**합니다. 파일별 Factory 작업은 분산되지만 완전한 탐색 기준 YAML 파싱 횟수는 기존 Root/Vine 2파일 대비 최대 9/2배(모놀리식 대비 9배)입니다. 일반 Worker가 실제 소유 파일 하나를 로드할 때는 전체 카탈로그 1회입니다. 새 샤드 수가 YAML·파일 I/O 비용을 높이므로, 실제 파일별 파싱 시간과 전체 프로세서 CPU·I/O, 스케줄 지연을 비교해야 합니다. 이 변경은 DAG별 Python 사전 생성이나 YAML 캐싱 추가가 아닙니다.
- 단일 파일에서 약 1,029개 DAG를 처리한 운영 사례의 영향을 나누기 위한 구조 변경입니다. DB 경합을 원인으로 확정하거나, 매 파싱마다 모든 serialized DAG가 다시 기록된다고 전제하지 않습니다. 스케줄러/DB 처리량이나 SLA 개선을 보장하지 않습니다.

## 테스트 환경 교체 절차

1. **배포 전 목록 및 복구본 확보.** 기존 배포 산출물과 Composer DAG 버킷의 실제 파일 목록, Root/Vine ID·task ID·스케줄·의존성·pause 상태·file location을 기록합니다. 이전 파일과 공용 의존성의 복구본은 `dags/` 외부에 둡니다. 외부 배포 파이프라인이 `main.py`/`main_vines.py` 고정 복사 또는 추가 전용 동기화를 하는지 확인하고 교체 절차에 반영합니다. 이 저장소에는 Composer 배포 워크플로가 없습니다.
2. **새 실행 진입을 통제하고 기존 run을 마칩니다.** 마지막 배치 이후 다음 스케줄 전에 테스트 창을 확보합니다. 수동/API/외부 트리거·backfill·clear를 막고, 진행 중 Root→Vine 작업은 끝나도록 둡니다. 영향받는 Root/Vine의 queued/running run과 미완료 태스크를 확인합니다. pause만으로 drain되지 않습니다. 기존 실행이 남으면 교체를 연기합니다. 모두 끝난 뒤 기존 pause 상태를 기록하고 대상 DAG를 pause한 다음 다시 확인합니다.
3. **옛 진입점을 치우고 새 진입점을 게시합니다.** 배포 대상에 남은 `main.py` 또는 분리 1차의 `main_vines.py`를 제거하거나 DAG 탐색 범위 밖으로 이동하고, 버킷 및 Composer 동기화에서 제거를 확인합니다. 다음으로 새 `framework/dag_entrypoint.py`를 배포하고 동기화한 뒤 `main_roots.py` 및 `main_vines_00.py`~`main_vines_07.py`를 게시합니다(기존 Root 파일이 준비되어 있으면 같은 경로로 유지). **예전과 새 Vine 진입점이 함께 발견되어서는 안 됩니다. 8개 샤드 전체를 한 릴리스로 관리**하며 누락 또는 중복이 있으면 스케줄을 재개하지 않습니다. 버킷 복사로 교체가 원자적이지 않아 테스트 창 내 일시 미등록은 가능하므로, 동기화 확인 없이 교체하지 않습니다. 광범위한 삭제 동기화 대신 해당 파일만 표적으로 처리합니다.
4. **ID 목록·file location·실제 worker를 확인합니다.** 신규 import 오류/중복이 없는지, 교체 전후의 ID·스케줄·태스크 목록 및 의존성이 일치하는지, 각 DAG의 등록 파일이 Root 또는 예상 해시 샤드와 일치하는지 보고 8개 파일 존재 및 샤드별 등록 건수를 확인합니다. UI 표시만으로 worker 동기화는 보장되지 않습니다. 업로드 후 Cloud Storage sync 완료 로그를 확인하고, 격리된 테스트 입력으로 **실제 Root와 Vine 워커 태스크**를 실행해 새 file location 및 성공을 확인합니다. (별도 테스트 DAG 또는 통제된 창에서 제한적으로 unpause.) 같은 ID의 기존 이력과 pause 상태도 확인하고, DAG 삭제 API로 metadata를 지우지 않습니다.
5. **검증 뒤 스케줄 복구.** 이전 pause 상태를 복구하고 진입 통제를 풉니다. Root 예정/실제 시작 지연, file별 파싱 시간 및 오류, worker 첫 태스크 시작, 전체 배치 종료 시간을 전후 같은 기준으로 비교합니다. 로그는 `dag_registered`, `dag_registration_failed`, `pipeline_catalog_load_failed`, `dag_registration_completed`를 `dag_kind` 및 Vine의 `vine_shard`와 함께 확인합니다.

## 롤백

2번의 실행 통제와 대기 완료 기준을 동일하게 적용합니다. 8개 신규 Vine 샤드를 **모두** 탐색 범위에서 제거하고 동기화를 확인한 뒤, 되돌릴 버전의 공용 helper/의존성과 이전 진입점을 세트로 복구합니다. 직전 분리판(`d4f5fc3`)으로 되돌릴 경우 Root 파일을 유지하고 호환 helper와 `main_vines.py`를 복구합니다. 모놀리식으로 돌아갈 때는 Root 파일도 제거한 후 `main.py`를 복구합니다. 신규 샤드와 이전 Vine 진입점을 동시에 두지 않습니다. 복구 후 동일 DAG ID·이력·기존 file location·worker 실행을 검증하고 pause 상태를 복구합니다. 파일 복원은 이미 수행된 SQL·파일 전송 등 데이터 변경의 롤백이 아닙니다.

[Google Composer DAG 업데이트 및 worker 동기화 안내](https://docs.cloud.google.com/composer/docs/composer-3/manage-dags#update_a_dag_that_has_active_dag_runs) · [Airflow 3.3.1 DAG pause 동작](https://airflow.apache.org/docs/apache-airflow/3.3.1/core-concepts/dags.html#dag-pausing-deactivation-and-deletion)

## 로컬 회귀 테스트

```sh
python -m unittest discover -s tests -p test_dag_entrypoints.py -v
python -m unittest discover -s tests -v
```

테스트는 실제 진입점·카탈로그·Root/Vine Factory를 사용하되, Airflow 및 provider 경계는 로컬 테스트 대역입니다. Composer 환경의 DAG 등록·기존 file location 전환·워커 동기화 및 실제 워커 실행은 테스트 환경 승인 이후 별도 확인 항목입니다.
