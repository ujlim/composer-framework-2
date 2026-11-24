# Composer DAG 진입점 분리: Root / Vine

- 작성: LIM UI JIN
- 변경일: 2026-11-24
- 비교 기준: `faab5bd2`의 `dags/main.py`. 이 브랜치는 소스 변경이며 운영 배포나 Composer 설정 변경이 아닙니다.

## 변경 내용

| 구분 | 교체 전 | 교체 후 |
|---|---|---|
| 예정 실행과 Root → Vine 호출 | `dags/main.py` | `dags/main_roots.py` |
| 스케줄 없는 Vine 및 Grape 작업 | `dags/main.py` | `dags/main_vines.py` |
| YAML 탐색, 모델 및 Factory | 공용 | 동일한 `PipelineCatalogLoader`, `RootFactory`, `VineFactory` 사용 |

DAG ID, 스케줄, catchup, 업무일자, 태스크 ID·의존성·콜백 및 YAML 경로/양식은 변경하지 않습니다. Root가 참조할 Vine 목록도 공용 카탈로그에서 가져옵니다. 단, Root와 Vine이 **같은 DAG ID**를 선언하면 두 진입점 모두 DAG를 등록하기 전에 두 YAML 경로가 표시되는 import 오류로 중단합니다. 별도 파일의 등록 순서에 따라 하나가 우연히 등록되는 동작을 허용하지 않습니다. 동일 종류의 기존 카탈로그 중복 검사와 유효한 YAML 사이의 개별 Factory 실패 격리는 유지합니다.

- DAG processor는 각 파일에서 해당 종류의 DAG만 생성합니다. Worker 파싱 컨텍스트에 이미 알려진 ID가 있으면 그 ID를 소유한 파일에서 해당 DAG만 만들고 상대 파일은 0개를 만듭니다. 카탈로그에 없는 ID에 대한 기존 fallback은 파일별 소유 범위 안에서 유지합니다.
- 두 파일이 **각각 전체 YAML 카탈로그를 로드**합니다. 이 패치는 DAG 생성·import의 단위를 분리하는 것이며, YAML을 한 번만 읽는 최적화나 DAG별 Python 사전 생성은 아닙니다. Root import가 Vine 생성 및 Vine 실행기 import와 분리되지만, Vine 쪽 부하와 카탈로그 I/O는 남습니다.
- 이번에는 Vine을 별도 샤드로 나누지 않았습니다. 샤드마다 전체 카탈로그를 재로딩하면 파싱 비용이 늘 수 있으므로, 두 파일 기준에서 Vine 파싱 시간과 프로세서 여유를 먼저 확인합니다.
- 단일 파일에서 약 1,029개 DAG를 처리한 운영 사례의 영향을 나누기 위한 구조 변경입니다. DB 경합을 원인으로 확정하거나, 매 파싱마다 모든 serialized DAG가 다시 기록된다고 전제하지 않습니다. 스케줄러/DB 처리량이나 SLA 개선을 보장하지 않습니다.

## 테스트 환경 교체 절차

1. **배포 전 목록 및 복구본 확보.** 기존 배포 산출물과 Composer DAG 버킷의 실제 파일 목록, Root/Vine ID·task ID·스케줄·의존성·pause 상태·file location을 기록합니다. 이전 파일과 공용 의존성의 복구본은 `dags/` 외부에 둡니다. 외부 배포 파이프라인이 `main.py` 고정 복사 또는 추가 전용 동기화를 하는지 확인하고 교체 절차에 반영합니다. 이 저장소에는 Composer 배포 워크플로가 없습니다.
2. **새 실행 진입을 통제하고 기존 run을 마칩니다.** 마지막 배치 이후 다음 스케줄 전에 테스트 창을 확보합니다. 수동/API/외부 트리거·backfill·clear를 막고, 진행 중 Root→Vine 작업은 끝나도록 둡니다. 영향받는 Root/Vine의 queued/running run과 미완료 태스크를 확인합니다. pause만으로 drain되지 않습니다. 기존 실행이 남으면 교체를 연기합니다. 모두 끝난 뒤 기존 pause 상태를 기록하고 대상 DAG를 pause한 다음 다시 확인합니다.
3. **옛 진입점을 치우고 새 진입점을 게시합니다.** 공유 helper `framework/dag_entrypoint.py`와 필요한 의존 파일을 먼저 준비합니다. 배포 대상의 기존 `dags/main.py`를 제거하거나 DAG 탐색 범위 밖으로 이동하고, 버킷과 Composer 동기화에서 제거를 확인한 다음 `main_roots.py`, `main_vines.py`를 게시합니다. **기존과 새 진입점이 함께 발견되어서는 안 됩니다.** 버킷 복사 한 번으로 교체가 원자적이지 않으며, 정해진 테스트 창의 일시적인 미등록 구간이 있을 수 있습니다. 광범위한 삭제 동기화는 피하고 기존 진입점만 표적으로 처리합니다.
4. **ID 목록·file location·실제 worker를 확인합니다.** 신규 import 오류/중복이 없는지, 교체 전후의 ID·스케줄·태스크 목록 및 의존성이 일치하는지, 각 DAG의 등록 파일이 새 Root 또는 Vine 파일인지 봅니다. UI 표시만으로 worker 동기화는 보장되지 않습니다. 업로드 후 Cloud Storage sync 완료 로그를 확인하고, 격리된 테스트 입력으로 **실제 Root와 Vine 워커 태스크**를 실행해 새 file location 및 성공을 확인합니다. (별도 테스트 DAG 또는 통제된 창에서 제한적으로 unpause.) 같은 ID의 기존 이력과 pause 상태도 확인하고, DAG 삭제 API로 metadata를 지우지 않습니다.
5. **검증 뒤 스케줄 복구.** 이전 pause 상태를 복구하고 진입 통제를 풉니다. Root 예정/실제 시작 지연, file별 파싱 시간 및 오류, worker 첫 태스크 시작, 전체 배치 종료 시간을 전후 같은 기준으로 비교합니다. 로그는 `dag_registered`, `dag_registration_failed`, `pipeline_catalog_load_failed`, `dag_registration_completed`를 `dag_kind`와 함께 확인합니다.

## 롤백

2번의 실행 통제와 대기 완료 기준을 동일하게 적용합니다. 두 신규 진입점을 **모두** 탐색 범위에서 제거하고 동기화를 확인한 뒤 이전 `main.py`와 필요한 호환 의존성을 복구합니다. 새 진입점과 이전 진입점을 동시에 두지 않습니다. 복구 후 동일 DAG ID·이력·기존 file location·worker 실행을 검증하고 pause 상태를 복구합니다. 파일 복원은 이미 수행된 SQL·파일 전송 등 데이터 변경의 롤백이 아닙니다.

[Google Composer DAG 업데이트 및 worker 동기화 안내](https://docs.cloud.google.com/composer/docs/composer-3/manage-dags#update_a_dag_that_has_active_dag_runs) · [Airflow 3.3.1 DAG pause 동작](https://airflow.apache.org/docs/apache-airflow/3.3.1/core-concepts/dags.html#dag-pausing-deactivation-and-deletion)

## 로컬 회귀 테스트

```sh
python -m unittest discover -s tests -p test_dag_entrypoints.py -v
python -m unittest discover -s tests -v
```

테스트는 실제 진입점·카탈로그·Root/Vine Factory를 사용하되, Airflow 및 provider 경계는 로컬 테스트 대역입니다. Composer 환경의 DAG 등록·기존 file location 전환·워커 동기화 및 실제 워커 실행은 테스트 환경 승인 이후 별도 확인 항목입니다.
