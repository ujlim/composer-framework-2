# DAG parsing 성능 최소 패치 (Composer / Airflow 3.3.1)

## 목표
- DAG Processor의 전체 DAG 탐색 결과는 변경하지 않는다.
- Worker가 특정 태스크를 실행하기 위해 `main.py`를 재해석할 때 `get_parsing_context().dag_id`에 해당하는 Root/Vine DAG만 생성한다.
- YAML 설정, 작업 순서, Executor 구현, Root/Vine 인터페이스, Trigger 설정은 변경하지 않는다.
- YAML 파일은 기존처럼 모두 읽으므로 GCS I/O 최적화는 **이번 범위가 아니다**.

## 기준 브랜치 / 변경 파일
- Base: `feature/26.1004.airflow-trigger-tuning`
- Work: `feature/26.1008.dag-parse-optimization`
- Runtime 변경: `dags/main.py` **한 파일**
- 회귀 테스트: `tests/test_dag_parsing_context.py`

## 동작
- `get_parsing_context().dag_id is None`: 모든 Vine/Root를 이전과 동일하게 생성 및 등록.
- `dag_id`가 지정된 경우: 매칭되는 Vine 또는 Root만 생성 및 등록.
- 지정한 DAG가 등록되지 않으면 기존과 같이 DAG import 실패를 명확히 보고.
- 각 DAG 생성 실패의 로그·격리 정책은 유지.

## 계측 로그
`DAG_PARSE_TIMING` 검색어로 Composer의 Worker / DAG Processor 로그 확인.

| stage | 측정 범위 |
| --- | --- |
| `catalog_load` | 모든 Root/Vine YAML 카탈로그 탐색·로드 |
| `catalog_load_failed` | YAML 카탈로그 로딩 실패 시점 |
| `vine_create` | 개별 Vine 생성 및 등록(실패 포함) |
| `vine_stage` | Vine 생성 단계 전체 |
| `root_create` | 개별 Root 생성 및 등록(실패 포함) |
| `root_stage` | Root 생성 단계 전체 |
| `total_registration` | 파싱 컨텍스트 조회 이후 전체 등록 단계 |

로그에는 `duration_ms`, `target_dag_id`가 포함됩니다. 단계별로 `root_count`, `vine_count`, `selected_count`, `registered_dag_count` 등의 필드도 남습니다.

## 로컬 회귀 테스트

```bash
python -m unittest discover -s tests -v
python -m py_compile dags/main.py
```

테스트는 Airflow 의존성을 mock 처리하여 전체 DAG 탐색, 단일 Root/Vine 선택, 실패 격리, 대상 미존재, 로깅을 검증합니다. **실제 Composer 3.3.1에서의 성능/호환성 검증을 대신하지는 않습니다.**

## 배포·관찰 절차

1. 테스트 환경에 `dags/main.py`만 배포하고 Airflow UI Import Errors가 없는지 확인.
2. DAG Processor의 `target_dag_id=None` 전체 생성·등록 수를 기존과 비교.
3. Root 하나와 Vine 하나를 수동 실행하여 Worker 로그에서 해당 DAG만 생성되는지 확인.
4. `catalog_load`와 `total_registration` 시간을 전후 비교하고 Worker CPU 및 배치 종료 시각 관찰.
5. 운영 적용 시 일단 별도로 조정한 600초 import timeout을 유지하고, 여러 배치 안정화 이후 재평가.

문제 발생 시에는 이 브랜치의 `dags/main.py`만 직전 운영 버전으로 다시 배포하면 코드 레벨에서 되돌릴 수 있습니다. (Composer 버전 다운그레이드가 아님)
