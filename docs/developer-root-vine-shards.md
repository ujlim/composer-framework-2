<!-- Author: LIM UI JIN | Created: 2026-11-25 -->
# 개발자 버전 Root/Vine DAG 진입점 분할

기준은 개발자 브랜치 `feature/26.1112.developer-selective-parsing`의
`faab5bd`입니다. LibYAML 사용과 Airflow parsing context를 유지합니다.
운영 YAML·DAG ID·팩토리 코드는 변경하지 않습니다.

- Root: `main_roots.py` 한 파일에서 등록합니다.
- Vine: 폴더명 `pipelines/vines/<폴더명>/config.yaml`을 UTF-8 SHA-256으로
  해시하고 첫 8바이트를 빅엔디언 정수로 바꾼 뒤 8로 나눈 나머지가
  `main_vines_00.py`~`main_vines_07.py`의 번호입니다. 폴더명이 DAG ID와
  같으면 DAG ID로 해시한 것과 같습니다. 예: `sales_daily` → 04,
  `sales_monthly` → 03. 서로 다른 폴더가 같은 파일 번호를 받는 것은 정상입니다.
- 실제 DAG ID와 Root 참조는 YAML 기준입니다. 이름이 다른 기존 폴더는
  이름을 바꾸지 않아도 됩니다. 워커는 Airflow가 선택한 진입 파일에서
  이름이 같은 폴더를 먼저 보고, 필요한 경우에만 해당 파일 소유 범위에서
  찾습니다. 요청 DAG가 없으면 전체 등록으로 되돌아가지 않습니다.
- 프레임워크의 중복 ID 사전 검사는 제거했습니다. 폴더 규칙은 같은
  디렉터리 안의 같은 폴더명만 막으므로, 서로 다른 폴더의 같은 YAML ID나
  Root/Vine 간 충돌은 막지 못합니다. Airflow 자체의 중복 동작은 그대로이며,
  중복 정의가 있을 때 어느 것이 유효할지에 의존하지 마세요.
- YAML 문법 오류, 잘못된 DAG ID, DAG 생성 오류와 Root의 Vine 참조 검증은
  중복 검사와 별개로 유지합니다.

## YAML 비용

전체 탐색 시 Vine 파일들은 각각 자기 폴더의 YAML만 읽습니다. Root 파일은
실제 Vine ID를 기준으로 참조를 검증하므로 Vine YAML을 한 번 더 읽습니다.
Vine 1,000개, Root 29개라면 YAML 읽기·역직렬화 호출은 총 2,029회입니다.
이는 호출 수이며 Composer 파싱 시간이나 CPU 절감 실측값이 아닙니다.
폴더명=ID인 Vine 실행 파일에서는 일반적으로 요청된 YAML 한 개만 읽습니다.
Root 실행 파일은 선택된 Root와 실제 Vine ID 목록을 읽습니다.

## 배포 전

1. 동일한 개발 DAG 묶음으로 변경 전후 DAG ID, 스케줄, 태스크 수 및 Root의
   Vine 참조가 유지되는지 확인합니다. Root와 Vine의 실제 워커 실행도 확인합니다.
2. `main.py`와 새 진입 파일을 함께 활성화하지 않습니다. 관리된 전환
   시점에 기존 파일을 제거하고, 새 파일 9개와 공통 코드가 같은 릴리스로
   동기화되게 합니다. 진행 중인 실행을 먼저 정리하고 파일 경로 갱신 및
   DAG 등록이 완료됐는지 확인합니다.
3. 되돌릴 때도 새 진입 파일 9개를 제거하고 기존 `main.py` 및 기존 공통
   코드를 같은 릴리스로 복구합니다.
4. 같은 Airflow 빌드와 파서 설정에서 파일별 파싱 시간, 전체 파싱 시간,
   파싱 오류와 실제 예약 실행 생성 지연을 함께 비교합니다.
