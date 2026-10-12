# Composer Framework 사용자 가이드

이 가이드는 `feature/26.0921.dataflow` 브랜치의 소스 `09900a81b7bff965b72e17ee44599a11010636b1`(2026-10-08)를 기준으로, 개발된 YAML 기능 전체를 정리합니다. 작성일: 2026-10-12. 기존 예제·설명과 충돌하면 실행기 구현을 기준으로 합니다.

## 읽는 순서

1. [YAML 작성: Root, Vine, 공통 옵션, 업무일자](01-yaml-common.md)
2. [BigQuery / PostgreSQL](02-sql.md)
3. [GCS / SFTP Runner / FIN 센서](03-storage-sftp.md)
4. [Dataflow JDBC](04-dataflow.md)
5. [배포 준비, 연결·권한, 실행·장애 대응](05-operations.md)

각 파라미터 표는 **구분 / YAML 파라미터 / 필수 / 기본값·권장값 / 설명 / 예시** 순서입니다. `O`는 필수, `X`는 선택, `조건부`는 설명의 조건을 만족할 때 필요합니다. 자료형은 설명에 표시합니다. **기본값은 미지정 시 코드가 적용하는 값**, 권장값은 운영자가 판단할 값이며 동일하지 않습니다. `—`는 기본값 없음입니다. 예시의 프로젝트·버킷·Connection·SA는 실제 환경 값으로 교체해야 합니다.

## 전체 기능 지도

| 영역 | 지원 기능 | 문서 |
|---|---|---|
| 구성·오케스트레이션 | YAML 자동 탐색, Root→Vine 트리거, Vine→Grape 실행, 의존성 DAG 검증, 병렬 분기·합류, 기본 옵션 상속 | [공통](01-yaml-common.md) |
| 업무일자 | scheduled/manual 분기, daily/monthly/yearly 기간, 상대 날짜·월초/월말/연초/연말, Jinja와 XCom | [공통](01-yaml-common.md) |
| BigQuery | `bigquery_sql`, `bigquery_procedure` | [SQL](02-sql.md) |
| PostgreSQL | `postgres_sql`, `postgres_to_gcs` | [SQL](02-sql.md) |
| GCS | `gcs_copy_object`, `gcs_delete_object`, `gcs_move_prefix`, `gcs_merge_csv` | [스토리지](03-storage-sftp.md) |
| SFTP | `gcs_to_sftp_runner`, CSV 병합, 행 수/파일 수 FIN, 대상별 인증·경로 | [스토리지](03-storage-sftp.md) |
| 대기 | `gcs_fin_file_sensor`(정확한 GCS 객체 존재 대기) | [스토리지](03-storage-sftp.md) |
| Dataflow | `jdbc_to_gcs`, `jdbc_to_bigquery`, DB2/Oracle/Vertica JDBC, 자체 Flex Template, 스키마 | [Dataflow](04-dataflow.md) |
| 운영 도구 | 이벤트 로그, JDBC 테스트, 이미지·템플릿 빌드, Dataflow 로그 조회, DAG 백업 | [운영](05-operations.md) |

지원 Grape type은 총 **12개**입니다. YAML은 범용 Airflow Operator 선언 언어가 아닙니다. `type`은 [VineFactory](../../dags/framework/vine_factory.py)에 등록된 값만 허용하며, 표에 없는 임의 Airflow 옵션은 전달되지 않습니다. 소스에 없는 기능을 예제 이름이나 기존 문서만 보고 사용하지 마세요.

## 빠른 시작

1. [공통 장의 최소 Root/Vine 예제](01-yaml-common.md#최소-rootvine-예제)를 각각 `dags/pipelines/roots/<폴더>/config.yaml`, `dags/pipelines/vines/<폴더>/config.yaml`에 둡니다.
2. Vine의 작업에 필요한 Connection과 권한을 준비합니다. 처음에는 읽기 전용 SQL이나 전용 테스트 버킷으로 검증합니다.
3. DAG 등록 로그와 Graph를 확인하고 수동 실행 시 `business_date`를 `YYYY-MM-DD`로 입력합니다.
4. 업무 결과를 확인한 뒤 Root의 스케줄을 운영 일정에 맞춥니다. 자동 운영 전 [체크리스트](05-operations.md)를 확인합니다.

이 변경은 문서만 추가하며 Composer 배포, 외부 전송, DB 실행, IAM 변경을 수행하지 않습니다.
