# composer-framework-2

## 사용자 가이드

[YAML 기반 Composer Framework 사용자 가이드](docs/user-guide/README.md): Root/Vine 공통 설정, 업무일자, 12개 Grape 유형, 연결·권한, 실행 예제 및 운영 주의사항.


## Runtime option updates

See [SFTP connection order, BigQuery durable compatibility and polling defaults](docs/runtime-options-update.md)
for behavior, explicit overrides, deployment checks and rollback guidance.

## Selective DAG parsing

[선택적 DAG 생성·전체 이름 유일성 검사 가이드](docs/selective-dag-parsing.md):
전체 scheduler 발견을 유지하면서 실행 대상 DAG만 생성합니다. 이름→폴더 조회 CLI,
기존 이름 충돌 보고, 배포 검증과 롤백 절차를 포함합니다.
