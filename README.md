# composer-framework-2

## 사용자 가이드

[YAML 기반 Composer Framework 사용자 가이드](docs/user-guide/README.md): Root/Vine 공통 설정, 업무일자, 12개 Grape 유형, 연결·권한, 실행 예제 및 운영 주의사항.

## Selective worker DAG parsing

See [selective DAG parsing](docs/selective-dag-parsing.md) for the deployment-time
ID manifest, Airflow compatibility, full-catalog fallback, tests, safe release
promotion, performance measurement and rollback. The optimization does not
change the generated task graph or deploy anything automatically.

## Runtime option updates

See [SFTP connection order, BigQuery durable compatibility and polling defaults](docs/runtime-options-update.md)
for behavior, explicit overrides, deployment checks and rollback guidance.
