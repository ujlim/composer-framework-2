# ujlim_pg_operator_sample

`postgres_sql` Grape가 Cloud KMS로 암호화된 PostgreSQL 접속 JSON을 복호화한 뒤 SQL 파일을 실행하는 샘플입니다.
Daily 스케줄은 Vine이 아니라 `pipelines/roots/ujlim_pg_operator_sample_daily` Root가 매일 02:00(Asia/Seoul)에 이 Vine을 호출합니다.

## 1. PostgreSQL credential JSON

평문 파일은 로컬 임시 파일로만 만들고 Git에 커밋하지 않습니다.

```json
{
  "host": "10.0.0.10",
  "port": 5432,
  "database": "appdb",
  "user": "app_user",
  "password": "REPLACE_PASSWORD",
  "sslmode": "require"
}
```

필수 필드는 `host`, `database`, `user`, `password`이며 `port` 기본값은 5432입니다.

## 2. KMS 암호화

```bash
gcloud kms encrypt \
  --project=REPLACE_PROJECT \
  --location=asia-northeast3 \
  --keyring=REPLACE_KEYRING \
  --key=REPLACE_KEY \
  --plaintext-file=pg-credentials.json \
  --ciphertext-file=pg-credentials.bin

base64 -w 0 pg-credentials.bin > pg-credentials.b64
```

`pg-credentials.b64`의 한 줄 값을 Vine config의 `encrypted_credentials`에 넣습니다.
Composer 실행 서비스 계정(또는 impersonation 대상)은 해당 CryptoKey에 `roles/cloudkms.cryptoKeyDecrypter` 권한이 필요합니다.

## 3. Composer dependency

Composer 환경에 PostgreSQL Python driver가 있어야 합니다. 권장 설치 항목은 `apache-airflow-providers-postgres`이며,
executor는 `psycopg`를 먼저 사용하고 없으면 `psycopg2`를 사용합니다.

## 4. Network

Cloud SQL PostgreSQL이 private IP를 사용한다면 Composer worker에서 해당 private IP:5432로 라우팅/방화벽 접근이 가능해야 합니다.
접속정보는 로그/XCom으로 반환하지 않습니다.
