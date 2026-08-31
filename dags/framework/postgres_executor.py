from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from airflow.providers.google.cloud.hooks.kms import CloudKMSHook
from airflow.providers.standard.operators.python import PythonOperator

from framework.logger import task_failure_callback, task_success_callback
from framework.models import LoadedConfig
from framework.utils import merge_dicts, read_text, resolve_child_file, validate_airflow_id


_REQUIRED_CREDENTIAL_FIELDS = {"host", "database", "user", "password"}


def _decrypt_credentials(
    *,
    key_name: str,
    ciphertext: str,
    gcp_conn_id: str,
    impersonation_chain: str | list[str] | None,
) -> dict[str, Any]:
    if not isinstance(key_name, str) or not key_name.strip():
        raise ValueError("connection.kms_key_name is required")
    if not isinstance(ciphertext, str) or not ciphertext.strip():
        raise ValueError("connection.encrypted_credentials is required")

    hook = CloudKMSHook(
        gcp_conn_id=gcp_conn_id,
        impersonation_chain=impersonation_chain,
    )
    plaintext = hook.decrypt(key_name=key_name.strip(), ciphertext=ciphertext.strip())

    try:
        credentials = json.loads(plaintext.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("KMS plaintext must be a UTF-8 JSON object") from exc

    if not isinstance(credentials, dict):
        raise ValueError("KMS plaintext must be a JSON object")

    missing = sorted(
        name for name in _REQUIRED_CREDENTIAL_FIELDS
        if not isinstance(credentials.get(name), str) or not credentials.get(name).strip()
    )
    if missing:
        raise ValueError(f"KMS credentials missing required fields: {', '.join(missing)}")

    return credentials


def _connect_postgres(credentials: dict[str, Any], connect_timeout_seconds: int):
    kwargs = {
        "host": credentials["host"],
        "port": int(credentials.get("port", 5432)),
        "dbname": credentials["database"],
        "user": credentials["user"],
        "password": credentials["password"],
        "connect_timeout": connect_timeout_seconds,
    }
    for optional in ("sslmode", "sslrootcert", "sslcert", "sslkey", "application_name"):
        value = credentials.get(optional)
        if value not in (None, ""):
            kwargs[optional] = value

    try:
        import psycopg

        return psycopg.connect(**kwargs)
    except ImportError:
        try:
            import psycopg2

            return psycopg2.connect(**kwargs)
        except ImportError as exc:
            raise RuntimeError(
                "PostgreSQL driver is not installed. Install apache-airflow-providers-postgres "
                "(or psycopg/psycopg2) in the Composer environment."
            ) from exc


def execute_postgres_sql(
    *,
    sql: str,
    parameters: dict[str, Any] | None,
    kms_key_name: str,
    encrypted_credentials: str,
    gcp_conn_id: str = "google_cloud_default",
    impersonation_chain: str | list[str] | None = None,
    autocommit: bool = False,
    fetch: str = "none",
    max_fetch_rows: int = 100,
    connect_timeout_seconds: int = 30,
) -> dict[str, Any]:
    """Decrypt PostgreSQL credentials with Cloud KMS and execute one SQL script."""
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("sql must not be empty")
    if parameters is not None and not isinstance(parameters, dict):
        raise ValueError("parameters must be a mapping")

    fetch_mode = str(fetch or "none").lower()
    if fetch_mode not in {"none", "one", "all"}:
        raise ValueError("options.fetch must be one of: none, one, all")
    if max_fetch_rows < 1:
        raise ValueError("options.max_fetch_rows must be >= 1")

    credentials = _decrypt_credentials(
        key_name=kms_key_name,
        ciphertext=encrypted_credentials,
        gcp_conn_id=gcp_conn_id,
        impersonation_chain=impersonation_chain,
    )
    connection = _connect_postgres(credentials, connect_timeout_seconds)
    connection.autocommit = bool(autocommit)

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters or None)
            rowcount = cursor.rowcount
            columns: list[str] = []
            rows: list[Any] = []

            if fetch_mode != "none" and cursor.description:
                columns = [item.name if hasattr(item, "name") else item[0] for item in cursor.description]
                if fetch_mode == "one":
                    row = cursor.fetchone()
                    rows = [] if row is None else [list(row)]
                else:
                    rows = [list(row) for row in cursor.fetchmany(max_fetch_rows)]

        if not connection.autocommit:
            connection.commit()

        return {
            "rowcount": rowcount,
            "columns": columns,
            "rows": rows,
            "fetch_mode": fetch_mode,
        }
    except Exception:
        if not connection.autocommit:
            connection.rollback()
        raise
    finally:
        connection.close()


class PostgresExecutor:
    """Build Cloud SQL for PostgreSQL Grapes using KMS-encrypted credentials."""

    @classmethod
    def create_task(
        cls,
        *,
        dag,
        vine_config: LoadedConfig,
        grape: dict[str, Any],
        runtime: dict[str, Any],
        defaults: dict[str, Any],
    ) -> PythonOperator:
        grape_id = validate_airflow_id(grape.get("grape_id"), "grape.grape_id")
        if grape.get("type") != "postgres_sql":
            raise ValueError(f"Unsupported PostgreSQL grape type: {grape.get('type')}")

        sql_file = grape.get("sql_file")
        if not isinstance(sql_file, str) or not sql_file.strip():
            raise ValueError(f"{grape_id}: sql_file is required")
        sql_path = resolve_child_file(vine_config.base_dir, sql_file)
        sql = read_text(sql_path)

        connection = grape.get("connection") or {}
        if not isinstance(connection, dict):
            raise ValueError(f"{grape_id}: connection must be a mapping")

        options = merge_dicts(defaults, grape.get("options", {}))
        parameters = grape.get("parameters", {})
        if not isinstance(parameters, dict):
            raise ValueError(f"{grape_id}: parameters must be a mapping")

        return PythonOperator(
            task_id=grape_id,
            python_callable=execute_postgres_sql,
            op_kwargs={
                "sql": sql,
                "parameters": parameters,
                "kms_key_name": connection.get("kms_key_name"),
                "encrypted_credentials": connection.get("encrypted_credentials"),
                "gcp_conn_id": runtime.get("gcp_conn_id", "google_cloud_default"),
                "impersonation_chain": runtime.get("impersonation_chain"),
                "autocommit": bool(options.get("autocommit", False)),
                "fetch": options.get("fetch", "none"),
                "max_fetch_rows": int(options.get("max_fetch_rows", 100)),
                "connect_timeout_seconds": int(options.get("connect_timeout_seconds", 30)),
            },
            retries=int(options.get("retries", 1)),
            retry_delay=timedelta(seconds=int(options.get("retry_delay_seconds", 300))),
            execution_timeout=timedelta(seconds=int(options.get("execution_timeout_seconds", 7200))),
            pool=options.get("pool"),
            priority_weight=int(options.get("priority_weight", 1)),
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback,
            dag=dag,
        )
