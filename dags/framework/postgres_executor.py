# Author: LIM UI JIN
# Created: 2026-09-16

from __future__ import annotations

import csv
import io
from datetime import timedelta
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.standard.operators.python import PythonOperator

from framework.logger import task_failure_callback, task_success_callback
from framework.models import LoadedConfig
from framework.utils import merge_dicts, read_text, resolve_child_file, validate_airflow_id


def _connect_postgres(conn_id: str, connect_timeout_seconds: int):
    if not isinstance(conn_id, str) or not conn_id.strip():
        raise ValueError("connection_id is required")

    conn = BaseHook.get_connection(conn_id.strip())
    kwargs = {
        "host": conn.host,
        "port": int(conn.port or 5432),
        "dbname": conn.schema,
        "user": conn.login,
        "password": conn.password,
        "connect_timeout": connect_timeout_seconds,
    }
    extra = conn.extra_dejson or {}
    for optional in ("sslmode", "sslrootcert", "sslcert", "sslkey", "application_name"):
        value = extra.get(optional)
        if value not in (None, ""):
            kwargs[optional] = value

    missing = [name for name in ("host", "dbname", "user", "password") if not kwargs.get(name)]
    if missing:
        raise ValueError(f"Airflow connection '{conn_id}' missing required fields: {', '.join(missing)}")

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
    connection_id: str,
    autocommit: bool = False,
    fetch: str = "none",
    max_fetch_rows: int = 100,
    connect_timeout_seconds: int = 30,
) -> dict[str, Any]:
    """Execute PostgreSQL SQL using an Airflow Connection resolved by the configured secrets backend."""
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("sql must not be empty")
    if parameters is not None and not isinstance(parameters, dict):
        raise ValueError("parameters must be a mapping")

    fetch_mode = str(fetch or "none").lower()
    if fetch_mode not in {"none", "one", "all"}:
        raise ValueError("options.fetch must be one of: none, one, all")
    if max_fetch_rows < 1:
        raise ValueError("options.max_fetch_rows must be >= 1")

    connection = _connect_postgres(connection_id, connect_timeout_seconds)
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


def export_postgres_to_gcs(
    *,
    sql: str,
    parameters: dict[str, Any] | None,
    connection_id: str,
    bucket: str,
    object_name: str,
    gcp_conn_id: str = "google_cloud_default",
    impersonation_chain: str | list[str] | None = None,
    chunk_rows: int = 5000,
    connect_timeout_seconds: int = 30,
    include_header: bool = True,
) -> dict[str, Any]:
    """Stream a PostgreSQL query to a CSV object in GCS without fetchall()."""
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("sql must not be empty")
    if parameters is not None and not isinstance(parameters, dict):
        raise ValueError("parameters must be a mapping")
    if not bucket or not object_name:
        raise ValueError("destination.bucket and destination.object are required")
    if chunk_rows < 1:
        raise ValueError("options.chunk_rows must be >= 1")

    connection = _connect_postgres(connection_id, connect_timeout_seconds)
    hook = GCSHook(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)

    # SpooledTemporaryFile keeps small exports in memory and transparently spills larger files to disk.
    import tempfile

    total_rows = 0
    try:
        with tempfile.SpooledTemporaryFile(mode="w+", max_size=16 * 1024 * 1024, newline="", encoding="utf-8") as tmp:
            writer = csv.writer(tmp)
            with connection.cursor() as cursor:
                cursor.execute(sql, parameters or None)
                if cursor.description is None:
                    raise ValueError("postgres_to_gcs requires a SELECT/query that returns rows")

                columns = [item.name if hasattr(item, "name") else item[0] for item in cursor.description]
                if include_header:
                    writer.writerow(columns)

                while True:
                    rows = cursor.fetchmany(chunk_rows)
                    if not rows:
                        break
                    writer.writerows(rows)
                    total_rows += len(rows)

            tmp.flush()
            tmp.seek(0)
            payload = tmp.read().encode("utf-8")
            hook.upload(
                bucket_name=bucket,
                object_name=object_name.lstrip("/"),
                data=payload,
                mime_type="text/csv",
            )
    finally:
        connection.close()

    return {
        "row_count": total_rows,
        "destination": f"gs://{bucket}/{object_name.lstrip('/')}",
        "format": "csv",
    }


class PostgresExecutor:
    """Build PostgreSQL SQL and PostgreSQL-to-GCS Grapes using Airflow Connections."""

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
        grape_type = grape.get("type")
        if grape_type not in {"postgres_sql", "postgres_to_gcs"}:
            raise ValueError(f"Unsupported PostgreSQL grape type: {grape_type}")

        sql_file = grape.get("sql_file")
        if not isinstance(sql_file, str) or not sql_file.strip():
            raise ValueError(f"{grape_id}: sql_file is required")
        sql_path = resolve_child_file(vine_config.base_dir, sql_file)
        sql = read_text(sql_path)

        connection_id = grape.get("connection_id") or runtime.get("postgres_connection_id")
        if not isinstance(connection_id, str) or not connection_id.strip():
            raise ValueError(f"{grape_id}: connection_id is required")

        options = merge_dicts(defaults, grape.get("options", {}))
        parameters = grape.get("parameters", {})
        if not isinstance(parameters, dict):
            raise ValueError(f"{grape_id}: parameters must be a mapping")

        common = {
            "sql": sql,
            "parameters": parameters,
            "connection_id": connection_id,
            "connect_timeout_seconds": int(options.get("connect_timeout_seconds", 30)),
        }

        if grape_type == "postgres_sql":
            python_callable = execute_postgres_sql
            op_kwargs = {
                **common,
                "autocommit": bool(options.get("autocommit", False)),
                "fetch": options.get("fetch", "none"),
                "max_fetch_rows": int(options.get("max_fetch_rows", 100)),
            }
        else:
            destination = grape.get("destination") or {}
            if not isinstance(destination, dict):
                raise ValueError(f"{grape_id}: destination must be a mapping")
            python_callable = export_postgres_to_gcs
            op_kwargs = {
                **common,
                "bucket": destination.get("bucket"),
                "object_name": destination.get("object"),
                "gcp_conn_id": runtime.get("gcp_conn_id", "google_cloud_default"),
                "impersonation_chain": runtime.get("impersonation_chain"),
                "chunk_rows": int(options.get("chunk_rows", 5000)),
                "include_header": bool(options.get("include_header", True)),
            }

        return PythonOperator(
            task_id=grape_id,
            python_callable=python_callable,
            op_kwargs=op_kwargs,
            retries=int(options.get("retries", 1)),
            retry_delay=timedelta(seconds=int(options.get("retry_delay_seconds", 300))),
            execution_timeout=timedelta(seconds=int(options.get("execution_timeout_seconds", 7200))),
            pool=options.get("pool"),
            priority_weight=int(options.get("priority_weight", 1)),
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback,
            dag=dag,
        )
