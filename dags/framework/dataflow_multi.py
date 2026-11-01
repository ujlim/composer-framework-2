# Author: 임의진 (LIM UI JIN)
# Created: 2026-10-31
"""Opt-in manifest staging for the common JDBC launch/observe lifecycle."""
from __future__ import annotations

import hashlib
import json
import re
import uuid

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from framework.utils import read_text, resolve_child_file

ID = re.compile(r"[a-z][a-z0-9_]{0,62}\Z")
TABLE = re.compile(r"[a-z][a-z0-9-]{4,61}[a-z0-9]:[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\Z")


def validate_tables(tables, *, allow_templates=False):
    if not isinstance(tables, list) or not tables:
        raise ValueError("tables must be a non-empty list")
    ids, targets = set(), set()
    for item in tables:
        if not isinstance(item, dict) or set(item) != {"id", "sql", "destination"}:
            raise ValueError("Invalid table entry keys")
        name = item["id"]
        if not isinstance(name, str) or not ID.fullmatch(name) or name in ids:
            raise ValueError("Invalid or duplicate table id")
        ids.add(name)
        if not isinstance(item["sql"], str) or not item["sql"].strip():
            raise ValueError("Table SQL must be non-empty")
        dest = item["destination"]
        if not isinstance(dest, dict) or set(dest) != {"table", "schema", "write_disposition", "create_disposition"}:
            raise ValueError("Invalid table destination keys")
        table = dest["table"]
        is_template = isinstance(table, str) and allow_templates and "{{" in table and "}}" in table
        if (not isinstance(table, str) or (not TABLE.fullmatch(table) and not is_template)
                or table.lower() in targets):
            raise ValueError("Invalid or duplicate BigQuery destination")
        targets.add(table.lower())
        if dest["write_disposition"] not in {"WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"}:
            raise ValueError("Invalid write disposition")
        if dest["create_disposition"] not in {"CREATE_IF_NEEDED", "CREATE_NEVER"}:
            raise ValueError("Invalid create disposition")
        schema = dest["schema"]
        if not isinstance(schema, dict) or set(schema) != {"fields"} or not isinstance(schema["fields"], list) or not schema["fields"]:
            raise ValueError("Schema must contain non-empty fields")
        names = set()
        for field in schema["fields"]:
            if not isinstance(field, dict) or set(field) - {"name", "type", "mode", "description"}:
                raise ValueError("Multi-table mode supports flat BigQuery schemas only")
            name = field.get("name")
            if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,299}", name) or name.lower() in names:
                raise ValueError("Invalid or duplicate schema field")
            names.add(name.lower())
            if field.get("type") not in {"STRING", "BYTES", "INTEGER", "INT64", "FLOAT", "FLOAT64", "BOOLEAN", "BOOL", "NUMERIC", "BIGNUMERIC", "DATE", "DATETIME", "TIME", "TIMESTAMP", "GEOGRAPHY", "JSON"}:
                raise ValueError("Unsupported schema field type")
            if field.get("mode", "NULLABLE") not in {"NULLABLE", "REQUIRED"}:
                raise ValueError("Invalid schema field mode")
    return tables


def create_multi_task(executor, dag, vine_config, grape, runtime, defaults):
    if "sql_file" in grape or "destination" in grape:
        raise ValueError("Multi-table grape uses tables, not top-level sql_file/destination")
    raw = grape.get("tables")
    if not isinstance(raw, list) or not raw:
        raise ValueError("tables must be a non-empty list")
    tables = []
    for item in raw:
        if not isinstance(item, dict) or set(item) != {"id", "sql_file", "destination"}:
            raise ValueError("Invalid table configuration keys")
        dest = item["destination"]
        if not isinstance(dest, dict) or set(dest) - {"table", "schema_file", "write_disposition", "create_disposition"}:
            raise ValueError("Invalid destination configuration keys")
        try:
            sql = read_text(resolve_child_file(vine_config.base_dir, item["sql_file"]))
            schema = json.loads(read_text(resolve_child_file(vine_config.base_dir, dest["schema_file"])))
        except (KeyError, TypeError, ValueError, OSError):
            raise ValueError("Cannot load table SQL/schema inside Vine directory") from None
        tables.append({"id": item["id"], "sql": sql, "destination": {
            "table": dest.get("table"), "schema": schema,
            "write_disposition": dest.get("write_disposition", "WRITE_APPEND"),
            "create_disposition": dest.get("create_disposition", "CREATE_IF_NEEDED"),
        }})
    validate_tables(tables, allow_templates=True)
    # Reuse the unchanged single-table factory for common runtime/options checks.
    first = raw[0]
    single = {**grape, "type": "jdbc_to_bigquery", "sql_file": first["sql_file"], "destination": first["destination"]}
    task = executor.create_task(dag=dag, vine_config=vine_config, grape=single, runtime=runtime, defaults=defaults)
    task.tables = tables
    task.sql = ""  # Avoid rendering the first SQL twice; all tables render together.
    task.destination = {}
    return task


def prepare_manifest(task, context):
    """Stage rendered, credential-free assets before the common launch path."""
    tables = validate_tables(task.tables)
    if any("{{" in item["sql"] or "{%" in item["sql"] for item in tables):
        raise ValueError("Table SQL must be fully rendered before upload")
    run_key = hashlib.sha256(str(context["run_id"]).encode()).hexdigest()[:20]
    prefix = f"multi/{run_key}/{uuid.uuid4().hex}"
    hook = GCSHook(gcp_conn_id=task.gcp_conn_id, impersonation_chain=task.impersonation_chain)

    def upload(name, data, mime):
        path = f"{prefix}/{name}"
        hook.upload(bucket_name=task.system_bucket, object_name=path,
                    data=data.encode("utf-8"), mime_type=mime)
        return f"gs://{task.system_bucket}/{path}"

    entries = []
    for item in tables:
        name, dest = item["id"], item["destination"]
        entries.append({"id": name,
                        "query_uri": upload(f"{name}.sql", item["sql"], "text/plain"),
                        "schema_uri": upload(f"{name}.schema.json", json.dumps(dest["schema"]), "application/json"),
                        "destination": {key: dest[key] for key in ("table", "write_disposition", "create_disposition")}})
    return upload("manifest.json", json.dumps({"version": 1, "tables": entries}), "application/json")
