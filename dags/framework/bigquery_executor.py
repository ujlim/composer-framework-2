from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from framework.logger import task_failure_callback, task_success_callback
from framework.models import LoadedConfig
from framework.utils import merge_dicts, read_text, resolve_child_file, validate_airflow_id

_PARAMETER_TYPES = {
    "STRING", "BYTES", "INT64", "INTEGER", "FLOAT64", "FLOAT",
    "NUMERIC", "BIGNUMERIC", "BOOL", "BOOLEAN", "TIMESTAMP",
    "DATE", "TIME", "DATETIME", "GEOGRAPHY", "JSON",
}
_BQ_LABEL_RE = re.compile(r"[^a-z0-9_-]")


def _label(value: str) -> str:
    value = _BQ_LABEL_RE.sub("_", value.lower())[:63]
    return value or "unknown"


class BigQueryExecutor:
    """
    Builds BigQueryInsertJobOperator tasks for Grape definitions.

    Supported Grape types:
      - bigquery_sql: reads sql_file
      - bigquery_procedure: builds CALL statement from procedure + parameters
    """

    @classmethod
    def create_task(
        cls,
        *,
        dag,
        vine_config: LoadedConfig,
        grape: dict[str, Any],
        runtime: dict[str, Any],
        defaults: dict[str, Any],
    ) -> BigQueryInsertJobOperator:
        grape_id = validate_airflow_id(grape.get("grape_id"), "grape.grape_id")
        grape_type = grape.get("type")
        if grape_type not in {"bigquery_sql", "bigquery_procedure"}:
            raise ValueError(f"Unsupported BigQuery grape type: {grape_type}")

        options = merge_dicts(defaults, grape.get("options", {}))
        parameters = grape.get("parameters", {})
        query_parameters = cls._build_query_parameters(parameters)

        if grape_type == "bigquery_sql":
            sql_file = grape.get("sql_file")
            if not isinstance(sql_file, str):
                raise ValueError(f"{grape_id}: sql_file is required")
            sql_path = resolve_child_file(vine_config.base_dir, sql_file)
            sql = read_text(sql_path)
        else:
            procedure = grape.get("procedure")
            if not isinstance(procedure, str) or not procedure.strip():
                raise ValueError(f"{grape_id}: procedure is required")
            cls._validate_object_name(procedure, "procedure")
            args = ", ".join(f"@{name}" for name in parameters)
            sql = f"CALL `{procedure}`({args});"

        query_config: dict[str, Any] = {
            "query": sql,
            "useLegacySql": False,
            "priority": str(options.get("priority", "INTERACTIVE")).upper(),
        }
        if query_parameters:
            query_config["parameterMode"] = "NAMED"
            query_config["queryParameters"] = query_parameters

        labels = {
            "framework": "root-vine-grape",
            "vine": _label(vine_config.id),
            "grape": _label(grape_id),
        }
        labels.update({
            str(key): _label(str(value))
            for key, value in runtime.get("labels", {}).items()
        })

        return BigQueryInsertJobOperator(
            task_id=grape_id,
            configuration={
                "query": query_config,
                "labels": labels,
            },
            project_id=runtime.get("project_id"),
            location=runtime.get("location", "asia-northeast3"),
            gcp_conn_id=runtime.get("gcp_conn_id", "google_cloud_default"),
            impersonation_chain=runtime.get("impersonation_chain"),
            deferrable=bool(options.get("deferrable", True)),
            force_rerun=bool(options.get("force_rerun", False)),
            retries=int(options.get("retries", 1)),
            retry_delay=timedelta(seconds=int(options.get("retry_delay_seconds", 300))),
            execution_timeout=timedelta(
                seconds=int(options.get("execution_timeout_seconds", 7200))
            ),
            pool=options.get("pool"),
            priority_weight=int(options.get("priority_weight", 1)),
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback,
            dag=dag,
        )

    @staticmethod
    def _build_query_parameters(parameters: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(parameters, dict):
            raise ValueError("parameters must be a mapping")

        result: list[dict[str, Any]] = []
        for name, spec in parameters.items():
            if not isinstance(name, str) or not name:
                raise ValueError("Parameter name must be a non-empty string")
            if not isinstance(spec, dict):
                raise ValueError(f"Parameter '{name}' must be a mapping")

            parameter_type = str(spec.get("type", "STRING")).upper()
            if parameter_type not in _PARAMETER_TYPES:
                raise ValueError(
                    f"Parameter '{name}' has unsupported scalar type '{parameter_type}'"
                )
            if "value" not in spec:
                raise ValueError(f"Parameter '{name}' requires value")

            value = spec["value"]
            # configuration is templated by BigQueryInsertJobOperator, so Jinja values
            # such as {{ dag_run.conf.get('batch_date', ds) }} are resolved at runtime.
            if isinstance(value, bool):
                encoded_value: Any = "true" if value else "false"
            elif value is None:
                encoded_value = None
            else:
                encoded_value = str(value)

            result.append({
                "name": name,
                "parameterType": {"type": parameter_type},
                "parameterValue": {"value": encoded_value},
            })
        return result

    @staticmethod
    def _validate_object_name(value: str, name: str) -> None:
        # Minimal protection for project.dataset.routine identifiers.
        parts = value.split(".")
        if len(parts) not in {2, 3} or any(not part.strip() for part in parts):
            raise ValueError(
                f"{name} must be dataset.object or project.dataset.object: {value}"
            )
        invalid = re.compile(r"[^A-Za-z0-9_-]")
        if any(invalid.search(part) for part in parts):
            raise ValueError(f"{name} contains invalid characters: {value}")
