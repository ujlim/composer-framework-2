from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

_LOGGER_PREFIX = "vine_framework"
_SENSITIVE = ("password", "secret", "token", "private_key", "credential")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"{_LOGGER_PREFIX}.{name}")


def _mask(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "***" if any(word in key.lower() for word in _SENSITIVE) else _mask(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_mask(item) for item in value]
    return value


def log_event(event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **_mask(fields),
    }
    get_logger("events").info(json.dumps(payload, ensure_ascii=False, default=str))


def task_success_callback(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    duration = None
    if ti.start_date and ti.end_date:
        duration = (ti.end_date - ti.start_date).total_seconds()

    log_event(
        "task_success",
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        try_number=ti.try_number,
        duration_seconds=duration,
    )


def task_failure_callback(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    log_event(
        "task_failure",
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        try_number=ti.try_number,
        exception=str(context.get("exception")),
        log_url=getattr(ti, "log_url", None),
    )


def dag_success_callback(context: dict[str, Any]) -> None:
    dag_run = context["dag_run"]
    log_event(
        "dag_success",
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,
        logical_date=dag_run.logical_date,
    )


def dag_failure_callback(context: dict[str, Any]) -> None:
    dag_run = context["dag_run"]
    log_event(
        "dag_failure",
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,
        logical_date=dag_run.logical_date,
        exception=str(context.get("exception")),
    )
