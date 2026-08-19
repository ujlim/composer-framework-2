from __future__ import annotations

from datetime import date
from typing import Any

import pendulum
from airflow.operators.python import get_current_context


def _normalize_business_date(value: Any) -> str | None:
    """Validate and normalize a business date as YYYY-MM-DD."""
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(
            f"business_date must be YYYY-MM-DD, got: {value!r}"
        ) from exc

    return parsed.isoformat()


def _dag_run_type(dag_run: Any) -> str:
    run_type = getattr(dag_run, "run_type", "")
    return str(getattr(run_type, "value", run_type)).lower()


def resolve_root_business_date(
    *,
    timezone: str = "Asia/Seoul",
    source: str = "data_interval_end",
    offset_days: int = -1,
) -> str:
    """Resolve Root business_date for scheduled and manual runs."""
    context = get_current_context()
    dag_run = context["dag_run"]
    params = context.get("params") or {}

    conf = getattr(dag_run, "conf", None) or {}
    explicit = _normalize_business_date(conf.get("business_date"))
    if explicit:
        return explicit

    run_type = _dag_run_type(dag_run)

    if run_type == "scheduled":
        if source != "data_interval_end":
            raise ValueError(
                f"Unsupported business_date.source: {source!r}. "
                "Only 'data_interval_end' is currently supported."
            )

        interval_end = context.get("data_interval_end")
        if interval_end is None:
            raise ValueError(
                "Scheduled run has no data_interval_end; "
                "cannot calculate business_date."
            )

        return (
            pendulum.instance(interval_end)
            .in_timezone(timezone)
            .add(days=int(offset_days))
            .date()
            .isoformat()
        )

    manual = _normalize_business_date(params.get("business_date"))
    if manual:
        return manual

    raise ValueError(
        "business_date is required for a non-scheduled Root run. "
        "Enter Business Date in the Airflow Trigger DAG screen "
        "using YYYY-MM-DD."
    )


def resolve_vine_business_date() -> str:
    """Resolve Vine business_date inherited from Root or entered manually."""
    context = get_current_context()
    dag_run = context["dag_run"]
    params = context.get("params") or {}

    conf = getattr(dag_run, "conf", None) or {}
    inherited = _normalize_business_date(conf.get("business_date"))
    if inherited:
        return inherited

    manual = _normalize_business_date(params.get("business_date"))
    if manual:
        return manual

    raise ValueError(
        "business_date is required for Vine execution. "
        "Run the Vine from a Root DAG or enter Business Date "
        "in the Airflow Trigger DAG screen using YYYY-MM-DD."
    )
