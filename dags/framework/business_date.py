# Author: LIM UI JIN
# Created: 2026-09-09

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pendulum

try:
    from airflow.sdk import get_current_context
except ImportError:
    from airflow.operators.python import get_current_context


_PERIOD_TYPES = {"daily", "monthly", "yearly"}
_ADJUSTMENTS = {"none", "month_start", "month_end", "year_start", "year_end"}


def _normalize_business_date(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"business_date must be YYYY-MM-DD, got: {value!r}") from exc
    return parsed.isoformat()


def _normalize_period_type(value: Any) -> str:
    period_type = str(value or "daily").strip().lower()
    if period_type not in _PERIOD_TYPES:
        raise ValueError(
            f"business_date.type must be one of {sorted(_PERIOD_TYPES)}, got: {value!r}"
        )
    return period_type


def _dag_run_type(dag_run: Any) -> str:
    run_type = getattr(dag_run, "run_type", "")
    return str(getattr(run_type, "value", run_type)).lower()


def _apply_date_offset(anchor: date, name: str, spec: Any) -> str:
    if not isinstance(spec, dict):
        raise ValueError(f"date_offsets.{name} must be a mapping")

    years = int(spec.get("years", 0))
    months = int(spec.get("months", 0))
    days = int(spec.get("days", 0))
    adjust = str(spec.get("adjust", "none")).strip().lower()

    if adjust not in _ADJUSTMENTS:
        raise ValueError(
            f"date_offsets.{name}.adjust must be one of {sorted(_ADJUSTMENTS)}, got: {adjust!r}"
        )

    value = pendulum.datetime(anchor.year, anchor.month, anchor.day, tz="UTC").add(
        years=years,
        months=months,
        days=days,
    )

    if adjust == "month_start":
        value = value.start_of("month")
    elif adjust == "month_end":
        value = value.end_of("month")
    elif adjust == "year_start":
        value = value.start_of("year")
    elif adjust == "year_end":
        value = value.end_of("year")

    return value.date().isoformat()


def _build_business_context(
    business_date: str,
    period_type: str,
    date_offsets: dict[str, Any] | None = None,
) -> dict[str, str]:
    anchor = date.fromisoformat(business_date)
    period_type = _normalize_period_type(period_type)

    if period_type == "daily":
        period_start = anchor
        period_end = anchor
    elif period_type == "monthly":
        period_start = anchor.replace(day=1)
        if anchor.month == 12:
            next_month = date(anchor.year + 1, 1, 1)
        else:
            next_month = date(anchor.year, anchor.month + 1, 1)
        period_end = next_month - timedelta(days=1)
    else:
        period_start = date(anchor.year, 1, 1)
        period_end = date(anchor.year, 12, 31)

    result = {
        "business_date": anchor.isoformat(),
        "period_type": period_type,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
    }

    for name, spec in (date_offsets or {}).items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("date_offsets key must be a non-empty string")
        if name in result:
            raise ValueError(f"date_offsets key '{name}' is reserved by business context")
        result[name] = _apply_date_offset(anchor, name, spec)

    return result


def resolve_root_business_date(
    *,
    period_type: str = "daily",
    timezone: str = "Asia/Seoul",
    source: str = "data_interval_end",
    offset_days: int = -1,
    date_offsets: dict[str, Any] | None = None,
) -> dict[str, str]:
    context = get_current_context()
    dag_run = context["dag_run"]
    params = context.get("params") or {}
    period_type = _normalize_period_type(period_type)

    conf = getattr(dag_run, "conf", None) or {}
    explicit = _normalize_business_date(conf.get("business_date"))
    if explicit:
        return _build_business_context(explicit, period_type, date_offsets)

    run_type = _dag_run_type(dag_run)
    if run_type == "scheduled":
        if source != "data_interval_end":
            raise ValueError(
                f"Unsupported business_date.source: {source!r}. Only 'data_interval_end' is currently supported."
            )
        interval_end = context.get("data_interval_end")
        if interval_end is None:
            raise ValueError("Scheduled run has no data_interval_end; cannot calculate business_date.")
        business_date = (
            pendulum.instance(interval_end)
            .in_timezone(timezone)
            .add(days=int(offset_days))
            .date()
            .isoformat()
        )
        return _build_business_context(business_date, period_type, date_offsets)

    manual = _normalize_business_date(params.get("business_date"))
    if manual:
        return _build_business_context(manual, period_type, date_offsets)

    raise ValueError(
        "business_date is required for a non-scheduled Root run. "
        "Enter Business Date in the Airflow Trigger DAG screen using YYYY-MM-DD."
    )


def resolve_vine_business_date(
    *,
    period_type: str = "daily",
    date_offsets: dict[str, Any] | None = None,
) -> dict[str, str]:
    context = get_current_context()
    dag_run = context["dag_run"]
    params = context.get("params") or {}

    conf = getattr(dag_run, "conf", None) or {}
    inherited = _normalize_business_date(conf.get("business_date"))
    if inherited:
        inherited_type = _normalize_period_type(conf.get("period_type") or period_type)
        return _build_business_context(inherited, inherited_type, date_offsets)

    manual = _normalize_business_date(params.get("business_date"))
    if manual:
        return _build_business_context(manual, _normalize_period_type(period_type), date_offsets)

    raise ValueError(
        "business_date is required for Vine execution. "
        "Run the Vine from a Root DAG or enter Business Date in the Airflow Trigger DAG screen using YYYY-MM-DD."
    )
