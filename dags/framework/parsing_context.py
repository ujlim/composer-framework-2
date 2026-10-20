"""Public Airflow parsing-context API compatibility (never infer from task env vars)."""
from __future__ import annotations

import logging

LOGGER = logging.getLogger(__name__)


def get_target_dag_id() -> str | None:
    try:
        from airflow.sdk import get_parsing_context  # Airflow 3
    except ImportError:
        try:
            from airflow.utils.dag_parsing_context import get_parsing_context  # Airflow 2.4+
        except ImportError:
            LOGGER.warning("Parsing context API unavailable; using full DAG parsing")
            return None
    # An available API that fails must not silently select a different execution mode.
    return get_parsing_context().dag_id
