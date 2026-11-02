# Author: 임의진 (LIM UI JIN)
# Created: 2026-11-02
# Updated: 2026-11-02
"""Version-compatible, fail-open access to Airflow's public parsing context."""
from __future__ import annotations

import logging

LOGGER = logging.getLogger(__name__)


def get_target_dag_id() -> str | None:
    """Return a requested DAG ID, or None for complete scheduler discovery.

    Airflow 3 exposes this via SDK; Airflow 2.4+ uses dag_parsing_context.
    Do not read Airflow's private environment variables or cache the result.
    An unavailable/broken context must never accidentally hide scheduler DAGs.
    """
    try:
        try:
            from airflow.sdk import get_parsing_context
        except ImportError:
            from airflow.utils.dag_parsing_context import get_parsing_context
        dag_id = get_parsing_context().dag_id
        if dag_id is not None and (not isinstance(dag_id, str) or not dag_id.strip()):
            raise ValueError("Invalid parsing context DAG ID")
        return dag_id
    except Exception:
        LOGGER.warning(
            "Airflow parsing context unavailable; using full DAG discovery",
            exc_info=True,
        )
        return None
