# Author: LIM UI JIN
# Created: 2026-11-25
"""Airflow entrypoint for Vine shard 00."""

from airflow.models.dag import DAG  # Airflow safe-mode discovery.

from framework.dag_entrypoints import load_vines

globals().update(load_vines(__file__, 0))
