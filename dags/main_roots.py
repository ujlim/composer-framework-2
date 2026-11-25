# Author: LIM UI JIN
# Created: 2026-11-25
"""Airflow entrypoint for Root DAGs."""

from airflow.models.dag import DAG  # Airflow safe-mode discovery.

from framework.dag_entrypoints import load_roots

globals().update(load_roots(__file__))
