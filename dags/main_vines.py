"""Airflow discovery entrypoint for unscheduled Vine DAGs."""

from pathlib import Path

# Keep a real airflow import and DAG token for Airflow discovery safe mode.
from airflow.models.dag import DAG

from framework.dag_entrypoint import load_dags
from framework.vine_factory import VineFactory


globals().update(load_dags(Path(__file__), __name__, "vine", DAG, VineFactory))
