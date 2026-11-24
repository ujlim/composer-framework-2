"""Airflow discovery entrypoint for Root DAGs (including scheduled Roots)."""

from pathlib import Path

# Keep a real airflow import and DAG token for Airflow discovery safe mode.
from airflow.models.dag import DAG

from framework.dag_entrypoint import load_dags
from framework.root_factory import RootFactory


globals().update(load_dags(Path(__file__), __name__, "root", DAG, RootFactory))
