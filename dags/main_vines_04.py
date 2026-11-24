# Author: LIM UI JIN
# Created: 2026-11-24

"""Airflow discovery entrypoint for Vine DAG shard 04."""

from pathlib import Path

# Keep a real airflow import and DAG token for Airflow discovery safe mode.
from airflow.models.dag import DAG

from framework.dag_entrypoint import load_dags


globals().update(load_dags(Path(__file__), __name__, "vine", DAG, vine_shard=4))
