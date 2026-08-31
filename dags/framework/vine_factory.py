from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from framework.bigquery_executor import BigQueryExecutor
from framework.business_date import resolve_vine_business_date
from framework.fin_file_sensor_executor import FinFileSensorExecutor
from framework.gcs_executor import GCSExecutor
from framework.logger import dag_failure_callback, dag_success_callback, task_failure_callback, task_success_callback
from framework.models import LoadedConfig
from framework.postgres_executor import PostgresExecutor
from framework.sftp_runner_executor import SftpRunnerExecutor
from framework.utils import require_list, require_mapping, validate_airflow_id, validate_dependency_graph


class VineFactory:
    """Creates an unscheduled Vine DAG containing executable Grape tasks."""

    def __init__(self, config: LoadedConfig):
        self.config = config
        self.data = config.data

    def create(self) -> DAG:
        vine = require_mapping(self.data.get("vine"), "vine")
        vine_id = validate_airflow_id(vine.get("vine_id"), "vine.vine_id")
        if vine.get("schedule") not in (None, "", False):
            raise ValueError(f"Vine '{vine_id}' must not define a schedule")

        runtime = require_mapping(self.data.get("runtime", {}), "runtime")
        defaults = require_mapping(self.data.get("grape_defaults", {}), "grape_defaults")
        business_date_policy = require_mapping(self.data.get("business_date", {}), "business_date")
        date_offsets = require_mapping(business_date_policy.get("date_offsets", {}), "business_date.date_offsets")
        grapes = require_list(self.data.get("grapes"), "grapes")
        if not grapes:
            raise ValueError(f"Vine '{vine_id}' requires at least one grape")

        grape_ids: list[str] = []
        dependencies: dict[str, list[str]] = {}
        for grape in grapes:
            require_mapping(grape, f"{vine_id}.grape")
            grape_id = validate_airflow_id(grape.get("grape_id"), "grape.grape_id")
            grape_ids.append(grape_id)
            parents = grape.get("depends_on", [])
            if not isinstance(parents, list) or not all(isinstance(x, str) for x in parents):
                raise ValueError(f"{grape_id}.depends_on must be a list of grape IDs")
            dependencies[grape_id] = parents

        validate_dependency_graph(grape_ids, dependencies, f"Vine '{vine_id}'")

        timezone = vine.get("timezone", "Asia/Seoul")
        start_date = pendulum.parse(str(vine.get("start_date", "2026-01-01")), tz=timezone)
        dag = DAG(
            dag_id=vine_id,
            description=vine.get("description"),
            schedule=None,
            start_date=start_date,
            catchup=False,
            max_active_runs=int(vine.get("max_active_runs", 1)),
            dagrun_timeout=timedelta(seconds=int(vine.get("dagrun_timeout_seconds", 21600))),
            tags=list(dict.fromkeys(["vine", *vine.get("tags", [])])),
            default_args={"owner": vine.get("owner", "data-engineering")},
            params={
                "business_date": Param(
                    default=None,
                    type=["null", "string"],
                    format="date",
                    title="Business Date",
                    description="Vine 직접 Manual 실행 시 필수 입력. Root에서 호출되면 Root의 business context를 상속합니다.",
                )
            },
            on_success_callback=dag_success_callback,
            on_failure_callback=dag_failure_callback,
        )

        start = EmptyOperator(task_id="vine_start", dag=dag)
        end = EmptyOperator(task_id="vine_end", dag=dag)
        resolve_business_date = PythonOperator(
            task_id="resolve_business_date",
            python_callable=resolve_vine_business_date,
            op_kwargs={
                "period_type": business_date_policy.get("type", "daily"),
                "date_offsets": date_offsets,
            },
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback,
            dag=dag,
        )
        start >> resolve_business_date

        task_map: dict[str, Any] = {}
        for grape in grapes:
            grape_type = grape.get("type")
            if grape_type in {"bigquery_sql", "bigquery_procedure"}:
                task = BigQueryExecutor.create_task(
                    dag=dag,
                    vine_config=self.config,
                    grape=grape,
                    runtime=runtime,
                    defaults=defaults,
                )
            elif grape_type in {"gcs_copy_object", "gcs_delete_object", "gcs_move_prefix"}:
                task = GCSExecutor.create_task(dag=dag, grape=grape, runtime=runtime, defaults=defaults)
            elif grape_type == "postgres_sql":
                task = PostgresExecutor.create_task(
                    dag=dag,
                    vine_config=self.config,
                    grape=grape,
                    runtime=runtime,
                    defaults=defaults,
                )
            elif grape_type == "gcs_to_sftp_runner":
                task = SftpRunnerExecutor.create_task(dag=dag, grape=grape, defaults=defaults)
            elif grape_type == "gcs_fin_file_sensor":
                task = FinFileSensorExecutor.create_task(dag=dag, grape=grape, runtime=runtime, defaults=defaults)
            else:
                raise ValueError(
                    f"Vine '{vine_id}' grape '{grape.get('grape_id')}' has unsupported type '{grape_type}'"
                )
            task_map[grape["grape_id"]] = task

        for grape_id, parents in dependencies.items():
            if parents:
                for parent in parents:
                    task_map[parent] >> task_map[grape_id]
            else:
                resolve_business_date >> task_map[grape_id]

        children = {parent for parents in dependencies.values() for parent in parents}
        leaf_ids = [grape_id for grape_id in grape_ids if grape_id not in children]
        for grape_id in leaf_ids:
            task_map[grape_id] >> end

        return dag
