# Author: LIM UI JIN
# Created: 2026-09-09

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from framework.business_date import resolve_root_business_date
from framework.logger import dag_failure_callback, dag_success_callback, task_failure_callback, task_success_callback
from framework.models import LoadedConfig
from framework.utils import require_list, require_mapping, validate_airflow_id, validate_dependency_graph


class RootFactory:
    """Creates a scheduled Root DAG that triggers and waits for Vine DAGs."""

    def __init__(self, config: LoadedConfig, available_vine_ids: set[str]):
        self.config = config
        self.data = config.data
        self.available_vine_ids = available_vine_ids

    def create(self) -> DAG:
        root = require_mapping(self.data.get("root"), "root")
        root_id = validate_airflow_id(root.get("root_id"), "root.root_id")
        vines = require_list(self.data.get("vines"), "vines")
        if not vines:
            raise ValueError(f"Root '{root_id}' requires at least one vine")

        defaults = require_mapping(self.data.get("defaults", {}), "defaults")
        parameters = require_mapping(self.data.get("parameters", {}), "parameters")
        business_date_policy = require_mapping(self.data.get("business_date", {}), "business_date")
        date_offsets = require_mapping(business_date_policy.get("date_offsets", {}), "business_date.date_offsets")

        vine_ids: list[str] = []
        dependencies: dict[str, list[str]] = {}
        for item in vines:
            require_mapping(item, f"{root_id}.vine")
            vine_id = validate_airflow_id(item.get("vine_id"), "vine.vine_id")
            if vine_id not in self.available_vine_ids:
                raise ValueError(f"Root '{root_id}' references unregistered Vine '{vine_id}'")
            vine_ids.append(vine_id)
            parents = item.get("depends_on", [])
            if not isinstance(parents, list) or not all(isinstance(x, str) for x in parents):
                raise ValueError(f"{vine_id}.depends_on must be a list")
            dependencies[vine_id] = parents

        validate_dependency_graph(vine_ids, dependencies, f"Root '{root_id}'")

        timezone = root.get("timezone", "Asia/Seoul")
        start_date = pendulum.parse(str(root.get("start_date", "2026-01-01")), tz=timezone)
        schedule = root.get("schedule")
        if schedule in ("", False):
            schedule = None

        dag = DAG(
            dag_id=root_id,
            description=root.get("description"),
            schedule=schedule,
            start_date=start_date,
            catchup=bool(root.get("catchup", False)),
            max_active_runs=int(root.get("max_active_runs", 1)),
            dagrun_timeout=timedelta(seconds=int(root.get("dagrun_timeout_seconds", 43200))),
            tags=list(dict.fromkeys(["root", *root.get("tags", [])])),
            default_args={"owner": root.get("owner", "batch-integration")},
            params={
                "business_date": Param(
                    default=None,
                    type=["null", "string"],
                    format="date",
                    title="Business Date",
                    description="Manual 실행 시 필수 입력. Daily/Monthly/Yearly 업무 기준일 (YYYY-MM-DD). Scheduled 실행 시 Root 정책으로 자동 계산됩니다.",
                )
            },
            on_success_callback=dag_success_callback,
            on_failure_callback=dag_failure_callback,
        )

        start = EmptyOperator(task_id="root_start", dag=dag)
        end = EmptyOperator(task_id="root_end", dag=dag)
        resolve_business_date = PythonOperator(
            task_id="resolve_business_date",
            python_callable=resolve_root_business_date,
            op_kwargs={
                "period_type": business_date_policy.get("type", "daily"),
                "timezone": business_date_policy.get("timezone", timezone),
                "source": business_date_policy.get("source", "data_interval_end"),
                "offset_days": int(business_date_policy.get("offset_days", -1)),
                "date_offsets": date_offsets,
            },
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback,
            dag=dag,
        )
        start >> resolve_business_date

        conf = {
            name: (spec.get("value") if isinstance(spec, dict) else spec)
            for name, spec in parameters.items()
        }
        context_task = "ti.xcom_pull(task_ids='resolve_business_date')"
        conf.update(
            {
                "business_date": "{{ " + context_task + "['business_date'] }}",
                "period_type": "{{ " + context_task + "['period_type'] }}",
                "period_start": "{{ " + context_task + "['period_start'] }}",
                "period_end": "{{ " + context_task + "['period_end'] }}",
            }
        )
        for offset_name in date_offsets:
            if offset_name in conf:
                raise ValueError(f"date_offsets key '{offset_name}' conflicts with Root parameter")
            conf[offset_name] = "{{ " + context_task + "['" + offset_name + "'] }}"

        task_map = {}
        for item in vines:
            vine_id = item["vine_id"]
            task_id = validate_airflow_id(item.get("trigger_id", f"trigger_{vine_id}"), f"{vine_id}.trigger_id")
            options = {**defaults, **item.get("options", {})}
            trigger = TriggerDagRunOperator(
                task_id=task_id,
                trigger_dag_id=vine_id,
                trigger_run_id=(f"root__{root_id}__{{{{ dag_run.run_id }}}}__{vine_id}"),
                conf=conf,
                wait_for_completion=bool(options.get("wait_for_completion", True)),
                poke_interval=int(options.get("poke_interval_seconds", 30)),
                reset_dag_run=bool(options.get("reset_dag_run", False)),
                deferrable=bool(options.get("deferrable", True)),
                execution_timeout=timedelta(seconds=int(options.get("execution_timeout_seconds", 21600))),
                on_success_callback=task_success_callback,
                on_failure_callback=task_failure_callback,
                dag=dag,
            )
            task_map[vine_id] = trigger

        for vine_id, parents in dependencies.items():
            if parents:
                for parent in parents:
                    task_map[parent] >> task_map[vine_id]
            else:
                resolve_business_date >> task_map[vine_id]

        children = {parent for parents in dependencies.values() for parent in parents}
        leaf_ids = [vine_id for vine_id in vine_ids if vine_id not in children]
        for vine_id in leaf_ids:
            task_map[vine_id] >> end

        return dag
