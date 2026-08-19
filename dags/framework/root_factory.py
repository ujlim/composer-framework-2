from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

from framework.logger import (
    dag_failure_callback,
    dag_success_callback,
    task_failure_callback,
    task_success_callback,
)
from framework.models import LoadedConfig
from framework.utils import (
    require_list,
    require_mapping,
    validate_airflow_id,
    validate_dependency_graph,
)


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

        vine_ids: list[str] = []
        dependencies: dict[str, list[str]] = {}
        for item in vines:
            require_mapping(item, f"{root_id}.vine")
            vine_id = validate_airflow_id(item.get("vine_id"), "vine.vine_id")
            if vine_id not in self.available_vine_ids:
                raise ValueError(
                    f"Root '{root_id}' references unregistered Vine '{vine_id}'"
                )
            vine_ids.append(vine_id)
            parents = item.get("depends_on", [])
            if not isinstance(parents, list) or not all(isinstance(x, str) for x in parents):
                raise ValueError(f"{vine_id}.depends_on must be a list")
            dependencies[vine_id] = parents

        validate_dependency_graph(vine_ids, dependencies, f"Root '{root_id}'")

        timezone = root.get("timezone", "Asia/Seoul")
        start_date = pendulum.parse(
            str(root.get("start_date", "2026-01-01")),
            tz=timezone,
        )

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
            dagrun_timeout=timedelta(
                seconds=int(root.get("dagrun_timeout_seconds", 43200))
            ),
            tags=list(dict.fromkeys(["root", *root.get("tags", [])])),
            default_args={"owner": root.get("owner", "batch-integration")},
            on_success_callback=dag_success_callback,
            on_failure_callback=dag_failure_callback,
        )

        start = EmptyOperator(task_id="root_start", dag=dag)
        end = EmptyOperator(task_id="root_end", dag=dag)

        conf = {
            name: (
                spec.get("value")
                if isinstance(spec, dict)
                else spec
            )
            for name, spec in parameters.items()
        }

        task_map = {}
        for item in vines:
            vine_id = item["vine_id"]
            task_id = validate_airflow_id(
                item.get("trigger_id", f"trigger_{vine_id}"),
                f"{vine_id}.trigger_id",
            )
            options = {**defaults, **item.get("options", {})}

            trigger = TriggerDagRunOperator(
                task_id=task_id,
                trigger_dag_id=vine_id,
                trigger_run_id=(
                    f"root__{root_id}"
                    f"__{{{{ dag_run.run_id }}}}"
                    f"__{vine_id}"
                ),
                conf=conf,
                wait_for_completion=bool(options.get("wait_for_completion", True)),
                poke_interval=int(options.get("poke_interval_seconds", 30)),
                reset_dag_run=bool(options.get("reset_dag_run", False)),
                deferrable=bool(options.get("deferrable", True)),
                execution_timeout=timedelta(
                    seconds=int(options.get("execution_timeout_seconds", 21600))
                ),
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
                start >> task_map[vine_id]

        children = {parent for parents in dependencies.values() for parent in parents}
        leaf_ids = [vine_id for vine_id in vine_ids if vine_id not in children]
        for vine_id in leaf_ids:
            task_map[vine_id] >> end

        return dag
