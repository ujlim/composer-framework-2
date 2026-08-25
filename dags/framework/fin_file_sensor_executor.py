from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from framework.logger import task_failure_callback, task_success_callback
from framework.utils import merge_dicts, validate_airflow_id


class FinFileSensorExecutor:
    """Build a GCS FIN-file existence sensor task from a Grape definition."""

    @classmethod
    def create_task(
        cls,
        *,
        dag,
        grape: dict[str, Any],
        runtime: dict[str, Any],
        defaults: dict[str, Any],
    ) -> GCSObjectExistenceSensor:
        grape_id = validate_airflow_id(grape.get("grape_id"), "grape.grape_id")
        if grape.get("type") != "gcs_fin_file_sensor":
            raise ValueError(f"Unsupported FIN-file sensor grape type: {grape.get('type')}")

        source = grape.get("source") or {}
        if not isinstance(source, dict):
            raise ValueError(f"{grape_id}: source must be a mapping")

        bucket = source.get("bucket")
        object_name = source.get("object")
        if not isinstance(bucket, str) or not bucket.strip():
            raise ValueError(f"{grape_id}: source.bucket is required")
        if not isinstance(object_name, str) or not object_name.strip():
            raise ValueError(f"{grape_id}: source.object is required")

        options = merge_dicts(defaults, grape.get("options", {}))
        timeout_seconds = int(options.get("timeout_seconds", options.get("execution_timeout_seconds", 21600)))

        return GCSObjectExistenceSensor(
            task_id=grape_id,
            bucket=bucket,
            object=object_name,
            google_cloud_conn_id=runtime.get("gcp_conn_id", "google_cloud_default"),
            impersonation_chain=runtime.get("impersonation_chain"),
            poke_interval=int(options.get("poke_interval_seconds", 60)),
            timeout=timeout_seconds,
            mode=str(options.get("mode", "reschedule")),
            deferrable=bool(options.get("deferrable", True)),
            soft_fail=bool(options.get("soft_fail", False)),
            retries=int(options.get("retries", 0)),
            retry_delay=timedelta(seconds=int(options.get("retry_delay_seconds", 60))),
            execution_timeout=timedelta(seconds=timeout_seconds),
            pool=options.get("pool"),
            priority_weight=int(options.get("priority_weight", 1)),
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback,
            dag=dag,
        )
