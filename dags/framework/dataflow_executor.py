# Author: LIM UI JIN
# Created: 2026-09-30

from __future__ import annotations

import json
import logging
import re
import time
from datetime import timedelta
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from framework.logger import task_failure_callback, task_success_callback
from framework.models import LoadedConfig
from framework.utils import merge_dicts, read_text, resolve_child_file, validate_airflow_id

LOGGER = logging.getLogger(__name__)

_DRIVER_CLASS = {
    "db2": "com.ibm.db2.jcc.DB2Driver",
    "oracle": "oracle.jdbc.OracleDriver",
    "vertica": "com.vertica.jdbc.Driver",
}
_TERMINAL_STATES = {"JOB_STATE_DONE", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_DRAINED", "JOB_STATE_UPDATED"}
_SUCCESS_STATES = {"JOB_STATE_DONE", "JOB_STATE_UPDATED"}


def _jdbc_url(db_type: str, conn) -> str:
    extra = conn.extra_dejson or {}
    if extra.get("jdbc_url"):
        return str(extra["jdbc_url"])
    if not conn.host or not conn.schema:
        raise ValueError("Airflow Connection requires host/schema or extra.jdbc_url")
    if db_type == "db2":
        return f"jdbc:db2://{conn.host}:{conn.port or 50000}/{conn.schema}"
    if db_type == "oracle":
        return f"jdbc:oracle:thin:@//{conn.host}:{conn.port or 1521}/{conn.schema}"
    if db_type == "vertica":
        return f"jdbc:vertica://{conn.host}:{conn.port or 5433}/{conn.schema}"
    raise ValueError(f"Unsupported db_type: {db_type}")


def _safe_job_name(value: str) -> str:
    value = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
    return re.sub(r"-+", "-", value)[:55].rstrip("-") or "jdbc-export"


class DataflowJdbcOperator(BaseOperator):
    """Launch and wait for the UBIS JDBC export Flex Template."""

    template_fields = ("sql", "destination", "job_name_prefix")

    def __init__(self, *, sql, db_type, connection_id, driver_jars, password_secret,
                 destination_type, destination, project_id, region, template_gcs_path,
                 system_bucket, subnetwork, service_account_email, staging_location=None,
                 temp_location=None, gcp_conn_id="google_cloud_default", impersonation_chain=None,
                 poll_interval_seconds=30, job_name_prefix="jdbc-export", **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.db_type = db_type
        self.connection_id = connection_id
        self.driver_jars = driver_jars
        self.password_secret = password_secret
        self.destination_type = destination_type
        self.destination = destination
        self.project_id = project_id
        self.region = region
        self.template_gcs_path = template_gcs_path
        self.system_bucket = system_bucket.replace("gs://", "").strip("/")
        self.subnetwork = subnetwork
        self.service_account_email = service_account_email
        self.staging_location = staging_location or f"gs://{self.system_bucket}/staging"
        self.temp_location = temp_location or f"gs://{self.system_bucket}/temp"
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.poll_interval_seconds = poll_interval_seconds
        self.job_name_prefix = job_name_prefix

    def execute(self, context):
        conn = BaseHook.get_connection(self.connection_id)
        extra = conn.extra_dejson or {}
        password_secret = self.password_secret or extra.get("password_secret")
        if not password_secret:
            raise ValueError(
                f"Airflow Connection '{self.connection_id}' must define extra.password_secret "
                "or source.password_secret; DB passwords are not passed as Dataflow parameters."
            )
        if not conn.login:
            raise ValueError(f"Airflow Connection '{self.connection_id}' requires login")

        run_id = context["run_id"]
        dag_id = context["dag"].dag_id
        sql_object = f"sql/{dag_id}/{run_id}/{self.task_id}.sql"
        GCSHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain).upload(
            bucket_name=self.system_bucket,
            object_name=sql_object,
            data=self.sql.encode("utf-8"),
            mime_type="text/plain",
        )
        query_uri = f"gs://{self.system_bucket}/{sql_object}"

        parameters = {
            "db_type": self.db_type,
            "connection_url": _jdbc_url(self.db_type, conn),
            "username": conn.login,
            "password_secret": str(password_secret),
            "driver_jars": self.driver_jars,
            "query_uri": query_uri,
            "destination_type": self.destination_type,
        }
        if self.destination_type == "gcs":
            parameters.update({
                "output_path": str(self.destination["path"]),
                "output_format": str(self.destination.get("format", "json")),
            })
        else:
            parameters.update({
                "output_table": str(self.destination["table"]),
                "bq_schema": str(self.destination["schema"]),
                "write_disposition": str(self.destination.get("write_disposition", "WRITE_APPEND")),
                "create_disposition": str(self.destination.get("create_disposition", "CREATE_IF_NEEDED")),
            })

        from googleapiclient.discovery import build

        service = build("dataflow", "v1b3", cache_discovery=False)
        suffix = context["ts_nodash"].lower().replace("t", "-")
        job_name = _safe_job_name(f"{self.job_name_prefix}-{self.db_type}-{suffix}")
        body = {
            "launchParameter": {
                "jobName": job_name,
                "containerSpecGcsPath": self.template_gcs_path,
                "parameters": parameters,
                "environment": {
                    "serviceAccountEmail": self.service_account_email,
                    "subnetwork": self.subnetwork,
                    "ipConfiguration": "WORKER_IP_PRIVATE",
                    "stagingLocation": self.staging_location,
                    "tempLocation": self.temp_location,
                },
            }
        }
        LOGGER.info("Launching Dataflow job=%s query_uri=%s", job_name, query_uri)
        response = service.projects().locations().flexTemplates().launch(
            projectId=self.project_id, location=self.region, body=body
        ).execute()
        job = response.get("job") or {}
        job_id = job.get("id")
        if not job_id:
            raise RuntimeError(f"Dataflow launch did not return job id: {response}")

        while True:
            current = service.projects().locations().jobs().get(
                projectId=self.project_id, location=self.region, jobId=job_id
            ).execute()
            state = current.get("currentState")
            LOGGER.info("Dataflow job=%s id=%s state=%s", job_name, job_id, state)
            if state in _TERMINAL_STATES:
                if state not in _SUCCESS_STATES:
                    raise RuntimeError(f"Dataflow job {job_id} ended in {state}")
                return {"job_id": job_id, "job_name": job_name, "state": state, "query_uri": query_uri}
            time.sleep(self.poll_interval_seconds)


class DataflowExecutor:
    """Build JDBC-to-GCS and JDBC-to-BigQuery Dataflow Grapes."""

    @classmethod
    def create_task(cls, *, dag, vine_config: LoadedConfig, grape: dict[str, Any], runtime: dict[str, Any], defaults: dict[str, Any]):
        grape_id = validate_airflow_id(grape.get("grape_id"), "grape.grape_id")
        grape_type = grape.get("type")
        if grape_type not in {"jdbc_to_gcs", "jdbc_to_bigquery"}:
            raise ValueError(f"Unsupported Dataflow grape type: {grape_type}")

        source = grape.get("source") or {}
        db_type = str(source.get("db_type", "")).lower()
        if db_type not in _DRIVER_CLASS:
            raise ValueError(f"{grape_id}: source.db_type must be db2, oracle, or vertica")
        connection_id = source.get("connection_id")
        driver_jars = source.get("driver_jars")
        if not connection_id or not driver_jars:
            raise ValueError(f"{grape_id}: source.connection_id and source.driver_jars are required")

        sql_file = grape.get("sql_file")
        if not sql_file:
            raise ValueError(f"{grape_id}: sql_file is required")
        sql = read_text(resolve_child_file(vine_config.base_dir, sql_file))

        df = runtime.get("dataflow") or {}
        required = ["project_id", "region", "template_gcs_path", "system_bucket", "subnetwork", "service_account_email"]
        missing = [key for key in required if not df.get(key)]
        if missing:
            raise ValueError(f"{grape_id}: runtime.dataflow missing: {', '.join(missing)}")

        destination = grape.get("destination") or {}
        if grape_type == "jdbc_to_gcs":
            if not destination.get("path"):
                raise ValueError(f"{grape_id}: destination.path is required")
            destination_type = "gcs"
        else:
            if not destination.get("table"):
                raise ValueError(f"{grape_id}: destination.table is required")
            schema_file = destination.get("schema_file")
            if not schema_file:
                raise ValueError(f"{grape_id}: destination.schema_file is required")
            schema = json.loads(read_text(resolve_child_file(vine_config.base_dir, schema_file)))
            destination = {**destination, "schema": json.dumps(schema, separators=(",", ":"))}
            destination_type = "bigquery"

        options = merge_dicts(defaults, grape.get("options", {}))
        return DataflowJdbcOperator(
            task_id=grape_id, dag=dag, sql=sql, db_type=db_type, connection_id=str(connection_id),
            driver_jars=str(driver_jars), password_secret=source.get("password_secret"),
            destination_type=destination_type, destination=destination,
            project_id=str(df["project_id"]), region=str(df["region"]),
            template_gcs_path=str(df["template_gcs_path"]), system_bucket=str(df["system_bucket"]),
            subnetwork=str(df["subnetwork"]), service_account_email=str(df["service_account_email"]),
            staging_location=df.get("staging_location"), temp_location=df.get("temp_location"),
            gcp_conn_id=runtime.get("gcp_conn_id", "google_cloud_default"),
            impersonation_chain=runtime.get("impersonation_chain"),
            poll_interval_seconds=int(options.get("poll_interval_seconds", 30)),
            job_name_prefix=str(options.get("job_name_prefix", grape_id)),
            retries=int(options.get("retries", 1)),
            retry_delay=timedelta(seconds=int(options.get("retry_delay_seconds", 300))),
            execution_timeout=timedelta(seconds=int(options.get("execution_timeout_seconds", 21600))),
            pool=options.get("pool"), priority_weight=int(options.get("priority_weight", 1)),
            on_success_callback=task_success_callback, on_failure_callback=task_failure_callback,
        )
