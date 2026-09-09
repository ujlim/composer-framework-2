# Author: LIM UI JIN
# Created: 2026-09-09

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.standard.operators.python import PythonOperator

from framework.logger import task_failure_callback, task_success_callback
from framework.utils import merge_dicts, validate_airflow_id


def _normalize_prefix(value: str) -> str:
    return str(value or "").lstrip("/")


def _require_gcs_name(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def copy_gcs_object(
    *,
    source_bucket: str,
    source_object: str,
    destination_bucket: str,
    destination_object: str,
    overwrite: bool = False,
    gcp_conn_id: str = "google_cloud_default",
    impersonation_chain: str | list[str] | None = None,
) -> dict[str, Any]:
    """Copy one GCS object and verify size/CRC32C after the copy."""
    source_bucket = _require_gcs_name(source_bucket, "source_bucket")
    source_object = _require_gcs_name(source_object, "source_object").lstrip("/")
    destination_bucket = _require_gcs_name(destination_bucket, "destination_bucket")
    destination_object = _require_gcs_name(destination_object, "destination_object").lstrip("/")

    hook = GCSHook(
        gcp_conn_id=gcp_conn_id,
        impersonation_chain=impersonation_chain,
    )
    client = hook.get_conn()
    source_bucket_obj = client.bucket(source_bucket)
    destination_bucket_obj = client.bucket(destination_bucket)

    source_blob = source_bucket_obj.get_blob(source_object)
    if source_blob is None:
        raise FileNotFoundError(f"Source object does not exist: gs://{source_bucket}/{source_object}")

    destination_blob = destination_bucket_obj.blob(destination_object)
    if destination_blob.exists() and not overwrite:
        raise FileExistsError(
            f"Destination already exists: gs://{destination_bucket}/{destination_object}. "
            "Set overwrite=true only when replacement is intended."
        )

    hook.copy(
        source_bucket=source_bucket,
        source_object=source_object,
        destination_bucket=destination_bucket,
        destination_object=destination_object,
    )

    destination_blob = destination_bucket_obj.get_blob(destination_object)
    if destination_blob is None:
        raise RuntimeError(
            f"Copy verification failed: gs://{destination_bucket}/{destination_object} was not found"
        )
    if source_blob.size != destination_blob.size or source_blob.crc32c != destination_blob.crc32c:
        raise RuntimeError(
            "Copy verification mismatch: "
            f"gs://{source_bucket}/{source_object} -> "
            f"gs://{destination_bucket}/{destination_object}"
        )

    return {
        "source": f"gs://{source_bucket}/{source_object}",
        "destination": f"gs://{destination_bucket}/{destination_object}",
        "size": destination_blob.size,
        "crc32c": destination_blob.crc32c,
    }


def delete_gcs_object(
    *,
    bucket: str,
    object_name: str,
    ignore_if_missing: bool = False,
    gcp_conn_id: str = "google_cloud_default",
    impersonation_chain: str | list[str] | None = None,
) -> dict[str, Any]:
    """Delete exactly one GCS object."""
    bucket = _require_gcs_name(bucket, "bucket")
    object_name = _require_gcs_name(object_name, "object_name").lstrip("/")

    hook = GCSHook(
        gcp_conn_id=gcp_conn_id,
        impersonation_chain=impersonation_chain,
    )
    client = hook.get_conn()
    blob = client.bucket(bucket).get_blob(object_name)

    if blob is None:
        if ignore_if_missing:
            return {
                "object": f"gs://{bucket}/{object_name}",
                "deleted": False,
                "reason": "not_found",
            }
        raise FileNotFoundError(f"GCS object does not exist: gs://{bucket}/{object_name}")

    hook.delete(bucket_name=bucket, object_name=object_name)
    return {
        "object": f"gs://{bucket}/{object_name}",
        "deleted": True,
    }


def move_gcs_prefix(
    *,
    source_bucket: str,
    source_prefix: str,
    destination_bucket: str,
    destination_prefix: str = "",
    delete_source: bool = True,
    allow_empty: bool = False,
    overwrite: bool = False,
    preserve_relative_path: bool = True,
    gcp_conn_id: str = "google_cloud_default",
    impersonation_chain: str | list[str] | None = None,
) -> dict[str, Any]:
    """Copy all objects under a GCS prefix and delete source objects only after verification."""
    source_prefix = _normalize_prefix(source_prefix)
    destination_prefix = _normalize_prefix(destination_prefix)
    if not source_bucket or not destination_bucket:
        raise ValueError("source_bucket and destination_bucket are required")
    if not source_prefix:
        raise ValueError("source_prefix must not be empty; refusing a whole-bucket move")

    hook = GCSHook(
        gcp_conn_id=gcp_conn_id,
        impersonation_chain=impersonation_chain,
    )
    objects = [name for name in hook.list(bucket_name=source_bucket, prefix=source_prefix) if not name.endswith("/")]
    if not objects:
        if allow_empty:
            return {"source_count": 0, "copied_count": 0, "deleted_count": 0}
        raise FileNotFoundError(f"No GCS objects found under gs://{source_bucket}/{source_prefix}")

    client = hook.get_conn()
    source_bucket_obj = client.bucket(source_bucket)
    destination_bucket_obj = client.bucket(destination_bucket)
    copied: list[tuple[str, str]] = []

    for source_object in objects:
        relative = source_object[len(source_prefix):].lstrip("/") if preserve_relative_path else source_object.rsplit("/", 1)[-1]
        if not relative:
            continue
        destination_object = f"{destination_prefix.rstrip('/')}/{relative}" if destination_prefix else relative

        destination_blob = destination_bucket_obj.blob(destination_object)
        if destination_blob.exists() and not overwrite:
            raise FileExistsError(
                f"Destination already exists: gs://{destination_bucket}/{destination_object}. "
                "Set overwrite=true only when replacement is intended."
            )

        hook.copy(
            source_bucket=source_bucket,
            source_object=source_object,
            destination_bucket=destination_bucket,
            destination_object=destination_object,
        )

        source_blob = source_bucket_obj.get_blob(source_object)
        destination_blob = destination_bucket_obj.get_blob(destination_object)
        if source_blob is None or destination_blob is None:
            raise RuntimeError(f"Copy verification failed for {source_object}")
        if source_blob.size != destination_blob.size or source_blob.crc32c != destination_blob.crc32c:
            raise RuntimeError(
                "Copy verification mismatch: "
                f"gs://{source_bucket}/{source_object} -> gs://{destination_bucket}/{destination_object}"
            )
        copied.append((source_object, destination_object))

    deleted_count = 0
    if delete_source:
        for source_object, _ in copied:
            hook.delete(bucket_name=source_bucket, object_name=source_object)
            deleted_count += 1

    return {
        "source_count": len(objects),
        "copied_count": len(copied),
        "deleted_count": deleted_count,
        "source_prefix": f"gs://{source_bucket}/{source_prefix}",
        "destination_prefix": f"gs://{destination_bucket}/{destination_prefix}",
    }


class GCSExecutor:
    """Build GCS object/prefix Grapes for Vine DAGs."""

    @classmethod
    def create_task(
        cls,
        *,
        dag,
        grape: dict[str, Any],
        runtime: dict[str, Any],
        defaults: dict[str, Any],
    ) -> PythonOperator:
        grape_id = validate_airflow_id(grape.get("grape_id"), "grape.grape_id")
        grape_type = grape.get("type")
        if grape_type not in {"gcs_copy_object", "gcs_delete_object", "gcs_move_prefix"}:
            raise ValueError(f"Unsupported GCS grape type: {grape_type}")

        options = merge_dicts(defaults, grape.get("options", {}))
        source = grape.get("source") or {}
        if not isinstance(source, dict):
            raise ValueError(f"{grape_id}: source must be a mapping")

        common = {
            "gcp_conn_id": runtime.get("gcp_conn_id", "google_cloud_default"),
            "impersonation_chain": runtime.get("impersonation_chain"),
        }

        if grape_type == "gcs_copy_object":
            destination = grape.get("destination") or {}
            if not isinstance(destination, dict):
                raise ValueError(f"{grape_id}: destination must be a mapping")
            python_callable = copy_gcs_object
            op_kwargs = {
                "source_bucket": source.get("bucket"),
                "source_object": source.get("object"),
                "destination_bucket": destination.get("bucket"),
                "destination_object": destination.get("object"),
                "overwrite": bool(options.get("overwrite", False)),
                **common,
            }
        elif grape_type == "gcs_delete_object":
            python_callable = delete_gcs_object
            op_kwargs = {
                "bucket": source.get("bucket"),
                "object_name": source.get("object"),
                "ignore_if_missing": bool(options.get("ignore_if_missing", False)),
                **common,
            }
        else:
            destination = grape.get("destination") or {}
            if not isinstance(destination, dict):
                raise ValueError(f"{grape_id}: destination must be a mapping")
            python_callable = move_gcs_prefix
            op_kwargs = {
                "source_bucket": source.get("bucket"),
                "source_prefix": source.get("prefix"),
                "destination_bucket": destination.get("bucket"),
                "destination_prefix": destination.get("prefix", ""),
                "delete_source": bool(options.get("delete_source", True)),
                "allow_empty": bool(options.get("allow_empty", False)),
                "overwrite": bool(options.get("overwrite", False)),
                "preserve_relative_path": bool(options.get("preserve_relative_path", True)),
                **common,
            }

        return PythonOperator(
            task_id=grape_id,
            python_callable=python_callable,
            op_kwargs=op_kwargs,
            retries=int(options.get("retries", 1)),
            retry_delay=timedelta(seconds=int(options.get("retry_delay_seconds", 300))),
            execution_timeout=timedelta(seconds=int(options.get("execution_timeout_seconds", 7200))),
            pool=options.get("pool"),
            priority_weight=int(options.get("priority_weight", 1)),
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback,
            dag=dag,
        )
