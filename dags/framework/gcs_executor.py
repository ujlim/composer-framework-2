# Author: LIM UI JIN
# Created: 2026-09-09

from __future__ import annotations

import csv
import fnmatch
import logging
import tempfile
from datetime import timedelta
from pathlib import Path
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

    hook = GCSHook(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)
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
    object_name: str | None = None,
    object_pattern: str | None = None,
    ignore_if_missing: bool = False,
    gcp_conn_id: str = "google_cloud_default",
    impersonation_chain: str | list[str] | None = None,
) -> dict[str, Any]:
    """Delete one object or multiple objects matched by a glob pattern such as *.csv."""
    bucket = _require_gcs_name(bucket, "bucket")
    object_name = str(object_name or "").lstrip("/")
    object_pattern = str(object_pattern or "").lstrip("/")

    if bool(object_name) == bool(object_pattern):
        raise ValueError("Specify exactly one of source.object or source.pattern")

    hook = GCSHook(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)

    if object_name:
        # Keep single-object delete semantics consistent with pattern delete:
        # a missing object must flow through the common ignore_if_missing handling.
        if hook.exists(bucket_name=bucket, object_name=object_name):
            candidates = [object_name]
        else:
            candidates = []
        mode = "object"
        requested = object_name
    else:
        wildcard_pos = min(
            [pos for token in ("*", "?", "[") if (pos := object_pattern.find(token)) >= 0],
            default=len(object_pattern),
        )
        static_part = object_pattern[:wildcard_pos]
        prefix = static_part.rsplit("/", 1)[0] + "/" if "/" in static_part else ""
        listed = [name for name in hook.list(bucket_name=bucket, prefix=prefix) if not name.endswith("/")]
        candidates = sorted(name for name in listed if fnmatch.fnmatchcase(name, object_pattern))
        mode = "pattern"
        requested = object_pattern

    if not candidates:
        logging.info("GCS_DELETE_MATCH_NONE bucket=%s %s=%s", bucket, mode, requested)
        if ignore_if_missing:
            logging.info(
                "GCS_DELETE_SKIPPED reason=NO_MATCH ignore_if_missing=true bucket=%s %s=%s",
                bucket, mode, requested,
            )
            return {
                "bucket": bucket,
                "requested": requested,
                "matched_count": 0,
                "deleted_count": 0,
                "skipped": True,
                "skip_reason": "NO_MATCH",
            }
        raise FileNotFoundError(f"No GCS objects matched: gs://{bucket}/{requested}")

    logging.info("GCS_DELETE_MATCH bucket=%s %s=%s matched=%d", bucket, mode, requested, len(candidates))
    for name in candidates:
        logging.info("GCS_DELETE_OBJECT_START object=gs://%s/%s", bucket, name)
        hook.delete(bucket_name=bucket, object_name=name)
        logging.info("GCS_DELETE_OBJECT_COMPLETE object=gs://%s/%s", bucket, name)

    return {
        "bucket": bucket,
        "requested": requested,
        "matched_count": len(candidates),
        "deleted_count": len(candidates),
        "deleted_objects": candidates,
    }


def merge_gcs_csv(
    *,
    bucket: str,
    source_prefix: str,
    destination_object: str,
    pattern: str = "*.csv",
    csv_header: bool = True,
    csv_encoding: str = "utf-8",
    delete_source: bool = False,
    allow_empty: bool = False,
    overwrite: bool = False,
    fin_filename: str | None = None,
    file_count_fin_filename: str | None = None,
    gcp_conn_id: str = "google_cloud_default",
    impersonation_chain: str | list[str] | None = None,
) -> dict[str, Any]:
    """Merge CSV objects in GCS into one GCS object without SFTP transfer."""
    bucket = _require_gcs_name(bucket, "bucket")
    source_prefix = _normalize_prefix(_require_gcs_name(source_prefix, "source_prefix"))
    destination_object = _require_gcs_name(destination_object, "destination_object").lstrip("/")
    pattern = _require_gcs_name(pattern, "pattern")
    csv_encoding = _require_gcs_name(csv_encoding, "csv_encoding")
    if not source_prefix:
        raise ValueError("source_prefix must not be empty; refusing a whole-bucket merge")

    hook = GCSHook(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)
    client = hook.get_conn()
    bucket_obj = client.bucket(bucket)

    objects = []
    for name in hook.list(bucket_name=bucket, prefix=source_prefix):
        if name.endswith("/") or name == destination_object:
            continue
        relative = name[len(source_prefix):].lstrip("/")
        if fnmatch.fnmatchcase(relative, pattern) or fnmatch.fnmatchcase(name, pattern):
            objects.append(name)
    objects = sorted(objects)

    if not objects:
        logging.info("GCS_MERGE_SOURCE_NONE bucket=%s prefix=%s pattern=%s", bucket, source_prefix, pattern)
        if allow_empty:
            return {"source_count": 0, "merged_rows": 0, "deleted_count": 0}
        raise FileNotFoundError(
            f"No GCS objects matched gs://{bucket}/{source_prefix} pattern={pattern}"
        )

    destination_blob = bucket_obj.blob(destination_object)
    if destination_blob.exists() and not overwrite:
        raise FileExistsError(
            f"Destination already exists: gs://{bucket}/{destination_object}. Set overwrite=true to replace it."
        )

    logging.info(
        "GCS_MERGE_START bucket=%s prefix=%s pattern=%s source_files=%d destination=gs://%s/%s delete_source=%s",
        bucket, source_prefix, pattern, len(objects), bucket, destination_object, delete_source,
    )
    for name in objects:
        logging.info("GCS_MERGE_SOURCE object=gs://%s/%s", bucket, name)

    source_total = 0
    header_written = False
    with tempfile.TemporaryDirectory(prefix="gcs-merge-") as temp_dir:
        merged_path = Path(temp_dir) / "merged.csv"
        with merged_path.open("w", encoding=csv_encoding, newline="") as out_fh:
            writer = csv.writer(out_fh, lineterminator="\n")
            for index, name in enumerate(objects, start=1):
                part_path = Path(temp_dir) / f"part-{index:06d}.csv"
                bucket_obj.blob(name).download_to_filename(str(part_path))
                row_count = 0
                with part_path.open("r", encoding=csv_encoding, newline="") as in_fh:
                    reader = csv.reader(in_fh)
                    for row_index, row in enumerate(reader):
                        if csv_header and row_index == 0:
                            if not header_written:
                                writer.writerow(row)
                                header_written = True
                            continue
                        writer.writerow(row)
                        row_count += 1
                source_total += row_count
                logging.info("GCS_MERGE_SOURCE_COMPLETE object=gs://%s/%s rows=%d", bucket, name, row_count)

        merged_rows = 0
        with merged_path.open("r", encoding=csv_encoding, newline="") as merged_fh:
            total_lines = sum(1 for _ in csv.reader(merged_fh))
            merged_rows = max(0, total_lines - (1 if csv_header and total_lines else 0))

        if source_total != merged_rows:
            logging.error("GCS_MERGE_ROW_VERIFICATION source_rows=%d merged_rows=%d result=FAIL", source_total, merged_rows)
            raise RuntimeError(
                f"GCS CSV merge row verification failed: source_rows={source_total}, merged_rows={merged_rows}"
            )
        logging.info("GCS_MERGE_ROW_VERIFICATION source_rows=%d merged_rows=%d result=PASS", source_total, merged_rows)

        destination_blob.upload_from_filename(str(merged_path))

    uploaded = bucket_obj.get_blob(destination_object)
    if uploaded is None:
        raise RuntimeError(f"Merged object was not found after upload: gs://{bucket}/{destination_object}")
    logging.info(
        "GCS_MERGE_COMPLETE destination=gs://%s/%s source_files=%d rows=%d size=%s",
        bucket, destination_object, len(objects), merged_rows, uploaded.size,
    )

    # FIN files describe the final merged data object in GCS.
    # row-count FIN = merged data rows; file-count FIN = final data object count (1).
    fin_objects: list[str] = []
    for fin_type, fin_name, fin_content in (
        ("row_count", fin_filename, merged_rows),
        ("file_count", file_count_fin_filename, 1),
    ):
        if fin_name is None:
            continue
        fin_name = _require_gcs_name(fin_name, f"{fin_type}_fin_filename").lstrip("/")
        fin_blob = bucket_obj.blob(fin_name)
        if fin_blob.exists() and not overwrite:
            raise FileExistsError(
                f"FIN object already exists: gs://{bucket}/{fin_name}. Set overwrite=true to replace it."
            )
        logging.info(
            "GCS_FIN_CREATE_START type=%s object=gs://%s/%s content=%d",
            fin_type, bucket, fin_name, fin_content,
        )
        fin_blob.upload_from_string(f"{fin_content}\n", content_type="text/plain")
        logging.info(
            "GCS_FIN_CREATE_COMPLETE type=%s object=gs://%s/%s content=%d",
            fin_type, bucket, fin_name, fin_content,
        )
        fin_objects.append(fin_name)

    deleted_count = 0
    if delete_source:
        logging.info("GCS_MERGE_SOURCE_DELETE_START files=%d", len(objects))
        for name in objects:
            logging.info("GCS_MERGE_SOURCE_DELETE object=gs://%s/%s", bucket, name)
            hook.delete(bucket_name=bucket, object_name=name)
            deleted_count += 1
        logging.info("GCS_MERGE_SOURCE_DELETE_COMPLETE deleted=%d", deleted_count)
    else:
        logging.info("GCS_MERGE_SOURCE_DELETE_SKIPPED files=%d", len(objects))

    return {
        "bucket": bucket,
        "source_prefix": source_prefix,
        "pattern": pattern,
        "source_count": len(objects),
        "merged_object": destination_object,
        "merged_rows": merged_rows,
        "row_count_fin": fin_filename,
        "file_count_fin": file_count_fin_filename,
        "fin_objects": fin_objects,
        "deleted_count": deleted_count,
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

    hook = GCSHook(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)
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
        if grape_type not in {"gcs_copy_object", "gcs_delete_object", "gcs_move_prefix", "gcs_merge_csv"}:
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
                "object_pattern": source.get("pattern"),
                "ignore_if_missing": bool(options.get("ignore_if_missing", False)),
                **common,
            }
        elif grape_type == "gcs_merge_csv":
            destination = grape.get("destination") or {}
            if not isinstance(destination, dict):
                raise ValueError(f"{grape_id}: destination must be a mapping")
            python_callable = merge_gcs_csv
            op_kwargs = {
                "bucket": source.get("bucket"),
                "source_prefix": source.get("prefix"),
                "destination_object": destination.get("object"),
                "pattern": source.get("pattern", "*.csv"),
                "csv_header": bool(options.get("csv_header", True)),
                "csv_encoding": options.get("csv_encoding", "utf-8"),
                "delete_source": bool(options.get("delete_source", False)),
                "allow_empty": bool(options.get("allow_empty", False)),
                "overwrite": bool(options.get("overwrite", False)),
                "fin_filename": options.get("fin_filename"),
                "file_count_fin_filename": options.get("file_count_fin_filename"),
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
