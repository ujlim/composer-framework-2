#!/usr/bin/env python3
# Author: LIM UI JIN
# Created: 2026-09-15

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import posixpath
import tempfile
import time
from pathlib import Path
from typing import Any

import google.auth
import paramiko
from google.auth import impersonated_credentials
from google.cloud import secretmanager, storage

CONFIG_PATH = Path(os.environ.get("SFTP_RUNNER_CONFIG", "/engn/sftp-runner/conf/targets.json"))
DEFAULT_KNOWN_HOSTS = "/engn/sftp-runner/conf/known_hosts"
DEFAULT_TEMP_DIR = "/data/sftp-runner/tmp"
DEFAULT_LOG_DIR = "/logs/sftp-runner"
CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
SFTP_CONNECT_MAX_ATTEMPTS = 3
SFTP_CONNECT_RETRY_SECONDS = 5


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def setup_logging(config: dict) -> None:
    runner = config.get("runner") or {}
    log_dir = Path(runner.get("log_dir", DEFAULT_LOG_DIR))
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=[logging.FileHandler(log_dir / "sftp-runner.log"), logging.StreamHandler()])


def build_google_credentials(config: dict) -> tuple[Any, str | None]:
    runner = config.get("runner") or {}
    source_credentials, detected_project_id = google.auth.default(scopes=[CLOUD_PLATFORM_SCOPE])
    project_id = runner.get("project_id") or detected_project_id
    target_principal = runner.get("service_account")
    if not target_principal:
        logging.warning("runner.service_account is not configured; using VM/Application Default Credentials directly")
        return source_credentials, project_id
    lifetime = int(runner.get("impersonation_lifetime_seconds", 3600))
    if lifetime < 300 or lifetime > 3600:
        raise ValueError("runner.impersonation_lifetime_seconds must be between 300 and 3600")
    credentials = impersonated_credentials.Credentials(source_credentials=source_credentials, target_principal=str(target_principal), target_scopes=[CLOUD_PLATFORM_SCOPE], lifetime=lifetime)
    logging.info("Using impersonated Google service account: %s", target_principal)
    return credentials, project_id


def load_target(config: dict, name: str) -> dict:
    target = (config.get("targets") or {}).get(name)
    if not isinstance(target, dict):
        raise ValueError(f"Unknown SFTP target: {name}")
    for key in ("host", "username"):
        if not target.get(key):
            raise ValueError(f"Target '{name}' requires '{key}'")
    return target


def access_secret(project_id: str, secret_id: str, credentials: Any) -> str:
    client = secretmanager.SecretManagerServiceClient(credentials=credentials)
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def mkdir_p(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    current = "/" if remote_dir.startswith("/") else ""
    for part in [p for p in remote_dir.split("/") if p]:
        current = posixpath.join(current, part)
        try:
            sftp.stat(current)
        except FileNotFoundError:
            sftp.mkdir(current)


def connect(config: dict, target: dict, google_credentials: Any) -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    runner = config.get("runner") or {}
    auth_type = str(target.get("auth_type", "password")).lower()
    kwargs = dict(hostname=target["host"], port=int(target.get("port", 22)), username=target["username"], timeout=int(target.get("connect_timeout_seconds", 30)), banner_timeout=int(target.get("banner_timeout_seconds", 30)), auth_timeout=int(target.get("auth_timeout_seconds", 30)), look_for_keys=False, allow_agent=False)
    if auth_type == "password":
        secret_id = target.get("password_secret")
        project_id = target.get("secret_project_id") or runner.get("project_id")
        if not secret_id or not project_id:
            raise ValueError("password auth requires password_secret and secret project_id")
        kwargs["password"] = access_secret(str(project_id), str(secret_id), google_credentials)
    elif auth_type == "private_key":
        if not target.get("key_file"):
            raise ValueError("private_key auth requires key_file")
        kwargs["key_filename"] = target["key_file"]
    else:
        raise ValueError(f"Unsupported auth_type: {auth_type}")

    max_attempts = int(target.get("connect_max_attempts", SFTP_CONNECT_MAX_ATTEMPTS))
    retry_seconds = int(target.get("connect_retry_seconds", SFTP_CONNECT_RETRY_SECONDS))
    if max_attempts < 1:
        raise ValueError("connect_max_attempts must be >= 1")
    if retry_seconds < 0:
        raise ValueError("connect_retry_seconds must be >= 0")

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        client = paramiko.SSHClient()
        client.load_host_keys(runner.get("known_hosts", DEFAULT_KNOWN_HOSTS))
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        logging.info(
            "SFTP_CONNECT_ATTEMPT attempt=%d/%d host=%s port=%s user=%s auth_type=%s",
            attempt, max_attempts, target["host"], target.get("port", 22), target["username"], auth_type,
        )
        try:
            client.connect(**kwargs)
            transport = client.get_transport()
            logging.info(
                "SSH_AUTH_COMPLETE attempt=%d/%d host=%s port=%s active=%s authenticated=%s remote_version=%s",
                attempt, max_attempts, target["host"], target.get("port", 22),
                transport.is_active() if transport else None,
                transport.is_authenticated() if transport else None,
                transport.remote_version if transport else None,
            )
            logging.info(
                "SFTP_SUBSYSTEM_OPEN_START attempt=%d/%d host=%s port=%s",
                attempt, max_attempts, target["host"], target.get("port", 22),
            )
            sftp = client.open_sftp()
            logging.info(
                "SFTP_SUBSYSTEM_OPEN_COMPLETE attempt=%d/%d host=%s port=%s",
                attempt, max_attempts, target["host"], target.get("port", 22),
            )
            return client, sftp
        except Exception as exc:
            last_error = exc
            transport = client.get_transport()
            logging.exception(
                "SFTP_CONNECT_ATTEMPT_FAIL attempt=%d/%d host=%s port=%s active=%s authenticated=%s",
                attempt, max_attempts, target["host"], target.get("port", 22),
                transport.is_active() if transport else None,
                transport.is_authenticated() if transport else None,
            )
            client.close()
            if attempt < max_attempts:
                logging.warning(
                    "SFTP_RECONNECT_WAIT attempt=%d/%d seconds=%d",
                    attempt, max_attempts, retry_seconds,
                )
                time.sleep(retry_seconds)

    assert last_error is not None
    logging.error("SFTP_CONNECT_EXHAUSTED attempts=%d host=%s port=%s", max_attempts, target["host"], target.get("port", 22))
    raise last_error


def count_csv_rows(path: Path, has_header: bool, encoding: str) -> int:
    with path.open("r", encoding=encoding, newline="") as fh:
        reader = csv.reader(fh)
        count = sum(1 for _ in reader)
    return max(0, count - (1 if has_header and count else 0))


def merge_csv_blobs(blobs: list[Any], temp_dir: Path, has_header: bool, encoding: str) -> tuple[Path, int, list[tuple[str, int]]]:
    fd, merged_name = tempfile.mkstemp(prefix="sftp-runner-merged-", suffix=".csv", dir=temp_dir)
    os.close(fd)
    merged_path = Path(merged_name)
    source_counts: list[tuple[str, int]] = []
    try:
        with merged_path.open("w", encoding=encoding, newline="") as out_fh:
            writer = csv.writer(out_fh, lineterminator="\n")
            header_written = False
            for blob in sorted(blobs, key=lambda b: b.name):
                fd, part_name = tempfile.mkstemp(prefix="sftp-runner-part-", suffix=".csv", dir=temp_dir)
                os.close(fd)
                part_path = Path(part_name)
                try:
                    blob.download_to_filename(str(part_path))
                    rows = count_csv_rows(part_path, has_header, encoding)
                    source_counts.append((blob.name, rows))
                    logging.info("SOURCE_FILE file=%s rows=%d encoding=%s", blob.name, rows, encoding)
                    with part_path.open("r", encoding=encoding, newline="") as in_fh:
                        reader = csv.reader(in_fh)
                        first = True
                        for record in reader:
                            if first and has_header:
                                first = False
                                if header_written:
                                    continue
                                header_written = True
                            else:
                                first = False
                            writer.writerow(record)
                finally:
                    part_path.unlink(missing_ok=True)
        source_total = sum(rows for _, rows in source_counts)
        merged_rows = count_csv_rows(merged_path, has_header, encoding)
        logging.info("SOURCE_TOTAL files=%d rows=%d", len(source_counts), source_total)
        logging.info("MERGE_COMPLETE file=%s rows=%d encoding=%s", merged_path.name, merged_rows, encoding)
        if source_total != merged_rows:
            logging.error("ROW_VERIFICATION source_rows=%d merged_rows=%d result=FAIL", source_total, merged_rows)
            raise RuntimeError(f"CSV row verification failed: source_rows={source_total}, merged_rows={merged_rows}")
        logging.info("ROW_VERIFICATION source_rows=%d merged_rows=%d result=PASS", source_total, merged_rows)
        return merged_path, merged_rows, source_counts
    except Exception:
        merged_path.unlink(missing_ok=True)
        raise


def upload_atomic(sftp: paramiko.SFTPClient, local_path: Path, remote_path: str, overwrite: bool) -> None:
    mkdir_p(sftp, posixpath.dirname(remote_path))
    if not overwrite:
        try:
            sftp.stat(remote_path)
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError(f"Remote file already exists: {remote_path}")
    uploading_path = remote_path + ".uploading"
    try:
        sftp.remove(uploading_path)
    except FileNotFoundError:
        pass
    sftp.put(str(local_path), uploading_path, confirm=True)
    if sftp.stat(uploading_path).st_size != local_path.stat().st_size:
        raise RuntimeError(f"SFTP size verification failed: {remote_path}")
    if overwrite:
        try:
            sftp.remove(remote_path)
        except FileNotFoundError:
            pass
    sftp.rename(uploading_path, remote_path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--remote-dir", required=True)
    parser.add_argument("--allow-empty", action="store_true")
    parser.add_argument("--delete-source", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--merge-filename")
    parser.add_argument("--csv-header", action="store_true")
    parser.add_argument("--csv-encoding", default="utf-8")
    parser.add_argument("--fin-filename")
    parser.add_argument("--file-count-fin-filename")
    args = parser.parse_args()

    config = load_config()
    setup_logging(config)
    logging.info("TRANSFER_MODE mode=%s merge_filename=%s csv_header=%s csv_encoding=%s fin_filename=%s file_count_fin_filename=%s", "MERGE" if args.merge_filename else "NORMAL", args.merge_filename or "-", args.csv_header, args.csv_encoding, args.fin_filename or "-", args.file_count_fin_filename or "-")
    runner = config.get("runner") or {}
    temp_dir = Path(runner.get("temp_dir", DEFAULT_TEMP_DIR))
    temp_dir.mkdir(parents=True, exist_ok=True)
    credentials, project_id = build_google_credentials(config)
    prefix = args.prefix.lstrip("/")
    if not prefix:
        raise ValueError("--prefix must not be empty; refusing a whole-bucket transfer")
    storage_client = storage.Client(project=project_id, credentials=credentials)
    bucket = storage_client.bucket(args.bucket)
    blobs = [b for b in storage_client.list_blobs(args.bucket, prefix=prefix) if not b.name.endswith("/")]
    logging.info("GCS_SOURCE_DISCOVERED bucket=%s prefix=%s files=%d", args.bucket, prefix, len(blobs))
    if not blobs:
        if args.allow_empty:
            logging.info("No objects found under gs://%s/%s", args.bucket, prefix)
            return 0
        raise FileNotFoundError(f"No objects found under gs://{args.bucket}/{prefix}")

    target = load_target(config, args.target)
    ssh, sftp = connect(config, target, credentials)
    merged_path: Path | None = None
    try:
        if args.merge_filename:
            logging.info("MERGE_START bucket=%s prefix=%s files=%d encoding=%s", args.bucket, prefix, len(blobs), args.csv_encoding)
            merged_path, merged_rows, _ = merge_csv_blobs(blobs, temp_dir, args.csv_header, args.csv_encoding)
            remote_data = posixpath.join(args.remote_dir.rstrip("/"), args.merge_filename)
            upload_atomic(sftp, merged_path, remote_data, args.overwrite)
            logging.info("SFTP_DATA_COMPLETE file=%s rows=%d", remote_data, merged_rows)
            if args.fin_filename:
                fd, fin_name = tempfile.mkstemp(prefix="sftp-runner-fin-", dir=temp_dir)
                os.close(fd)
                fin_path = Path(fin_name)
                try:
                    fin_path.write_text(f"{merged_rows}\n", encoding="utf-8")
                    remote_fin = posixpath.join(args.remote_dir.rstrip("/"), args.fin_filename)
                    logging.info("FIN_CREATED file=%s content=%d", args.fin_filename, merged_rows)
                    upload_atomic(sftp, fin_path, remote_fin, args.overwrite)
                    logging.info("SFTP_FIN_COMPLETE type=row_count file=%s", remote_fin)
                finally:
                    fin_path.unlink(missing_ok=True)
            if args.file_count_fin_filename:
                fd, fin_name = tempfile.mkstemp(prefix="sftp-runner-file-count-fin-", dir=temp_dir)
                os.close(fd)
                fin_path = Path(fin_name)
                try:
                    fin_path.write_text(f"{len(blobs)}\n", encoding="utf-8")
                    remote_fin = posixpath.join(args.remote_dir.rstrip("/"), args.file_count_fin_filename)
                    logging.info("FIN_CREATED type=file_count file=%s content=%d", args.file_count_fin_filename, len(blobs))
                    upload_atomic(sftp, fin_path, remote_fin, args.overwrite)
                    logging.info("SFTP_FIN_COMPLETE type=file_count file=%s", remote_fin)
                finally:
                    fin_path.unlink(missing_ok=True)
            if args.delete_source:
                for blob in blobs:
                    blob.delete()
            logging.info("JOB_COMPLETE result=SUCCESS source_files=%d rows=%d", len(blobs), merged_rows)
            return 0

        uploaded: list[str] = []
        for blob in blobs:
            relative = blob.name[len(prefix):].lstrip("/")
            if not relative:
                continue
            remote_path = posixpath.join(args.remote_dir.rstrip("/"), relative)
            fd, tmp_name = tempfile.mkstemp(prefix="sftp-runner-", dir=temp_dir)
            os.close(fd)
            tmp_path = Path(tmp_name)
            try:
                blob.download_to_filename(str(tmp_path))
                upload_atomic(sftp, tmp_path, remote_path, args.overwrite)
            finally:
                tmp_path.unlink(missing_ok=True)
            uploaded.append(blob.name)
            logging.info("Uploaded gs://%s/%s -> %s:%s", args.bucket, blob.name, args.target, remote_path)
        if args.delete_source:
            for object_name in uploaded:
                bucket.blob(object_name).delete()
        logging.info("Transfer complete target=%s bucket=%s prefix=%s uploaded=%d", args.target, args.bucket, prefix, len(uploaded))
        return 0
    except Exception:
        logging.exception("JOB_ABORTED result=FAIL")
        raise
    finally:
        if merged_path:
            merged_path.unlink(missing_ok=True)
        sftp.close()
        ssh.close()


if __name__ == "__main__":
    raise SystemExit(main())
