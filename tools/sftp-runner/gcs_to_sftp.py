#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import posixpath
import tempfile
from pathlib import Path

import paramiko
from google.cloud import secretmanager, storage

CONFIG_PATH = Path(os.environ.get("SFTP_RUNNER_CONFIG", "/engn/sftp-runner/conf/targets.json"))
DEFAULT_KNOWN_HOSTS = "/engn/sftp-runner/conf/known_hosts"
DEFAULT_TEMP_DIR = "/data/sftp-runner/tmp"
DEFAULT_LOG_DIR = "/logs/sftp-runner"


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def setup_logging(config: dict) -> None:
    runner = config.get("runner") or {}
    log_dir = Path(runner.get("log_dir", DEFAULT_LOG_DIR))
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "sftp-runner.log"),
            logging.StreamHandler(),
        ],
    )


def load_target(config: dict, name: str) -> dict:
    target = (config.get("targets") or {}).get(name)
    if not isinstance(target, dict):
        raise ValueError(f"Unknown SFTP target: {name}")
    for key in ("host", "username"):
        if not target.get(key):
            raise ValueError(f"Target '{name}' requires '{key}'")
    return target


def access_secret(project_id: str, secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
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


def connect(config: dict, target: dict) -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    runner = config.get("runner") or {}
    known_hosts = runner.get("known_hosts", DEFAULT_KNOWN_HOSTS)
    auth_type = str(target.get("auth_type", "password")).lower()

    client = paramiko.SSHClient()
    client.load_host_keys(known_hosts)
    client.set_missing_host_key_policy(paramiko.RejectPolicy())

    kwargs = dict(
        hostname=target["host"],
        port=int(target.get("port", 22)),
        username=target["username"],
        timeout=int(target.get("connect_timeout_seconds", 30)),
        banner_timeout=int(target.get("banner_timeout_seconds", 30)),
        auth_timeout=int(target.get("auth_timeout_seconds", 30)),
        look_for_keys=False,
        allow_agent=False,
    )

    if auth_type == "password":
        secret_id = target.get("password_secret")
        project_id = target.get("secret_project_id") or runner.get("project_id")
        if not secret_id or not project_id:
            raise ValueError("password auth requires password_secret and secret project_id")
        kwargs["password"] = access_secret(project_id, secret_id)
    elif auth_type == "private_key":
        key_file = target.get("key_file")
        if not key_file:
            raise ValueError("private_key auth requires key_file")
        kwargs["key_filename"] = key_file
    else:
        raise ValueError(f"Unsupported auth_type: {auth_type}")

    client.connect(**kwargs)
    return client, client.open_sftp()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--remote-dir", required=True)
    parser.add_argument("--allow-empty", action="store_true")
    parser.add_argument("--delete-source", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    config = load_config()
    setup_logging(config)
    runner = config.get("runner") or {}
    temp_dir = Path(runner.get("temp_dir", DEFAULT_TEMP_DIR))
    temp_dir.mkdir(parents=True, exist_ok=True)

    prefix = args.prefix.lstrip("/")
    if not prefix:
        raise ValueError("--prefix must not be empty; refusing a whole-bucket transfer")

    target = load_target(config, args.target)
    storage_client = storage.Client()
    bucket = storage_client.bucket(args.bucket)
    blobs = [b for b in storage_client.list_blobs(args.bucket, prefix=prefix) if not b.name.endswith("/")]
    if not blobs:
        if args.allow_empty:
            logging.info("No objects found under gs://%s/%s", args.bucket, prefix)
            return 0
        raise FileNotFoundError(f"No objects found under gs://{args.bucket}/{prefix}")

    ssh, sftp = connect(config, target)
    uploaded: list[str] = []
    try:
        for blob in blobs:
            relative = blob.name[len(prefix):].lstrip("/")
            if not relative:
                continue
            remote_path = posixpath.join(args.remote_dir.rstrip("/"), relative)
            mkdir_p(sftp, posixpath.dirname(remote_path))
            if not args.overwrite:
                try:
                    sftp.stat(remote_path)
                except FileNotFoundError:
                    pass
                else:
                    raise FileExistsError(f"Remote file already exists: {remote_path}")

            with tempfile.NamedTemporaryFile(prefix="sftp-runner-", dir=temp_dir, delete=True) as tmp:
                blob.download_to_filename(tmp.name)
                local_size = os.path.getsize(tmp.name)
                sftp.put(tmp.name, remote_path, confirm=True)
                remote_size = sftp.stat(remote_path).st_size
                if remote_size != local_size:
                    raise RuntimeError(
                        f"SFTP size verification failed for {blob.name}: local={local_size}, remote={remote_size}"
                    )
            uploaded.append(blob.name)
            logging.info("Uploaded gs://%s/%s -> %s:%s", args.bucket, blob.name, args.target, remote_path)

        deleted_count = 0
        if args.delete_source:
            for object_name in uploaded:
                bucket.blob(object_name).delete()
                deleted_count += 1

        logging.info(
            "Transfer complete target=%s bucket=%s prefix=%s uploaded=%d deleted=%d",
            args.target, args.bucket, prefix, len(uploaded), deleted_count,
        )
        return 0
    finally:
        sftp.close()
        ssh.close()


if __name__ == "__main__":
    raise SystemExit(main())
