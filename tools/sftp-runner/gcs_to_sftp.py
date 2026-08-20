#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import posixpath
import tempfile
from pathlib import Path

import paramiko
from google.cloud import storage

CONFIG_PATH = Path(os.environ.get("SFTP_RUNNER_CONFIG", "/etc/sftp-runner/targets.json"))


def load_target(name: str) -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        config = json.load(fh)
    target = (config.get("targets") or {}).get(name)
    if not isinstance(target, dict):
        raise ValueError(f"Unknown SFTP target: {name}")
    for key in ("host", "username", "key_file"):
        if not target.get(key):
            raise ValueError(f"Target '{name}' requires '{key}'")
    return target


def mkdir_p(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    current = "/" if remote_dir.startswith("/") else ""
    for part in [p for p in remote_dir.split("/") if p]:
        current = posixpath.join(current, part)
        try:
            sftp.stat(current)
        except FileNotFoundError:
            sftp.mkdir(current)


def connect(target: dict) -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(
        hostname=target["host"],
        port=int(target.get("port", 22)),
        username=target["username"],
        key_filename=target["key_file"],
        timeout=int(target.get("connect_timeout_seconds", 30)),
        banner_timeout=int(target.get("banner_timeout_seconds", 30)),
        auth_timeout=int(target.get("auth_timeout_seconds", 30)),
        look_for_keys=False,
        allow_agent=False,
    )
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

    prefix = args.prefix.lstrip("/")
    if not prefix:
        raise ValueError("--prefix must not be empty; refusing a whole-bucket transfer")

    target = load_target(args.target)
    storage_client = storage.Client()
    bucket = storage_client.bucket(args.bucket)
    blobs = [blob for blob in storage_client.list_blobs(args.bucket, prefix=prefix) if not blob.name.endswith("/")]
    if not blobs:
        if args.allow_empty:
            print(json.dumps({"uploaded_count": 0, "deleted_count": 0}))
            return 0
        raise FileNotFoundError(f"No objects found under gs://{args.bucket}/{prefix}")

    ssh, sftp = connect(target)
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

            with tempfile.NamedTemporaryFile(prefix="sftp-runner-", delete=True) as tmp:
                blob.download_to_filename(tmp.name)
                local_size = os.path.getsize(tmp.name)
                sftp.put(tmp.name, remote_path, confirm=True)
                remote_size = sftp.stat(remote_path).st_size
                if remote_size != local_size:
                    raise RuntimeError(
                        f"SFTP size verification failed for {blob.name}: local={local_size}, remote={remote_size}"
                    )
            uploaded.append(blob.name)

        deleted_count = 0
        if args.delete_source:
            # Two-phase behavior: only delete GCS objects after every SFTP upload succeeded.
            for object_name in uploaded:
                bucket.blob(object_name).delete()
                deleted_count += 1

        print(json.dumps({
            "target": args.target,
            "bucket": args.bucket,
            "prefix": prefix,
            "remote_dir": args.remote_dir,
            "uploaded_count": len(uploaded),
            "deleted_count": deleted_count,
        }))
        return 0
    finally:
        sftp.close()
        ssh.close()


if __name__ == "__main__":
    raise SystemExit(main())
