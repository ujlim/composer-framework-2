# Author: LIM UI JIN
# Created: 2026-09-15

from __future__ import annotations

import shlex
from datetime import timedelta
from typing import Any

from framework.logger import task_failure_callback, task_success_callback
from framework.utils import merge_dicts, validate_airflow_id


def _arg(value: Any) -> str:
    """Shell-quote a command argument without corrupting Airflow Jinja templates.

    shlex.quote() rewrites embedded single quotes. That is safe for a shell, but a
    Jinja expression such as::

        {{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}

    is rendered by Airflow *after* this command string is built. Rewriting the
    quotes before Jinja rendering corrupts the expression. For templated values,
    keep the Jinja expression intact inside a shell double-quoted argument and
    escape only characters that are significant to the shell. Non-templated
    values continue to use shlex.quote().
    """
    text = str(value)
    if "{{" in text or "{%" in text or "{#" in text:
        escaped = (
            text.replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("$", "\\$")
            .replace("`", "\\`")
        )
        return f'"{escaped}"'
    return shlex.quote(text)


class SftpRunnerExecutor:
    """Execute a GCS-to-on-prem SFTP transfer on a dedicated Runner VM."""

    @classmethod
    def create_task(cls, *, dag, grape: dict[str, Any], defaults: dict[str, Any]) -> Any:
        try:
            from airflow.providers.ssh.operators.ssh import SSHOperator
        except ImportError as exc:
            raise ImportError(
                "gcs_to_sftp_runner requires apache-airflow-providers-ssh. "
                "Install a Composer-compatible provider version before enabling this grape."
            ) from exc

        grape_id = validate_airflow_id(grape.get("grape_id"), "grape.grape_id")
        if grape.get("type") != "gcs_to_sftp_runner":
            raise ValueError(f"Unsupported SFTP Runner grape type: {grape.get('type')}")

        options = merge_dicts(defaults, grape.get("options", {}))
        runner = grape.get("runner") or {}
        source = grape.get("source") or {}
        destination = grape.get("destination") or {}
        if not all(isinstance(x, dict) for x in (runner, source, destination)):
            raise ValueError(f"{grape_id}: runner/source/destination must be mappings")

        ssh_conn_id = runner.get("ssh_conn_id")
        target = destination.get("target")
        bucket = source.get("bucket")
        prefix = source.get("prefix")
        remote_dir = destination.get("remote_dir")
        if not all(isinstance(x, str) and x.strip() for x in (ssh_conn_id, target, bucket, prefix, remote_dir)):
            raise ValueError(
                f"{grape_id}: runner.ssh_conn_id, source.bucket, source.prefix, "
                "destination.target and destination.remote_dir are required"
            )

        runner_command = runner.get("command", "/engn/sftp-runner/venv/bin/python /engn/sftp-runner/app/gcs_to_sftp.py")
        command = " ".join([str(runner_command), "--target", _arg(target), "--bucket", _arg(bucket), "--prefix", _arg(prefix), "--remote-dir", _arg(remote_dir)])
        if bool(options.get("allow_empty", False)):
            command += " --allow-empty"
        if bool(options.get("delete_source", False)):
            command += " --delete-source"
        if bool(options.get("overwrite", False)):
            command += " --overwrite"

        merge = options.get("merge") or {}
        if bool(merge.get("enabled", False)):
            filename = destination.get("filename") or merge.get("filename")
            if not isinstance(filename, str) or not filename.strip():
                raise ValueError(f"{grape_id}: merge enabled requires destination.filename")
            command += " --merge-filename " + _arg(filename)
            if bool(merge.get("header", True)):
                command += " --csv-header"

            verification = options.get("verification") or {}
            if verification and not bool(verification.get("row_count", True)):
                raise ValueError(f"{grape_id}: merged CSV currently requires row_count verification")

            fin = options.get("fin") or {}
            if bool(fin.get("enabled", False)):
                fin_filename = fin.get("filename")
                if not isinstance(fin_filename, str) or not fin_filename.strip():
                    raise ValueError(f"{grape_id}: fin enabled requires options.fin.filename")
                command += " --fin-filename " + _arg(fin_filename)

        timeout_seconds = int(options.get("execution_timeout_seconds", 7200))
        return SSHOperator(
            task_id=grape_id,
            ssh_conn_id=ssh_conn_id,
            command=command,
            conn_timeout=int(options.get("conn_timeout_seconds", 30)),
            cmd_timeout=timeout_seconds,
            get_pty=False,
            retries=int(options.get("retries", 1)),
            retry_delay=timedelta(seconds=int(options.get("retry_delay_seconds", 300))),
            execution_timeout=timedelta(seconds=timeout_seconds),
            pool=options.get("pool"),
            priority_weight=int(options.get("priority_weight", 1)),
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback,
            dag=dag,
        )
