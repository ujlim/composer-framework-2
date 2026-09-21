# GCS move / SFTP Runner grapes

## GCS date-prefix move

```yaml
- grape_id: move_daily_files
  type: gcs_move_prefix
  source:
    bucket: source-bucket
    prefix: inbound/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/
  destination:
    bucket: archive-bucket
    prefix: inbound/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/
  options:
    delete_source: true
    overwrite: false
    allow_empty: false
    preserve_relative_path: true
```

The task lists the source prefix, copies every object, verifies destination size and CRC32C, then deletes source objects only after all copies have succeeded.

## GCS to on-prem SFTP through Runner VM

```yaml
- grape_id: send_daily_files
  type: gcs_to_sftp_runner
  runner:
    ssh_conn_id: conn-sftp-runner
  source:
    bucket: outbound-bucket
    prefix: outbound/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}/
  destination:
    target: onprem-ubis
    remote_dir: /data/inbound/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}
  options:
    delete_source: false
    overwrite: false
    allow_empty: false
    execution_timeout_seconds: 7200
```

Composer SSHes to the Runner VM and runs:

```text
/engn/sftp-runner/venv/bin/python /engn/sftp-runner/app/gcs_to_sftp.py
```

## Runner VM filesystem

```text
/engn/sftp-runner/
├── app/
│   └── gcs_to_sftp.py
├── conf/
│   ├── targets.json
│   └── known_hosts
└── venv/

/data/sftp-runner/
└── tmp/

/logs/sftp-runner/
└── sftp-runner.log

/home/sftprunner/
└── .ssh/
    └── authorized_keys
```

`/engn` contains the program and configuration, `/data` contains temporary transfer files, and `/logs` contains Runner logs. `authorized_keys` is only for Composer -> Runner VM SSH authentication.

## Password authentication (default)

The default target authentication is password. The password itself must not be written in `targets.json`; only the Secret Manager secret name is stored.

```json
{
  "runner": {
    "project_id": "gcp-prod-edp-sa-ubi",
    "temp_dir": "/data/sftp-runner/tmp",
    "log_dir": "/logs/sftp-runner",
    "known_hosts": "/engn/sftp-runner/conf/known_hosts"
  },
  "targets": {
    "onprem-ubis": {
      "host": "172.20.10.50",
      "port": 22,
      "username": "ubis_batch",
      "auth_type": "password",
      "password_secret": "sftp-onprem-ubis-password"
    }
  }
}
```

The Runner service account needs `roles/secretmanager.secretAccessor` on the required Secret Manager secret and the required Cloud Storage object permissions on the source bucket.

## known_hosts

`/engn/sftp-runner/conf/known_hosts` verifies the identity of the destination SFTP server. It is required even when password authentication is used. Register and verify each target server host key before enabling transfers.

## Optional private-key authentication

Private-key authentication remains supported per target:

```json
{
  "host": "172.20.30.70",
  "port": 22,
  "username": "batch_user",
  "auth_type": "private_key",
  "key_file": "/engn/sftp-runner/conf/keys/onprem-dw"
}
```
