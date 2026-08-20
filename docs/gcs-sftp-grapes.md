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
    target: onprem-a
    remote_dir: /data/inbound/{{ ti.xcom_pull(task_ids='resolve_business_date')['business_date'] }}
  options:
    delete_source: false
    overwrite: false
    allow_empty: false
    execution_timeout_seconds: 7200
```

The Composer task SSHes only to the Runner VM. The Runner reads GCS using its VM service account and connects to the registered on-prem SFTP target using a private key and strict known_hosts verification.
