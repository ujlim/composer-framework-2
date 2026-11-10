"""
Cloud Composer 3 / Apache Airflow DAG entry point.

Every invalid pipeline is quarantined independently. A duplicate DAG ID
quarantines *all* conflicting definitions; no file-order winner is selected.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

# Airflow DAG discovery safe mode requires a real airflow import in this file.
from airflow.models.dag import DAG

from framework.config_loader import PipelineCatalogLoader
from framework.logger import get_logger, log_event
from framework.root_factory import RootFactory, UnregisteredVineError
from framework.vine_factory import VineFactory


LOGGER = get_logger(__name__)

DAGS_DIR = Path(__file__).resolve().parent
PIPELINES_DIR = DAGS_DIR / "pipelines"

registered_dag_ids: set[str] = set()
registered_vine_ids: set[str] = set()
registration_errors: list[dict[str, Any]] = []


class DagRegistrationError(ValueError):
    """An invalid or duplicate object returned by a DAG factory."""

    def __init__(self, error_code: str, message: str):
        self.error_code = error_code
        super().__init__(message)


def register_dag(dag: DAG, dag_kind: str, config_path: Path) -> None:
    """Expose a DAG at module scope without overwriting framework symbols."""

    if not isinstance(dag, DAG):
        raise DagRegistrationError(
            "INVALID_DAG_OBJECT",
            f"{dag_kind} factory returned an invalid object from {config_path}: "
            f"expected airflow.models.DAG, got {type(dag)!r}",
        )

    if not dag.dag_id:
        raise DagRegistrationError(
            "INVALID_DAG_OBJECT",
            f"{dag_kind} factory returned a DAG without dag_id: {config_path}",
        )

    if dag.dag_id in registered_dag_ids:
        raise DagRegistrationError(
            "DUPLICATE_DAG_ID",
            f"Duplicate DAG ID during registration: {dag.dag_id}, "
            f"config_path={config_path}",
        )

    # Airflow discovers DAG objects in module globals by value, not by the
    # Python variable name. Never allow an ID like 'LOGGER' or 'register_dag'
    # to overwrite a framework global and break all subsequent registration.
    globals()[f"_pipeline_dag__{dag.dag_id}"] = dag
    registered_dag_ids.add(dag.dag_id)

    LOGGER.info(
        "Registered DAG id=%s kind=%s config=%s",
        dag.dag_id, dag_kind, config_path,
        extra={
            "event": "dag_registered",
            "dag_kind": dag_kind,
            "dag_id": dag.dag_id,
            "config_path": str(config_path),
        },
    )
    log_event(
        "dag_registered",
        dag_kind=dag_kind,
        dag_id=dag.dag_id,
        config_path=str(config_path),
    )


def _record_registration_failure(
    dag_kind: str,
    config_path: Path,
    error: Exception,
    dag_id: str | None = None,
) -> None:
    error_code = (
        error.error_code if isinstance(error, DagRegistrationError)
        else "ROOT_VINE_NOT_REGISTERED" if isinstance(error, UnregisteredVineError)
        else "DAG_FACTORY_ERROR"
    )
    record = {
        "error_code": error_code,
        "dag_kind": dag_kind,
        "config_path": str(config_path),
        "dag_id": dag_id,
        "error": str(error),
        "related_paths": [],
    }
    registration_errors.append(record)

    LOGGER.exception(
        "[%s] Failed to register %s DAG from %s: %s",
        error_code, dag_kind, config_path, error,
    )
    log_event("dag_registration_failed", **record)


if not PIPELINES_DIR.is_dir():
    raise FileNotFoundError(
        f"Pipelines directory does not exist: {PIPELINES_DIR}"
    )

try:
    catalog = PipelineCatalogLoader(PIPELINES_DIR).load()
except Exception:
    LOGGER.exception(
        "Failed to load pipeline catalog",
        extra={
            "event": "pipeline_catalog_load_failed",
            "pipelines_dir": str(PIPELINES_DIR),
        },
    )
    raise


# Config errors must be visible even while healthy DAGs keep registering.
for issue in catalog.errors:
    record = issue.as_dict()
    registration_errors.append(record)
    LOGGER.error(
        "[%s] Quarantined %s config=%s id=%s: %s",
        issue.error_code,
        issue.dag_kind,
        issue.config_path,
        issue.dag_id,
        issue.error,
    )
    log_event("dag_config_rejected", **record)


# Register Vine DAGs first, tracking actual successful registrations.
for vine_config in catalog.vines:
    try:
        vine_dag = VineFactory(vine_config).create()
        register_dag(
            dag=vine_dag,
            dag_kind="vine",
            config_path=vine_config.source_path,
        )
        registered_vine_ids.add(vine_dag.dag_id)
    except Exception as exc:
        _record_registration_failure(
            "vine", vine_config.source_path, exc, vine_config.id
        )


# Root references may point ONLY to Vines that actually registered, not just
# IDs from YAML files that failed DAG factory validation.
for root_config in catalog.roots:
    try:
        root_dag = RootFactory(
            root_config,
            registered_vine_ids,
        ).create()
        register_dag(
            dag=root_dag,
            dag_kind="root",
            config_path=root_config.source_path,
        )
    except Exception as exc:
        _record_registration_failure(
            "root", root_config.source_path, exc, root_config.id
        )


LOGGER.info(
    "DAG registration completed: registered=%s errors=%s",
    len(registered_dag_ids),
    len(registration_errors),
    extra={
        "event": "dag_registration_completed",
        "registered_dag_count": len(registered_dag_ids),
        "registration_error_count": len(registration_errors),
        "registered_dag_ids": sorted(registered_dag_ids),
    },
)
log_event(
    "dag_registration_completed",
    registered_dag_count=len(registered_dag_ids),
    registration_error_count=len(registration_errors),
    registered_dag_ids=sorted(registered_dag_ids),
    registered_vine_ids=sorted(registered_vine_ids),
)


# Fail loudly only if no DAG was registered. An import-level exception when
# healthy DAGs exist would defeat the point of per-pipeline isolation.
if not registered_dag_ids:
    if registration_errors:
        lines = [
            f"- code={item['error_code']}, kind={item['dag_kind']}, "
            f"config={item['config_path']}, error={item['error']}"
            for item in registration_errors[:25]
        ]
        remaining = len(registration_errors) - len(lines)
        if remaining:
            lines.append(f"... and {remaining} additional error(s); see parser logs.")
        raise RuntimeError(
            "No DAGs registered; all discovered pipeline configurations were "
            "rejected or failed to build.\n" + "\n".join(lines)
        )

    raise RuntimeError(
        "No pipeline configurations were discovered under "
        f"{PIPELINES_DIR}. Expected paths such as "
        "'pipelines/vines/*/config.yaml' or "
        "'pipelines/roots/*/config.yaml'."
    )
