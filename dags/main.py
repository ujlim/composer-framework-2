"""
Cloud Composer 3 / Apache Airflow DAG entry point.

Auto-discovers:
- pipelines/roots/*/config.yaml
- pipelines/vines/*/config.yaml

A broken pipeline is isolated so other DAGs can still be registered.
"""

from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Any

# 중요:
# Airflow DAG discovery safe mode가 이 파일을 DAG 파일로 인식하도록
# 실제 airflow 모듈을 import합니다.
from airflow.models.dag import DAG
from airflow.sdk import get_parsing_context

from framework.config_loader import PipelineCatalogLoader
from framework.logger import get_logger
from framework.root_factory import RootFactory
from framework.vine_factory import VineFactory


LOGGER = get_logger(__name__)

# DAG Processor: dag_id=None -> 전체 DAG 등록.
# Worker task execution: dag_id=<requested> -> 해당 DAG만 생성.
# YAML 카탈로그 로딩 방식과 Root/Vine YAML 규격은 변경하지 않습니다.
PARSE_STARTED_AT = perf_counter()
TARGET_DAG_ID = get_parsing_context().dag_id


def log_parse_timing(stage: str, started_at: float, **fields: Any) -> None:
    """DAG import 병목을 식별하기 위한 경과 시간 로그 (밀리초)."""
    duration_ms = round((perf_counter() - started_at) * 1000, 2)
    LOGGER.info(
        "DAG_PARSE_TIMING stage=%s duration_ms=%.2f target_dag_id=%s",
        stage,
        duration_ms,
        TARGET_DAG_ID,
        extra={
            "event": "dag_parse_timing",
            "stage": stage,
            "duration_ms": duration_ms,
            "target_dag_id": TARGET_DAG_ID,
            **fields,
        },
    )


DAGS_DIR = Path(__file__).resolve().parent
PIPELINES_DIR = DAGS_DIR / "pipelines"

registered_dag_ids: set[str] = set()
registration_errors: list[dict[str, Any]] = []


def register_dag(dag: DAG, dag_kind: str, config_path: Path) -> None:
    """Airflow가 발견할 수 있도록 DAG를 module globals에 등록합니다."""

    if not isinstance(dag, DAG):
        raise TypeError(
            f"{dag_kind} factory returned an invalid object: "
            f"expected airflow.models.DAG, got {type(dag)!r}"
        )

    if not dag.dag_id:
        raise ValueError(
            f"{dag_kind} factory returned a DAG without dag_id: "
            f"{config_path}"
        )

    if dag.dag_id in registered_dag_ids:
        raise ValueError(
            f"Duplicate DAG ID detected: {dag.dag_id}, "
            f"config_path={config_path}"
        )

    # Airflow DAG parser가 module-level DAG 객체를 발견하도록 등록합니다.
    globals()[dag.dag_id] = dag
    registered_dag_ids.add(dag.dag_id)

    LOGGER.info(
        "Registered DAG",
        extra={
            "event": "dag_registered",
            "dag_kind": dag_kind,
            "dag_id": dag.dag_id,
            "config_path": str(config_path),
        },
    )


if not PIPELINES_DIR.is_dir():
    raise FileNotFoundError(
        f"Pipelines directory does not exist: {PIPELINES_DIR}"
    )

catalog_load_started_at = perf_counter()
try:
    catalog = PipelineCatalogLoader(PIPELINES_DIR).load()
except Exception:
    log_parse_timing("catalog_load_failed", catalog_load_started_at)
    LOGGER.exception(
        "Failed to load pipeline catalog",
        extra={
            "event": "pipeline_catalog_load_failed",
            "pipelines_dir": str(PIPELINES_DIR),
        },
    )
    # Catalog 자체를 읽지 못하면 어떤 DAG도 만들 수 없으므로
    # Airflow Import Error에 원인이 표시되도록 다시 발생시킵니다.
    raise

log_parse_timing(
    "catalog_load", catalog_load_started_at,
    root_count=len(catalog.roots), vine_count=len(catalog.vines),
)


# Vine DAG를 먼저 등록합니다.
vine_stage_started_at = perf_counter()
vine_selected_count = 0
for vine_config in catalog.vines:
    if TARGET_DAG_ID is not None and vine_config.id != TARGET_DAG_ID:
        continue

    vine_selected_count += 1
    vine_create_started_at = perf_counter()
    try:
        vine_dag = VineFactory(vine_config).create()

        register_dag(
            dag=vine_dag,
            dag_kind="vine",
            config_path=vine_config.source_path,
        )

    except Exception as exc:
        registration_errors.append(
            {
                "dag_kind": "vine",
                "config_path": str(vine_config.source_path),
                "error": repr(exc),
            }
        )

        LOGGER.exception(
            "Failed to register Vine DAG",
            extra={
                "event": "dag_registration_failed",
                "dag_kind": "vine",
                "config_path": str(vine_config.source_path),
            },
        )
    finally:
        log_parse_timing(
            "vine_create", vine_create_started_at, dag_id=vine_config.id,
        )

log_parse_timing("vine_stage", vine_stage_started_at, selected_count=vine_selected_count)


# Root DAG를 나중에 등록합니다.
root_stage_started_at = perf_counter()
root_selected_count = 0
for root_config in catalog.roots:
    if TARGET_DAG_ID is not None and root_config.id != TARGET_DAG_ID:
        continue

    root_selected_count += 1
    root_create_started_at = perf_counter()
    try:
        root_dag = RootFactory(
            root_config,
            catalog.vine_ids,
        ).create()

        register_dag(
            dag=root_dag,
            dag_kind="root",
            config_path=root_config.source_path,
        )

    except Exception as exc:
        registration_errors.append(
            {
                "dag_kind": "root",
                "config_path": str(root_config.source_path),
                "error": repr(exc),
            }
        )

        LOGGER.exception(
            "Failed to register Root DAG",
            extra={
                "event": "dag_registration_failed",
                "dag_kind": "root",
                "config_path": str(root_config.source_path),
            },
        )
    finally:
        log_parse_timing(
            "root_create", root_create_started_at, dag_id=root_config.id,
        )

log_parse_timing("root_stage", root_stage_started_at, selected_count=root_selected_count)


LOGGER.info(
    "DAG registration completed",
    extra={
        "event": "dag_registration_completed",
        "registered_dag_count": len(registered_dag_ids),
        "registration_error_count": len(registration_errors),
        "registered_dag_ids": sorted(registered_dag_ids),
    },
)


# 설정 파일은 발견됐는데 모든 DAG 생성이 실패한 경우,
# 조용히 DAG 0개로 끝내지 않고 Import Error를 발생시킵니다.
configured_pipeline_count = len(catalog.vines) + len(catalog.roots)

if configured_pipeline_count > 0 and not registered_dag_ids:
    error_summary = "\n".join(
        (
            f"- kind={item['dag_kind']}, "
            f"config={item['config_path']}, "
            f"error={item['error']}"
        )
        for item in registration_errors
    )

    raise RuntimeError(
        "No DAGs were registered although pipeline configurations "
        f"were discovered.\n{error_summary}"
    )


# 설정 파일 자체가 발견되지 않은 경우도 명확하게 표시합니다.
if configured_pipeline_count == 0:
    raise RuntimeError(
        "No pipeline configurations were discovered under "
        f"{PIPELINES_DIR}. Expected paths such as "
        "'pipelines/vines/*/config.yaml' or "
        "'pipelines/roots/*/config.yaml'."
    )

log_parse_timing(
    "total_registration", PARSE_STARTED_AT,
    registered_dag_count=len(registered_dag_ids),
    registration_error_count=len(registration_errors),
)