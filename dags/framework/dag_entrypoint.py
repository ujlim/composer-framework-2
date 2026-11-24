"""Shared registration and parsing-context logic for the two DAG entrypoints."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Literal

try:
    from airflow.sdk import get_parsing_context
except ImportError:
    from airflow.utils.dag_parsing_context import get_parsing_context

from framework.config_loader import PipelineCatalog, PipelineCatalogLoader
from framework.logger import get_logger
from framework.models import LoadedConfig

Kind = Literal["root", "vine"]


def _factory_id(config: LoadedConfig, kind: Kind) -> str | None:
    """Use the same section and whitespace normalization as the factories."""
    section = config.data.get(kind)
    value = section.get(f"{kind}_id") if isinstance(section, dict) else None
    return value.strip() if isinstance(value, str) else None


def _check_cross_kind_ids(catalog: PipelineCatalog) -> None:
    vine_paths = {
        _factory_id(vine, "vine"): vine.source_path for vine in catalog.vines
    }
    for root in catalog.roots:
        dag_id = _factory_id(root, "root")
        if dag_id and dag_id in vine_paths:
            raise ValueError(
                f"Duplicate DAG ID across Root and Vine '{dag_id}': "
                f"{root.source_path} and {vine_paths[dag_id]}"
            )


def load_dags(
    entrypoint: Path,
    module_name: str,
    kind: Kind,
    dag_class: type,
    factory: Callable[..., Any],
) -> dict[str, Any]:
    """Return the DAG globals owned by exactly one entrypoint file."""
    logger: logging.Logger = get_logger(module_name)
    pipelines_dir = entrypoint.resolve().parent / "pipelines"
    if not pipelines_dir.is_dir():
        raise FileNotFoundError(f"Pipelines directory does not exist: {pipelines_dir}")

    try:
        catalog = PipelineCatalogLoader(pipelines_dir).load()
        # DagBag imports these files independently: neither file can rely on a
        # process-local registration set in the other to detect a collision.
        _check_cross_kind_ids(catalog)
    except Exception:
        logger.exception(
            "Failed to load pipeline catalog",
            extra={
                "event": "pipeline_catalog_load_failed",
                "dag_kind": kind,
                "pipelines_dir": str(pipelines_dir),
            },
        )
        raise

    if not (catalog.roots or catalog.vines):
        raise RuntimeError(
            "No pipeline configurations were discovered under "
            f"{pipelines_dir}. Expected paths such as "
            "'pipelines/vines/*/config.yaml' or "
            "'pipelines/roots/*/config.yaml'."
        )

    target = get_parsing_context().dag_id
    own_configs, other_configs = (
        (catalog.roots, catalog.vines) if kind == "root" else (catalog.vines, catalog.roots)
    )
    other_kind: Kind = "vine" if kind == "root" else "root"
    configs = own_configs
    if target:
        selected = [item for item in own_configs if _factory_id(item, kind) == target]
        if selected or any(_factory_id(item, other_kind) == target for item in other_configs):
            configs = selected
        # Preserve the developer version's global-unknown fallback, but build
        # only this file's DAGs. A known target in the other file builds none.

    dags: dict[str, Any] = {}
    errors: list[str] = []
    vine_ids = catalog.vine_ids if kind == "root" and configs else None
    for config in configs:
        config_path = str(config.source_path)
        fields = {"dag_kind": kind, "config_path": config_path}
        try:
            builder = factory(config, vine_ids) if kind == "root" else factory(config)
            dag = builder.create()
            if not isinstance(dag, dag_class):
                raise TypeError(f"{kind} factory returned a non-DAG object: {type(dag)!r}")
            if not dag.dag_id or dag.dag_id != _factory_id(config, kind):
                raise ValueError(f"{kind} factory returned an unexpected DAG ID: {dag.dag_id!r}")
            if dag.dag_id in dags:
                raise ValueError(f"Duplicate DAG ID detected: {dag.dag_id}, config_path={config_path}")
            dags[dag.dag_id] = dag
            logger.info(
                "Registered DAG",
                extra={**fields, "event": "dag_registered", "dag_id": dag.dag_id},
            )
        except Exception as exc:
            errors.append(f"- kind={kind}, config={config_path}, error={exc!r}")
            logger.exception(
                f"Failed to register {kind.title()} DAG",
                extra={**fields, "event": "dag_registration_failed"},
            )

    logger.info(
        "DAG registration completed",
        extra={
            "event": "dag_registration_completed",
            "dag_kind": kind,
            "registered_dag_count": len(dags),
            "registration_error_count": len(errors),
            "registered_dag_ids": sorted(dags),
        },
    )

    # Having only one kind (or loading the other file for a known target) is
    # valid. But do not silently ignore a nonempty set of failed candidates.
    if configs and not dags:
        raise RuntimeError(
            "No DAGs were registered although pipeline configurations "
            f"were discovered.\n" + "\n".join(errors)
        )

    # Airflow discovers module-global DAG objects by type. A prefixed name
    # prevents a YAML ID from replacing module metadata or imported symbols.
    return {f"_pipeline_dag_{dag_id}": dag for dag_id, dag in dags.items()}
