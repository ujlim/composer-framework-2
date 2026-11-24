# Author: LIM UI JIN
# Created: 2026-11-24
# Updated: 2026-11-24 (Vine SHA-256 shards)

"""Shared registration and parsing-context logic for Root and Vine entrypoints."""

from __future__ import annotations

import hashlib
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

# Part of the deployment/file-location contract. Changing this number moves DAGs
# between files and requires an explicit migration of the whole shard set.
VINE_SHARD_COUNT = 8


def vine_shard_for(dag_id: str) -> int:
    """Stable owner: first 64 bits of SHA-256 over the canonical UTF-8 DAG ID."""
    digest = hashlib.sha256(dag_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % VINE_SHARD_COUNT


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
    factory: Callable[..., Any] | None = None,
    *,
    vine_shard: int | None = None,
) -> dict[str, Any]:
    """Return the DAG globals owned by exactly one entrypoint file."""
    logger: logging.Logger = get_logger(module_name)
    if kind == "vine":
        if vine_shard is None:
            raise ValueError("Unsharded Vine entrypoint is incompatible; remove main_vines.py")
        if not 0 <= vine_shard < VINE_SHARD_COUNT:
            raise ValueError(f"Invalid Vine shard: {vine_shard!r}")
    elif vine_shard is not None:
        raise ValueError("Root entrypoint cannot declare a Vine shard")

    target = get_parsing_context().dag_id
    shard_fields = {} if vine_shard is None else {"vine_shard": vine_shard}
    if target and vine_shard is not None and vine_shard_for(target) != vine_shard:
        # This file cannot own the requested DAG, regardless of catalog contents.
        # Avoid discovery/YAML work as well as Factory work on this path.
        logger.info(
            "Vine shard does not own the worker parsing target",
            extra={"event": "dag_shard_target_skipped", "dag_id": target, **shard_fields},
        )
        return {}

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
                **shard_fields,
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

    own_configs, other_configs = (
        (catalog.roots, catalog.vines) if kind == "root" else (catalog.vines, catalog.roots)
    )
    other_kind: Kind = "vine" if kind == "root" else "root"
    selected = [item for item in own_configs if _factory_id(item, kind) == target] if target else []
    if selected or (target and any(_factory_id(item, other_kind) == target for item in other_configs)):
        # Skip full shard assignment when the worker target is known.
        configs = selected
    elif vine_shard is not None:
        configs = [
            item for item in own_configs
            # Let the owning factory report malformed IDs too, rather than
            # silently dropping a parsed YAML that cannot declare a real DAG.
            if vine_shard_for(_factory_id(item, "vine") or item.id.strip()) == vine_shard
        ]
    else:
        configs = own_configs
    # Compatibility: an unknown target falls back within its computed shard
    # (or all Roots). A known target in the other kind builds none.

    dags: dict[str, Any] = {}
    errors: list[str] = []
    if configs:
        if kind == "root":
            if factory is None:
                from framework.root_factory import RootFactory
                factory = RootFactory
            factory_args = (catalog.vine_ids,)
        else:
            if factory is None:
                from framework.vine_factory import VineFactory
                factory = VineFactory
            factory_args = ()
    for config in configs:
        config_path = str(config.source_path)
        fields = {"dag_kind": kind, "config_path": config_path, **shard_fields}
        try:
            builder = factory(config, *factory_args)
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
            **shard_fields,
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
