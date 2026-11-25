# Author: LIM UI JIN
# Created: 2026-11-25
"""Split developer-version DAG discovery without a duplicate-validation pass."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator
from pathlib import Path

from airflow.models.dag import DAG

try:
    from airflow.sdk import get_parsing_context
except ImportError:
    from airflow.utils.dag_parsing_context import get_parsing_context

from framework.config_loader import PipelineCatalogLoader
from framework.logger import get_logger
from framework.models import LoadedConfig
from framework.utils import validate_airflow_id

VINE_SHARD_COUNT = 8
LOGGER = get_logger(__name__)


def vine_shard(folder_name: str) -> int:
    digest = hashlib.sha256(folder_name.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % VINE_SHARD_COUNT


def _loader(entry_file: str) -> PipelineCatalogLoader:
    pipelines_dir = Path(entry_file).resolve().parent / "pipelines"
    if not pipelines_dir.is_dir():
        raise FileNotFoundError(f"Pipelines directory does not exist: {pipelines_dir}")
    return PipelineCatalogLoader(pipelines_dir)


def _configs(
    loader: PipelineCatalogLoader, paths: Iterable[Path], kind: str, target: str | None = None
) -> Iterator[tuple[str, LoadedConfig]]:
    # When folder and ID match, try that path first. If they don't (legacy
    # examples), only search the paths owned by this entrypoint.
    ordered = sorted(paths, key=lambda p: p.parent.name != target) if target is not None else paths
    for path in ordered:
        config = loader.read(path, kind)
        if config is None:
            continue
        try:
            dag_id = validate_airflow_id(config.data[kind][f"{kind}_id"], f"{kind}.{kind}_id")
        except (KeyError, TypeError, ValueError):
            LOGGER.exception("Invalid %s DAG ID in %s", kind, path)
            continue
        if target is None or dag_id == target:
            yield dag_id, config
            if target is not None:
                return


def _register(kind: str, configs: Iterable[tuple[str, LoadedConfig]], factory) -> dict[str, DAG]:
    dags: dict[str, DAG] = {}
    for _, config in configs:
        try:
            dag = factory(config).create()
            if not isinstance(dag, DAG) or not dag.dag_id:
                raise TypeError(f"{kind} factory returned an invalid DAG: {config.source_path}")
            dags[dag.dag_id] = dag
            LOGGER.info("Registered DAG", extra={"event": "dag_registered", "dag_kind": kind,
                                                "dag_id": dag.dag_id, "config_path": str(config.source_path)})
        except Exception:
            LOGGER.exception("Failed to register %s DAG", kind,
                             extra={"event": "dag_registration_failed", "dag_kind": kind,
                                    "config_path": str(config.source_path)})
    return dags


def load_vines(entry_file: str, shard: int) -> dict[str, DAG]:
    target = get_parsing_context().dag_id
    loader = _loader(entry_file)
    paths = [p for p in loader.paths("vine") if vine_shard(p.parent.name) == shard]
    if not paths:
        return {}
    from framework.vine_factory import VineFactory

    dags = _register("vine", _configs(loader, paths, "vine", target), VineFactory)
    if target is None and not dags:
        raise RuntimeError(f"No DAGs registered in nonempty Vine shard {shard:02d}; see parser logs")
    return dags


def load_roots(entry_file: str) -> dict[str, DAG]:
    target = get_parsing_context().dag_id
    loader = _loader(entry_file)
    roots = list(_configs(loader, loader.paths("root"), "root", target))
    if not roots and target is not None:
        return {}
    # RootFactory still validates references against actual YAML Vine IDs,
    # including legacy folders whose names do not match their IDs.
    vine_ids = {dag_id for dag_id, _ in _configs(loader, loader.paths("vine"), "vine")}
    if not roots and not vine_ids:
        raise RuntimeError("No valid Root or Vine pipeline configurations were discovered")
    from framework.root_factory import RootFactory

    dags = _register("root", roots, lambda config: RootFactory(config, vine_ids))
    if target is None and roots and not dags:
        raise RuntimeError("No DAGs registered for valid Root IDs; see parser logs")
    return dags
