# Author: LIM UI JIN
# Created: 2026-09-09

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from framework.models import LoadedConfig
from framework.pipeline_manifest import MANIFEST_NAME, load_entry, read_manifest
from framework.utils import require_mapping

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineCatalog:
    roots: list[LoadedConfig]
    vines: list[LoadedConfig]
    indexed_vine_ids: frozenset[str] | None = None
    total_count: int | None = None
    parse_mode: str = "full"

    @property
    def vine_ids(self) -> set[str]:
        if self.indexed_vine_ids is not None:
            return set(self.indexed_vine_ids)
        return {config.id for config in self.vines}


class PipelineCatalogLoader:
    """
    Loads developer-owned YAML files.

    Expected paths:
      pipelines/roots/*/config.yaml
      pipelines/vines/*/config.yaml
    """

    def __init__(self, pipelines_dir: Path):
        self.pipelines_dir = pipelines_dir

    def load(self, target_dag_id: str | None = None) -> PipelineCatalog:
        manifest_path = self.pipelines_dir / MANIFEST_NAME
        disabled = os.environ.get("FRAMEWORK_DISABLE_SELECTIVE_PARSING") == "1"
        if manifest_path.exists() and not disabled:
            entries = read_manifest(self.pipelines_dir)
            selected = entries if target_dag_id is None else [
                entry for entry in entries if entry["dag_id"] == target_dag_id
            ]
            if not selected:
                raise ValueError(f"Requested DAG not in deployment manifest: {target_dag_id}")
            roots, vines = [], []
            for entry in selected:
                config = load_entry(self.pipelines_dir, entry)
                (roots if entry["kind"] == "root" else vines).append(config)
            return PipelineCatalog(
                roots=roots, vines=vines,
                indexed_vine_ids=frozenset(e["dag_id"] for e in entries if e["kind"] == "vine"),
                total_count=len(entries),
                parse_mode="manifest_full" if target_dag_id is None else "manifest_target",
            )
        if target_dag_id is not None:
            LOGGER.warning(
                "Selective YAML fast path unavailable (%s); loading full catalog for target %s",
                "disabled" if disabled else "missing manifest", target_dag_id,
            )
        roots = self._load_group(self.pipelines_dir / "roots", "root")
        vines = self._load_group(self.pipelines_dir / "vines", "vine")
        self._validate_unique_ids(roots, "root")
        self._validate_unique_ids(vines, "vine")
        self._validate_unique_ids(roots + vines, "DAG")
        return PipelineCatalog(roots=roots, vines=vines, total_count=len(roots) + len(vines),
                               parse_mode="full" if target_dag_id is None else "full_fallback")

    def _load_group(self, group_dir: Path, expected_key: str) -> list[LoadedConfig]:
        configs: list[LoadedConfig] = []
        if not group_dir.exists():
            LOGGER.warning("Pipeline directory does not exist: %s", group_dir)
            return configs

        for path in sorted(group_dir.glob("*/config.yaml")):
            try:
                with path.open("r", encoding="utf-8") as stream:
                    raw = yaml.safe_load(stream) or {}
                data = require_mapping(raw, str(path))
                if expected_key not in data:
                    raise ValueError(f"Top-level '{expected_key}' section is required")
                configs.append(LoadedConfig(data=data, source_path=path))
            except Exception:
                # Keep loading other configs. Factory-level validation is also isolated in main.py.
                LOGGER.exception("Unable to load pipeline config: %s", path)

        return configs

    @staticmethod
    def _validate_unique_ids(configs: list[LoadedConfig], kind: str) -> None:
        seen: dict[str, Path] = {}
        for config in configs:
            config_id = config.id
            if config_id in seen:
                raise ValueError(
                    f"Duplicate {kind} ID '{config_id}': "
                    f"{seen[config_id]} and {config.source_path}"
                )
            seen[config_id] = config.source_path
