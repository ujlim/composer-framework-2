# Author: LIM UI JIN
# Created: 2026-09-09

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import yaml

from framework.models import LoadedConfig
from framework.utils import require_mapping

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineCatalog:
    roots: list[LoadedConfig]
    vines: list[LoadedConfig]

    @property
    def vine_ids(self) -> set[str]:
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

    def load(self) -> PipelineCatalog:
        roots = self._load_group(self.pipelines_dir / "roots", "root")
        vines = self._load_group(self.pipelines_dir / "vines", "vine")
        self._validate_unique_ids(roots, "root")
        self._validate_unique_ids(vines, "vine")
        return PipelineCatalog(roots=roots, vines=vines)

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
