# Author: LIM UI JIN
# Created: 2026-09-09
# Updated: 2026-11-25 by LIM UI JIN

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from framework.models import LoadedConfig
from framework.utils import require_mapping

_YAML_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)

LOGGER = logging.getLogger(__name__)


class PipelineCatalogLoader:
    """
    Loads developer-owned YAML files.

    Expected paths:
      pipelines/roots/*/config.yaml
      pipelines/vines/*/config.yaml
    """

    def __init__(self, pipelines_dir: Path):
        self.pipelines_dir = pipelines_dir

    def paths(self, kind: str) -> list[Path]:
        return sorted((self.pipelines_dir / f"{kind}s").glob("*/config.yaml"))

    @staticmethod
    def read(path: Path, kind: str) -> LoadedConfig | None:
        try:
            with path.open("r", encoding="utf-8") as stream:
                raw = yaml.load(stream, Loader=_YAML_LOADER) or {}
            data = require_mapping(raw, str(path))
            if kind not in data:
                raise ValueError(f"Top-level '{kind}' section is required")
            return LoadedConfig(data=data, source_path=path)
        except Exception:
            LOGGER.exception("Unable to load pipeline config: %s", path)
            return None
