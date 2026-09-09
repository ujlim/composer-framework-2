# Author: LIM UI JIN
# Created: 2026-09-09

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LoadedConfig:
    """A parsed YAML config plus its owning directory."""

    data: dict[str, Any]
    source_path: Path

    @property
    def base_dir(self) -> Path:
        return self.source_path.parent

    @property
    def id(self) -> str:
        if "root" in self.data:
            return str(self.data["root"]["root_id"])
        return str(self.data["vine"]["vine_id"])
