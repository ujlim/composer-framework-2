# Author: LIM UI JIN
# Created: 2026-09-09
# Updated: 2026-11-10 - isolate invalid DAG configurations

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from framework.models import LoadedConfig
from framework.utils import require_mapping, validate_airflow_id

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineIssue:
    """A rejected configuration with stable, searchable diagnostics."""

    error_code: str
    dag_kind: str
    config_path: Path
    error: str
    dag_id: str | None = None
    related_paths: tuple[Path, ...] = ()

    def as_dict(self) -> dict:
        return {
            "error_code": self.error_code,
            "dag_kind": self.dag_kind,
            "config_path": str(self.config_path),
            "dag_id": self.dag_id,
            "error": self.error,
            "related_paths": [str(path) for path in self.related_paths],
        }


@dataclass(frozen=True)
class PipelineCatalog:
    roots: list[LoadedConfig]
    vines: list[LoadedConfig]
    errors: list[PipelineIssue] = field(default_factory=list)

    @property
    def vine_ids(self) -> set[str]:
        """Valid YAML Vine IDs, not necessarily DAGs registered by Airflow."""
        return {config.id for config in self.vines}


class PipelineCatalogLoader:
    """Discover each one-level Root/Vine directory and quarantine bad entries.

    A duplicate ID is *never* resolved by file order: all configurations
    claiming it (including Root versus Vine) are rejected together.
    """

    def __init__(self, pipelines_dir: Path):
        self.pipelines_dir = pipelines_dir

    def load(self) -> PipelineCatalog:
        errors: list[PipelineIssue] = []
        roots = self._load_group(self.pipelines_dir / "roots", "root", errors)
        vines = self._load_group(self.pipelines_dir / "vines", "vine", errors)

        # DAG IDs share one namespace across Roots and Vines.
        by_id: dict[str, list[tuple[str, LoadedConfig]]] = defaultdict(list)
        for kind, configs in (("root", roots), ("vine", vines)):
            for config in configs:
                by_id[config.id].append((kind, config))

        rejected_paths: set[Path] = set()
        for dag_id, entries in sorted(by_id.items()):
            if len(entries) < 2:
                continue
            paths = tuple(sorted((config.source_path for _, config in entries)))
            for kind, config in entries:
                rejected_paths.add(config.source_path)
                errors.append(
                    PipelineIssue(
                        error_code="DUPLICATE_DAG_ID",
                        dag_kind=kind,
                        config_path=config.source_path,
                        dag_id=dag_id,
                        related_paths=paths,
                        error=(
                            f"DAG ID '{dag_id}' is defined in multiple configurations: "
                            + ", ".join(map(str, paths))
                            + ". All conflicting DAGs are quarantined."
                        ),
                    )
                )

        return PipelineCatalog(
            roots=[c for c in roots if c.source_path not in rejected_paths],
            vines=[c for c in vines if c.source_path not in rejected_paths],
            errors=errors,
        )

    def _load_group(
        self, group_dir: Path, kind: str, errors: list[PipelineIssue]
    ) -> list[LoadedConfig]:
        configs: list[LoadedConfig] = []
        if not group_dir.exists():
            LOGGER.warning("Pipeline directory does not exist: %s", group_dir)
            return configs

        # Check each immediate pipeline folder, including missing config.yaml.
        for folder in sorted(group_dir.iterdir()):
            if not folder.is_dir() or folder.name.startswith(".") or folder.name == "__pycache__":
                continue

            path = folder / "config.yaml"
            if not path.is_file():
                errors.append(
                    PipelineIssue(
                        "MISSING_CONFIG_FILE",
                        kind,
                        path,
                        f"Pipeline folder '{folder}' has no config.yaml.",
                    )
                )
                continue

            try:
                with path.open("r", encoding="utf-8") as stream:
                    raw = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                errors.append(
                    PipelineIssue("YAML_PARSE_ERROR", kind, path, str(exc))
                )
                continue
            except (OSError, UnicodeError) as exc:
                errors.append(
                    PipelineIssue("YAML_READ_ERROR", kind, path, str(exc))
                )
                continue

            try:
                data = require_mapping(raw, str(path))
                require_mapping(data.get(kind), f"{path}: top-level '{kind}'")
            except (TypeError, ValueError) as exc:
                errors.append(
                    PipelineIssue("INVALID_CONFIG_STRUCTURE", kind, path, str(exc))
                )
                continue

            try:
                dag_id = validate_airflow_id(
                    data[kind].get(f"{kind}_id"), f"{kind}.{kind}_id"
                )
            except (TypeError, ValueError) as exc:
                errors.append(
                    PipelineIssue("INVALID_DAG_ID", kind, path, str(exc))
                )
                continue

            if folder.name != dag_id:
                errors.append(
                    PipelineIssue(
                        "FOLDER_ID_MISMATCH",
                        kind,
                        path,
                        (
                            f"Folder name '{folder.name}' must exactly match "
                            f"{kind}.{kind}_id '{dag_id}'."
                        ),
                        dag_id=dag_id,
                    )
                )
                continue

            configs.append(LoadedConfig(data=data, source_path=path))

        return configs
