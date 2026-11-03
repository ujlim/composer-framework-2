# Author: 임의진 (LIM UI JIN)
# Created: 2026-09-09
# Updated: 2026-11-03
"""Build a fresh, complete in-memory catalog before selecting DAG factories."""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from framework.models import LoadedConfig
from framework.utils import require_mapping, validate_airflow_id

LOGGER = logging.getLogger(__name__)


def _catalog_id(value: str) -> str:
    dag_id = validate_airflow_id(value, 'dag_id')
    if len(dag_id) > 250:
        raise ValueError('DAG ID exceeds Airflow length limit')
    return dag_id


def _inspect_yaml(stream: str) -> tuple[bool, set[str]]:
    """Detect repeated mapping keys and collect valid ID aliases before loading.

    A normal SafeLoader keeps only the final duplicate key. Inspection happens
    on the syntax tree so all valid aliases in a rejected document are blocked.
    """
    node = yaml.compose(stream, Loader=yaml.SafeLoader)
    aliases: set[str] = set()
    duplicate = False
    visited: set[int] = set()

    def walk(value):
        nonlocal duplicate
        if id(value) in visited:
            return
        visited.add(id(value))
        if isinstance(value, yaml.MappingNode):
            keys = set()
            for key, child in value.value:
                # Complex keys cannot be handled safely by the application.
                if not isinstance(key, yaml.ScalarNode):
                    duplicate = True
                else:
                    identity = (key.tag, key.value)
                    if identity in keys:
                        duplicate = True
                    keys.add(identity)
                walk(child)
        elif isinstance(value, yaml.SequenceNode):
            for child in value.value:
                walk(child)

    def effective_values(mapping, name, seen=None):
        # Explicit values override inherited ones. When explicit keys repeat,
        # retain every value rather than picking one. In a merge sequence YAML
        # gives precedence to the first source containing the key.
        seen = set() if seen is None else seen
        if not isinstance(mapping, yaml.MappingNode) or id(mapping) in seen:
            return []
        seen = seen | {id(mapping)}
        direct = [value for key, value in mapping.value
                  if isinstance(key, yaml.ScalarNode)
                  and key.tag != 'tag:yaml.org,2002:merge' and key.value == name]
        if direct:
            return direct
        found = []
        for key, value in mapping.value:
            if isinstance(key, yaml.ScalarNode) and key.tag == 'tag:yaml.org,2002:merge':
                sources = value.value if isinstance(value, yaml.SequenceNode) else [value]
                for source in sources:
                    values = effective_values(source, name, seen)
                    if values:
                        found.extend(values)
                        break
        return found

    for kind in ('root', 'vine'):
        for section in effective_values(node, kind):
            for value in effective_values(section, kind + '_id'):
                if (isinstance(value, yaml.ScalarNode)
                        and value.tag == 'tag:yaml.org,2002:str'):
                    try:
                        aliases.add(_catalog_id(value.value))
                    except ValueError:
                        pass  # Never log arbitrary invalid values.
    if node is not None:
        walk(node)
    return duplicate, aliases


@dataclass(frozen=True)
class PipelineCatalog:
    roots: list[LoadedConfig]
    vines: list[LoadedConfig]
    # A list of paths is retained for every blocked identifier; never choose a winner.
    blocked_ids: dict[str, tuple[str, ...]] = field(default_factory=dict)
    affected_roots: dict[str, dict[str, object]] = field(default_factory=dict)
    discovered_count: int = 0

    @property
    def vine_ids(self) -> set[str]:
        return {config.id for config in self.vines}


class PipelineCatalogLoader:
    """Load all root and vine configs, then quarantine ambiguous DAG IDs."""

    def __init__(self, pipelines_dir: Path):
        self.pipelines_dir = pipelines_dir

    def load(self) -> PipelineCatalog:
        rejected: dict[str, set[str]] = defaultdict(set)
        roots, root_count = self._load_group(self.pipelines_dir / 'roots', 'root', rejected)
        vines, vine_count = self._load_group(self.pipelines_dir / 'vines', 'vine', rejected)
        definitions: dict[str, list[LoadedConfig]] = defaultdict(list)
        for config in roots + vines:
            definitions[config.id].append(config)

        blocked: dict[str, tuple[str, ...]] = {}
        for dag_id in definitions.keys() | rejected.keys():
            configs = definitions.get(dag_id, [])
            if len(configs) > 1 or dag_id in rejected:
                blocked[dag_id] = tuple(sorted({str(c.source_path) for c in configs} | rejected[dag_id]))

        affected: dict[str, dict[str, object]] = {}
        for root in roots:
            # Match RootFactory's normalized vine_id, but inspect every root in the
            # full catalog, not only the target selected for construction.
            refs = root.data.get('vines')
            if not isinstance(refs, list):
                continue  # the factory will report the invalid root independently
            hits = sorted({item['vine_id'].strip() for item in refs
                           if isinstance(item, dict) and isinstance(item.get('vine_id'), str)
                           and item['vine_id'].strip() in blocked})
            if hits:
                affected[str(root.source_path)] = {'dag_id': root.id, 'blocked_ids': hits}

        return PipelineCatalog(
            roots=[c for c in roots if c.id not in blocked and str(c.source_path) not in affected],
            vines=[c for c in vines if c.id not in blocked],
            blocked_ids=blocked, affected_roots=affected,
            discovered_count=root_count + vine_count,
        )

    def _load_group(self, group_dir: Path, expected_key: str,
                    rejected: dict[str, set[str]]) -> tuple[list[LoadedConfig], int]:
        configs: list[LoadedConfig] = []
        paths = sorted(group_dir.glob('*/config.yaml')) if group_dir.exists() else []
        if not group_dir.exists():
            LOGGER.warning('Pipeline directory does not exist: %s', group_dir)
        for path in paths:
            try:
                with path.open('r', encoding='utf-8') as stream:
                    content = stream.read()
                repeated, aliases = _inspect_yaml(content)
                if repeated:
                    for dag_id in aliases:
                        rejected[dag_id].add(str(path))
                    raise ValueError('Repeated YAML mapping key')
                data = require_mapping(yaml.safe_load(content) or {}, str(path))
                # A dual-section document has no unambiguous identity. Block any
                # identifiable aliases, including matches in otherwise valid files.
                if 'root' in data and 'vine' in data:
                    for kind in ('root', 'vine'):
                        section = data.get(kind)
                        if isinstance(section, dict):
                            value = section.get(kind + '_id')
                            try:
                                rejected[_catalog_id(value)].add(str(path))
                            except ValueError:
                                pass
                    raise ValueError('Config contains both root and vine sections')
                section = require_mapping(data.get(expected_key), expected_key)
                raw_id = section.get(expected_key + '_id')
                try:
                    _catalog_id(raw_id)
                except ValueError:
                    raise
                configs.append(LoadedConfig(data=data, source_path=path))
            except Exception:
                # Do not log YAML contents or exception text, which can contain secrets.
                LOGGER.error('Unable to load pipeline config: %s', path,
                             extra={'event': 'pipeline_config_invalid', 'config_path': str(path)})
        return configs, len(paths)
