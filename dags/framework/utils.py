from __future__ import annotations

import re
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Iterable

ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


def require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a mapping")
    return value


def require_list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")
    return value


def require_non_empty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def validate_airflow_id(value: str, name: str) -> str:
    value = require_non_empty_string(value, name)
    if not ID_PATTERN.fullmatch(value):
        raise ValueError(
            f"{name}='{value}' contains unsupported characters; "
            "use letters, numbers, '.', '_' or '-'"
        )
    return value


def resolve_child_file(base_dir: Path, relative_path: str) -> Path:
    """
    Resolve a developer-owned file while preventing ../ path traversal.
    """
    candidate = (base_dir / relative_path).resolve()
    base = base_dir.resolve()
    if candidate != base and base not in candidate.parents:
        raise ValueError(f"Path escapes pipeline directory: {relative_path}")
    if not candidate.is_file():
        raise FileNotFoundError(f"Referenced file does not exist: {candidate}")
    return candidate


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Small recursive merge used only for framework defaults."""
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def validate_dependency_graph(
    node_ids: Iterable[str],
    dependencies: dict[str, list[str]],
    graph_name: str,
) -> None:
    """
    Validate missing references and cycles with Kahn's algorithm.
    """
    ids = list(node_ids)
    id_set = set(ids)

    if len(ids) != len(id_set):
        raise ValueError(f"{graph_name}: duplicate IDs found")

    indegree = {node_id: 0 for node_id in ids}
    children: dict[str, list[str]] = defaultdict(list)

    for node_id in ids:
        for parent in dependencies.get(node_id, []):
            if parent not in id_set:
                raise ValueError(
                    f"{graph_name}: '{node_id}' depends on unknown ID '{parent}'"
                )
            if parent == node_id:
                raise ValueError(f"{graph_name}: '{node_id}' cannot depend on itself")
            children[parent].append(node_id)
            indegree[node_id] += 1

    queue = deque(node_id for node_id, degree in indegree.items() if degree == 0)
    visited = 0

    while queue:
        current = queue.popleft()
        visited += 1
        for child in children[current]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if visited != len(ids):
        cyclic = sorted(node_id for node_id, degree in indegree.items() if degree > 0)
        raise ValueError(f"{graph_name}: dependency cycle detected near {cyclic}")
