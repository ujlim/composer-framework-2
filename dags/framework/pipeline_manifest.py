"""Deployment-time DAG ID index. Runtime reads are side-effect free.

Deploy the manifest and its pipelines as one immutable release. A worker verifies
its selected YAML; only full validation can verify non-selected content.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path, PurePosixPath

import yaml

from framework.models import LoadedConfig
from framework.utils import require_mapping, validate_airflow_id

MANIFEST_NAME = ".dag-manifest.json"
SCHEMA_VERSION = 1


def read_config(path: Path, kind: str, content: bytes | None = None) -> LoadedConfig:
    raw = yaml.safe_load(path.read_bytes() if content is None else content) or {}
    data = require_mapping(raw, str(path))
    section = require_mapping(data.get(kind), f"{path}: {kind}")
    other = "vine" if kind == "root" else "root"
    if other in data:
        raise ValueError(f"Ambiguous root/vine sections: {path}")
    raw_id = section.get(f"{kind}_id")
    dag_id = validate_airflow_id(raw_id, f"{path}: {kind}_id")
    if raw_id != dag_id:
        raise ValueError(f"DAG ID must not contain surrounding whitespace: {path}")
    return LoadedConfig(data=data, source_path=path)


def config_paths(directory: Path) -> list[Path]:
    return sorted([*directory.glob("roots/*/config.yaml"), *directory.glob("vines/*/config.yaml")])


def build_manifest(directory: Path) -> dict:
    """Strict: invalid YAML/IDs or duplicates cannot produce a release index."""
    entries = []
    seen = set()
    for path in config_paths(directory):
        if directory.resolve() not in path.resolve().parents:
            raise ValueError(f"Pipeline config escapes release: {path}")
        kind = "root" if path.parent.parent.name == "roots" else "vine"
        content = path.read_bytes()
        config = read_config(path, kind, content)
        if config.id in seen:
            raise ValueError(f"Duplicate DAG ID: {config.id}")
        seen.add(config.id)
        entries.append({"dag_id": config.id, "kind": kind,
                        "path": path.relative_to(directory).as_posix(),
                        "sha256": hashlib.sha256(content).hexdigest()})
    if not entries:
        raise ValueError(f"No pipeline configurations found: {directory}")
    return {"schema_version": SCHEMA_VERSION, "entries": entries}


def write_manifest(directory: Path) -> Path:
    """Build step only. Never called by an Airflow parse."""
    manifest = build_manifest(directory)
    target = directory / MANIFEST_NAME
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary.replace(target)
    return target


def read_manifest(directory: Path) -> list[dict]:
    manifest = json.loads((directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    if (not isinstance(manifest, dict)
            or type(manifest.get("schema_version")) is not int
            or manifest.get("schema_version") != SCHEMA_VERSION):
        raise ValueError("Unsupported DAG manifest schema; rebuild deployment manifest")
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ValueError("DAG manifest entries must be a non-empty list")
    ids, paths = set(), set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("Invalid DAG manifest entry")
        dag_id = validate_airflow_id(entry.get("dag_id"), "manifest.dag_id")
        kind, relative = entry.get("kind"), entry.get("path")
        if kind not in ("root", "vine") or not isinstance(relative, str):
            raise ValueError("Invalid DAG manifest kind/path")
        parts = PurePosixPath(relative).parts
        if (len(parts) != 3 or parts[0] != kind + "s" or parts[2] != "config.yaml"
                or parts[1] in (".", "..") or "\\" in relative
                or PurePosixPath(relative).as_posix() != relative):
            raise ValueError(f"Unsafe DAG manifest path: {relative}")
        if dag_id != entry["dag_id"] or dag_id in ids or relative in paths:
            raise ValueError(f"Duplicate or invalid DAG manifest ID/path: {dag_id}")
        if not isinstance(entry.get("sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]):
            raise ValueError(f"Invalid DAG manifest digest: {dag_id}")
        ids.add(dag_id)
        paths.add(relative)
    # Directory listing is cheap and does not open non-selected YAML/SQL files.
    actual = {p.relative_to(directory).as_posix() for p in config_paths(directory)}
    if paths != actual:
        raise ValueError("Stale DAG manifest inventory; deploy a consistent release")
    return entries


def load_entry(directory: Path, entry: dict) -> LoadedConfig:
    path = directory / entry["path"]
    if directory.resolve() not in path.resolve().parents:
        raise ValueError(f"DAG manifest path escapes release: {entry['path']}")
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != entry["sha256"]:
        raise ValueError(f"Stale DAG manifest digest: {entry['dag_id']}; redeploy release")
    config = read_config(path, entry["kind"], content)
    if config.id != entry["dag_id"]:
        raise ValueError(f"DAG manifest ID mismatch: {entry['dag_id']}")
    return config


def validate_manifest(directory: Path) -> None:
    """Full release check, including non-target YAML. Run before deployment."""
    for entry in read_manifest(directory):
        load_entry(directory, entry)
