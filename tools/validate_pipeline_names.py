#!/usr/bin/env python3
# Author: 임의진 (LIM UI JIN)
# Created: 2026-11-02
# Updated: 2026-11-02
"""Validate a fresh pipeline name catalog and resolve a DAG ID or folder name.

No Airflow imports, cloud operations, persistent index, or filesystem writes.
Folder/ID mismatches are warnings unless --require-id-folder-match is set.
Any duplicate ID, folder name, or cross-namespace alias makes lookup unsafe.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

import yaml

ID_PATTERN = re.compile(r"[A-Za-z0-9_.-]+\Z")


class UniqueKeyLoader(yaml.SafeLoader):
    """Do not silently overwrite a misspelled/duplicated YAML mapping key."""


def _unique_mapping(loader, node, deep=False):
    # Validate only keys written in this mapping. YAML merge keys intentionally
    # allow an explicit value to override an inherited one, just like safe_load.
    seen = set()
    for key_node, _ in node.value:
        if key_node.tag == "tag:yaml.org,2002:merge":
            key = ("merge", "<<")
        else:
            key = ("key", loader.construct_object(key_node, deep=deep))
        if key in seen:
            raise ValueError(f"Duplicate YAML key: {key[1]!r}")
        seen.add(key)
    loader.flatten_mapping(node)
    return yaml.SafeLoader.construct_mapping(loader, node, deep=deep)



UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _unique_mapping)


def inspect_catalog(pipelines_dir: Path, *, require_id_folder_match=False):
    """Return deterministic entries, errors and mismatch warnings; never skip errors."""
    base = pipelines_dir.resolve()
    entries, errors, warnings = [], [], []
    if not base.is_dir():
        return {"entries": [], "errors": [f"Pipelines directory does not exist: {base}"], "warnings": []}
    for group, kind in (("roots", "root"), ("vines", "vine")):
        for path in sorted((base / group).glob("*/config.yaml")):
            try:
                resolved = path.resolve()
                if base not in resolved.parents:
                    raise ValueError("Config path escapes pipelines directory")
                with path.open(encoding="utf-8") as stream:
                    data = yaml.load(stream, Loader=UniqueKeyLoader)
                if not isinstance(data, dict):
                    raise ValueError("Config must be a mapping")
                if set(data) & {"root", "vine"} != {kind}:
                    raise ValueError(f"Exactly the '{kind}' section is required (not both root and vine)")
                section = data[kind]
                if not isinstance(section, dict):
                    raise ValueError(f"'{kind}' must be a mapping")
                raw = section.get(f"{kind}_id")
                if not isinstance(raw, str) or not raw.strip():
                    raise ValueError(f"'{kind}_id' must be a nonempty string")
                dag_id = raw.strip()
                if not ID_PATTERN.fullmatch(dag_id) or len(dag_id) > 250:
                    raise ValueError("DAG ID must be at most 250 ASCII letters, digits, '.', '_' or '-'")
                folder = path.parent.name
                entry = {"kind": kind, "dag_id": dag_id, "folder_name": folder,
                         "config_path": str(resolved), "folder_path": str(path.parent.resolve())}
                entries.append(entry)
                if dag_id != folder:
                    message = f"ID/folder mismatch: DAG '{dag_id}', folder '{folder}', config={path}"
                    (errors if require_id_folder_match else warnings).append(message)
            except (OSError, ValueError, TypeError, yaml.YAMLError) as exc:
                errors.append(f"Invalid config {path}: {exc}")
    if not entries and not errors:
        errors.append(f"No pipeline configurations found under {base}")
    for label, field in (("DAG ID", "dag_id"), ("folder name", "folder_name")):
        names = {}
        for item in entries:
            names.setdefault(item[field], []).append(item["config_path"])
        for name, paths in sorted(names.items()):
            if len(paths) > 1:
                errors.append(f"Duplicate {label} '{name}': {'; '.join(paths)}")
    aliases = {}
    for item in entries:
        for name in {item["dag_id"], item["folder_name"]}:
            aliases.setdefault(name, set()).add(item["config_path"])
    for name, paths in sorted(aliases.items()):
        if len(paths) > 1:
            errors.append(f"Ambiguous name '{name}' maps to multiple configs: {'; '.join(sorted(paths))}")
    return {"entries": entries, "errors": errors, "warnings": warnings}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipelines-dir", type=Path,
                        default=Path(__file__).resolve().parents[1] / "dags" / "pipelines")
    parser.add_argument("--name", help="Exact, case-sensitive DAG ID or folder name to resolve")
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--require-id-folder-match", action="store_true")
    args = parser.parse_args(argv)
    report = inspect_catalog(args.pipelines_dir, require_id_folder_match=args.require_id_folder_match)
    report["lookup"] = None
    if args.name is not None and not report["errors"]:
        matches = [item for item in report["entries"] if args.name in {item["dag_id"], item["folder_name"]}]
        if len(matches) == 1:
            report["lookup"] = matches[0]
        else:
            report["errors"].append(f"Name not found: {args.name!r}")
    report["valid"] = not report["errors"]
    if args.as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for warning in report["warnings"]:
            print(f"WARNING: {warning}", file=sys.stderr)
        for error in report["errors"]:
            print(f"ERROR: {error}", file=sys.stderr)
        if report["valid"]:
            if args.name is not None:
                print(report["lookup"]["config_path"])
            else:
                print(f"Validated {len(report['entries'])} pipelines: globally unique IDs, folders and aliases")
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
