#!/usr/bin/env python3
"""Build/verify a deployment index without importing Airflow or making network calls."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "dags"))
from framework.pipeline_manifest import validate_manifest, write_manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pipelines_dir", type=Path)
    parser.add_argument("--check", action="store_true", help="Verify existing index; do not write")
    args = parser.parse_args()
    if args.check:
        validate_manifest(args.pipelines_dir)
        print("Manifest matches all pipeline YAML files")
    else:
        print(write_manifest(args.pipelines_dir))


if __name__ == "__main__":
    main()
