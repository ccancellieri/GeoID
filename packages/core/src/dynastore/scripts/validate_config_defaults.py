#!/usr/bin/env python3
"""Validate JSON seed files against the registered ``PluginConfig`` classes.

Walks a directory of ``*.json`` config-seed files and asserts each
``class_key`` resolves via ``resolve_config_class`` and each ``value`` parses
under the resolved ``PluginConfig``'s pydantic schema. Designed for CI: catches
the silent-typo class of bug (PascalCase ``class_key`` that the seeder would
silently skip) before deploy.

Usage::

    python -m dynastore.scripts.validate_config_defaults [DEFAULTS_DIR ...]

If no directory is passed, defaults to ``$DYNASTORE_CONFIG_ROOT/defaults``
(falling back to the dynastore-installed default).

Exit codes: ``0`` clean, ``1`` validation failure, ``2`` usage error.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Tuple


def _discover_plugin_configs() -> None:
    """Trigger entry-point loading so PluginConfig subclasses register."""
    from dynastore.tools.discovery import discover_and_load_plugins

    discover_and_load_plugins("dynastore.modules")
    discover_and_load_plugins("dynastore.extensions")


def _validate_file(path: Path) -> List[str]:
    """Return a list of human-readable problems for one file (empty = clean)."""
    from dynastore.modules.db_config.platform_config_service import (
        list_registered_configs,
        resolve_config_class,
    )

    errs: List[str] = []
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return [f"unreadable: {exc}"]

    if not isinstance(payload, dict):
        return ["top-level JSON must be an object"]

    class_key = payload.get("class_key")
    if not class_key:
        errs.append("missing 'class_key'")
        return errs
    if not isinstance(class_key, str):
        return [f"'class_key' must be a string, got {type(class_key).__name__}"]

    cls = resolve_config_class(class_key)
    if cls is None:
        registered = sorted(list_registered_configs().keys())
        hint = ""
        snake_guess = class_key[0].lower() + "".join(
            ("_" + c.lower()) if c.isupper() else c for c in class_key[1:]
        )
        if snake_guess != class_key and snake_guess in registered:
            hint = f" (did you mean {snake_guess!r}? class_key uses snake_case)"
        errs.append(f"unknown class_key {class_key!r}{hint}")
        return errs

    value = payload.get("value")
    if not isinstance(value, dict):
        errs.append("'value' must be a JSON object")
        return errs

    try:
        cls.model_validate(value)
    except Exception as exc:  # noqa: BLE001 — surface pydantic msg verbatim
        errs.append(f"value does not validate against {cls.__name__}: {exc}")
    return errs


def _default_dirs() -> List[Path]:
    from dynastore.modules.db_config.instance import DEFAULTS_DIR

    return [DEFAULTS_DIR]


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "dirs",
        nargs="*",
        type=Path,
        help="Directory/-ies to scan for *.json seed files. "
             "Defaults to DYNASTORE_CONFIG_ROOT/defaults.",
    )
    args = parser.parse_args(argv)

    _discover_plugin_configs()

    dirs = args.dirs or _default_dirs()
    failures: List[Tuple[Path, List[str]]] = []
    checked = 0
    for d in dirs:
        if not d.exists():
            print(f"validate_config_defaults: {d} does not exist — skipped",
                  file=sys.stderr)
            continue
        for path in sorted(d.glob("*.json")):
            checked += 1
            errs = _validate_file(path)
            if errs:
                failures.append((path, errs))

    if failures:
        for path, errs in failures:
            for e in errs:
                print(f"FAIL {path}: {e}", file=sys.stderr)
        print(
            f"validate_config_defaults: {len(failures)}/{checked} file(s) FAILED",
            file=sys.stderr,
        )
        return 1

    print(f"validate_config_defaults: {checked} file(s) OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
