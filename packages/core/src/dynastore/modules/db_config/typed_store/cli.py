#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Schema CLI for the typed-config store.

JSON schemas are not persisted; they are generated on demand from each
registered class. These subcommands operate on the live class registry and on
the ``schema_id`` version tags actually serialized in config rows.

Subcommands:

* ``list``   — print ``(class_key, current schema_id)`` for every registered
  ``PersistentModel`` (generated from the class, no DB read).
* ``audit``  — for every registered ``PersistentModel``, report whether its
  current ``schema_id`` matches the version tags serialized in
  ``configs.platform_configs`` rows, and whether each distinct stored
  ``schema_id`` has a migrator path to the current hash. Exits non-zero on
  drift. (Audits platform-level rows; per-tenant config tables are not
  enumerated.)
* ``diff``   — pretty JSON-schema diff between two registered classes' current
  schemas (by ``class_key``).

Run::

    python -m dynastore.modules.db_config.typed_store.cli list
    python -m dynastore.modules.db_config.typed_store.cli audit
    python -m dynastore.modules.db_config.typed_store.cli diff <class_key_a> <class_key_b>

``DATABASE_URL`` must be set for ``audit``; dynastore plugin discovery is
performed so every ``PersistentModel`` subclass is known to
:class:`TypedModelRegistry`.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any, Dict, List

from sqlalchemy.ext.asyncio import create_async_engine

from dynastore.modules.db_config.typed_store import config_queries as _cq
from dynastore.tools.typed_store import TypedModelRegistry
from dynastore.tools.typed_store.migrations import find_path


def _engine(url: str | None = None):
    url = url or os.environ["DATABASE_URL"]
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return create_async_engine(url)


async def _discover() -> None:
    """Import all dynastore modules so every PersistentModel is registered."""
    # Lazy import to avoid pulling the world when the CLI is --help'd.
    from dynastore.tools.discovery import discover_and_load_plugins

    try:
        discover_and_load_plugins("dynastore.modules")
        discover_and_load_plugins("dynastore.extensions")
    except Exception:  # pragma: no cover - best-effort
        pass


async def cmd_list(args: argparse.Namespace) -> int:
    await _discover()
    for model in TypedModelRegistry.all().values():
        print(f"{model.class_key()}\t{model.schema_id()}")
    return 0


async def cmd_audit(args: argparse.Namespace) -> int:
    await _discover()
    engine = _engine(args.database_url)
    async with engine.connect() as conn:
        stored = await _cq.list_platform_config_schema_ids.execute(conn)
    await engine.dispose()
    stored = stored or []

    by_key: Dict[str, List[str]] = {}
    for class_key, schema_id in stored:
        by_key.setdefault(class_key, []).append(schema_id)

    drift: List[str] = []
    missing_migrator: List[str] = []
    ok = 0

    for model in TypedModelRegistry.all().values():
        key = model.class_key()
        current = model.schema_id()
        stored_ids = by_key.get(key, [])

        if current not in stored_ids and stored_ids:
            drift.append(f"{key}: current {current!s} not in serialized rows")

        for sid in stored_ids:
            if sid == current:
                continue
            try:
                find_path(sid, current)
                ok += 1
            except LookupError:
                missing_migrator.append(
                    f"{key}: no migrator path {sid} -> {current}"
                )

    for line in drift:
        print(f"DRIFT   {line}")
    for line in missing_migrator:
        print(f"MISSING {line}")
    print(f"checked={len(TypedModelRegistry.all())} "
          f"migratable={ok} drift={len(drift)} missing={len(missing_migrator)}")
    return 1 if (drift or missing_migrator) else 0


async def cmd_diff(args: argparse.Namespace) -> int:
    await _discover()
    by_key: Dict[str, Any] = {
        m.class_key(): m for m in TypedModelRegistry.all().values()
    }
    if args.class_key_a not in by_key or args.class_key_b not in by_key:
        print("one or both class_keys not registered", file=sys.stderr)
        return 2
    a = json.dumps(
        by_key[args.class_key_a].model_json_schema(), indent=2, sort_keys=True
    ).splitlines()
    b = json.dumps(
        by_key[args.class_key_b].model_json_schema(), indent=2, sort_keys=True
    ).splitlines()
    import difflib

    for line in difflib.unified_diff(
        a, b, fromfile=args.class_key_a, tofile=args.class_key_b, lineterm=""
    ):
        print(line)
    return 0


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser("dynastore-schemas")
    p.add_argument("--database-url", default=None, help="overrides $DATABASE_URL")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    sub.add_parser("audit")
    d = sub.add_parser("diff")
    d.add_argument("class_key_a")
    d.add_argument("class_key_b")
    args = p.parse_args(argv)

    handlers = {"list": cmd_list, "audit": cmd_audit, "diff": cmd_diff}
    return asyncio.run(handlers[args.cmd](args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
