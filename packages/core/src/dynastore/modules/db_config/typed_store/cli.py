#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.

"""Schema-registry CLI for the typed-config store.

Subcommands:

* ``list``   — print ``(class_key, schema_id, created_at)`` from ``configs.schemas``.
* ``audit``  — report, for every currently-imported ``PersistentModel``, whether
  its computed ``schema_id`` matches a row in the registry and whether every
  distinct stored ``schema_id`` for that ``class_key`` has a migrator path to the
  current hash. Exits non-zero on drift.
* ``diff``   — pretty JSON-schema diff between two stored schema_ids.

Run::

    python -m dynastore.modules.db_config.typed_store.cli list
    python -m dynastore.modules.db_config.typed_store.cli audit
    python -m dynastore.modules.db_config.typed_store.cli diff <id_a> <id_b>

``DATABASE_URL`` must be set; dynastore plugin discovery is performed so every
``PersistentModel`` subclass is known to :class:`TypedModelRegistry`.
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
    engine = _engine(args.database_url)
    async with engine.connect() as conn:
        rows = await _cq.list_schemas.execute(conn)
    for class_key, schema_id, created_at in rows or []:
        print(f"{class_key}\t{schema_id}\t{created_at.isoformat()}")
    await engine.dispose()
    return 0


async def cmd_audit(args: argparse.Namespace) -> int:
    await _discover()
    engine = _engine(args.database_url)
    async with engine.connect() as conn:
        stored = await _cq.list_schemas_keys.execute(conn)
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
            drift.append(f"{key}: current {current!s} not in registry")

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
    engine = _engine(args.database_url)
    async with engine.connect() as conn:
        rows = await _cq.get_schemas_by_ids.execute(conn, ids=[args.id_a, args.id_b])
    await engine.dispose()
    by_id: Dict[str, Any] = {sid: js for sid, js in rows or []}
    if args.id_a not in by_id or args.id_b not in by_id:
        print("one or both schema_ids not found", file=sys.stderr)
        return 2
    a = json.dumps(by_id[args.id_a], indent=2, sort_keys=True).splitlines()
    b = json.dumps(by_id[args.id_b], indent=2, sort_keys=True).splitlines()
    import difflib

    for line in difflib.unified_diff(a, b, fromfile=args.id_a, tofile=args.id_b, lineterm=""):
        print(line)
    return 0


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser("dynastore-schemas")
    p.add_argument("--database-url", default=None, help="overrides $DATABASE_URL")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    sub.add_parser("audit")
    d = sub.add_parser("diff")
    d.add_argument("id_a")
    d.add_argument("id_b")
    args = p.parse_args(argv)

    handlers = {"list": cmd_list, "audit": cmd_audit, "diff": cmd_diff}
    return asyncio.run(handlers[args.cmd](args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
