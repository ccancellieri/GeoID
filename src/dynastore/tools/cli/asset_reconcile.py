#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.

"""Asset reconcile CLI — disaster-recovery drift sweep.

Walks PG asset rows, compares each row against the routed ``AssetStore``
read driver (e.g. Elasticsearch) and reports drift. With ``--apply`` it
replays a fresh ``ASSET_UPDATE`` event through the outbox so the existing
``AssetEntitySyncSubscriber`` (and ``BucketAnnotationPatcher``, if loaded)
re-mirror the row to every downstream driver.

Run::

    python -m dynastore.tools.cli.asset_reconcile [--catalog X] [--collection Y] [--dry-run]

``DATABASE_URL`` must be set; dynastore plugin discovery is performed so
``AssetsProtocol`` and the read driver are available.

Exit codes:
    0 — no drift, or apply mode succeeded for all drifted rows
    1 — drift detected (dry-run) or apply mode reported errors
    2 — invocation error (missing protocols, bad args)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import List, Optional

logger = logging.getLogger("dynastore.asset_reconcile")


async def _discover() -> None:
    from dynastore.tools.discovery import discover_and_load_plugins

    discover_and_load_plugins("dynastore.modules")
    discover_and_load_plugins("dynastore.extensions")


def _normalise_doc(doc: dict) -> dict:
    """Project an asset doc to the canonical comparison set."""
    asset_type = doc.get("asset_type")
    asset_type_str = (
        getattr(asset_type, "value", None)
        if asset_type is not None and hasattr(asset_type, "value")
        else asset_type
    )
    return {
        "asset_id": doc.get("asset_id"),
        "catalog_id": doc.get("catalog_id"),
        "collection_id": doc.get("collection_id"),
        "uri": doc.get("uri"),
        "owned_by": doc.get("owned_by"),
        "asset_type": asset_type_str,
    }


async def _iter_catalog_ids(
    only_catalog: Optional[str],
) -> List[str]:
    if only_catalog:
        return [only_catalog]
    from dynastore.modules import get_protocol
    from dynastore.models.protocols.catalogs import CatalogsProtocol

    catalogs_svc = get_protocol(CatalogsProtocol)
    if catalogs_svc is None:
        raise RuntimeError("CatalogsProtocol not registered")

    out: List[str] = []
    offset = 0
    page = 100
    while True:
        rows = await catalogs_svc.list_catalogs(limit=page, offset=offset)
        if not rows:
            break
        out.extend(c.id for c in rows)
        if len(rows) < page:
            break
        offset += page
    return out


async def _iter_assets(
    assets_svc,
    catalog_id: str,
    collection_id: Optional[str],
):
    offset = 0
    page = 100
    while True:
        rows = await assets_svc.list_assets(
            catalog_id=catalog_id,
            collection_id=collection_id,
            limit=page,
            offset=offset,
        )
        if not rows:
            return
        for row in rows:
            yield row
        if len(rows) < page:
            return
        offset += page


async def _read_index_doc(catalog_id: str, collection_id: Optional[str], asset_id: str):
    """Best-effort: read from the routed index driver, return None on miss/error."""
    from dynastore.modules.storage.router import get_asset_index_drivers

    indexers = await get_asset_index_drivers(catalog_id, collection_id)
    if not indexers:
        return None
    for entry in indexers:
        try:
            doc = await entry.driver.get_asset(
                catalog_id, asset_id, collection_id=collection_id,
            )
        except Exception as exc:
            logger.debug(
                "reconcile: indexer '%s' get_asset failed for %s/%s: %s",
                entry.driver_ref, catalog_id, asset_id, exc,
            )
            continue
        if doc is not None:
            return doc
    return None


async def _replay_event(asset_doc: dict) -> bool:
    """Re-emit ASSET_UPDATE through the bridge so subscribers re-mirror.

    Returns True on emit-success.
    """
    from dynastore.modules.catalog.asset_service import AssetEventType
    from dynastore.modules.catalog.catalog_module import _asset_event_bridge

    try:
        await _asset_event_bridge(
            AssetEventType.ASSET_UPDATED, asset_doc, db_resource=None,
        )
        return True
    except Exception as exc:
        logger.warning(
            "reconcile: replay failed for %s/%s: %s",
            asset_doc.get("catalog_id"), asset_doc.get("asset_id"), exc,
        )
        return False


async def cmd_reconcile(args: argparse.Namespace) -> int:
    await _discover()
    from dynastore.modules import get_protocol
    from dynastore.models.protocols.assets import AssetsProtocol

    assets_svc = get_protocol(AssetsProtocol)
    if assets_svc is None:
        print("AssetsProtocol not registered", file=sys.stderr)
        return 2

    catalogs = await _iter_catalog_ids(args.catalog)
    if not catalogs:
        print("no catalogs found")
        return 0

    total = 0
    drifted = 0
    replayed = 0
    failed = 0

    for catalog_id in catalogs:
        async for asset in _iter_assets(assets_svc, catalog_id, args.collection):
            total += 1
            pg_doc = asset.model_dump() if hasattr(asset, "model_dump") else dict(asset)
            pg_canon = _normalise_doc(pg_doc)
            idx_doc = await _read_index_doc(
                catalog_id, asset.collection_id, asset.asset_id,
            )
            if idx_doc is not None:
                idx_canon = _normalise_doc(idx_doc)
                if pg_canon == idx_canon:
                    continue
            drifted += 1
            print(
                f"DRIFT  {catalog_id}/{asset.collection_id or '<catalog-tier>'}/"
                f"{asset.asset_id}  (index={'missing' if idx_doc is None else 'stale'})"
            )
            if not args.apply:
                continue
            if await _replay_event(pg_doc):
                replayed += 1
            else:
                failed += 1

    print(
        f"checked={total} drifted={drifted} replayed={replayed} failed={failed}"
    )
    if args.apply:
        return 1 if failed else 0
    return 1 if drifted else 0


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        "dynastore-asset-reconcile",
        description="Detect and (with --apply) repair PG↔index asset drift.",
    )
    p.add_argument("--catalog", default=None, help="restrict to a single catalog id")
    p.add_argument(
        "--collection", default=None,
        help="restrict to a single collection id (requires --catalog)",
    )
    p.add_argument(
        "--apply", action="store_true",
        help="replay ASSET_UPDATE events for drifted rows (default: dry-run)",
    )
    p.add_argument(
        "--log-level", default=os.environ.get("LOG_LEVEL", "WARNING"),
    )
    args = p.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.WARNING),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.collection and not args.catalog:
        print("--collection requires --catalog", file=sys.stderr)
        return 2

    return asyncio.run(cmd_reconcile(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
