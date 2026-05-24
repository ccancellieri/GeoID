"""``TileCacheBulkIndexer`` — adapt tile-cache invalidation to the
:class:`BulkIndexer` Protocol so it drains off the OUTBOX (issue #1292).

Despite the ``Indexer`` name, this adapter does NOT index documents — it
*deletes* (marks stale) the cached tiles a feature write made stale, via
the registered :class:`TileStorageProtocol`. It exists so the
write-reactive tile-cache participant rides the exact same retry-guaranteed
OUTBOX drain machinery as the ES secondary indexer, without being a
``CollectionItemsStore`` items driver (tiles aren't entities).

Each :class:`IndexableOp` carries one coalesced batch of affected tiles in
its ``payload`` (encoded by ``tile_cache_sync._encode_tile_payload``). The
adapter expands the payload to tile coordinates and deletes each one.
Deleting an absent tile is a no-op success (idempotent / order-tolerant —
mark-stale beats overwrite), so retries never corrupt state.

Classification:

* All tiles in the op deleted (or already absent) → ``passed``.
* No ``TileStorageProtocol`` registered, or the store errors → the whole
  op goes ``transient`` (retry) — the store may come back; we must not
  drop the obligation.
* A structurally invalid payload (no decodable tiles) → ``passed`` (there
  is nothing to do; retrying won't help and it is not a poison).
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional, Sequence, Tuple
from uuid import UUID

from dynastore.models.protocols.indexing import (
    BulkIndexResult,
    IndexableOp,
)

logger = logging.getLogger(__name__)


class TileCacheBulkIndexer:
    """Drain-side participant that deletes stale tiles for an op batch."""

    indexer_id: str = "tile_cache_invalidator"
    preferred_chunk_size: int = 256

    def __init__(self, provider: Optional[Any] = None) -> None:
        # When ``provider`` is None the indexer resolves the registered
        # ``TileStorageProtocol`` lazily per call, so a provider that
        # registers after construction is still picked up.
        self._provider = provider

    def _resolve_provider(self) -> Optional[Any]:
        if self._provider is not None:
            return self._provider
        from dynastore.modules import get_protocol
        from dynastore.modules.tiles.tiles_module import TileStorageProtocol

        return get_protocol(TileStorageProtocol)

    async def index_bulk(self, ops: Sequence[IndexableOp]) -> BulkIndexResult:
        from dynastore.modules.tiles.tile_cache_sync import (
            SERVED_TILE_FORMATS,
            decode_tile_payload,
        )

        passed: List[UUID] = []
        transient: List[Tuple[UUID, str]] = []
        poison: List[Tuple[UUID, str]] = []

        if not ops:
            return BulkIndexResult(passed=[], transient=[], poison=[])

        provider = self._resolve_provider()
        if provider is None:
            # No reader/store available right now — retry the whole batch;
            # a provider may register (or a degraded store may recover).
            reason = "no TileStorageProtocol registered"
            return BulkIndexResult(
                passed=[],
                transient=[(op.op_id, reason) for op in ops],
                poison=[],
            )

        # Prefer the coordinate-level primitive: it drops EVERY cached variant
        # of a coordinate — all served formats (mvt + pbf) and every
        # effective_cache_id (bare collection, ``@hash`` parameterized,
        # multi-collection comma-joined). The per-format ``delete_tile`` only
        # matches the bare collection id, so suffixed / multi-collection tiles
        # would survive an edit (#1292). Fall back to it for providers that
        # don't implement the variant primitive yet.
        delete_variants = getattr(provider, "delete_tile_variants", None)
        delete_tile = getattr(provider, "delete_tile", None)
        if delete_variants is None and delete_tile is None:
            reason = "TileStorageProtocol has no delete_tile / delete_tile_variants"
            return BulkIndexResult(
                passed=[],
                transient=[(op.op_id, reason) for op in ops],
                poison=[],
            )

        async def _delete_coord(tms_id: str, z: int, x: int, y: int) -> bool:
            if delete_variants is not None:
                ok = await delete_variants(
                    op.catalog_id, op.collection_id, tms_id, z, x, y,
                    SERVED_TILE_FORMATS,
                )
                return ok is not False
            # Legacy fallback: delete every served format under the bare
            # collection id (cache-id-suffixed variants are not reachable here).
            # ``delete_tile`` is non-None here — the guard above returns early
            # only when BOTH primitives are absent.
            assert delete_tile is not None
            all_ok = True
            for fmt in SERVED_TILE_FORMATS:
                ok = await delete_tile(
                    op.catalog_id, op.collection_id, tms_id, z, x, y, fmt,
                )
                if ok is False:
                    all_ok = False
            return all_ok

        for op in ops:
            tiles = decode_tile_payload(op.payload)
            if not tiles:
                # Nothing decodable — nothing to invalidate. Not a failure.
                passed.append(op.op_id)
                continue
            try:
                all_ok = True
                for (tms_id, z, x, y) in tiles:
                    if not await _delete_coord(tms_id, z, x, y):
                        all_ok = False
                if all_ok:
                    passed.append(op.op_id)
                else:
                    transient.append(
                        (op.op_id, "one or more tile deletes reported failure")
                    )
            except Exception as exc:  # noqa: BLE001 — store hiccup = retry
                transient.append((op.op_id, f"{type(exc).__name__}: {exc}"))

        return BulkIndexResult(passed=passed, transient=transient, poison=poison)


__all__: List[str] = ["TileCacheBulkIndexer"]
