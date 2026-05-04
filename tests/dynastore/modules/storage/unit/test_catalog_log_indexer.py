"""Unit tests for :class:`LogCatalogIndexer` (M3.2).

Covers:

- Protocol satisfaction: ``LogCatalogIndexer`` is structurally
  compatible with :class:`CatalogStore`.
- ``get_catalog_metadata`` is the deliberate no-op.
- ``upsert_catalog_metadata`` logs the sorted key list, not values.
- ``delete_catalog_metadata`` logs ``soft`` flag alongside id.
- ``is_available`` is ``True`` unconditionally.
- Integration shape: when registered as the sole INDEX driver,
  :class:`ReindexWorker` dispatches upserts + deletes to it without
  raising.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_satisfies_catalog_metadata_store_protocol():
    """Structural check — isinstance against the runtime-checkable Protocol."""
    from dynastore.models.protocols.entity_store import CatalogStore
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )

    indexer = LogCatalogIndexer()
    assert isinstance(indexer, CatalogStore), (
        "LogCatalogIndexer should satisfy CatalogStore structurally"
    )


@pytest.mark.asyncio
async def test_get_returns_none():
    """Log indexer has no retrievable state; always returns None."""
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )

    indexer = LogCatalogIndexer()
    assert await indexer.get_catalog_metadata("cat-any") is None
    assert await indexer.get_catalog_metadata(
        "cat-any", context={"foo": "bar"},
    ) is None


@pytest.mark.asyncio
async def test_is_available_is_always_true():
    """Logging backend is always considered available."""
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )

    assert await LogCatalogIndexer().is_available() is True


@pytest.mark.asyncio
async def test_get_driver_config_returns_none():
    """No per-driver config — return None to match the PG Primary shape."""
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )

    assert await LogCatalogIndexer().get_driver_config("cat-any") is None


@pytest.mark.asyncio
async def test_upsert_logs_sorted_key_list_not_values(caplog):
    """Log line carries keys, not values (values can contain PII / noise)."""
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )

    indexer = LogCatalogIndexer()
    envelope = {
        "title": {"en": "Sensitive Catalog Name"},
        "description": {"en": "A description that may contain PII"},
        "stac_version": "1.1.0",
        "extra_metadata": {"secret_token": "should-not-log"},
    }
    with caplog.at_level(
        logging.INFO,
        logger="dynastore.modules.storage.drivers.catalog_log_indexer",
    ):
        await indexer.upsert_catalog_metadata("cat-42", envelope)

    log_msg = " ".join(r.message for r in caplog.records)
    # Sorted keys appear.
    for key in ("description", "extra_metadata", "stac_version", "title"):
        assert key in log_msg
    # Values do NOT appear — neither the Title string nor the token.
    assert "Sensitive Catalog Name" not in log_msg
    assert "should-not-log" not in log_msg
    # catalog_id is present for correlation.
    assert "cat-42" in log_msg


@pytest.mark.asyncio
async def test_upsert_with_empty_envelope_logs_empty_key_list(caplog):
    """Empty metadata still logs (freshness-only bump semantics).

    The PG Primary drivers treat empty payload as an updated_at-only
    bump.  An INDEX driver doesn't own freshness timestamps, but
    it still deserves a log entry so operators can see "a mutation
    happened, even if it was an updated_at-only bump upstream".
    """
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )

    indexer = LogCatalogIndexer()
    with caplog.at_level(
        logging.INFO,
        logger="dynastore.modules.storage.drivers.catalog_log_indexer",
    ):
        await indexer.upsert_catalog_metadata("cat-42", {})

    assert any(
        "cat-42" in r.message and "[]" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_delete_logs_soft_flag(caplog):
    """Delete log line carries the ``soft`` flag so hard / soft are distinguishable."""
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )

    indexer = LogCatalogIndexer()
    with caplog.at_level(
        logging.INFO,
        logger="dynastore.modules.storage.drivers.catalog_log_indexer",
    ):
        await indexer.delete_catalog_metadata("cat-7", soft=True)
        await indexer.delete_catalog_metadata("cat-8", soft=False)

    lines = [r.message for r in caplog.records]
    assert any("cat-7" in m and "soft=True" in m for m in lines)
    assert any("cat-8" in m and "soft=False" in m for m in lines)


# ---------------------------------------------------------------------------
# Integration with ReindexWorker — the INDEX-role entry point
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reindex_worker_dispatches_upsert_to_log_indexer(caplog):
    """Handing a ``catalog_metadata_changed`` event (upsert) through the
    worker with LogCatalogIndexer registered → exactly one info log line
    naming the catalog.

    This pins the wiring contract: LogCatalogIndexer, reindex_worker
    dispatch, and the CatalogStore protocol all line up
    correctly.  A future real Indexer can copy the same test shape
    swapping only the backend assertion.
    """
    from dynastore.modules.storage.routing_config import OperationDriverEntry
    from dynastore.modules.catalog.reindex_worker import ReindexWorker
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )

    indexer = LogCatalogIndexer()
    entry = OperationDriverEntry(driver_id="LogCatalogIndexer")

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, indexer)],
        get_catalog_metadata=AsyncMock(
            return_value={
                "title": {"en": "Catalog X"},
                "description": {"en": "Description X"},
                "stac_version": "1.1.0",
            },
        ),
    )

    event = {
        "event_id": "evt-1",
        "event_type": "catalog_metadata_changed",
        "payload": {
            "catalog_id": "cat-99",
            "domain": "core",
            "operation": "upsert",
        },
    }
    with caplog.at_level(
        logging.INFO,
        logger="dynastore.modules.storage.drivers.catalog_log_indexer",
    ):
        results = await worker.handle_batch([event])

    assert len(results) == 1
    assert results[0].succeeded is True
    assert results[0].should_retry is False
    assert any("cat-99" in r.message and "upsert" in r.message
               for r in caplog.records)


@pytest.mark.asyncio
async def test_reindex_worker_dispatches_delete_to_log_indexer(caplog):
    """Delete operation routes through ``delete_catalog_metadata``,
    not ``upsert_catalog_metadata`` — verify the worker's operation
    branching still lines up with the log indexer."""
    from dynastore.modules.storage.routing_config import OperationDriverEntry
    from dynastore.modules.catalog.reindex_worker import ReindexWorker
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )

    indexer = LogCatalogIndexer()
    entry = OperationDriverEntry(driver_id="LogCatalogIndexer")

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, indexer)],
        get_catalog_metadata=AsyncMock(return_value=None),
    )

    event = {
        "event_id": "evt-del-1",
        "event_type": "catalog_metadata_changed",
        "payload": {
            "catalog_id": "cat-gone",
            "domain": "core",
            "operation": "soft_delete",
        },
    }
    with caplog.at_level(
        logging.INFO,
        logger="dynastore.modules.storage.drivers.catalog_log_indexer",
    ):
        results = await worker.handle_batch([event])

    assert results[0].succeeded is True
    assert any(
        "cat-gone" in r.message and "delete" in r.message
        and "soft=True" in r.message
        for r in caplog.records
    )


def test_class_attributes_match_documented_shape():
    """Pin the advertised protocol facts so doc + code don't drift."""
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )

    assert LogCatalogIndexer.capabilities == frozenset()
    assert LogCatalogIndexer.sla is None
