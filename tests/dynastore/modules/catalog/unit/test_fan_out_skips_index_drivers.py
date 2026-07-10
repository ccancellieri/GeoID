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

"""Regression: the storage write fan-out must NOT call ``write_entities`` on
item-indexer drivers (#1289).

An item-indexer driver (``is_item_indexer = True`` — the public/private ES
drivers) lives in ``operations[INDEX]``, never ``operations[WRITE]``, and
is propagated by the *index dispatcher* (``_dispatch_index_upsert`` →
``index_bulk``), which stamps the canonical identity fields
(``_external_id`` / ``_asset_id``) onto the index payload.

Before this fix the same driver was ALSO fanned out as a secondary *storage*
driver via ``_fan_out_to_secondary_drivers`` → ``write_entities``. That second
write rebuilds the tenant doc from the read-back Feature, which carries
neither ``_external_id`` nor ``_asset_id`` (the fan-out call sites pass no
``context``), so it produces an identity-less doc. The two writes target the
same index/_id and race (the async secondary write is fire-and-forget), so a
last-writer-wins ordering could silently drop the identity fields. Indexer
drivers are owned by the dispatcher; the storage fan-out must skip them.
"""
from __future__ import annotations

from typing import Any, List

import pytest

from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.router import ResolvedDriver
from dynastore.modules.storage.routing_config import FailurePolicy


class _RecordingDriver:
    """Minimal driver stub recording ``write_entities`` invocations."""

    is_item_indexer: bool = False

    def __init__(self) -> None:
        self.writes: List[Any] = []

    async def write_entities(self, catalog_id, collection_id, entities, **kwargs):
        self.writes.append((catalog_id, collection_id, entities, kwargs))
        return entities


class _PrimaryStorageDriver(_RecordingDriver):
    is_item_indexer = False


class _SecondaryStorageDriver(_RecordingDriver):
    is_item_indexer = False


class _SecondaryIndexerDriver(_RecordingDriver):
    """An ES-style driver that is also an item indexer (dispatcher-owned)."""

    is_item_indexer = True


@pytest.mark.asyncio
async def test_fan_out_skips_item_indexer_secondary(monkeypatch):
    primary = _PrimaryStorageDriver()
    storage_secondary = _SecondaryStorageDriver()
    indexer_secondary = _SecondaryIndexerDriver()

    resolved = [
        ResolvedDriver(driver=primary, on_failure=FailurePolicy.FATAL),
        ResolvedDriver(driver=storage_secondary, on_failure=FailurePolicy.WARN),
        ResolvedDriver(driver=indexer_secondary, on_failure=FailurePolicy.WARN),
    ]

    async def _fake_get_write_drivers(catalog_id, collection_id=None, **kwargs):
        return resolved

    # Patched on the source module — the method imports it at call time.
    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_write_drivers",
        _fake_get_write_drivers,
    )

    svc = ItemService(engine=None)
    await svc._fan_out_to_secondary_drivers(
        "cat_a", "col_a", [{"id": "geoid-1"}],
    )

    # Position 0 (primary) is always skipped — written by the caller's branch.
    assert primary.writes == []
    # A genuine secondary storage driver IS written via the fan-out.
    assert len(storage_secondary.writes) == 1
    # The item-indexer secondary is owned by the index dispatcher and MUST NOT
    # be double-written here (that path drops identity → #1289 race).
    assert indexer_secondary.writes == []
