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

"""Regression tests for #2249 — ITEM_DELETION event must carry the
external id on the geoid-fallback delete path.

Background: when a collection has no active sidecar mapping,
``delete_item`` falls back to a direct geoid match.  Before the fix the
emitted ``ITEM_DELETION`` event was keyed by the geoid, so the
item->asset forward cascade (which keys ``asset_references.ref_id`` on
the *external* id stamped at harvest) never fired and assets were
orphaned.

The fix: on the fallback branch, resolve the external id via
``resolve_external_id_by_geoid`` while the row is still active and
prefer it as the ``item_id`` argument of the event.  When no external-id
mapping exists (``resolve_external_id_by_geoid`` returns ``None``), the
geoid is used as the existing behaviour.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

GEOID = "11111111-1111-1111-1111-111111111111"
EXTERNAL_ID = "feature-ext-001"


def _make_fake_self(*, resolve_external_id_return: Any):
    """Build a minimal stand-in for ItemQueryMixin/ItemService.

    Only the attributes accessed by ``delete_item`` on the UUID/geoid
    fallback branch are wired; everything else is an AsyncMock no-op.
    """
    fake_self = MagicMock()
    fake_self.engine = MagicMock()
    fake_self._capture_prior_bbox_for_delete = AsyncMock(return_value=None)
    fake_self._resolve_physical_schema = AsyncMock(return_value="s_cat1")
    fake_self._resolve_physical_table = AsyncMock(return_value="items_col1")
    fake_self._enqueue_index_deletes = AsyncMock(return_value=None)
    fake_self.resolve_external_id_by_geoid = AsyncMock(
        return_value=resolve_external_id_return
    )
    return fake_self


@asynccontextmanager
async def _stub_tx(_engine):
    yield MagicMock()


def _make_patches(*, soft_delete_rows: int):
    """Return a list of ``patch(...)`` context managers for the DB/event seams."""
    fake_driver = MagicMock()
    # col_config=None → skip sidecar branch, go straight to geoid fallback
    fake_driver.get_driver_config = AsyncMock(return_value=None)

    mock_soft_delete = MagicMock()
    mock_soft_delete.execute = AsyncMock(return_value=soft_delete_rows)

    return [
        patch(
            "dynastore.modules.catalog.item_query.managed_transaction",
            new=_stub_tx,
        ),
        patch(
            "dynastore.modules.catalog.item_service.soft_delete_item_query",
            new=mock_soft_delete,
        ),
        patch(
            "dynastore.modules.storage.router.get_driver",
            new=AsyncMock(return_value=fake_driver),
        ),
        patch(
            "dynastore.modules.catalog.tools.recalculate_and_update_extents",
            new=AsyncMock(return_value=None),
        ),
    ]


@pytest.mark.asyncio
async def test_item_deletion_event_uses_external_id_when_resolved() -> None:
    """When ``resolve_external_id_by_geoid`` returns a value the emitted
    ITEM_DELETION event must carry that external id as ``item_id`` so the
    item->asset forward cascade can match ``asset_references.ref_id``.
    """
    from dynastore.modules.catalog import item_query as item_query_mod
    from dynastore.modules.catalog.event_service import CatalogEventType

    emitted: Dict[str, Any] = {}

    fake_events = AsyncMock()

    async def _capture_emit(*, event_type, **kwargs):
        emitted["event_type"] = event_type
        emitted.update(kwargs)

    fake_events.emit = AsyncMock(side_effect=_capture_emit)

    fake_self = _make_fake_self(resolve_external_id_return=EXTERNAL_ID)

    patches = _make_patches(soft_delete_rows=1)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patch(
            "dynastore.modules.catalog.item_query.get_protocol",
            return_value=fake_events,
        ),
    ):
        rows = await item_query_mod.ItemQueryMixin.delete_item(
            fake_self,
            catalog_id="cat1",
            collection_id="col1",
            item_id=GEOID,
        )

    assert rows == 1, "soft-delete should have returned 1 deleted row"
    assert emitted.get("event_type") == CatalogEventType.ITEM_DELETION
    assert emitted.get("item_id") == EXTERNAL_ID, (
        f"ITEM_DELETION event must carry the external id '{EXTERNAL_ID}' "
        f"(not the geoid '{GEOID}') so the forward cascade fires; "
        f"got item_id={emitted.get('item_id')!r}"
    )
    assert emitted.get("payload", {}).get("original_id") == GEOID, (
        "original_id in payload must still be the geoid for audit purposes"
    )
    # Verify resolve_external_id_by_geoid was called with the right args
    fake_self.resolve_external_id_by_geoid.assert_awaited_once_with(
        "cat1", "col1", GEOID, None,
    )


@pytest.mark.asyncio
async def test_item_deletion_event_falls_back_to_geoid_when_no_external_id() -> None:
    """When ``resolve_external_id_by_geoid`` returns ``None`` (no external-id
    mapping exists for this collection) the event must fall back to the geoid
    — preserving the pre-fix behaviour for collections without sidecar mappings.
    """
    from dynastore.modules.catalog import item_query as item_query_mod
    from dynastore.modules.catalog.event_service import CatalogEventType

    emitted: Dict[str, Any] = {}

    fake_events = AsyncMock()

    async def _capture_emit(*, event_type, **kwargs):
        emitted["event_type"] = event_type
        emitted.update(kwargs)

    fake_events.emit = AsyncMock(side_effect=_capture_emit)

    # resolve_external_id_by_geoid returns None → should keep the geoid
    fake_self = _make_fake_self(resolve_external_id_return=None)

    patches = _make_patches(soft_delete_rows=1)
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patch(
            "dynastore.modules.catalog.item_query.get_protocol",
            return_value=fake_events,
        ),
    ):
        rows = await item_query_mod.ItemQueryMixin.delete_item(
            fake_self,
            catalog_id="cat1",
            collection_id="col1",
            item_id=GEOID,
        )

    assert rows == 1
    assert emitted.get("event_type") == CatalogEventType.ITEM_DELETION
    assert emitted.get("item_id") == GEOID, (
        f"Without an external-id mapping the event must fall back to the "
        f"geoid '{GEOID}'; got item_id={emitted.get('item_id')!r}"
    )
    assert emitted.get("payload", {}).get("original_id") == GEOID
