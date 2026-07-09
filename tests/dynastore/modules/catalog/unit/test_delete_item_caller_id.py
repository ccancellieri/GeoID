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

"""Contract tests for the DELETE-path principal attribution (#1407).

Covers two seams:

1. ``OGCServiceMixin._principal_caller_id(request)`` — converts a
   ``Principal`` on ``request.state`` into a ``"{provider}:{subject_id}"``
   ``caller_id`` string (or ``None`` for anonymous / partial principals).
2. ``ItemQueryMixin.delete_item`` — forwards the ``caller_id`` kwarg to
   ``enqueue_tile_invalidation_task`` so the post-commit invalidation row is
   attributed to the same principal as the create/update path
   (#1404/#1405).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.models.auth import Principal


def test_principal_caller_id_authenticated() -> None:
    req = SimpleNamespace(
        state=SimpleNamespace(
            principal=Principal(provider="keycloak", subject_id="alice@fao.org"),
        )
    )
    assert (
        OGCServiceMixin._principal_caller_id(req)  # type: ignore[arg-type]
        == "keycloak:alice@fao.org"
    )


def test_principal_caller_id_bare_subject_when_provider_missing() -> None:
    req = SimpleNamespace(
        state=SimpleNamespace(principal=Principal(provider=None, subject_id="bob")),
    )
    assert OGCServiceMixin._principal_caller_id(req) == "bob"  # type: ignore[arg-type]


def test_principal_caller_id_anonymous_returns_none() -> None:
    req_no_principal = SimpleNamespace(state=SimpleNamespace(principal=None))
    assert (
        OGCServiceMixin._principal_caller_id(req_no_principal)  # type: ignore[arg-type]
        is None
    )

    req_no_attr = SimpleNamespace(state=SimpleNamespace())
    assert (
        OGCServiceMixin._principal_caller_id(req_no_attr)  # type: ignore[arg-type]
        is None
    )


def test_principal_caller_id_partial_principal_returns_none() -> None:
    req = SimpleNamespace(
        state=SimpleNamespace(
            principal=Principal(provider="local", subject_id=None),
        )
    )
    assert OGCServiceMixin._principal_caller_id(req) is None  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_delete_item_forwards_caller_id_to_invalidation() -> None:
    """``delete_item(... caller_id=...)`` reaches ``enqueue_tile_invalidation_task``
    so the tile-invalidation row is attributed to the originating principal.
    """
    from dynastore.modules.catalog import item_query as item_query_mod

    captured: Dict[str, Any] = {}

    async def _fake_enqueue(
        catalog_id, collection_id, results, *, engine, schema,
        prior_bboxes=None, caller_id=None, **_kwargs,
    ):
        captured["caller_id"] = caller_id
        captured["prior_bboxes"] = prior_bboxes
        return 1

    fake_self = MagicMock()
    fake_self.engine = MagicMock()
    fake_self._capture_prior_bbox_for_delete = AsyncMock(
        return_value=(0.0, 0.0, 1.0, 1.0),
    )
    fake_self._resolve_physical_schema = AsyncMock(return_value="s_abc")
    fake_self._resolve_physical_table = AsyncMock(return_value="items_t")
    fake_self.resolve_external_id_by_geoid = AsyncMock(return_value=None)
    fake_self._enqueue_index_deletes = AsyncMock(return_value=None)

    fake_conn = MagicMock()

    @asynccontextmanager
    async def _fake_tx(_engine):
        yield fake_conn

    fake_driver = MagicMock()
    fake_driver.get_driver_config = AsyncMock(return_value=None)

    mock_query = MagicMock()
    mock_query.execute = AsyncMock(return_value=1)

    with patch.object(
        item_query_mod, "managed_transaction", new=_fake_tx,
    ), patch(
        "dynastore.modules.catalog.item_service.soft_delete_item_query",
        new=mock_query,
    ), patch(
        "dynastore.modules.storage.router.get_driver",
        new=AsyncMock(return_value=fake_driver),
    ), patch(
        "dynastore.modules.catalog.tools.recalculate_and_update_extents",
        new=AsyncMock(return_value=None),
    ), patch(
        "dynastore.modules.tiles.tile_cache_sync.enqueue_tile_invalidation_task",
        new=AsyncMock(side_effect=_fake_enqueue),
    ):
        rows = await item_query_mod.ItemQueryMixin.delete_item(
            fake_self,
            catalog_id="cat1",
            collection_id="col1",
            item_id="11111111-1111-1111-1111-111111111111",
            caller_id="user:alice",
        )

    assert rows == 1
    assert captured["caller_id"] == "user:alice"
    assert captured["prior_bboxes"] == [(0.0, 0.0, 1.0, 1.0)]


@pytest.mark.asyncio
async def test_delete_item_default_caller_id_is_none() -> None:
    """Omitting ``caller_id`` forwards ``None`` and lets the enqueue's
    ``"system:tile_cache_invalidation"`` fallback take over (verified by
    the existing tile_cache_sync contract test).
    """
    from dynastore.modules.catalog import item_query as item_query_mod

    captured: Dict[str, Any] = {}

    async def _fake_enqueue(
        catalog_id, collection_id, results, *, engine, schema,
        prior_bboxes=None, caller_id=None, **_kwargs,
    ):
        captured["caller_id"] = caller_id
        return 1

    fake_self = MagicMock()
    fake_self.engine = MagicMock()
    fake_self._capture_prior_bbox_for_delete = AsyncMock(
        return_value=(0.0, 0.0, 1.0, 1.0),
    )
    fake_self._resolve_physical_schema = AsyncMock(return_value="s_abc")
    fake_self._resolve_physical_table = AsyncMock(return_value="items_t")
    fake_self.resolve_external_id_by_geoid = AsyncMock(return_value=None)
    fake_self._enqueue_index_deletes = AsyncMock(return_value=None)

    fake_conn = MagicMock()

    @asynccontextmanager
    async def _fake_tx(_engine):
        yield fake_conn

    fake_driver = MagicMock()
    fake_driver.get_driver_config = AsyncMock(return_value=None)

    mock_query = MagicMock()
    mock_query.execute = AsyncMock(return_value=1)

    with patch.object(
        item_query_mod, "managed_transaction", new=_fake_tx,
    ), patch(
        "dynastore.modules.catalog.item_service.soft_delete_item_query",
        new=mock_query,
    ), patch(
        "dynastore.modules.storage.router.get_driver",
        new=AsyncMock(return_value=fake_driver),
    ), patch(
        "dynastore.modules.catalog.tools.recalculate_and_update_extents",
        new=AsyncMock(return_value=None),
    ), patch(
        "dynastore.modules.tiles.tile_cache_sync.enqueue_tile_invalidation_task",
        new=AsyncMock(side_effect=_fake_enqueue),
    ):
        rows = await item_query_mod.ItemQueryMixin.delete_item(
            fake_self,
            catalog_id="cat1",
            collection_id="col1",
            item_id="11111111-1111-1111-1111-111111111111",
        )

    assert rows == 1
    assert captured["caller_id"] is None
