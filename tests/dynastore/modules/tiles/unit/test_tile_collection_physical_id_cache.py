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

"""Unit tests for tile-path physical-id resolution.

Tile storage no longer owns a cache: bare logical ids resolve through
``CatalogsProtocol.resolve_physical_id`` (no ``db_resource``), which serves
from the shared ``_collection_physical_id_cache`` accelerator owned by the
catalog service.  These tests verify the tile layer's own behaviour:

1. Bare ids delegate to ``resolve_physical_id`` without a connection so the
   shared cache is used (not bypassed).
2. Parameterized / composite ids are returned as-is without resolving.
3. A resolver failure falls back to the logical id.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_storage(physical_id="c_phys", *, raise_exc=False):
    from dynastore.modules.tiles.tiles_module import TilePGPreseedStorage

    storage = TilePGPreseedStorage.__new__(TilePGPreseedStorage)
    storage.engine = MagicMock()
    cats = MagicMock()
    if raise_exc:
        cats.resolve_physical_id = AsyncMock(side_effect=RuntimeError("boom"))
    else:
        cats.resolve_physical_id = AsyncMock(return_value=physical_id)
    storage.catalogs = cats
    return storage, cats


@pytest.mark.asyncio
async def test_bare_id_delegates_to_resolver_without_db_resource():
    """A bare logical id resolves via resolve_physical_id with no ctx/connection.

    Passing no ``db_resource`` is what lets the shared
    ``_collection_physical_id_cache`` serve the result instead of being
    bypassed, so the tile path must never hand the resolver a connection.
    """
    storage, cats = _make_storage("c_phys_a")

    result = await storage._resolve_collection_physical_id("cat_a", "col_a")

    assert result == "c_phys_a"
    cats.resolve_physical_id.assert_awaited_once_with(
        "cat_a", "col_a", allow_missing=True
    )
    # No ctx / db_resource handed to the resolver (would defeat the cache).
    _, kwargs = cats.resolve_physical_id.call_args
    assert "ctx" not in kwargs


@pytest.mark.asyncio
async def test_parameterized_and_composite_ids_pass_through():
    """IDs with '@' or ',' are returned as-is without resolving."""
    storage, cats = _make_storage("c_never_called")

    parameterized = await storage._resolve_collection_physical_id("cat", "col@abc123")
    multi = await storage._resolve_collection_physical_id("cat", "col_a,col_b")

    assert parameterized == "col@abc123"
    assert multi == "col_a,col_b"
    cats.resolve_physical_id.assert_not_called()


@pytest.mark.asyncio
async def test_resolver_failure_falls_back_to_logical_id():
    """If resolution raises, the logical id is used so tiles still key somewhere."""
    storage, cats = _make_storage(raise_exc=True)

    result = await storage._resolve_collection_physical_id("cat_x", "col_x")

    assert result == "col_x"
