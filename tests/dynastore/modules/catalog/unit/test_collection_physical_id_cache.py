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

"""Unit tests for the shared collection physical-id accelerator.

``_collection_physical_id_cache`` is the single L1/L2 cache that both the tile
storage and the log writer now use (via ``resolve_physical_id`` with no
``db_resource``).  It replaces the bespoke per-path caches.  Tests assert:

1. A repeated lookup for the same (catalog, collection) pays the DB once.
2. ``_invalidate_collection_physical_id_cache`` forces a fresh read.
3. ``_invalidate_collection_lifecycle_caches`` (the rename/delete hook) drops
   the entry.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


def _fake_service(physical_id="c_phys"):
    service = MagicMock()
    service._resolve_collection_physical_id_db = AsyncMock(return_value=physical_id)
    return service


@pytest.mark.asyncio
async def test_collection_physical_id_cached_on_second_call():
    from dynastore.modules.catalog.catalog_service import (
        _collection_physical_id_cache,
        _invalidate_collection_physical_id_cache,
    )

    _invalidate_collection_physical_id_cache("cat_a", "col_a")
    service = _fake_service("c_phys_a")

    first = await _collection_physical_id_cache(service, "cat_a", "col_a")
    second = await _collection_physical_id_cache(service, "cat_a", "col_a")

    assert first == "c_phys_a"
    assert second == "c_phys_a"
    assert service._resolve_collection_physical_id_db.await_count == 1


@pytest.mark.asyncio
async def test_invalidate_forces_fresh_read():
    from dynastore.modules.catalog.catalog_service import (
        _collection_physical_id_cache,
        _invalidate_collection_physical_id_cache,
    )

    _invalidate_collection_physical_id_cache("cat_b", "col_b")
    service = _fake_service("c_phys_b")
    await _collection_physical_id_cache(service, "cat_b", "col_b")
    assert service._resolve_collection_physical_id_db.await_count == 1

    _invalidate_collection_physical_id_cache("cat_b", "col_b")
    service._resolve_collection_physical_id_db = AsyncMock(return_value="c_phys_b_v2")
    fresh = await _collection_physical_id_cache(service, "cat_b", "col_b")

    assert fresh == "c_phys_b_v2"
    assert service._resolve_collection_physical_id_db.await_count == 1


@pytest.mark.asyncio
async def test_lifecycle_invalidation_drops_collection_physical_id_cache(monkeypatch):
    """The rename/delete cache hook busts the shared collection physical-id entry."""
    from dynastore.modules.catalog.catalog_service import (
        _collection_physical_id_cache,
        _invalidate_collection_physical_id_cache,
    )
    import dynastore.modules.catalog.collection_service as cs

    _invalidate_collection_physical_id_cache("cat_c", "col_c")
    service = _fake_service("c_phys_c")
    await _collection_physical_id_cache(service, "cat_c", "col_c")
    assert service._resolve_collection_physical_id_db.await_count == 1

    # Neutralize the other caches the lifecycle hook touches so this unit test
    # exercises only the collection physical-id invalidation.
    monkeypatch.setattr(cs, "_invalidate_collection_model_cache", lambda *a: None)
    monkeypatch.setattr(
        "dynastore.modules.storage.router.invalidate_router_cache",
        lambda *a, **k: None,
        raising=False,
    )
    monkeypatch.setattr(
        "dynastore.modules.catalog.config_service.invalidate_collection_config_cache",
        lambda *a, **k: None,
        raising=False,
    )

    cs._invalidate_collection_lifecycle_caches("cat_c", "col_c")

    service._resolve_collection_physical_id_db = AsyncMock(return_value="c_phys_c_v2")
    result = await _collection_physical_id_cache(service, "cat_c", "col_c")

    assert result == "c_phys_c_v2"
    assert service._resolve_collection_physical_id_db.await_count == 1
