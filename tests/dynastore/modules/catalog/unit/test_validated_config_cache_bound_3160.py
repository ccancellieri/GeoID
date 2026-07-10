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

"""#3160 — ``ConfigService._validated_config_cache`` must stay bounded.

The validated-config memo is keyed by (class_key, catalog_id,
collection_id): before #3160 it was a plain dict that grew with every
triple ever resolved in the process. Any scan touching many collections
(the cold-boot deny-policy restore being the pathological case) inflated
it without limit and drove serving workers into OOM.

The memo is identity-guarded against its source objects, so correctness
never depends on an entry surviving — these tests pin that the LRU bound
evicts the oldest entries and that an evicted triple simply re-validates
on next use.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig


def _make_bounded_service(monkeypatch, maxsize: int):
    import dynastore.modules.catalog.config_service as svc_mod
    from dynastore.modules.catalog.config_service import ConfigService

    monkeypatch.setattr(svc_mod, "_VALIDATED_CONFIG_CACHE_MAXSIZE", maxsize)

    svc = ConfigService(engine=None, catalog_manager=MagicMock())
    svc.get_catalog_defaults_snapshot = AsyncMock(return_value=None)

    # Stable identities: the memo hits only when base and deltas are the
    # exact objects a previous resolution saw.
    base = ItemsPostgresqlDriverConfig()
    platform_svc = MagicMock()
    platform_svc.get_config = AsyncMock(return_value=base)
    svc._get_platform_config_service = MagicMock(return_value=platform_svc)

    catalog_delta = {"physical_table": "tbl_catalog"}
    svc.get_catalog_config_internal_cached = AsyncMock(return_value=catalog_delta)

    collection_deltas: dict[str, dict] = {}

    async def _collection_delta(_catalog_id, collection_id, _class_key):
        return collection_deltas.setdefault(
            collection_id, {"physical_table": f"tbl_{collection_id}"}
        )

    svc.get_collection_config_internal_cached = AsyncMock(
        side_effect=_collection_delta
    )
    return svc


@pytest.mark.asyncio
async def test_cache_never_exceeds_bound_and_evicts_oldest(monkeypatch):
    svc = _make_bounded_service(monkeypatch, maxsize=3)

    for i in range(5):
        await svc.get_config(
            ItemsPostgresqlDriverConfig, catalog_id="cat", collection_id=f"col-{i}",
        )

    assert len(svc._validated_config_cache) == 3
    cached_collections = [key[2] for key in svc._validated_config_cache]
    assert cached_collections == ["col-2", "col-3", "col-4"]


@pytest.mark.asyncio
async def test_hit_refreshes_lru_position(monkeypatch):
    svc = _make_bounded_service(monkeypatch, maxsize=3)

    for i in range(3):
        await svc.get_config(
            ItemsPostgresqlDriverConfig, catalog_id="cat", collection_id=f"col-{i}",
        )
    # Touch col-0 (memo hit) so it becomes most-recent, then add one more.
    await svc.get_config(
        ItemsPostgresqlDriverConfig, catalog_id="cat", collection_id="col-0",
    )
    await svc.get_config(
        ItemsPostgresqlDriverConfig, catalog_id="cat", collection_id="col-3",
    )

    cached_collections = [key[2] for key in svc._validated_config_cache]
    assert "col-0" in cached_collections, "a memo hit must refresh LRU position"
    assert "col-1" not in cached_collections, "the oldest untouched entry is evicted"


@pytest.mark.asyncio
async def test_evicted_entry_revalidates_to_same_result(monkeypatch):
    svc = _make_bounded_service(monkeypatch, maxsize=2)

    first = await svc.get_config(
        ItemsPostgresqlDriverConfig, catalog_id="cat", collection_id="col-0",
    )
    for i in range(1, 4):  # push col-0 out of the bound
        await svc.get_config(
            ItemsPostgresqlDriverConfig, catalog_id="cat", collection_id=f"col-{i}",
        )
    assert "col-0" not in [key[2] for key in svc._validated_config_cache]

    again = await svc.get_config(
        ItemsPostgresqlDriverConfig, catalog_id="cat", collection_id="col-0",
    )
    assert again == first, "eviction must be invisible to callers"
    assert again.physical_table == "tbl_col-0"
