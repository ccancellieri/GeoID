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

"""Regression tests: collection hard-delete keeps the delete transaction
PG-only and lets the async cascade own external (Elasticsearch) teardown.

Root cause being guarded against: the hard-delete held ONE DB transaction open
across the storage purge AND the post-purge event handlers.  The
AFTER_COLLECTION_HARD_DELETION handler fanned per-asset Elasticsearch deletes
out *inside* that transaction, leaving the connection idle in-transaction past
``idle_in_transaction_session_timeout`` — PostgreSQL killed the backend and the
commit failed with ``cannot call Transaction.commit(): the underlying
connection is closed``.  A second defect: the cascade_cleanup task re-dropped
the same PG items table the inline purge was dropping, racing it for the table
lock (``LockNotAvailableError``).

The fix keeps the transaction PG-authoritative only and delegates all external
teardown to the async cascade:

1. ``AssetElasticsearchDriver.drop_storage`` is collection-granular (so the
   cascade can own asset-ES teardown without wiping sibling collections).
2. ``AssetService.delete_assets(external=False)`` skips the non-PG fan-out.
3. ``_on_collection_hard_deletion`` calls it with ``external=False``.
4. ``RoutingDrivenCascadeOwner`` no longer enumerates PG drivers (inline-owned),
   so the cascade never re-drops the PG items table.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# 1. AssetElasticsearchDriver.drop_storage — collection-granular
# ---------------------------------------------------------------------------


class _FakeIndices:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    async def delete(self, *, index, params=None):  # noqa: ANN001
        self.deleted.append(index)


class _FakeEs:
    def __init__(self) -> None:
        self.indices = _FakeIndices()
        self.delete_by_query_calls: list[dict] = []

    async def delete_by_query(self, *, index, body=None, params=None):  # noqa: ANN001
        self.delete_by_query_calls.append(
            {"index": index, "body": body, "params": params}
        )


@pytest.fixture
def asset_es_driver(monkeypatch):
    from dynastore.modules.storage.drivers import elasticsearch as es_mod

    drv = es_mod.AssetElasticsearchDriver()
    fake = _FakeEs()
    monkeypatch.setattr(drv, "_get_client", lambda: fake)
    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.mappings.get_assets_index_name",
        lambda prefix, catalog_id: f"{prefix}-{catalog_id}-assets",
        raising=True,
    )
    monkeypatch.setattr(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        lambda: "dynastore",
        raising=True,
    )
    return drv, fake


class TestAssetEsDropStorageGranularity:
    @pytest.mark.asyncio
    async def test_collection_scope_uses_delete_by_query(self, asset_es_driver):
        """A collection-scoped drop must delete only that collection's asset
        docs (delete_by_query on collection_id), NOT the whole catalog index."""
        drv, fake = asset_es_driver
        await drv.drop_storage("cat-a", "coll-1")

        assert len(fake.delete_by_query_calls) == 1
        call = fake.delete_by_query_calls[0]
        assert call["index"] == "dynastore-cat-a-assets"
        assert call["body"] == {"query": {"term": {"collection_id": "coll-1"}}}
        # The whole-index delete must NOT run — that would wipe sibling
        # collections' assets sharing the per-catalog index.
        assert fake.indices.deleted == []

    @pytest.mark.asyncio
    async def test_catalog_scope_drops_whole_index(self, asset_es_driver):
        """A catalog-scoped drop (no collection_id) still drops the index."""
        drv, fake = asset_es_driver
        await drv.drop_storage("cat-a", None)

        assert fake.indices.deleted == ["dynastore-cat-a-assets"]
        assert fake.delete_by_query_calls == []


# ---------------------------------------------------------------------------
# 2. RoutingDrivenCascadeOwner — PG drivers are inline-owned, not enumerated
# ---------------------------------------------------------------------------


class TestCascadeExcludesInlineOwnedPgDrivers:
    def test_is_inline_owned_driver(self):
        from dynastore.modules.storage.drivers.routing_driven_cascade_owner import (
            _is_inline_owned_driver,
        )

        assert _is_inline_owned_driver("items_postgresql_driver") is True
        assert _is_inline_owned_driver("asset_postgresql_driver") is True
        assert _is_inline_owned_driver("collection_postgresql_driver") is True
        # External / OTF backends are NOT inline-owned — cascade keeps them.
        assert _is_inline_owned_driver("items_elasticsearch_driver") is False
        assert _is_inline_owned_driver("asset_elasticsearch_driver") is False
        assert _is_inline_owned_driver("items_duckdb_driver") is False
        assert _is_inline_owned_driver("items_iceberg_driver") is False

    @pytest.mark.asyncio
    async def test_enumerate_skips_pg_keeps_es(self):
        """PG drivers must not be enumerated by the cascade (the delete
        transaction drops their storage inline); ES drivers must remain."""
        from dynastore.modules.catalog.resource_owner import ResourceScope, ScopeRef
        from dynastore.modules.storage.drivers.routing_driven_cascade_owner import (
            _enumerate_configured_drivers,
        )

        async def _mock_resolve(routing_cls, catalog_id, collection_id, op, hints):
            from dynastore.modules.storage.routing_config import ItemsRoutingConfig

            if routing_cls is ItemsRoutingConfig:
                return [
                    ("items_postgresql_driver", "FATAL", "SYNC"),
                    ("items_elasticsearch_driver", "WARN", "SYNC"),
                ]
            return []

        scope_ref = ScopeRef(
            scope=ResourceScope.COLLECTION,
            catalog_id="cat-a",
            collection_id="coll-1",
        )
        with patch(
            "dynastore.modules.storage.router._resolve_driver_ids_cached",
            new=AsyncMock(side_effect=_mock_resolve),
        ):
            result = await _enumerate_configured_drivers(scope_ref)

        driver_refs = [dr for _, dr in result]
        assert "items_postgresql_driver" not in driver_refs
        assert "items_elasticsearch_driver" in driver_refs


# ---------------------------------------------------------------------------
# 3. AssetService.delete_assets — external gating skips the non-PG fan-out
# ---------------------------------------------------------------------------


def _patch_pg_delete(monkeypatch, asset_rows: list[dict]) -> None:
    """Make AssetPostgresqlDriver(...).delete_assets_bulk return *asset_rows*."""
    fake_pg = MagicMock()
    fake_pg.delete_assets_bulk = AsyncMock(return_value=(len(asset_rows), asset_rows))
    monkeypatch.setattr(
        "dynastore.modules.catalog.drivers.pg_asset_driver.AssetPostgresqlDriver",
        lambda *a, **k: fake_pg,
        raising=True,
    )


class TestDeleteAssetsExternalGating:
    @pytest.mark.asyncio
    async def test_external_false_skips_fanout(self, monkeypatch):
        """With external=False the non-PG fan-out entry point (get_asset_driver)
        is never reached — no external driver I/O on the delete connection."""
        from dynastore.modules.catalog.asset_service import AssetService

        _patch_pg_delete(
            monkeypatch,
            [{"asset_id": "a1", "catalog_id": "cat", "collection_id": "col"}],
        )
        spy = AsyncMock()
        monkeypatch.setattr(
            "dynastore.modules.storage.router.get_asset_driver", spy, raising=True
        )

        svc = AssetService(engine=MagicMock(), event_emitter=None)
        n = await svc.delete_assets(
            "cat", collection_id="col", hard=True, external=False
        )

        assert n == 1
        spy.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_external_true_runs_fanout(self, monkeypatch):
        """The default (external=True) still enters the fan-out path."""
        from dynastore.models.protocols.storage_driver import Capability
        from dynastore.modules.catalog.asset_service import AssetService

        _patch_pg_delete(
            monkeypatch,
            [{"asset_id": "a1", "catalog_id": "cat", "collection_id": "col"}],
        )
        # Return a PG-capable write driver so the fan-out loop adds nothing and
        # makes no external delete calls — we only assert the path was entered.
        write_driver = MagicMock()
        write_driver.capabilities = frozenset({Capability.QUERY_FALLBACK_SOURCE})
        spy = AsyncMock(return_value=write_driver)
        monkeypatch.setattr(
            "dynastore.modules.storage.router.get_asset_driver", spy, raising=True
        )

        svc = AssetService(engine=MagicMock(), event_emitter=None)
        monkeypatch.setattr(
            svc, "_get_secondary_drivers", AsyncMock(return_value=[])
        )
        n = await svc.delete_assets("cat", collection_id="col", hard=True)

        assert n == 1
        spy.assert_awaited()


# ---------------------------------------------------------------------------
# 4. _on_collection_hard_deletion passes external=False (inline = PG-only)
# ---------------------------------------------------------------------------


class TestCollectionHardDeletionHandlerExternalFalse:
    @pytest.mark.asyncio
    async def test_handler_passes_external_false(self):
        from dynastore.modules.catalog.catalog_module import CatalogModule

        fake_assets = MagicMock()
        fake_assets.delete_assets = AsyncMock(return_value=1)
        sentinel_conn = object()

        with patch(
            "dynastore.modules.catalog.catalog_module.get_protocol",
            return_value=fake_assets,
        ):
            await CatalogModule._on_collection_hard_deletion(
                MagicMock(),  # self — unused by the handler
                "cat-a",
                "coll-1",
                db_resource=sentinel_conn,
            )

        fake_assets.delete_assets.assert_awaited_once()
        kwargs = fake_assets.delete_assets.await_args.kwargs
        assert kwargs["external"] is False
        assert kwargs["hard"] is True
        assert kwargs["catalog_id"] == "cat-a"
        assert kwargs["collection_id"] == "coll-1"
        assert kwargs["db_resource"] is sentinel_conn
