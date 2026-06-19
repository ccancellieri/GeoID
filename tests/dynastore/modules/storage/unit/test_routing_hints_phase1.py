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

"""Unit tests for routing-hints Phase 1.

Covers:
- supported_hints ClassVar on the 4 metadata drivers (step 2)
- READ default stays PG-only (step 3) — hint routing does not mutate the
  platform default; ES serves hinted reads only where it is registered
- resolve_routed hint filtering (step 4)
- get_collection_metadata / get_catalog_metadata dispatch (step 5)
- Cache bypass when hints non-empty; empty hints forwarded unchanged so the
  no-hint default read stays merge-all / byte-identical (step 6, requirement A)
- end-to-end hint threading through list/create paths (step 7)
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock


# ---------------------------------------------------------------------------
# Step 2: supported_hints on the 4 metadata drivers
# ---------------------------------------------------------------------------


def test_collection_es_driver_supported_hints():
    from dynastore.modules.elasticsearch.collection_es_driver import CollectionElasticsearchDriver
    from dynastore.modules.storage.hints import Hint
    hints = CollectionElasticsearchDriver.supported_hints
    assert Hint.GEOMETRY_SIMPLIFIED in hints
    assert Hint.METADATA in hints
    # ES must NOT carry GEOMETRY_EXACT
    assert Hint.GEOMETRY_EXACT not in hints


def test_catalog_es_driver_supported_hints():
    from dynastore.modules.elasticsearch.catalog_es_driver import CatalogElasticsearchDriver
    from dynastore.modules.storage.hints import Hint
    hints = CatalogElasticsearchDriver.supported_hints
    assert Hint.GEOMETRY_SIMPLIFIED in hints
    assert Hint.METADATA in hints
    assert Hint.GEOMETRY_EXACT not in hints


def test_collection_pg_driver_supported_hints():
    from dynastore.modules.storage.drivers.collection_postgresql import CollectionPostgresqlDriver
    from dynastore.modules.storage.hints import Hint
    hints = CollectionPostgresqlDriver.supported_hints
    assert Hint.GEOMETRY_EXACT in hints
    assert Hint.METADATA in hints
    # PG must NOT carry simplified geometry
    assert Hint.GEOMETRY_SIMPLIFIED not in hints


def test_catalog_pg_driver_supported_hints():
    from dynastore.modules.storage.drivers.catalog_postgresql import CatalogPostgresqlDriver
    from dynastore.modules.storage.hints import Hint
    hints = CatalogPostgresqlDriver.supported_hints
    assert Hint.GEOMETRY_EXACT in hints
    assert Hint.METADATA in hints
    assert Hint.GEOMETRY_SIMPLIFIED not in hints


def test_collection_es_auto_register_includes_read():
    from dynastore.modules.elasticsearch.collection_es_driver import CollectionElasticsearchDriver
    from dynastore.modules.storage.routing_config import Operation
    assert Operation.READ in CollectionElasticsearchDriver.auto_register_for_routing


def test_catalog_es_auto_register_includes_read():
    from dynastore.modules.elasticsearch.catalog_es_driver import CatalogElasticsearchDriver
    from dynastore.modules.storage.routing_config import Operation
    assert Operation.READ in CatalogElasticsearchDriver.auto_register_for_routing


# ---------------------------------------------------------------------------
# Step 3: READ default is PG system-of-record FIRST + an opt-in ES entry.
#
# The platform READ default now carries two entries: the untagged PG system
# of record (declared first) and a hint-tagged ES reader (hints={METADATA}).
# The ES entry is OPT-IN:
# - a no-hint read resolves PG ONLY (the resolver's no-hint READ filter drops
#   hint-tagged entries when an untagged default exists), so a plain read is
#   byte-identical to the pre-hint behaviour;
# - a read carrying prefer:es or METADATA is matched to ES, with PG kept as
#   the ordered fallback tail (ES miss → PG system of record).
# There is no geometry at the metadata level so geometry hints do not apply.
# ---------------------------------------------------------------------------


def test_collection_routing_config_read_default_pg_first_es_opt_in():
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation, FailurePolicy,
    )
    from dynastore.modules.storage.hints import Hint
    cfg = CollectionRoutingConfig()
    read = cfg.operations.get(Operation.READ, [])
    assert [e.driver_ref for e in read] == [
        "collection_postgresql_driver",
        "collection_elasticsearch_driver",
    ]
    pg, es = read
    # PG is the untagged system-of-record default (matches its full driver
    # surface); ES is the hint-tagged, non-fatal opt-in reader.
    assert not pg.hints
    assert pg.on_failure == FailurePolicy.FATAL
    assert es.hints == {Hint.METADATA}
    assert es.on_failure == FailurePolicy.WARN


def test_catalog_routing_config_read_default_pg_first_es_opt_in():
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, Operation, FailurePolicy,
    )
    from dynastore.modules.storage.hints import Hint
    cfg = CatalogRoutingConfig()
    read = cfg.operations.get(Operation.READ, [])
    assert [e.driver_ref for e in read] == [
        "catalog_postgresql_driver",
        "catalog_elasticsearch_driver",
    ]
    pg, es = read
    assert not pg.hints
    assert pg.on_failure == FailurePolicy.FATAL
    assert es.hints == {Hint.METADATA}
    assert es.on_failure == FailurePolicy.WARN


# ---------------------------------------------------------------------------
# Step 4: resolve_routed hint filtering
# ---------------------------------------------------------------------------


class _FakePgDriver:
    supported_hints = frozenset()


class _FakeEsDriver:
    supported_hints = frozenset()


@pytest.mark.asyncio
async def test_resolve_routed_empty_hints_returns_declared_order(monkeypatch):
    """Empty hints → all entries in declared order, no filtering."""
    from dynastore.modules.storage import routed_resolver
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation, OperationDriverEntry,
    )
    from dynastore.modules.storage.hints import Hint

    pg = _FakePgDriver()
    es = _FakeEsDriver()
    # ES has hints on the entry; PG has hints on the entry
    cfg = CollectionRoutingConfig(
        operations={
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    hints={Hint.GEOMETRY_SIMPLIFIED},
                ),
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    hints={Hint.GEOMETRY_EXACT},
                ),
            ],
        }
    )

    async def _fake_load(*a, **kw):
        return cfg

    monkeypatch.setattr(routed_resolver, "_load_routing_config", _fake_load)
    monkeypatch.setattr(
        routed_resolver, "_index_for",
        lambda rpc: {
            "collection_elasticsearch_driver": es,
            "collection_postgresql_driver": pg,
        },
    )

    resolved = await routed_resolver.resolve_routed(
        CollectionRoutingConfig, Operation.READ, "cat", "coll",
        hints=frozenset(),
    )
    refs = [e.driver_ref for e, _ in resolved]
    # No filter: declared order ES first, PG second
    assert refs == ["collection_elasticsearch_driver", "collection_postgresql_driver"]


@pytest.mark.asyncio
async def test_resolve_routed_geometry_exact_hint_selects_pg(monkeypatch):
    """hints={GEOMETRY_EXACT} → only PG matches, returned first."""
    from dynastore.modules.storage import routed_resolver
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation, OperationDriverEntry,
    )
    from dynastore.modules.storage.hints import Hint

    pg = _FakePgDriver()
    es = _FakeEsDriver()
    cfg = CollectionRoutingConfig(
        operations={
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    hints={Hint.GEOMETRY_SIMPLIFIED, Hint.METADATA},
                ),
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    hints={Hint.GEOMETRY_EXACT},
                ),
            ],
        }
    )

    async def _fake_load(*a, **kw):
        return cfg

    monkeypatch.setattr(routed_resolver, "_load_routing_config", _fake_load)
    monkeypatch.setattr(
        routed_resolver, "_index_for",
        lambda rpc: {
            "collection_elasticsearch_driver": es,
            "collection_postgresql_driver": pg,
        },
    )

    resolved = await routed_resolver.resolve_routed(
        CollectionRoutingConfig, Operation.READ, "cat", "coll",
        hints=frozenset({Hint.GEOMETRY_EXACT}),
    )
    refs = [e.driver_ref for e, _ in resolved]
    assert refs == ["collection_postgresql_driver"]
    assert resolved[0][1] is pg


@pytest.mark.asyncio
async def test_resolve_routed_geometry_simplified_hint_selects_es(monkeypatch):
    """hints={GEOMETRY_SIMPLIFIED} → ES wins as longest-match, PG excluded."""
    from dynastore.modules.storage import routed_resolver
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation, OperationDriverEntry,
    )
    from dynastore.modules.storage.hints import Hint

    pg = _FakePgDriver()
    es = _FakeEsDriver()
    cfg = CollectionRoutingConfig(
        operations={
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    hints={Hint.GEOMETRY_SIMPLIFIED, Hint.METADATA},
                ),
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    hints={Hint.GEOMETRY_EXACT},
                ),
            ],
        }
    )

    async def _fake_load(*a, **kw):
        return cfg

    monkeypatch.setattr(routed_resolver, "_load_routing_config", _fake_load)
    monkeypatch.setattr(
        routed_resolver, "_index_for",
        lambda rpc: {
            "collection_elasticsearch_driver": es,
            "collection_postgresql_driver": pg,
        },
    )

    resolved = await routed_resolver.resolve_routed(
        CollectionRoutingConfig, Operation.READ, "cat", "coll",
        hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}),
    )
    refs = [e.driver_ref for e, _ in resolved]
    # ES carries GEOMETRY_SIMPLIFIED; PG carries GEOMETRY_EXACT → only ES matches
    assert refs == ["collection_elasticsearch_driver"]
    assert resolved[0][1] is es


@pytest.mark.asyncio
async def test_resolve_routed_unmatched_hint_relaxes_to_full_list(monkeypatch):
    """When no driver matches the hint for a READ, relax to full ordered list."""
    from dynastore.modules.storage import routed_resolver
    from dynastore.modules.storage.routing_config import (
        CollectionRoutingConfig, Operation, OperationDriverEntry,
    )
    from dynastore.modules.storage.hints import Hint

    pg = _FakePgDriver()
    es = _FakeEsDriver()
    cfg = CollectionRoutingConfig(
        operations={
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    hints={Hint.GEOMETRY_SIMPLIFIED},
                ),
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    hints={Hint.GEOMETRY_EXACT},
                ),
            ],
        }
    )

    async def _fake_load(*a, **kw):
        return cfg

    monkeypatch.setattr(routed_resolver, "_load_routing_config", _fake_load)
    monkeypatch.setattr(
        routed_resolver, "_index_for",
        lambda rpc: {
            "collection_elasticsearch_driver": es,
            "collection_postgresql_driver": pg,
        },
    )

    # TILES hint is not declared by either entry → no match → relax
    resolved = await routed_resolver.resolve_routed(
        CollectionRoutingConfig, Operation.READ, "cat", "coll",
        hints=frozenset({Hint.TILES}),
    )
    refs = [e.driver_ref for e, _ in resolved]
    assert refs == ["collection_elasticsearch_driver", "collection_postgresql_driver"]


# ---------------------------------------------------------------------------
# Step 5: get_collection_metadata / get_catalog_metadata dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_collection_metadata_no_hints_merges_all_drivers():
    """Empty hints → existing merge-all (both drivers called, results merged)."""
    from dynastore.modules.catalog import collection_router

    pg_meta = {"title": "PG title", "license": "cc-by"}
    es_meta = {"description": "ES description"}

    pg_driver = MagicMock()
    pg_driver.get_metadata = AsyncMock(return_value=pg_meta)
    es_driver = MagicMock()
    es_driver.get_metadata = AsyncMock(return_value=es_meta)

    result = await collection_router.get_collection_metadata(
        "cat", "coll",
        hints=frozenset(),
        drivers=[pg_driver, es_driver],
    )
    assert result == {**pg_meta, **es_meta}
    pg_driver.get_metadata.assert_awaited_once()
    es_driver.get_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_collection_metadata_hinted_first_non_none():
    """Non-empty hints → first-non-None: ES returns a value, PG not called."""
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.hints import Hint

    es_meta = {"title": "ES title", "description": "ES desc"}

    es_driver = MagicMock()
    es_driver.get_metadata = AsyncMock(return_value=es_meta)
    pg_driver = MagicMock()
    pg_driver.get_metadata = AsyncMock(return_value={"title": "PG title"})

    result = await collection_router.get_collection_metadata(
        "cat", "coll",
        hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}),
        drivers=[es_driver, pg_driver],
    )
    # ES answered; PG must not have been called
    assert result == es_meta
    es_driver.get_metadata.assert_awaited_once()
    pg_driver.get_metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_collection_metadata_hinted_es_none_falls_to_pg():
    """Non-empty hints, ES returns None → falls through to PG (fail-open)."""
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.hints import Hint

    pg_meta = {"title": "PG title", "license": "cc-by"}

    es_driver = MagicMock()
    es_driver.get_metadata = AsyncMock(return_value=None)
    pg_driver = MagicMock()
    pg_driver.get_metadata = AsyncMock(return_value=pg_meta)

    result = await collection_router.get_collection_metadata(
        "cat", "coll",
        hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}),
        drivers=[es_driver, pg_driver],
    )
    assert result == pg_meta
    pg_driver.get_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_catalog_metadata_no_hints_merges_all_drivers():
    """Empty hints → existing merge-all."""
    from dynastore.modules.catalog import catalog_router

    pg_meta = {"title": "PG catalog", "id": "cat1"}
    es_meta = {"description": "ES catalog desc"}

    pg_driver = MagicMock()
    pg_driver.get_catalog_metadata = AsyncMock(return_value=pg_meta)
    es_driver = MagicMock()
    es_driver.get_catalog_metadata = AsyncMock(return_value=es_meta)

    result = await catalog_router.get_catalog_metadata(
        "cat1",
        hints=frozenset(),
        drivers=[pg_driver, es_driver],
    )
    assert result == {**pg_meta, **es_meta}
    pg_driver.get_catalog_metadata.assert_awaited_once()
    es_driver.get_catalog_metadata.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_catalog_metadata_hinted_es_none_falls_to_pg():
    """Non-empty hints, ES returns None → PG serves as fallback."""
    from dynastore.modules.catalog import catalog_router
    from dynastore.modules.storage.hints import Hint

    pg_meta = {"title": "PG catalog", "id": "cat1"}

    es_driver = MagicMock()
    es_driver.get_catalog_metadata = AsyncMock(return_value=None)
    pg_driver = MagicMock()
    pg_driver.get_catalog_metadata = AsyncMock(return_value=pg_meta)

    result = await catalog_router.get_catalog_metadata(
        "cat1",
        hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}),
        drivers=[es_driver, pg_driver],
    )
    assert result == pg_meta
    pg_driver.get_catalog_metadata.assert_awaited_once()


# ---------------------------------------------------------------------------
# Step 6: Cache bypass and GEOMETRY_EXACT substitution (requirement A + B)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_collection_model_no_hints_hits_cache(monkeypatch):
    """Empty hints → goes through _collection_model_cache (not bypass path)."""
    from dynastore.modules.catalog import collection_service

    sentinel = object()
    mock_cache = AsyncMock(return_value=sentinel)
    monkeypatch.setattr(collection_service, "_collection_model_cache", mock_cache)

    from dynastore.modules.catalog.collection_service import CollectionService
    svc = CollectionService.__new__(CollectionService)
    svc.engine = None

    result = await svc.get_collection_model("cat", "coll")
    assert result is sentinel
    mock_cache.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_collection_model_non_empty_hints_bypasses_cache(monkeypatch):
    """Non-empty hints → bypasses cache; calls _get_collection_model_logic directly."""
    from dynastore.modules.catalog import collection_service
    from dynastore.modules.storage.hints import Hint

    sentinel = object()

    # Make the cache a spy — it must NOT be called
    cache_spy = AsyncMock(return_value=sentinel)
    monkeypatch.setattr(collection_service, "_collection_model_cache", cache_spy)

    from dynastore.modules.catalog.collection_service import CollectionService
    svc = CollectionService.__new__(CollectionService)
    svc.engine = MagicMock()

    logic_sentinel = object()

    async def _fake_logic(self, cat, coll, conn, *, hints=frozenset()):
        return logic_sentinel

    monkeypatch.setattr(
        CollectionService, "_get_collection_model_logic", _fake_logic,
    )

    # Patch managed_transaction to yield a dummy conn
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_tx(engine):
        yield MagicMock()

    monkeypatch.setattr(collection_service, "managed_transaction", _fake_tx)

    result = await svc.get_collection_model(
        "cat", "coll", hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}),
    )
    assert result is logic_sentinel
    cache_spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_collection_model_logic_empty_hints_pass_through_to_router(monkeypatch):
    """_get_collection_model_logic with empty hints forwards an empty hint set.

    Requirement A: the no-hint default read is byte-identical to the
    pre-hints baseline.  Empty hints reach the router unchanged, which keeps
    the merge-all path (no first-non-None, no driver substitution), so a
    collection whose PG metadata slice is empty still resolves to a model
    rather than collapsing to ``None``.
    """
    from dynastore.modules.catalog.collection_service import CollectionService

    captured_hints: list = []

    async def _fake_route_get_metadata(cat, coll, *, hints=frozenset(), db_resource=None):
        captured_hints.append(hints)
        return {"title": "test", "license": "cc-by"}

    # Patch the collection_router import inside collection_service
    import dynastore.modules.catalog.collection_router as cr_mod
    monkeypatch.setattr(cr_mod, "get_collection_metadata", _fake_route_get_metadata)

    svc = CollectionService.__new__(CollectionService)
    svc.engine = None

    # Patch _resolve_physical_schema + exists query to proceed past guards
    async def _fake_schema(cat_id, db_resource=None):
        return "schema_cat"

    svc._resolve_physical_schema = _fake_schema

    # Patch _make_collection_exists_query to return something truthy
    import dynastore.modules.catalog.collection_service as cs_mod

    def _fake_exists_query(schema):
        q = MagicMock()
        q.execute = AsyncMock(return_value=True)
        return q

    monkeypatch.setattr(cs_mod, "_make_collection_exists_query", _fake_exists_query)

    conn = MagicMock()
    # Call with empty hints — must forward empty (no substitution).
    await svc._get_collection_model_logic("cat", "coll", conn, hints=frozenset())

    assert len(captured_hints) == 1
    assert captured_hints[0] == frozenset(), (
        f"Expected empty hints forwarded unchanged; got {captured_hints[0]}"
    )


@pytest.mark.asyncio
async def test_collection_model_logic_passes_through_non_empty_hints(monkeypatch):
    """_get_collection_model_logic with non-empty hints passes them through unchanged."""
    from dynastore.modules.catalog.collection_service import CollectionService
    from dynastore.modules.storage.hints import Hint

    captured_hints: list = []

    async def _fake_route_get_metadata(cat, coll, *, hints=frozenset(), db_resource=None):
        captured_hints.append(hints)
        return {"title": "test", "license": "cc-by"}

    import dynastore.modules.catalog.collection_router as cr_mod
    monkeypatch.setattr(cr_mod, "get_collection_metadata", _fake_route_get_metadata)

    svc = CollectionService.__new__(CollectionService)
    svc.engine = None

    async def _fake_schema(cat_id, db_resource=None):
        return "schema_cat"

    svc._resolve_physical_schema = _fake_schema

    import dynastore.modules.catalog.collection_service as cs_mod

    def _fake_exists_query(schema):
        q = MagicMock()
        q.execute = AsyncMock(return_value=True)
        return q

    monkeypatch.setattr(cs_mod, "_make_collection_exists_query", _fake_exists_query)

    conn = MagicMock()
    requested = frozenset({Hint.GEOMETRY_SIMPLIFIED})
    await svc._get_collection_model_logic("cat", "coll", conn, hints=requested)

    assert len(captured_hints) == 1
    assert captured_hints[0] == requested


# ---------------------------------------------------------------------------
# Characterization test (requirement A): default path response unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_characterization_geometry_exact_hint_prefers_pg(monkeypatch):
    """An explicit geometry_exact read resolves the PG driver first.

    With a hinted driver list ordered PG-first (as the resolver's hint
    filter produces for geometry_exact), get_collection_metadata returns the
    PG driver's result immediately and never consults ES."""
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.hints import Hint

    pg_meta = {"title": "PG title", "license": "cc-by", "id": "coll1"}

    pg_driver = MagicMock()
    pg_driver.get_metadata = AsyncMock(return_value=pg_meta)
    es_driver = MagicMock()
    es_driver.get_metadata = AsyncMock(return_value={"title": "ES title"})

    # hints={GEOMETRY_EXACT} → first-non-None → PG wins immediately (ES declared first
    # in the config, but the hint-filter puts PG first).
    # However in the drivers list we pass pg first (as GEOMETRY_EXACT matches pg).
    # Simulate the real scenario: hinted driver list has PG first.
    result = await collection_router.get_collection_metadata(
        "cat", "coll1",
        hints=frozenset({Hint.GEOMETRY_EXACT}),
        drivers=[pg_driver, es_driver],
    )
    assert result == pg_meta
    # PG answered immediately; ES must not have been called
    pg_driver.get_metadata.assert_awaited_once()
    es_driver.get_metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_hinted_read_does_not_cross_contaminate_cached_model(monkeypatch):
    """Requirement B: a geometry_simplified read bypasses cache; a subsequent
    no-hint read still gets the cached (GEOMETRY_EXACT/PG) result, not the
    simplified one."""
    from dynastore.modules.catalog import collection_service
    from dynastore.modules.storage.hints import Hint

    pg_model = MagicMock(name="pg_model")
    es_model = MagicMock(name="es_model")

    cache_hit_count = [0]

    async def _fake_cache(svc, cat, coll):
        cache_hit_count[0] += 1
        return pg_model

    monkeypatch.setattr(collection_service, "_collection_model_cache", _fake_cache)

    from dynastore.modules.catalog.collection_service import CollectionService
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_tx(engine):
        yield MagicMock()

    monkeypatch.setattr(collection_service, "managed_transaction", _fake_tx)

    async def _fake_logic(self, cat, coll, conn, *, hints=frozenset()):
        return es_model

    monkeypatch.setattr(CollectionService, "_get_collection_model_logic", _fake_logic)

    svc = CollectionService.__new__(CollectionService)
    svc.engine = MagicMock()

    # Hinted read (geometry_simplified) → bypasses cache → returns es_model
    result_hinted = await svc.get_collection_model(
        "cat", "coll", hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}),
    )
    assert result_hinted is es_model
    assert cache_hit_count[0] == 0  # cache NOT consulted for hinted read

    # No-hint read → hits cache → returns pg_model
    result_default = await svc.get_collection_model("cat", "coll")
    assert result_default is pg_model
    assert cache_hit_count[0] == 1  # cache consulted once for default read


# ---------------------------------------------------------------------------
# Step 7: LIST routes thread hints (the regression the bug names)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_collections_service_threads_hints_to_logic(monkeypatch):
    """CollectionService.list_collections forwards hints into _get_collection_model_logic."""
    from dynastore.modules.catalog import collection_service
    from dynastore.modules.catalog.collection_service import CollectionService
    from dynastore.modules.storage.hints import Hint
    from contextlib import asynccontextmanager

    captured: list = []

    async def _fake_logic(self, cat, coll, conn, *, hints=frozenset()):
        captured.append(hints)
        return MagicMock(id=coll)

    monkeypatch.setattr(CollectionService, "_get_collection_model_logic", _fake_logic)

    @asynccontextmanager
    async def _fake_tx(engine):
        yield MagicMock()

    monkeypatch.setattr(collection_service, "managed_transaction", _fake_tx)

    async def _fake_schema(self, cat, db_resource=None):
        return "public"

    monkeypatch.setattr(CollectionService, "_resolve_physical_schema", _fake_schema)

    def _fake_ids_query(schema):
        q = MagicMock()
        q.execute = AsyncMock(return_value=["coll_a"])
        return q

    monkeypatch.setattr(collection_service, "_make_collection_list_ids_query", _fake_ids_query)

    svc = CollectionService.__new__(CollectionService)
    svc.engine = MagicMock()

    hints_in = frozenset({Hint.GEOMETRY_SIMPLIFIED})
    result = await svc.list_collections("cat", hints=hints_in)

    assert len(result) == 1
    assert len(captured) == 1
    assert captured[0] == hints_in


@pytest.mark.asyncio
async def test_list_collections_service_no_hints_passes_empty_frozenset(monkeypatch):
    """CollectionService.list_collections with no hints passes empty frozenset → PG path."""
    from dynastore.modules.catalog import collection_service
    from dynastore.modules.catalog.collection_service import CollectionService
    from contextlib import asynccontextmanager

    captured: list = []

    async def _fake_logic(self, cat, coll, conn, *, hints=frozenset()):
        captured.append(hints)
        return MagicMock(id=coll)

    monkeypatch.setattr(CollectionService, "_get_collection_model_logic", _fake_logic)

    @asynccontextmanager
    async def _fake_tx(engine):
        yield MagicMock()

    monkeypatch.setattr(collection_service, "managed_transaction", _fake_tx)

    async def _fake_schema(self, cat, db_resource=None):
        return "public"

    monkeypatch.setattr(CollectionService, "_resolve_physical_schema", _fake_schema)

    def _fake_ids_query(schema):
        q = MagicMock()
        q.execute = AsyncMock(return_value=["coll_b"])
        return q

    monkeypatch.setattr(collection_service, "_make_collection_list_ids_query", _fake_ids_query)

    svc = CollectionService.__new__(CollectionService)
    svc.engine = MagicMock()

    # No hints supplied → empty frozenset forwarded unchanged → merge-all
    # default path (byte-identical to the pre-hints baseline).
    await svc.list_collections("cat")

    assert len(captured) == 1
    assert captured[0] == frozenset()


@pytest.mark.asyncio
async def test_create_collections_catalog_threads_hints_to_create_collection(monkeypatch):
    """stac_generator.create_collections_catalog forwards hints into create_collection."""
    from dynastore.extensions.stac import stac_generator
    from dynastore.modules.storage.hints import Hint

    captured_hints: list = []

    fake_coll = MagicMock()
    fake_coll.id = "coll_x"

    fake_catalogs_svc = MagicMock()
    fake_catalogs_svc.list_collections = AsyncMock(return_value=[fake_coll])

    monkeypatch.setattr(stac_generator, "get_protocol", lambda proto: fake_catalogs_svc)

    async def _fake_create_collection(req, catalog_id, collection_id, lang="en", hints=frozenset()):
        captured_hints.append(hints)
        return None  # None → skipped in output

    monkeypatch.setattr(stac_generator, "create_collection", _fake_create_collection)
    monkeypatch.setattr(stac_generator, "get_root_url", lambda req: "http://test")

    fake_request = MagicMock()

    hints_in = frozenset({Hint.METADATA})
    await stac_generator.create_collections_catalog(fake_request, "cat_a", lang="en", hints=hints_in)

    assert len(captured_hints) == 1
    assert captured_hints[0] == hints_in


@pytest.mark.asyncio
async def test_create_collections_catalog_no_hints_passes_empty_frozenset(monkeypatch):
    """create_collections_catalog with no hints arg passes empty frozenset to create_collection."""
    from dynastore.extensions.stac import stac_generator

    captured_hints: list = []

    fake_coll = MagicMock()
    fake_coll.id = "coll_y"

    fake_catalogs_svc = MagicMock()
    fake_catalogs_svc.list_collections = AsyncMock(return_value=[fake_coll])

    monkeypatch.setattr(stac_generator, "get_protocol", lambda proto: fake_catalogs_svc)

    async def _fake_create_collection(req, catalog_id, collection_id, lang="en", hints=frozenset()):
        captured_hints.append(hints)
        return None

    monkeypatch.setattr(stac_generator, "create_collection", _fake_create_collection)
    monkeypatch.setattr(stac_generator, "get_root_url", lambda req: "http://test")

    fake_request = MagicMock()

    # No hints arg → default empty frozenset → no-hint path unchanged
    await stac_generator.create_collections_catalog(fake_request, "cat_b")

    assert len(captured_hints) == 1
    assert captured_hints[0] == frozenset()


@pytest.mark.asyncio
async def test_catalog_service_list_collections_threads_hints(monkeypatch):
    """CatalogService.list_collections forwards hints to CollectionService.list_collections."""
    from dynastore.modules.catalog.catalog_service import CatalogService
    from dynastore.modules.catalog.collection_service import CollectionService
    from dynastore.modules.storage.hints import Hint

    captured: list = []

    async def _fake_col_list(self, cat, limit=10, offset=0, lang="en", ctx=None, q=None, *, hints=frozenset()):
        captured.append(hints)
        return []

    monkeypatch.setattr(CollectionService, "list_collections", _fake_col_list)

    svc = CatalogService.__new__(CatalogService)
    # _col_svc is a property backed by _collection_service
    svc._collection_service = CollectionService.__new__(CollectionService)

    hints_in = frozenset({Hint.GEOMETRY_SIMPLIFIED})
    await svc.list_collections("cat", hints=hints_in)

    assert len(captured) == 1
    assert captured[0] == hints_in


@pytest.mark.asyncio
async def test_catalog_service_list_collections_no_hints(monkeypatch):
    """CatalogService.list_collections with no hints passes empty frozenset."""
    from dynastore.modules.catalog.catalog_service import CatalogService
    from dynastore.modules.catalog.collection_service import CollectionService

    captured: list = []

    async def _fake_col_list(self, cat, limit=10, offset=0, lang="en", ctx=None, q=None, *, hints=frozenset()):
        captured.append(hints)
        return []

    monkeypatch.setattr(CollectionService, "list_collections", _fake_col_list)

    svc = CatalogService.__new__(CatalogService)
    svc._collection_service = CollectionService.__new__(CollectionService)

    await svc.list_collections("cat")

    assert len(captured) == 1
    assert captured[0] == frozenset()
