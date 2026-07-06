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

"""Regression tests for #2865's PG-fallback batch fetch: the auto-expanded
scope's fast path must only be taken for the PostgreSQL items READ driver.

``get_driver_config`` is polymorphic per driver class — PostgreSQL, BigQuery,
Iceberg, and DuckDB each return their own config type — while the batched
fetch (``ConfigsProtocol.get_configs_batch``) always resolves an
``ItemsPostgresqlDriverConfig``. These tests pin:

1. a non-PostgreSQL READ driver takes the original per-collection
   ``driver.get_driver_config`` loop, never the batch call;
2. a missing ``ConfigsProtocol`` also falls back to the per-collection loop
   instead of silently returning no results.

Both are exercised through ``search_items`` itself (not just the driver-type
branch in isolation) using the same COLUMNAR-sidecar ``ValueError`` guard the
existing sort-properties tests use to short-circuit before any SQL/DB work.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
    PostgresType,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)


def _columnar_cfg() -> ItemsPostgresqlDriverConfig:
    """A COLUMNAR-mode config — ``properties.*`` sort raises ValueError for
    it deep in ``search_items``, letting these tests stop before any SQL
    work regardless of which resolution path produced the config."""
    return ItemsPostgresqlDriverConfig(
        sidecars=[
            GeometriesSidecarConfig(),
            FeatureAttributeSidecarConfig(
                storage_mode=AttributeStorageMode.COLUMNAR,
                attribute_schema=[
                    AttributeSchemaEntry(name="eo_cloud_cover", type=PostgresType.TEXT)
                ],
            ),
        ],
        physical_table="items",
    )


def _make_unscoped_prop_sort_request():
    """An unscoped (bare) search request — triggers the auto-expand branch."""
    from dynastore.extensions.stac.search import ItemSearchRequest

    return ItemSearchRequest(
        catalog_id="cat-x",
        sortby=["-properties.eo:cloud_cover"],
    )


def _wire_catalogs_mock(col_id: str) -> MagicMock:
    catalogs_mock = MagicMock()
    catalogs_mock.list_collection_id_pairs = AsyncMock(return_value=[(col_id, col_id)])
    catalogs_mock.get_collection_column_names = AsyncMock(return_value=[])
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value="test_schema")
    return catalogs_mock


def _wire_get_protocol(monkeypatch, catalogs_mock, configs_mock):
    def _fake_get_protocol(proto):
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is ConfigsProtocol:
            return configs_mock
        return None

    monkeypatch.setattr(
        "dynastore.extensions.stac.search.get_protocol", _fake_get_protocol
    )


@pytest.mark.asyncio
async def test_auto_expanded_scope_non_pg_driver_uses_per_collection_loop(monkeypatch):
    """A non-PostgreSQL READ driver must never go through the PG-shaped
    batch fetch — it takes the original per-collection ``get_driver_config``
    loop instead."""
    import dynastore.extensions.stac.search as search_mod
    from dynastore.modules.stac.stac_config import StacPluginConfig

    col_id = "col-a"
    monkeypatch.setattr(
        search_mod, "_maybe_dispatch_to_es_search", AsyncMock(return_value=None),
    )

    catalogs_mock = _wire_catalogs_mock(col_id)
    configs_mock = MagicMock()
    configs_mock.get_configs_batch = AsyncMock(return_value={})
    _wire_get_protocol(monkeypatch, catalogs_mock, configs_mock)

    # A driver that is deliberately NOT ``ItemsPostgresqlDriver`` — stands in
    # for BigQuery/Iceberg/DuckDB, whose ``get_driver_config`` returns a
    # different config type than the batch call assumes.
    class _FakeNonPgDriver:
        pass

    fake_driver = _FakeNonPgDriver()
    fake_driver.get_driver_config = AsyncMock(return_value=_columnar_cfg())

    async def _fake_get_driver(op, cat_id, cid=None, **_kw):
        return fake_driver

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_driver", _fake_get_driver,
    )

    req = _make_unscoped_prop_sort_request()

    with pytest.raises(ValueError, match="COLUMNAR"):
        await search_mod.search_items(None, req, StacPluginConfig())  # type: ignore[arg-type]

    fake_driver.get_driver_config.assert_awaited()
    configs_mock.get_configs_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_auto_expanded_scope_missing_configs_protocol_falls_back(monkeypatch):
    """A PostgreSQL READ driver with no ``ConfigsProtocol`` registered must
    fall back to the per-collection loop, not silently return empty results."""
    import dynastore.extensions.stac.search as search_mod
    import dynastore.modules.storage.drivers.postgresql as pg_mod
    from dynastore.modules.stac.stac_config import StacPluginConfig

    col_id = "col-a"
    monkeypatch.setattr(
        search_mod, "_maybe_dispatch_to_es_search", AsyncMock(return_value=None),
    )

    catalogs_mock = _wire_catalogs_mock(col_id)
    _wire_get_protocol(monkeypatch, catalogs_mock, configs_mock=None)

    # Marker class swapped in for ``ItemsPostgresqlDriver`` so the resolved
    # fake driver passes the isinstance guard without standing up the real
    # (heavier) driver class.
    class _FakeMarkerPgDriver:
        pass

    monkeypatch.setattr(pg_mod, "ItemsPostgresqlDriver", _FakeMarkerPgDriver)

    fake_driver = _FakeMarkerPgDriver()
    fake_driver.get_driver_config = AsyncMock(return_value=_columnar_cfg())

    async def _fake_get_driver(op, cat_id, cid=None, **_kw):
        return fake_driver

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_driver", _fake_get_driver,
    )

    req = _make_unscoped_prop_sort_request()

    with pytest.raises(ValueError, match="COLUMNAR"):
        await search_mod.search_items(None, req, StacPluginConfig())  # type: ignore[arg-type]

    fake_driver.get_driver_config.assert_awaited()
