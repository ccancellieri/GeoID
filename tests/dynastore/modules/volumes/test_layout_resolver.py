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

"""Tests for the protocol-resolved physical layout path.

The volumes tiler must read the *actual* physical hub table, geometries
sidecar table, geometry column, and feature-id column — not the hardcoded
``<collection_id>`` / ``<collection_id>_geometries`` / ``geom`` / ``geoid``
conventions. ``CollectionPhysicalLayout`` + the ``layout_resolver`` on
``SidecarBoundsSource`` / ``SidecarGeometryFetcher`` carry those values
through to the emitted SQL.
"""

from __future__ import annotations

import asyncio
import inspect
from unittest.mock import patch

import pytest

from dynastore.modules.storage.drivers.pg_sidecars.naming import sidecar_table_name
from dynastore.modules.volumes.geometry_fetcher import SidecarGeometryFetcher
from dynastore.modules.volumes.sidecar_bounds import (
    CollectionPhysicalLayout,
    SidecarBoundsSource,
)


@pytest.fixture(autouse=True)
def _patch_is_async_resource_for_fake_conns():
    """Make DQLQuery treat fake test connections as async resources.

    Mirrors the fixture in ``test_sidecar_bounds`` — DQLQuery dispatches
    via ``is_async_resource(conn)``; fake conns with an ``async def
    execute`` would otherwise take the sync path and hit a real DB.
    """
    import dynastore.modules.db_config.query_executor as _qe

    _orig = _qe.is_async_resource

    def _patched(conn):
        if not _orig(conn):
            exec_attr = getattr(conn, "execute", None)
            if exec_attr is not None and (
                asyncio.iscoroutinefunction(exec_attr)
                or inspect.iscoroutinefunction(exec_attr)
            ):
                return True
        return _orig(conn)

    with patch.object(_qe, "is_async_resource", side_effect=_patched):
        yield


@pytest.fixture(autouse=True)
def _clear_get_bounds_cache():
    """Reset the @cached layer on get_bounds so cross-test cache state
    can't mask the SQL the resolver actually emits."""
    SidecarBoundsSource.get_bounds.cache_clear()
    yield
    SidecarBoundsSource.get_bounds.cache_clear()


# --- SSOT naming helper -----------------------------------------------


def test_sidecar_table_name_builds_convention():
    assert sidecar_table_name("denhaag", "geometries") == "denhaag_geometries"
    assert sidecar_table_name("phys_abc123", "geometries") == "phys_abc123_geometries"


# --- Fake connection plumbing (mirrors test_sidecar_bounds helpers) ---


class _FakeRow:
    def __init__(self, data: dict):
        self._data = data

    def _asdict(self):
        return dict(self._data)

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)


class _FakeResult:
    def __init__(self, rows):
        self._rows = [_FakeRow(r) if isinstance(r, dict) else r for r in rows]

    def all(self):
        return list(self._rows)

    def fetchall(self):
        return list(self._rows)


def _fake_connection_factory_returning(rows, sql_sink=None):
    class _Conn:
        async def execute(self, sql_clause, params=None, **_kw):
            if sql_sink is not None:
                sql_sink.append(str(sql_clause))
            return _FakeResult(rows)

    class _FactoryCM:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *a):
            return None

    def _call(*args, **kwargs):
        return _FactoryCM()

    return _call


def _layout_resolver(layout: CollectionPhysicalLayout):
    async def _r(catalog_id, collection_id):
        return layout

    return _r


# --- Bounds source: layout_resolver threads custom layout into SQL ----


@pytest.mark.asyncio
async def test_bounds_source_layout_resolver_threads_physical_layout():
    """A resolved physical table + geom column must appear in the SQL.

    Proves the tiler reads the machine-assigned physical table (not the
    collection_id) and a non-default geometry column.
    """
    fake_rows = [
        {"feature_id": "a", "min_x": 0, "min_y": 0, "min_z": 0,
         "max_x": 1, "max_y": 1, "max_z": 1},
    ]
    executed_sql: list = []
    layout = CollectionPhysicalLayout(
        schema="tenant_x",
        hub_table="phys_9f3",
        geometries_table="phys_9f3_geometries",
        geom_column="the_geom",
        feature_id_column="fid",
    )

    src = SidecarBoundsSource(
        connection_factory=_fake_connection_factory_returning(
            fake_rows, sql_sink=executed_sql,
        ),
        layout_resolver=_layout_resolver(layout),
    )

    bounds = await src.get_bounds("cat_layout_a", "col_layout_a")
    assert [b.feature_id for b in bounds] == ["a"]
    sql = executed_sql[0]
    assert '"tenant_x"."phys_9f3"' in sql
    assert '"tenant_x"."phys_9f3_geometries"' in sql
    assert 'g."the_geom"' in sql
    assert 'h."fid"' in sql


@pytest.mark.asyncio
async def test_bounds_source_layout_resolver_rejects_unsafe_resolved_column():
    """Identifiers from the resolver are still validated before SQL build."""
    bad_layout = CollectionPhysicalLayout(
        schema="s",
        hub_table="t",
        geometries_table="t_geometries",
        geom_column="geom; DROP TABLE x",
    )
    src = SidecarBoundsSource(
        connection_factory=_fake_connection_factory_returning([]),
        layout_resolver=_layout_resolver(bad_layout),
    )
    with pytest.raises(ValueError):
        await src.get_bounds("cat_bad_layout", "col_bad_layout")


# --- Geometry fetcher: same threading ---------------------------------


@pytest.mark.asyncio
async def test_geometry_fetcher_layout_resolver_threads_physical_layout():
    fake_rows = [
        {"feature_id": "a", "geom_wkb": b"\x01", "height": 3.0},
    ]
    executed_sql: list = []
    layout = CollectionPhysicalLayout(
        schema="tenant_y",
        hub_table="phys_abc",
        geometries_table="phys_abc_geometries",
        geom_column="the_geom",
        feature_id_column="fid",
    )

    fetcher = SidecarGeometryFetcher(
        connection_factory=_fake_connection_factory_returning(
            fake_rows, sql_sink=executed_sql,
        ),
        layout_resolver=_layout_resolver(layout),
    )

    geoms = await fetcher.get_geometries("cat_g", "col_g", ["a"])
    assert [g.feature_id for g in geoms] == ["a"]
    sql = executed_sql[0]
    assert '"tenant_y"."phys_abc"' in sql
    assert '"tenant_y"."phys_abc_geometries"' in sql
    assert 'g."the_geom"' in sql
    assert 'h."fid"' in sql
