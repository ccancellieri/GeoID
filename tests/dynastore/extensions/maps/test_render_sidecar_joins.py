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

"""Regression: ``maps_db.get_features_for_rendering`` must read ``geom`` and
``attributes`` from the PG hub table's sidecars, not from the hub table
itself.

The hub table only carries ``geoid``/``transaction_time``/``deleted_at`` —
``geom`` lives on the ``{physical_table}_geometries`` sidecar and
``attributes`` on the ``{physical_table}_attributes`` sidecar, both joined on
``geoid``. Selecting/filtering those columns straight off the hub table
raised ``UndefinedColumn`` (pgcode 42703) on every PG-backed render request.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.maps import maps_db


class _FakeDriver:
    def __init__(self, cfg: Any) -> None:
        self._cfg = cfg

    async def get_driver_config(self, schema: str, collection: str) -> Any:
        return self._cfg


def _layer_cfg(srid: int) -> MagicMock:
    cfg = MagicMock(name=f"layer_cfg_srid_{srid}")
    cfg.__srid__ = srid
    return cfg


def _patch_meta_resolution(monkeypatch) -> None:
    async def _fake_get_driver(_op, _schema, collection):
        return _FakeDriver(_layer_cfg(4326))

    async def _fake_get_table_column_names(_conn, _schema, _table):
        return ["geoid"]

    def _fake_driver_sidecars(cfg):
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
        )
        sc = MagicMock(spec=GeometriesSidecarConfig)
        sc.target_srid = cfg.__srid__
        return [sc]

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_driver", _fake_get_driver
    )
    monkeypatch.setattr(
        "dynastore.modules.db_config.shared_queries.get_table_column_names",
        _fake_get_table_column_names,
    )
    monkeypatch.setattr(
        "dynastore.modules.storage.drivers.pg_sidecars.driver_sidecars",
        _fake_driver_sidecars,
    )


@pytest.mark.asyncio
async def test_render_query_joins_geometry_and_attribute_sidecars(monkeypatch):
    _patch_meta_resolution(monkeypatch)

    captured_query = {}

    def _capture_dql_query(sql, **kwargs):
        captured_query["sql"] = sql
        return MagicMock(execute=AsyncMock(return_value=[]))

    with patch.object(maps_db, "DQLQuery", side_effect=_capture_dql_query):
        await maps_db.get_features_for_rendering(
            conn=AsyncMock(),
            schema="c_internal123",
            collections=["gaul_level_1"],
            bbox=[0, 0, 1, 1],
            crs="EPSG:4326",
            width=256,
            height=256,
        )

    sql = captured_query["sql"]

    # Hub table is aliased and JOINed to its geometry/attribute sidecars.
    assert 'FROM "c_internal123"."gaul_level_1" h' in sql
    assert (
        'JOIN "c_internal123"."gaul_level_1_geometries" g ON h.geoid = g.geoid' in sql
    )
    assert (
        'JOIN "c_internal123"."gaul_level_1_attributes" a ON h.geoid = a.geoid' in sql
    )

    # geom/attributes/geoid are selected off the correct aliased table, never
    # off the bare (column-less) hub table.
    assert "ST_SimplifyPreserveTopology(g.geom," in sql
    assert "ST_Intersects(g.geom," in sql
    assert "h.geoid" in sql
    assert "a.attributes" in sql
    assert "FROM \"c_internal123\".\"gaul_level_1\"\n" not in sql
