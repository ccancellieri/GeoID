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

"""Regression: ``maps_db.get_features_for_rendering`` must build its query
against the RESOLVED physical table, not the raw collection id, when they
diverge.

A collection whose ``physical_table`` diverges from its ``collection_id``
(driver config resolved via ``resolve_physical_table``, exactly as every
other physical read/write path in the codebase does) previously produced
a ``FROM "{schema}"."{collection_id}"`` query against a table that does not
exist, silently returning no rows (or erroring) even though the same
collection serves real data through every direct-by-id read path.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.maps import maps_db


class _FakeDriverWithDivergingPhysicalTable:
    """Driver whose physical table name differs from the collection id."""

    def __init__(self, physical_table: str, cfg: Any) -> None:
        self._physical_table = physical_table
        self._cfg = cfg

    async def resolve_physical_table(self, catalog_id, collection_id, *, db_resource=None):
        return self._physical_table

    async def get_driver_config(self, schema: str, collection: str) -> Any:
        return self._cfg


def _layer_cfg(srid: int) -> MagicMock:
    cfg = MagicMock(name=f"layer_cfg_srid_{srid}")
    cfg.__srid__ = srid
    return cfg


@pytest.mark.asyncio
async def test_query_uses_resolved_physical_table_not_collection_id(monkeypatch):
    """RED->GREEN: FROM clause and column lookup must target the resolved
    physical table, while the ``layer`` label keeps the external collection id."""

    physical_table = "physical_tbl_9f2"
    cfg = _layer_cfg(4326)

    async def _fake_get_driver(_op, _schema, _collection):
        return _FakeDriverWithDivergingPhysicalTable(physical_table, cfg)

    seen_table_lookups = []

    async def _fake_get_table_column_names(_conn, _schema, table):
        seen_table_lookups.append(table)
        return ["geoid", "attributes"]

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

    # Column introspection must run against the physical table, not the id.
    assert seen_table_lookups == [physical_table]

    # The rendered SQL must select FROM the physical table...
    assert f'"c_internal123"."{physical_table}"' in captured_query["sql"]
    # ...while still labelling the layer with the external collection id.
    assert "'gaul_level_1' as layer" in captured_query["sql"]
    # The raw collection id must never appear as a FROM target.
    assert '"c_internal123"."gaul_level_1"' not in captured_query["sql"]
