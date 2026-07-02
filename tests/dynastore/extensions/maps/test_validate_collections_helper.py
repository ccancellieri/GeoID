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

"""Regression: ``_validate_collections_helper`` must check the collection's
resolved physical table name, not its (id-shaped but not guaranteed
identical) ``collection_id``, against ``information_schema.tables``.

A collection whose ``physical_table`` diverges from its ``collection_id``
(driver config resolved via ``CatalogsProtocol.resolve_physical_table``,
exactly as every other physical read/write path in the codebase does)
false-negatived here, 404ing ``GET .../map`` even though the collection is
alive and the same id serves real data through every direct-by-id path
(items, tiles, single-collection GET).
"""

from __future__ import annotations

import sys
import types
from unittest.mock import AsyncMock, MagicMock

import pytest


def _install_osgeo_stubs() -> bool:
    """Install bare osgeo stubs; returns True if this call installed them
    (``osgeo`` was absent), i.e. it is safe for the caller to remove them
    again once the module import that needed them has completed — see
    ``_uninstall_osgeo_stubs``.
    """
    if "osgeo" in sys.modules:
        return False
    osgeo = types.ModuleType("osgeo")
    osgeo.__version__ = "3.x.stub"  # type: ignore[attr-defined]

    gdal = types.ModuleType("osgeo.gdal")
    gdal.Dataset = object  # type: ignore[attr-defined]
    gdal.GetDriverByName = lambda n: None  # type: ignore[attr-defined]
    gdal.VSIFOpenL = lambda *a: None  # type: ignore[attr-defined]
    gdal.VSIFSeekL = lambda *a: None  # type: ignore[attr-defined]
    gdal.VSIFTellL = lambda *a: 0  # type: ignore[attr-defined]
    gdal.VSIFReadL = lambda *a: b""  # type: ignore[attr-defined]
    gdal.VSIFCloseL = lambda *a: None  # type: ignore[attr-defined]
    gdal.Unlink = lambda *a: None  # type: ignore[attr-defined]
    gdal.RasterizeLayer = lambda *a, **kw: None  # type: ignore[attr-defined]

    ogr = types.ModuleType("osgeo.ogr")
    ogr.Geometry = object  # type: ignore[attr-defined]
    ogr.Layer = object  # type: ignore[attr-defined]
    ogr.Feature = lambda *a: None  # type: ignore[attr-defined]
    ogr.GetDriverByName = lambda n: None  # type: ignore[attr-defined]
    ogr.wkbLineString = 2  # type: ignore[attr-defined]
    ogr.wkbMultiPolygon = 6  # type: ignore[attr-defined]

    osr = types.ModuleType("osgeo.osr")
    osr.SpatialReference = object  # type: ignore[attr-defined]

    sys.modules.update({
        "osgeo": osgeo,
        "osgeo.gdal": gdal,
        "osgeo.ogr": ogr,
        "osgeo.osr": osr,
    })
    return True


def _uninstall_osgeo_stubs() -> None:
    """Remove the stub entries installed by ``_install_osgeo_stubs``.

    ``maps_service`` binds ``gdal``/``ogr``/``osr`` into its own module
    globals at import time, so these ``sys.modules`` entries aren't needed
    afterwards. Leaving them registered shadows the real ``osgeo`` package
    for every test module collected later in the same process — a later
    ``from osgeo import gdal; gdal.UseExceptions()`` (e.g.
    ``dynastore.modules.gdal.service``) would resolve to this bare stub and
    raise ``AttributeError: module 'osgeo.gdal' has no attribute
    'UseExceptions'`` instead of importing the real bindings.
    """
    for name in ("osgeo.osr", "osgeo.ogr", "osgeo.gdal", "osgeo"):
        sys.modules.pop(name, None)


_we_installed_osgeo_stubs = _install_osgeo_stubs()

from dynastore.extensions.maps import maps_service as ms  # noqa: E402

if _we_installed_osgeo_stubs:
    _uninstall_osgeo_stubs()


@pytest.mark.asyncio
async def test_uses_resolved_physical_table_not_raw_collection_id(monkeypatch):
    """RED→GREEN: the physical-existence check must query the RESOLVED
    physical table, not the raw collection id, when they diverge."""
    svc = MagicMock()
    svc.get_collection = AsyncMock(return_value=MagicMock())
    svc.resolve_physical_table = AsyncMock(return_value="physical_tbl_9f2")

    monkeypatch.setattr(ms, "get_protocol", lambda proto: svc)

    table_exists_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(
        ms.shared_queries.table_exists_query, "execute", table_exists_mock
    )

    conn = MagicMock()
    result = await ms._validate_collections_helper(
        conn, "c_internal123", ["gaul_level_1"]
    )

    assert result == ["gaul_level_1"]
    table_exists_mock.assert_awaited_once()
    _, kwargs = table_exists_mock.call_args
    # Must check existence of the RESOLVED physical table, never the
    # collection id verbatim — that is what produced the false-negative
    # "One or more collections not found." 404.
    assert kwargs["table"] == "physical_tbl_9f2"
    assert kwargs["schema"] == "c_internal123"


@pytest.mark.asyncio
async def test_falls_back_to_collection_id_when_resolution_returns_none(monkeypatch):
    """When no distinct physical_table is configured (the common case),
    behaviour is unchanged: the collection id itself is checked."""
    svc = MagicMock()
    svc.get_collection = AsyncMock(return_value=MagicMock())
    svc.resolve_physical_table = AsyncMock(return_value=None)

    monkeypatch.setattr(ms, "get_protocol", lambda proto: svc)

    table_exists_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(
        ms.shared_queries.table_exists_query, "execute", table_exists_mock
    )

    conn = MagicMock()
    result = await ms._validate_collections_helper(
        conn, "c_internal123", ["col_abc"]
    )

    assert result == ["col_abc"]
    _, kwargs = table_exists_mock.call_args
    assert kwargs["table"] == "col_abc"


@pytest.mark.asyncio
async def test_missing_physical_table_still_excludes_collection(monkeypatch):
    """A collection whose logical metadata exists but whose physical table
    genuinely does not (mid-provisioning) is still correctly excluded."""
    svc = MagicMock()
    svc.get_collection = AsyncMock(return_value=MagicMock())
    svc.resolve_physical_table = AsyncMock(return_value="physical_tbl_9f2")

    monkeypatch.setattr(ms, "get_protocol", lambda proto: svc)

    table_exists_mock = AsyncMock(return_value=False)
    monkeypatch.setattr(
        ms.shared_queries.table_exists_query, "execute", table_exists_mock
    )

    conn = MagicMock()
    result = await ms._validate_collections_helper(
        conn, "c_internal123", ["gaul_level_1"]
    )

    assert result == []
