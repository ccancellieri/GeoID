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

"""Tests for the bounded default preseed scope (#2813): `_bounded_default_scope`.

When no bbox is configured anywhere, the preseed task must derive a bbox
from the collection's STAC spatial extent (not the world) and cap the zoom
range, so an unbounded default scope can't fan out to an enormous tile
count on a job image without a bucket cache backend.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("morecantile")  # optional dep — skip when SCOPE excludes it

from dynastore.tasks.tiles_preseed.task import PRESEED_DEFAULT_MAX_ZOOM, _bounded_default_scope
from dynastore.modules.tiles.tiles_config import TilesPreseedConfig


def _fake_catalogs(bbox):
    catalogs = MagicMock()
    collection = MagicMock()
    collection.extent.spatial.bbox = bbox
    catalogs.get_collection = AsyncMock(return_value=collection)
    return catalogs


@pytest.mark.asyncio
async def test_derives_bbox_from_stac_spatial_extent():
    catalogs = _fake_catalogs([[10.0, 20.0, 30.0, 40.0]])
    bbox, max_zoom = await _bounded_default_scope(
        catalogs, "cat1", "coll1", TilesPreseedConfig(), runtime_max_zoom=12,
    )
    assert bbox == [(10.0, 20.0, 30.0, 40.0)]
    assert max_zoom == PRESEED_DEFAULT_MAX_ZOOM


@pytest.mark.asyncio
async def test_falls_back_to_world_bbox_when_extent_is_placeholder():
    """The Collection default extent bbox is [[0,0,0,0]] — a placeholder,
    not a real STAC extent — so this must fall back to the world bbox
    rather than silently scoping to a zero-area box."""
    catalogs = _fake_catalogs([[0.0, 0.0, 0.0, 0.0]])
    bbox, _max_zoom = await _bounded_default_scope(
        catalogs, "cat1", "coll1", TilesPreseedConfig(), runtime_max_zoom=12,
    )
    assert bbox == [(-180.0, -90.0, 180.0, 90.0)]


@pytest.mark.asyncio
async def test_falls_back_to_world_bbox_on_resolution_failure():
    catalogs = MagicMock()
    catalogs.get_collection = AsyncMock(side_effect=RuntimeError("boom"))
    bbox, max_zoom = await _bounded_default_scope(
        catalogs, "cat1", "coll1", TilesPreseedConfig(), runtime_max_zoom=12,
    )
    assert bbox == [(-180.0, -90.0, 180.0, 90.0)]
    assert max_zoom == PRESEED_DEFAULT_MAX_ZOOM


@pytest.mark.asyncio
async def test_zoom_capped_by_preseed_max_zoom_override():
    catalogs = _fake_catalogs([[10.0, 20.0, 30.0, 40.0]])
    preseed_config = TilesPreseedConfig(preseed_max_zoom=3)
    _bbox, max_zoom = await _bounded_default_scope(
        catalogs, "cat1", "coll1", preseed_config, runtime_max_zoom=12,
    )
    assert max_zoom == 3


@pytest.mark.asyncio
async def test_zoom_never_exceeds_runtime_max_zoom():
    """A preseed_max_zoom override higher than the catalog's serving max
    zoom is still capped at the serving max — never seeds tiles the live
    service won't serve."""
    catalogs = _fake_catalogs([[10.0, 20.0, 30.0, 40.0]])
    preseed_config = TilesPreseedConfig(preseed_max_zoom=20)
    _bbox, max_zoom = await _bounded_default_scope(
        catalogs, "cat1", "coll1", preseed_config, runtime_max_zoom=6,
    )
    assert max_zoom == 6


@pytest.mark.asyncio
async def test_default_cap_used_when_no_override_configured():
    catalogs = _fake_catalogs([[10.0, 20.0, 30.0, 40.0]])
    _bbox, max_zoom = await _bounded_default_scope(
        catalogs, "cat1", "coll1", TilesPreseedConfig(), runtime_max_zoom=20,
    )
    assert max_zoom == PRESEED_DEFAULT_MAX_ZOOM
