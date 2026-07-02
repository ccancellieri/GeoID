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

"""Tests for `TileSourceProtocol` / `PostgisTileSource`."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.tiles.tiles_source import PostgisTileSource, TileSourceNotSupported


def test_postgis_source_supports_pg_style_driver():
    source = PostgisTileSource()
    driver = MagicMock()
    driver._get_effective_driver_config = AsyncMock()
    assert source.supports(driver) is True


def test_postgis_source_rejects_driver_without_pg_duck_type():
    source = PostgisTileSource()
    driver = MagicMock(spec=[])  # no _get_effective_driver_config attribute
    assert source.supports(driver) is False


def test_tile_source_not_supported_message_names_driver_kind():
    exc = TileSourceNotSupported("SomeDriver")
    assert "SomeDriver" in str(exc)
    assert exc.driver_kind == "SomeDriver"


@pytest.mark.asyncio
async def test_render_tile_delegates_to_tiles_db(monkeypatch):
    import dynastore.modules.tiles.tiles_db as tiles_db

    monkeypatch.setattr(
        tiles_db, "get_features_as_mvt_filtered", AsyncMock(return_value=b"mvt-bytes")
    )
    source = PostgisTileSource()
    conn = MagicMock()
    result = await source.render_tile(
        conn,
        resolved_collections=[{"collection_id": "coll1"}],
        tms_def=MagicMock(),
        target_srid=3857,
        z="5",
        x=1,
        y=1,
    )
    assert result == b"mvt-bytes"
    tiles_db.get_features_as_mvt_filtered.assert_awaited_once()


@pytest.mark.asyncio
async def test_render_tile_swallows_value_error_as_none(monkeypatch):
    """A storage-resolution ValueError degrades to None (caller emits 204),
    it does not propagate as an unhandled error."""
    import dynastore.modules.tiles.tiles_db as tiles_db

    monkeypatch.setattr(
        tiles_db, "get_features_as_mvt_filtered", AsyncMock(side_effect=ValueError("no physical_table"))
    )
    source = PostgisTileSource()
    result = await source.render_tile(
        MagicMock(),
        resolved_collections=[{"collection_id": "coll1"}],
        tms_def=MagicMock(),
        target_srid=3857,
        z="5",
        x=1,
        y=1,
    )
    assert result is None
