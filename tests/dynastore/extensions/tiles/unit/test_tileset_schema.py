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

"""Unit tests for OGC API Tiles tilesets-list schema and conformance URI.

Covers:
- get_collection_tilesets returns TileSetList (not TileMatrixSetList).
- Each tileset entry carries dataType ('vector' or 'map').
- Links use rel='http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme' for the
  TMS definition and rel='self' for the tileset-metadata resource.
- get_collection_tileset returns a single TileSetItem with dataType='vector'.
- get_collection_map_tileset returns a single TileSetItem with dataType='map'.
- OGC_API_TILES_URIS includes the OGC API Maps tilesets conformance URI.
- Unknown TMS in tileset-metadata endpoints raises 404.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from dynastore.extensions.tiles.tiles_service import OGC_API_TILES_URIS, TilesService
from dynastore.modules.tiles.tiles_models import TileSetItem, TileSetList


_TILING_SCHEME_REL = "http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme"
_MAPS_TILESETS_URI = "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/tilesets"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service() -> TilesService:
    svc = object.__new__(TilesService)
    svc._ogc_catalogs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_configs_protocol = None  # type: ignore[attr-defined]
    svc._ogc_storage_protocol = None  # type: ignore[attr-defined]
    return svc


def _make_request() -> MagicMock:
    req = MagicMock()
    req.headers = {}
    req.url_for = MagicMock(side_effect=lambda name, **kw: f"http://testserver/{name}")
    return req


# ---------------------------------------------------------------------------
# Conformance URI — Finding 2
# ---------------------------------------------------------------------------


def test_maps_tilesets_conformance_uri_present():
    """OGC API Maps tilesets conformance URI must be in OGC_API_TILES_URIS."""
    assert _MAPS_TILESETS_URI in OGC_API_TILES_URIS, (
        f"Expected {_MAPS_TILESETS_URI!r} in OGC_API_TILES_URIS"
    )


# ---------------------------------------------------------------------------
# TileSetList schema — Finding 1
# ---------------------------------------------------------------------------


class TestGetCollectionTilesetsSchema:
    """get_collection_tilesets must return TileSetList, not TileMatrixSetList."""

    @pytest.mark.asyncio
    async def test_returns_tileset_list_type(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        with patch(
            "dynastore.extensions.tiles.tiles_service.tms_manager.list_custom_tms",
            AsyncMock(return_value=[]),
        ):
            result = await svc.get_collection_tilesets(
                catalog_id="cat",
                collection_id="coll",
                request=_make_request(),
            )

        assert isinstance(result, TileSetList), (
            f"Expected TileSetList, got {type(result).__name__}"
        )

    @pytest.mark.asyncio
    async def test_each_entry_has_data_type(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        with patch(
            "dynastore.extensions.tiles.tiles_service.tms_manager.list_custom_tms",
            AsyncMock(return_value=[]),
        ):
            result = await svc.get_collection_tilesets(
                catalog_id="cat",
                collection_id="coll",
                request=_make_request(),
            )

        for ts in result.tilesets:
            assert isinstance(ts, TileSetItem)
            assert ts.dataType in ("vector", "map", "coverage"), (
                f"Unexpected dataType {ts.dataType!r} on tileset {ts.id!r}"
            )

    @pytest.mark.asyncio
    async def test_vector_and_map_entries_present(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        with patch(
            "dynastore.extensions.tiles.tiles_service.tms_manager.list_custom_tms",
            AsyncMock(return_value=[]),
        ):
            result = await svc.get_collection_tilesets(
                catalog_id="cat",
                collection_id="coll",
                request=_make_request(),
            )

        data_types = {ts.dataType for ts in result.tilesets}
        assert "vector" in data_types, "Expected at least one vector tileset"
        assert "map" in data_types, "Expected at least one map tileset"

    @pytest.mark.asyncio
    async def test_tiling_scheme_rel_on_each_entry(self):
        """Every tileset entry must carry a tiling-scheme link."""
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        with patch(
            "dynastore.extensions.tiles.tiles_service.tms_manager.list_custom_tms",
            AsyncMock(return_value=[]),
        ):
            result = await svc.get_collection_tilesets(
                catalog_id="cat",
                collection_id="coll",
                request=_make_request(),
            )

        for ts in result.tilesets:
            rels = {link.rel for link in ts.links}
            assert _TILING_SCHEME_REL in rels, (
                f"Tileset {ts.id!r} (dataType={ts.dataType!r}) missing "
                f"tiling-scheme link; got rels={rels!r}"
            )

    @pytest.mark.asyncio
    async def test_self_rel_on_each_entry(self):
        """Every tileset entry must carry a self link."""
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        with patch(
            "dynastore.extensions.tiles.tiles_service.tms_manager.list_custom_tms",
            AsyncMock(return_value=[]),
        ):
            result = await svc.get_collection_tilesets(
                catalog_id="cat",
                collection_id="coll",
                request=_make_request(),
            )

        for ts in result.tilesets:
            rels = {link.rel for link in ts.links}
            assert "self" in rels, (
                f"Tileset {ts.id!r} (dataType={ts.dataType!r}) missing self link; "
                f"got rels={rels!r}"
            )

    @pytest.mark.asyncio
    async def test_self_link_not_tiling_scheme_rel(self):
        """The TMS definition link must not use rel='self'; self must point at the tileset."""
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        with patch(
            "dynastore.extensions.tiles.tiles_service.tms_manager.list_custom_tms",
            AsyncMock(return_value=[]),
        ):
            result = await svc.get_collection_tilesets(
                catalog_id="cat",
                collection_id="coll",
                request=_make_request(),
            )

        for ts in result.tilesets:
            for link in ts.links:
                # The tiling-scheme link must not also be the self link
                if link.rel == _TILING_SCHEME_REL:
                    assert link.rel != "self", (
                        f"tiling-scheme link must not carry rel='self' on tileset {ts.id!r}"
                    )


# ---------------------------------------------------------------------------
# get_collection_tileset (vector tileset-metadata) — Finding 1
# ---------------------------------------------------------------------------


class TestGetCollectionTileset:
    @pytest.mark.asyncio
    async def test_returns_vector_tileset_item(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        result = await svc.get_collection_tileset(
            catalog_id="cat",
            collection_id="coll",
            tileMatrixSetId="WebMercatorQuad",
            request=_make_request(),
        )

        assert isinstance(result, TileSetItem)
        assert result.dataType == "vector"
        assert result.id == "WebMercatorQuad"

    @pytest.mark.asyncio
    async def test_has_self_and_tiling_scheme_links(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        result = await svc.get_collection_tileset(
            catalog_id="cat",
            collection_id="coll",
            tileMatrixSetId="WebMercatorQuad",
            request=_make_request(),
        )

        rels = {link.rel for link in result.links}
        assert "self" in rels, f"Missing self link; got {rels!r}"
        assert _TILING_SCHEME_REL in rels, f"Missing tiling-scheme link; got {rels!r}"

    @pytest.mark.asyncio
    async def test_unknown_builtin_tms_falls_back_to_custom(self):
        """Unknown TMS not in builtins triggers a custom-TMS lookup."""
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        from dynastore.modules.tiles.tiles_models import TileMatrixSet

        fake_custom = TileMatrixSet(
            id="MyCustomTMS",
            title="My Custom TMS",
            crs="http://www.opengis.net/def/crs/EPSG/0/4326",
            tileMatrices=[],
        )

        with patch(
            "dynastore.extensions.tiles.tiles_service.tms_manager.get_custom_tms",
            AsyncMock(return_value=fake_custom),
        ):
            result = await svc.get_collection_tileset(
                catalog_id="cat",
                collection_id="coll",
                tileMatrixSetId="MyCustomTMS",
                request=_make_request(),
            )

        assert result.id == "MyCustomTMS"
        assert result.dataType == "vector"

    @pytest.mark.asyncio
    async def test_unknown_tms_raises_404(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        with patch(
            "dynastore.extensions.tiles.tiles_service.tms_manager.get_custom_tms",
            AsyncMock(return_value=None),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await svc.get_collection_tileset(
                    catalog_id="cat",
                    collection_id="coll",
                    tileMatrixSetId="NonExistentTMS",
                    request=_make_request(),
                )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_hidden_collection_raises_404(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock(
            side_effect=HTTPException(status_code=404, detail="Collection not found.")
        )

        with pytest.raises(HTTPException) as exc_info:
            await svc.get_collection_tileset(
                catalog_id="cat",
                collection_id="hidden-coll",
                tileMatrixSetId="WebMercatorQuad",
                request=_make_request(),
            )

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# get_collection_map_tileset (map tileset-metadata) — Finding 1
# ---------------------------------------------------------------------------


class TestGetCollectionMapTileset:
    @pytest.mark.asyncio
    async def test_returns_map_tileset_item(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        result = await svc.get_collection_map_tileset(
            catalog_id="cat",
            collection_id="coll",
            tms_id="WebMercatorQuad",
            request=_make_request(),
        )

        assert isinstance(result, TileSetItem)
        assert result.dataType == "map"
        assert result.id == "WebMercatorQuad"

    @pytest.mark.asyncio
    async def test_has_self_and_tiling_scheme_links(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        result = await svc.get_collection_map_tileset(
            catalog_id="cat",
            collection_id="coll",
            tms_id="WorldCRS84Quad",
            request=_make_request(),
        )

        rels = {link.rel for link in result.links}
        assert "self" in rels, f"Missing self link; got {rels!r}"
        assert _TILING_SCHEME_REL in rels, f"Missing tiling-scheme link; got {rels!r}"

    @pytest.mark.asyncio
    async def test_unknown_tms_raises_404(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock()

        with patch(
            "dynastore.extensions.tiles.tiles_service.tms_manager.get_custom_tms",
            AsyncMock(return_value=None),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await svc.get_collection_map_tileset(
                    catalog_id="cat",
                    collection_id="hidden-coll",
                    tms_id="NonExistent",
                    request=_make_request(),
                )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_hidden_collection_raises_404(self):
        svc = _make_service()
        svc._resolve_catalog_and_collection = AsyncMock(
            return_value=("internal-cat", "internal-coll")
        )
        svc._require_collection_visible = AsyncMock(
            side_effect=HTTPException(status_code=404, detail="Collection not found.")
        )

        with pytest.raises(HTTPException) as exc_info:
            await svc.get_collection_map_tileset(
                catalog_id="cat",
                collection_id="hidden-coll",
                tms_id="WebMercatorQuad",
                request=_make_request(),
            )

        assert exc_info.value.status_code == 404
