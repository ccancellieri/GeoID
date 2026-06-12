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

"""Unit tests for OGC API 3D GeoVolumes container routes (Core + SpatialQuery).

Tests use FastAPI TestClient with a minimal app. All collaborator calls
(catalog service, item search) are mocked on the service instance so no
database is touched.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

_CAT = "test-cat"
_COL_3D = "buildings-lod2"
_COL_PLAIN = "roads"

_COLLECTION_3D = MagicMock()
_COLLECTION_3D.id = _COL_3D
_COLLECTION_3D.title = "Buildings LoD2"
_COLLECTION_3D.model_extra = {
    "extras": {
        "cityjson:version": "2.0",
        "cityjson:transform": {"scale": [0.001, 0.001, 0.001], "translate": [0.0, 0.0, 0.0]},
        "geovolumes:zrange": {"zmin": 0.0, "zmax": 50.0},
    }
}
_COLLECTION_3D.extent = MagicMock()
_COLLECTION_3D.extent.spatial = MagicMock()
_COLLECTION_3D.extent.spatial.bbox = [[4.27, 52.06, 4.32, 52.09]]
_COLLECTION_3D.assets = {
    "tileset": {
        "href": "https://tiles.example.com/buildings/tileset.json",
        "roles": ["3dtiles"],
        "type": "application/json+3dtiles",
        "title": "3D Tiles tileset",
    }
}

_COLLECTION_PLAIN = MagicMock()
_COLLECTION_PLAIN.id = _COL_PLAIN
_COLLECTION_PLAIN.title = "Roads"
_COLLECTION_PLAIN.model_extra = {}
_COLLECTION_PLAIN.extent = None
_COLLECTION_PLAIN.assets = None

_ITEM_WITH_CITYJSON = MagicMock()
_ITEM_WITH_CITYJSON.id = "bldg-001"
_ITEM_WITH_CITYJSON.model_extra = {
    "extras": {"cityjson": {"type": "CityJSONFeature", "id": "bldg-001"}}
}
_ITEM_WITH_CITYJSON.properties = {"citygml_type": "Building"}


# ---------------------------------------------------------------------------
# App fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def volumes_app():
    """Minimal FastAPI app with VolumesService routes and mocked catalog service."""
    from dynastore.extensions.volumes.volumes_service import VolumesService

    svc = VolumesService()

    # Mock the catalogs protocol
    catalogs_mock = AsyncMock()
    catalogs_mock.list_collections = AsyncMock(
        return_value=[_COLLECTION_3D, _COLLECTION_PLAIN]
    )
    catalogs_mock.get_collection = AsyncMock(return_value=_COLLECTION_3D)
    catalogs_mock.search_items = AsyncMock(return_value=[_ITEM_WITH_CITYJSON])

    # stream_cityjsonseq consumes the streaming surface: an object whose
    # .items is an async iterator over features. Build a fresh iterator
    # per call so multiple requests in one test don't share state.
    async def _stream_items(*_a, **_k):
        async def _items_iter():
            yield _ITEM_WITH_CITYJSON

        stream_response = MagicMock()
        stream_response.items = _items_iter()
        return stream_response

    catalogs_mock.stream_items = AsyncMock(side_effect=_stream_items)

    svc._get_catalogs_service = AsyncMock(return_value=catalogs_mock)

    app = FastAPI()
    app.include_router(svc.router)
    return app, catalogs_mock


# ---------------------------------------------------------------------------
# Collections listing (3D filter)
# ---------------------------------------------------------------------------


def test_list_collections_returns_only_3d(volumes_app):
    app, _ = volumes_app
    r = TestClient(app).get(f"/volumes/catalogs/{_CAT}/collections")
    assert r.status_code == 200
    data = r.json()
    assert "collections" in data
    assert "links" in data
    ids = [c["id"] for c in data["collections"]]
    assert _COL_3D in ids
    assert _COL_PLAIN not in ids


def test_list_collections_has_collection_type(volumes_app):
    app, _ = volumes_app
    r = TestClient(app).get(f"/volumes/catalogs/{_CAT}/collections")
    assert r.status_code == 200
    coll = r.json()["collections"][0]
    assert coll["collectionType"] == "3dcontainer"


def test_list_collections_empty_when_no_3d(volumes_app):
    app, catalogs_mock = volumes_app
    catalogs_mock.list_collections = AsyncMock(return_value=[_COLLECTION_PLAIN])
    r = TestClient(app).get(f"/volumes/catalogs/{_CAT}/collections")
    assert r.status_code == 200
    assert r.json()["collections"] == []


# ---------------------------------------------------------------------------
# Single collection
# ---------------------------------------------------------------------------


def test_get_collection_returns_threedcontainer(volumes_app):
    app, _ = volumes_app
    r = TestClient(app).get(f"/volumes/catalogs/{_CAT}/collections/{_COL_3D}")
    assert r.status_code == 200
    data = r.json()
    assert data["id"] == _COL_3D
    assert data["collectionType"] == "3dcontainer"
    assert "contentExtent" in data
    assert len(data["contentExtent"]["bbox"]) == 6


def test_get_collection_content_has_3dtiles_link(volumes_app):
    """The runtime 3dtiles link points to /volumes/.../3dtiles/tileset.json."""
    app, _ = volumes_app
    r = TestClient(app).get(f"/volumes/catalogs/{_CAT}/collections/{_COL_3D}")
    assert r.status_code == 200
    content = r.json().get("content", [])
    rels = [lk["rel"] for lk in content]
    assert "http://www.opengis.net/def/rel/ogc/1.0/3dtiles" in rels
    # The href must be the runtime volumes endpoint (not an asset href)
    tiles_link = next(
        lk for lk in content
        if lk["rel"] == "http://www.opengis.net/def/rel/ogc/1.0/3dtiles"
    )
    assert "/volumes/catalogs/" in tiles_link["href"]
    assert "3dtiles/tileset.json" in tiles_link["href"]


def test_get_collection_content_has_cityjsonseq_link(volumes_app):
    app, _ = volumes_app
    r = TestClient(app).get(f"/volumes/catalogs/{_CAT}/collections/{_COL_3D}")
    assert r.status_code == 200
    content = r.json().get("content", [])
    types = [lk.get("type") for lk in content]
    assert "application/city+json" in types
    # cityjsonseq link must also point at /volumes/
    cjseq_link = next(lk for lk in content if lk.get("type") == "application/city+json")
    assert "/volumes/catalogs/" in cjseq_link["href"]


def test_get_collection_not_found_returns_404(volumes_app):
    app, catalogs_mock = volumes_app
    catalogs_mock.get_collection = AsyncMock(return_value=None)
    r = TestClient(app).get(f"/volumes/catalogs/{_CAT}/collections/missing")
    assert r.status_code == 404


def test_get_collection_non_3d_returns_404(volumes_app):
    app, catalogs_mock = volumes_app
    catalogs_mock.get_collection = AsyncMock(return_value=_COLLECTION_PLAIN)
    r = TestClient(app).get(f"/volumes/catalogs/{_CAT}/collections/{_COL_PLAIN}")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# CityJSONSeq streaming
# ---------------------------------------------------------------------------


def test_cityjsonseq_streams_header_and_items(volumes_app):
    app, _ = volumes_app
    r = TestClient(app).get(
        f"/volumes/catalogs/{_CAT}/collections/{_COL_3D}/cityjsonseq"
    )
    assert r.status_code == 200
    assert "city+json" in r.headers["content-type"]
    lines = [ln for ln in r.text.splitlines() if ln.strip()]
    # Line 1 is the header; subsequent lines are CityJSONFeature
    parsed = [json.loads(ln) for ln in lines]
    assert parsed[0]["type"] == "CityJSONSeq"
    assert "transform" in parsed[0]
    assert "version" in parsed[0]
    assert len(parsed) >= 2
    for feature_line in parsed[1:]:
        assert feature_line["type"] == "CityJSONFeature"


def test_cityjsonseq_content_type(volumes_app):
    app, _ = volumes_app
    r = TestClient(app).get(
        f"/volumes/catalogs/{_CAT}/collections/{_COL_3D}/cityjsonseq"
    )
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/city+json")


def test_cityjsonseq_not_found_returns_404(volumes_app):
    app, catalogs_mock = volumes_app
    catalogs_mock.get_collection = AsyncMock(return_value=None)
    r = TestClient(app).get(
        f"/volumes/catalogs/{_CAT}/collections/missing/cityjsonseq"
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Bbox spatial filter on collections listing
# ---------------------------------------------------------------------------


def test_list_collections_bbox_4_filters(volumes_app):
    """A 4-number bbox that overlaps the 3D collection passes it through."""
    app, _ = volumes_app
    r = TestClient(app).get(
        f"/volumes/catalogs/{_CAT}/collections?bbox=4.0,52.0,4.5,52.2"
    )
    assert r.status_code == 200
    ids = [c["id"] for c in r.json()["collections"]]
    assert _COL_3D in ids


def test_list_collections_bbox_excludes_non_overlapping(volumes_app):
    """A bbox that does NOT overlap the 3D collection yields empty list."""
    app, _ = volumes_app
    # Bounding box far away from _COLLECTION_3D (Netherlands ~4-5, 52)
    r = TestClient(app).get(
        f"/volumes/catalogs/{_CAT}/collections?bbox=10.0,10.0,11.0,11.0"
    )
    assert r.status_code == 200
    assert r.json()["collections"] == []


def test_list_collections_bbox_6_with_z_range_overlap(volumes_app):
    """A 6-number bbox with z range that overlaps passes the collection."""
    app, _ = volumes_app
    # zmin=0, zmax=50 on collection; filter zmin=10, zmax=30 — overlaps
    r = TestClient(app).get(
        f"/volumes/catalogs/{_CAT}/collections?bbox=4.0,52.0,10.0,4.5,52.2,30.0"
    )
    assert r.status_code == 200
    ids = [c["id"] for c in r.json()["collections"]]
    assert _COL_3D in ids


def test_list_collections_bbox_6_with_z_range_no_overlap(volumes_app):
    """A 6-number bbox with z range above the collection yields empty."""
    app, _ = volumes_app
    # zmin=100, zmax=200 — collection zmax is 50, so no z overlap
    r = TestClient(app).get(
        f"/volumes/catalogs/{_CAT}/collections?bbox=4.0,52.0,100.0,4.5,52.2,200.0"
    )
    assert r.status_code == 200
    assert r.json()["collections"] == []


def test_list_collections_malformed_bbox_returns_400(volumes_app):
    """A bbox with wrong arity (e.g. 3 numbers) returns HTTP 400."""
    app, _ = volumes_app
    r = TestClient(app).get(
        f"/volumes/catalogs/{_CAT}/collections?bbox=1.0,2.0,3.0"
    )
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# READ-path extras resolution (PG core driver shapes)
# ---------------------------------------------------------------------------


def test_get_extras_unwraps_localized_extra_metadata():
    """BaseMetadata always persists extra_metadata language-keyed; the READ
    path must unwrap {"en": {...}} before scanning for namespaced keys."""
    from dynastore.extensions.volumes.volumes_service import _get_extras

    coll = MagicMock()
    coll.model_extra = {}
    coll.extra_metadata = {"en": {"cityjson:version": "2.0"}}
    extras = _get_extras(coll)
    assert extras.get("cityjson:version") == "2.0"


def test_get_extras_localized_non_en_fallback():
    from dynastore.extensions.volumes.volumes_service import _get_extras

    coll = MagicMock()
    coll.model_extra = {}
    coll.extra_metadata = {"fr": {"cityjson:version": "2.0"}}
    extras = _get_extras(coll)
    assert extras.get("cityjson:version") == "2.0"


def test_collection_bbox_3d_falls_back_to_stamped_bbox():
    """When the extent column is empty/zero (no STAC sidecar), contentExtent
    must come from the geovolumes:bbox stamped into extras at ingest."""
    from dynastore.extensions.volumes.volumes_service import _collection_bbox_3d

    coll = MagicMock()
    coll.extent = None
    extras = {
        "geovolumes:bbox": [4.27, 52.06, 4.32, 52.09],
        "geovolumes:zrange": {"zmin": 1.0, "zmax": 50.0},
    }
    assert _collection_bbox_3d(coll, extras) == [4.27, 52.06, 1.0, 4.32, 52.09, 50.0]


def test_collection_bbox_3d_prefers_real_extent():
    from dynastore.extensions.volumes.volumes_service import _collection_bbox_3d

    coll = MagicMock()
    coll.extent.spatial.bbox = [[1.0, 2.0, 3.0, 4.0]]
    extras = {"geovolumes:bbox": [9.0, 9.0, 9.9, 9.9]}
    assert _collection_bbox_3d(coll, extras)[:2] == [1.0, 2.0]


# ---------------------------------------------------------------------------
# _normalize_bbox — 4-element vs 6-element bbox coercion
# ---------------------------------------------------------------------------


def test_normalize_bbox_6_element_preserves_order():
    """6-element CRS84h bbox [minLon,minLat,minH,maxLon,maxLat,maxH] must map to
    (minx, miny, zmin, maxx, maxy, zmax) in that exact order."""
    from dynastore.extensions.volumes.volumes_service import _normalize_bbox

    raw = [-75.62, 40.03, 0.0, -75.60, 40.05, 100.0]
    result = _normalize_bbox(raw)
    assert result == (-75.62, 40.03, 0.0, -75.60, 40.05, 100.0)


def test_normalize_bbox_4_element_zero_z():
    """4-element bbox gets zmin=zmax=0.0."""
    from dynastore.extensions.volumes.volumes_service import _normalize_bbox

    raw = [4.27, 52.06, 4.32, 52.09]
    result = _normalize_bbox(raw)
    assert result == (4.27, 52.06, 0.0, 4.32, 52.09, 0.0)


def test_normalize_bbox_none_on_garbage():
    """Non-numeric or too-short values return None."""
    from dynastore.extensions.volumes.volumes_service import _normalize_bbox

    assert _normalize_bbox(None) is None
    assert _normalize_bbox("not-a-bbox") is None
    assert _normalize_bbox([1.0, 2.0]) is None
    assert _normalize_bbox([0.0, 0.0, 0.0, 0.0]) is None
    assert _normalize_bbox(["a", "b", "c", "d"]) is None


def test_collection_bbox_3d_6element_stamped_correct_output():
    """Tiles3d-samples preset stamps a 6-element CRS84h bbox; the returned
    contentExtent must preserve the correct [minx,miny,zmin,maxx,maxy,zmax] order."""
    from dynastore.extensions.volumes.volumes_service import _collection_bbox_3d

    coll = MagicMock()
    coll.extent = None
    extras = {
        "geovolumes:bbox": [-75.62, 40.03, 0.0, -75.60, 40.05, 100.0],
    }
    result = _collection_bbox_3d(coll, extras)
    assert result == [-75.62, 40.03, 0.0, -75.60, 40.05, 100.0]


def test_collection_bbox_3d_6element_zero_z_picks_up_zrange():
    """6-element bbox where z values are both 0 should still be overridden by
    geovolumes:zrange when available."""
    from dynastore.extensions.volumes.volumes_service import _collection_bbox_3d

    coll = MagicMock()
    coll.extent = None
    extras = {
        "geovolumes:bbox": [-75.62, 40.03, 0.0, -75.60, 40.05, 0.0],
        "geovolumes:zrange": {"zmin": 5.0, "zmax": 200.0},
    }
    result = _collection_bbox_3d(coll, extras)
    assert result == [-75.62, 40.03, 5.0, -75.60, 40.05, 200.0]


def test_extract_cityjson_from_properties():
    """Items persist the CityJSONFeature under properties.cityjson — the
    only surface every driver round-trips."""
    from dynastore.extensions.volumes.volumes_service import _extract_cityjson

    feat = MagicMock()
    feat.properties = {"cityjson": {"type": "CityJSONFeature", "id": "b-1"}}
    feat.model_extra = {}
    assert _extract_cityjson(feat)["id"] == "b-1"


# ---------------------------------------------------------------------------
# _build_3d_container — external tileset URL support
# ---------------------------------------------------------------------------


def _make_external_collection(tileset_url: str, attribution: str | None = None) -> MagicMock:
    """Build a mock collection with geovolumes:tileset_url set."""
    coll = MagicMock()
    coll.id = "ext-tiles"
    coll.title = "External Tileset"
    extras: dict = {
        "geovolumes:enabled": True,
        "geovolumes:tileset_url": tileset_url,
        "geovolumes:bbox": [-75.62, 40.03, -75.60, 40.05],
        "geovolumes:zrange": {"zmin": 0.0, "zmax": 100.0},
    }
    if attribution:
        extras["geovolumes:attribution"] = attribution
    coll.model_extra = {"extras": extras}
    coll.extent = None
    return coll


def test_build_3d_container_external_url_emits_absolute_href():
    """When extras carry geovolumes:tileset_url the container href must be that URL."""
    from dynastore.extensions.volumes.volumes_service import _build_3d_container

    url = "https://example.com/tiles/tileset.json"
    coll = _make_external_collection(url)
    container = _build_3d_container(coll, "test-cat")

    tiles_link = next(
        lk for lk in (container.content or [])
        if lk.rel == "http://www.opengis.net/def/rel/ogc/1.0/3dtiles"
    )
    assert tiles_link.href == url
    assert "external" in (tiles_link.title or "").lower()


def test_build_3d_container_external_url_no_cityjsonseq_link():
    """External containers must NOT emit a CityJSONSeq alternate link."""
    from dynastore.extensions.volumes.volumes_service import _build_3d_container

    coll = _make_external_collection("https://example.com/tiles/tileset.json")
    container = _build_3d_container(coll, "test-cat")

    types = [lk.type for lk in (container.content or [])]
    assert "application/city+json" not in types, (
        "CityJSONSeq link must not appear for external 3D Tiles containers"
    )


def test_build_3d_container_external_url_attribution_passthrough():
    """When extras carry geovolumes:attribution it must appear in the container model."""
    from dynastore.extensions.volumes.volumes_service import _build_3d_container

    attribution = "© Test Attribution CC BY 4.0"
    coll = _make_external_collection(
        "https://example.com/tiles/tileset.json",
        attribution=attribution,
    )
    container = _build_3d_container(coll, "test-cat")
    assert container.attribution == attribution


def test_build_3d_container_native_cityjson_keeps_runtime_href_and_cityjsonseq():
    """CityJSON collections must keep the native runtime href and CityJSONSeq link."""
    from dynastore.extensions.volumes.volumes_service import _build_3d_container

    coll = MagicMock()
    coll.id = "denhaag"
    coll.title = "Den Haag"
    coll.model_extra = {
        "extras": {
            "cityjson:version": "2.0",
            "geovolumes:zrange": {"zmin": 0.0, "zmax": 50.0},
        }
    }
    coll.extent = MagicMock()
    coll.extent.spatial = MagicMock()
    coll.extent.spatial.bbox = [[4.27, 52.06, 4.32, 52.09]]

    container = _build_3d_container(coll, "test-cat")

    tiles_link = next(
        lk for lk in (container.content or [])
        if lk.rel == "http://www.opengis.net/def/rel/ogc/1.0/3dtiles"
    )
    # Native href must point to the runtime volumes endpoint
    assert "/volumes/catalogs/" in tiles_link.href
    assert "3dtiles/tileset.json" in tiles_link.href

    types = [lk.type for lk in (container.content or [])]
    assert "application/city+json" in types, (
        "CityJSONSeq link must be present for native CityJSON collections"
    )

    # No attribution on a bare CityJSON collection
    assert container.attribution is None


def test_build_3d_container_no_cityjsonseq_for_geovolumes_enabled_without_cityjson():
    """A collection with only geovolumes:enabled (no cityjson:version) and no
    tileset_url emits the native href but NO CityJSONSeq link."""
    from dynastore.extensions.volumes.volumes_service import _build_3d_container

    coll = MagicMock()
    coll.id = "generic-3d"
    coll.title = "Generic 3D"
    coll.model_extra = {
        "extras": {
            "geovolumes:enabled": True,
            "geovolumes:bbox": [-75.62, 40.03, -75.60, 40.05],
            "geovolumes:zrange": {"zmin": 0.0, "zmax": 100.0},
        }
    }
    coll.extent = None

    container = _build_3d_container(coll, "test-cat")

    tiles_link = next(
        lk for lk in (container.content or [])
        if lk.rel == "http://www.opengis.net/def/rel/ogc/1.0/3dtiles"
    )
    assert "/volumes/catalogs/" in tiles_link.href

    types = [lk.type for lk in (container.content or [])]
    assert "application/city+json" not in types
