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

"""HTTP endpoint tests for the Volumes (3D GeoVolumes) service."""

from __future__ import annotations

import json

import pytest


def _build_service():
    from dynastore.extensions.volumes.volumes_service import VolumesService
    return VolumesService()


# ---------------------------------------------------------------------------
# tileset.json
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tileset_json_returns_valid_skeleton():
    from unittest.mock import MagicMock

    from fastapi import Request

    svc = _build_service()
    request = MagicMock(spec=Request)
    request.url = "http://ex/volumes/catalogs/c/collections/l/3dtiles/tileset.json"

    resp = await svc.get_tileset_json("c", "l", request)
    body = b""
    async for chunk in resp.body_iterator:
        body += chunk if isinstance(chunk, bytes) else chunk.encode()

    doc = json.loads(body.decode())
    assert doc["asset"]["version"] == "1.0"
    assert "root" in doc
    assert resp.media_type == "application/json"


@pytest.mark.asyncio
async def test_tileset_json_template_points_at_tiles_subpath():
    from dynastore.extensions.volumes.config import VolumesConfig
    from dynastore.modules.volumes.bounds import FeatureBounds
    from dynastore.modules.volumes.tileset_builder import build_tileset

    base = "http://ex/volumes/catalogs/c/collections/l/3dtiles"
    ts = build_tileset(
        [FeatureBounds("f", 0, 0, 0, 1, 1, 1)],
        VolumesConfig(),
        content_uri_template=f"{base}/tiles/{{tile_id}}.b3dm",
    )
    assert ts["root"]["content"]["uri"].startswith(base + "/tiles/")
    assert ts["root"]["content"]["uri"].endswith(".b3dm")


@pytest.mark.asyncio
async def test_tileset_json_uses_registered_bounds_source(monkeypatch):
    import json
    from unittest.mock import MagicMock
    from fastapi import Request

    from dynastore.extensions.volumes.volumes_service import VolumesService
    from dynastore.modules.volumes.bounds import FeatureBounds
    import dynastore.extensions.volumes.volumes_service as mod

    mod._TILESET_CACHE.clear()

    class _Source:
        async def get_bounds(self, catalog_id, collection_id, *, limit=None):
            return [FeatureBounds("the-only-feature", 0, 0, 0, 1, 1, 1)]

    monkeypatch.setattr(mod, "get_protocol", lambda proto: _Source())

    svc = VolumesService()
    request = MagicMock(spec=Request)
    request.url = "http://ex/volumes/catalogs/c/collections/l/3dtiles/tileset.json"

    resp = await svc.get_tileset_json("c", "l", request)
    body = b""
    async for chunk in resp.body_iterator:
        body += chunk if isinstance(chunk, bytes) else chunk.encode()

    doc = json.loads(body.decode())
    assert "content" in doc["root"]
    assert doc["root"]["content"]["uri"].endswith(".b3dm")


@pytest.mark.asyncio
async def test_tileset_json_falls_back_to_empty_source(monkeypatch):
    import json
    from unittest.mock import MagicMock
    from fastapi import Request

    from dynastore.extensions.volumes.volumes_service import VolumesService
    import dynastore.extensions.volumes.volumes_service as mod

    mod._TILESET_CACHE.clear()
    monkeypatch.setattr(mod, "get_protocol", lambda proto: None)

    svc = VolumesService()
    request = MagicMock(spec=Request)
    request.url = "http://ex/volumes/catalogs/c/collections/l/3dtiles/tileset.json"

    resp = await svc.get_tileset_json("c", "l", request)
    body = b""
    async for chunk in resp.body_iterator:
        body += chunk if isinstance(chunk, bytes) else chunk.encode()

    doc = json.loads(body.decode())
    assert doc["root"]["boundingVolume"]["box"] == [0.0] * 12
    assert "content" not in doc["root"]


@pytest.mark.asyncio
async def test_tileset_json_degrades_when_bounds_source_raises(monkeypatch):
    """A collection with no geometries sidecar makes get_bounds raise;
    the service must serve an empty tileset rather than a 500."""
    import json
    from unittest.mock import MagicMock
    from fastapi import Request

    from dynastore.extensions.volumes.volumes_service import VolumesService
    import dynastore.extensions.volumes.volumes_service as mod

    mod._TILESET_CACHE.clear()

    class _RaisingSource:
        async def get_bounds(self, catalog_id, collection_id, *, limit=None):
            raise RuntimeError("relation \"x_geometries\" does not exist")

    monkeypatch.setattr(mod, "get_protocol", lambda proto: _RaisingSource())

    svc = VolumesService()
    request = MagicMock(spec=Request)
    request.url = "http://ex/volumes/catalogs/c/collections/l/3dtiles/tileset.json"

    resp = await svc.get_tileset_json("c", "l", request)
    body = b""
    async for chunk in resp.body_iterator:
        body += chunk if isinstance(chunk, bytes) else chunk.encode()

    doc = json.loads(body.decode())
    assert doc["root"]["boundingVolume"]["box"] == [0.0] * 12
    assert "content" not in doc["root"]

    mod._TILESET_CACHE.clear()


# ---------------------------------------------------------------------------
# lifespan wires the sidecar bounds source + geometry fetcher
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lifespan_registers_sidecar_bounds_source(monkeypatch):
    """Startup must wire the runtime tiler by calling
    register_sidecar_bounds_source exactly once."""
    from unittest.mock import MagicMock

    import dynastore.extensions.volumes.volumes_service as mod

    calls = []
    monkeypatch.setattr(
        mod, "register_sidecar_bounds_source", lambda: calls.append(True),
    )

    svc = mod.VolumesService()
    app = MagicMock()
    async with svc.lifespan(app):
        pass

    assert calls == [True]


@pytest.mark.asyncio
async def test_lifespan_survives_registration_failure(monkeypatch):
    """A registration failure must never block startup."""
    from unittest.mock import MagicMock

    import dynastore.extensions.volumes.volumes_service as mod

    def _boom():
        raise RuntimeError("DatabaseProtocol not registered")

    monkeypatch.setattr(mod, "register_sidecar_bounds_source", _boom)

    svc = mod.VolumesService()
    app = MagicMock()
    # Must not raise.
    async with svc.lifespan(app):
        pass


# ---------------------------------------------------------------------------
# Tile cache: second call returns cached tileset
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tileset_cached_after_first_build(monkeypatch):
    import dynastore.extensions.volumes.volumes_service as mod

    call_count = 0

    class _CountingSource:
        async def get_bounds(self, catalog_id, collection_id, *, limit=None):
            nonlocal call_count
            call_count += 1
            return []

    monkeypatch.setattr(mod, "get_protocol", lambda proto: _CountingSource())
    # Clear any pre-existing cache entry.
    mod._TILESET_CACHE.clear()

    from unittest.mock import MagicMock
    from fastapi import Request
    from dynastore.extensions.volumes.volumes_service import VolumesService

    svc = VolumesService()
    req = MagicMock(spec=Request)
    req.url = "http://ex/volumes/catalogs/cc/collections/ll/3dtiles/tileset.json"

    await svc.get_tileset_json("cc", "ll", req)
    await svc.get_tileset_json("cc", "ll", req)

    assert call_count == 1, "bounds source should only be called once (cache hit on second)"

    mod._TILESET_CACHE.clear()


# ---------------------------------------------------------------------------
# B3DM tile endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_tile_b3dm_returns_bytes_response(monkeypatch):
    from unittest.mock import MagicMock
    from fastapi import Request

    import dynastore.extensions.volumes.volumes_service as mod
    from dynastore.modules.volumes.bounds import FeatureBounds
    from dynastore.models.protocols.geometry_fetcher import FeatureGeometry

    pytest.importorskip("shapely")

    import shapely.wkb
    from shapely.geometry import Polygon

    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    wkb = shapely.wkb.dumps(poly)

    class _BoundsSource:
        async def get_bounds(self, cat, col, *, limit=None):
            return [FeatureBounds("f1", 0, 0, 0, 1, 1, 1)]

    class _Fetcher:
        async def get_geometries(self, cat, col, ids):
            return [FeatureGeometry("f1", wkb, height=3.0)]

    def _fake_get_protocol(proto):
        from dynastore.models.protocols.bounds_source import BoundsSourceProtocol
        from dynastore.models.protocols.geometry_fetcher import GeometryFetcherProtocol
        if proto is BoundsSourceProtocol:
            return _BoundsSource()
        if proto is GeometryFetcherProtocol:
            return _Fetcher()
        return None

    monkeypatch.setattr(mod, "get_protocol", _fake_get_protocol)
    mod._TILESET_CACHE.clear()

    svc = mod.VolumesService()
    req = MagicMock(spec=Request)
    req.url = "http://ex/volumes/catalogs/c/collections/col/3dtiles/tileset.json"

    # Build tileset so the cache is populated with correct tile_ids.
    await svc.get_tileset_json("c", "col", req)

    # Retrieve the root tile_id from the cached tileset.
    root = mod._TILESET_CACHE[("c", "col")][1]["root"]
    content_uri = root.get("content", {}).get("uri", "")
    tile_id = content_uri.rsplit("/", 1)[-1].split(".")[0]

    req2 = MagicMock(spec=Request)
    req2.url = f"http://ex/volumes/catalogs/c/collections/col/3dtiles/tiles/{tile_id}.b3dm"

    response = await svc.get_tile_b3dm("c", "col", tile_id, req2)
    assert response.media_type == "application/octet-stream"
    assert len(response.body) > 28  # header alone is 28 bytes
    assert response.body[:4] == b"b3dm"

    mod._TILESET_CACHE.clear()


# ---------------------------------------------------------------------------
# GLB tile endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_tile_glb_returns_gltf_binary(monkeypatch):
    from unittest.mock import MagicMock
    from fastapi import Request

    import dynastore.extensions.volumes.volumes_service as mod
    from dynastore.modules.volumes.bounds import FeatureBounds
    from dynastore.models.protocols.geometry_fetcher import FeatureGeometry

    pytest.importorskip("shapely")

    import shapely.wkb
    from shapely.geometry import Polygon

    poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    wkb = shapely.wkb.dumps(poly)

    class _BoundsSource:
        async def get_bounds(self, cat, col, *, limit=None):
            return [FeatureBounds("g1", 0, 0, 0, 1, 1, 1)]

    class _Fetcher:
        async def get_geometries(self, cat, col, ids):
            return [FeatureGeometry("g1", wkb, height=2.0)]

    def _fake_get_protocol(proto):
        from dynastore.models.protocols.bounds_source import BoundsSourceProtocol
        from dynastore.models.protocols.geometry_fetcher import GeometryFetcherProtocol
        if proto is BoundsSourceProtocol:
            return _BoundsSource()
        if proto is GeometryFetcherProtocol:
            return _Fetcher()
        return None

    monkeypatch.setattr(mod, "get_protocol", _fake_get_protocol)
    mod._TILESET_CACHE.clear()

    svc = mod.VolumesService()
    req = MagicMock(spec=Request)
    req.url = "http://ex/volumes/catalogs/c/collections/col2/3dtiles/tileset.json"
    await svc.get_tileset_json("c", "col2", req)

    root = mod._TILESET_CACHE[("c", "col2")][1]["root"]
    content_uri = root.get("content", {}).get("uri", "")
    tile_id = content_uri.rsplit("/", 1)[-1].split(".")[0]

    req2 = MagicMock(spec=Request)
    req2.url = f"http://ex/volumes/catalogs/c/collections/col2/3dtiles/tiles/{tile_id}.glb"

    response = await svc.get_tile_glb("c", "col2", tile_id, req2)
    assert response.media_type == "model/gltf-binary"
    assert response.body[:4] == b"glTF"

    mod._TILESET_CACHE.clear()


# ---------------------------------------------------------------------------
# 404 for unknown tile_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_b3dm_returns_404_for_unknown_tile(monkeypatch):
    from unittest.mock import MagicMock
    from fastapi import HTTPException, Request

    import dynastore.extensions.volumes.volumes_service as mod

    monkeypatch.setattr(mod, "get_protocol", lambda proto: None)
    mod._TILESET_CACHE.clear()

    svc = mod.VolumesService()
    req = MagicMock(spec=Request)
    req.url = "http://ex/volumes/catalogs/c/collections/empty/3dtiles/tileset.json"
    await svc.get_tileset_json("c", "empty", req)

    req2 = MagicMock(spec=Request)
    req2.url = "http://ex/volumes/catalogs/c/collections/empty/3dtiles/tiles/999.b3dm"

    with pytest.raises(HTTPException) as exc:
        await svc.get_tile_b3dm("c", "empty", "999", req2)
    assert exc.value.status_code == 404

    mod._TILESET_CACHE.clear()


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_metadata_emits_self_and_data_links():
    from unittest.mock import MagicMock
    from fastapi import Request

    svc = _build_service()
    request = MagicMock(spec=Request)
    request.url = "http://ex/volumes/catalogs/c/collections/l/3dtiles/metadata"

    payload = await svc.get_volumes_metadata("c", "l", request)
    assert payload["title"].startswith("3D GeoVolumes")
    rels = {lk["rel"] for lk in payload["links"]}
    assert rels == {"self", "data"}
    data_link = next(lk for lk in payload["links"] if lk["rel"] == "data")
    assert data_link["href"].endswith("/tileset.json")


@pytest.mark.asyncio
async def test_metadata_exposes_new_config_fields():
    from unittest.mock import MagicMock
    from fastapi import Request

    svc = _build_service()
    request = MagicMock(spec=Request)
    request.url = "http://ex/volumes/catalogs/c/collections/l/3dtiles/metadata"

    payload = await svc.get_volumes_metadata("c", "l", request)
    cfg = payload["config"]
    assert "default_extrusion_height" in cfg
    assert "supported_formats" in cfg
    assert isinstance(cfg["supported_formats"], list)
