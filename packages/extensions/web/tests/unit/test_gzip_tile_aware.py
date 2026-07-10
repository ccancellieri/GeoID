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

"""TileAwareGZipMiddleware must skip already-compressed image tiles.

PNG/JPEG/GeoTIFF map tiles are format-compressed already; gzipping them
again wastes CPU and memory for no size benefit. MVT and JSON responses
still compress well and must keep going through gzip.
"""

from fastapi import FastAPI
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.testclient import TestClient

from dynastore.extensions.web.gzip_middleware import TileAwareGZipMiddleware

_GZIP_HEADERS = {"Accept-Encoding": "gzip"}


def _build_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(TileAwareGZipMiddleware, minimum_size=1000, compresslevel=6)

    @app.get("/tile.png")
    def tile_png():
        return Response(content=b"\x89PNG" + b"x" * 2000, media_type="image/png")

    @app.get("/tile.mvt")
    def tile_mvt():
        return Response(
            content=b"y" * 2000, media_type="application/vnd.mapbox-vector-tile"
        )

    @app.get("/data.json")
    def data_json():
        return JSONResponse(content={"value": "z" * 2000})

    @app.get("/small.json")
    def small_json():
        return JSONResponse(content={"ok": True})

    @app.get("/redirect")
    def redirect():
        return RedirectResponse(url="/tile.png")

    return app


client = TestClient(_build_app())


def test_image_png_response_is_not_gzipped():
    resp = client.get("/tile.png", headers=_GZIP_HEADERS)
    assert resp.status_code == 200
    assert "content-encoding" not in resp.headers
    assert resp.content == b"\x89PNG" + b"x" * 2000


def test_mvt_tile_response_is_gzipped():
    resp = client.get("/tile.mvt", headers=_GZIP_HEADERS)
    assert resp.status_code == 200
    assert resp.headers.get("content-encoding") == "gzip"
    assert resp.content == b"y" * 2000


def test_json_response_is_gzipped():
    resp = client.get("/data.json", headers=_GZIP_HEADERS)
    assert resp.status_code == 200
    assert resp.headers.get("content-encoding") == "gzip"


def test_small_response_is_left_untouched():
    resp = client.get("/small.json", headers=_GZIP_HEADERS)
    assert resp.status_code == 200
    assert "content-encoding" not in resp.headers


def test_redirect_passes_through_uncompressed():
    resp = client.get("/redirect", headers=_GZIP_HEADERS, follow_redirects=False)
    assert resp.status_code == 307
    assert "content-encoding" not in resp.headers
