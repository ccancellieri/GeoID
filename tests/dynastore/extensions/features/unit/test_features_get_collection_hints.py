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

"""Task D.4 — OGCFeaturesService.get_collection forwards parsed hints.

Verifies that the OGC Features single-collection handler passes the
``request_hints`` dependency value into ``catalogs_svc.get_collection(hints=...)``.
"""

from __future__ import annotations

import pytest

from dynastore.modules.storage.hints import Hint


def _make_request(
    path: str = "/features/catalogs/cat/collections/col",
    query_string: bytes = b"hints=geometry_simplified",
):
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "GET",
        "scheme": "http",
        "server": ("test", 80),
        "path": path,
        "raw_path": path.encode(),
        "query_string": query_string,
        "headers": [(b"host", b"test")],
        "root_path": "",
    }
    return Request(scope)


class _FakeCatalogs:
    """Captures the hints kwarg passed to get_collection."""

    def __init__(self, collection_doc):
        self._doc = collection_doc
        self.captured_hints = None

    async def get_collection(self, catalog_id, collection_id, lang=None, hints=frozenset()):
        self.captured_hints = hints
        return self._doc

    async def get_collection_config(self, catalog_id, collection_id, ctx=None):
        return None


@pytest.mark.asyncio
async def test_features_get_collection_forwards_hints(monkeypatch):
    """OGCFeaturesService.get_collection passes request_hints to catalogs_svc.get_collection."""
    from dynastore.extensions.features.features_service import OGCFeaturesService

    svc = OGCFeaturesService.__new__(OGCFeaturesService)

    collection_doc = MagicMock()
    collection_doc.localize = lambda lang: (
        {"id": "col", "type": "Collection", "title": "Col", "links": []},
        lang,
    )

    fake_catalogs = _FakeCatalogs(collection_doc=collection_doc)

    async def _get_catalogs():
        return fake_catalogs

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)

    # Patch collection pipeline so it doesn't 404 on missing pipeline
    import dynastore.modules.catalog.collection_pipeline_runner as _pipeline
    async def _passthrough(catalog_id, collection_id, doc, context):
        return doc
    monkeypatch.setattr(_pipeline, "apply_collection_pipeline", _passthrough)

    hints = frozenset({Hint.GEOMETRY_SIMPLIFIED})
    request = _make_request(query_string=b"hints=geometry_simplified")

    await svc.get_collection(
        catalog_id="cat",
        collection_id="col",
        request=request,
        language="en",
        request_hints=hints,
    )

    assert fake_catalogs.captured_hints == hints


@pytest.mark.asyncio
async def test_features_get_collection_empty_hints(monkeypatch):
    """OGCFeaturesService.get_collection with no hints passes frozenset() to
    catalogs_svc.get_collection, preserving the no-hint PG-primary default."""
    from dynastore.extensions.features.features_service import OGCFeaturesService

    svc = OGCFeaturesService.__new__(OGCFeaturesService)

    collection_doc = MagicMock()
    collection_doc.localize = lambda lang: (
        {"id": "col", "type": "Collection", "title": "Col", "links": []},
        lang,
    )

    fake_catalogs = _FakeCatalogs(collection_doc=collection_doc)

    async def _get_catalogs():
        return fake_catalogs

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)

    import dynastore.modules.catalog.collection_pipeline_runner as _pipeline
    async def _passthrough(catalog_id, collection_id, doc, context):
        return doc
    monkeypatch.setattr(_pipeline, "apply_collection_pipeline", _passthrough)

    request = _make_request(query_string=b"")

    await svc.get_collection(
        catalog_id="cat",
        collection_id="col",
        request=request,
        language="en",
        request_hints=frozenset(),
    )

    assert fake_catalogs.captured_hints == frozenset()


# MagicMock import pulled to top level for the test functions above
from unittest.mock import MagicMock  # noqa: E402
