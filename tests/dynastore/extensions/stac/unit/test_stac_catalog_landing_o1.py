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

"""Regression coverage for the per-catalog STAC landing page (#2865).

``GET /stac/catalogs/{catalog_id}`` used to enumerate every collection in
the catalog to mint one ``child`` link each, hydrating each collection
through the routed READ driver along the way. That made the landing page's
cost scale with the catalog's collection count instead of being O(1); at a
few thousand collections the request stopped completing before the gateway
timeout. Discovery of a catalog's collections must be via the "data" link
to the paginated ``/collections`` endpoint, and the landing page must never
enumerate collections at all.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from starlette.requests import Request as StarletteRequest

from dynastore.models.shared_models import Catalog


def _make_request(path: str = "/stac/catalogs/fao") -> StarletteRequest:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": b"",
        "headers": [],
        "server": ("localhost", 80),
    }
    return StarletteRequest(scope)


class _FakeCatalogsService:
    """Stands in for CatalogsProtocol. ``list_collections`` fails loudly if
    the landing page ever calls it — the whole point of the fix under test."""

    def __init__(self, model: Catalog):
        self._model = model
        self.get_catalog_model = AsyncMock(return_value=model)

    async def list_collections(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError(
            "the per-catalog STAC landing page must not enumerate "
            "collections (#2865) — discovery is via the 'data' link"
        )


@pytest.mark.asyncio
async def test_create_catalog_never_enumerates_collections(monkeypatch):
    """The landing document is built with zero calls to list_collections,
    regardless of how many collections the catalog holds."""
    import dynastore.extensions.stac.stac_generator as gen

    model = Catalog(id="fao", title="FAO", description="FAO catalog")
    catalogs_svc = _FakeCatalogsService(model)
    monkeypatch.setattr(gen, "get_protocol", lambda _proto: catalogs_svc)

    result = await gen.create_catalog(
        _make_request(), catalog_id="fao", lang="en",
    )

    catalogs_svc.get_catalog_model.assert_awaited_once()
    assert result["id"] == "fao"


@pytest.mark.asyncio
async def test_create_catalog_required_links_survive(monkeypatch):
    """self, root, and data (collection discovery) links must all be present
    once the per-collection child-link enumeration is dropped."""
    import dynastore.extensions.stac.stac_generator as gen

    model = Catalog(id="fao", title="FAO", description="FAO catalog")
    catalogs_svc = _FakeCatalogsService(model)
    monkeypatch.setattr(gen, "get_protocol", lambda _proto: catalogs_svc)

    result = await gen.create_catalog(
        _make_request(), catalog_id="fao", lang="en",
    )

    links_by_rel = {}
    for link in result["links"]:
        links_by_rel.setdefault(link["rel"], []).append(link)

    assert "self" in links_by_rel
    assert "root" in links_by_rel
    assert "data" in links_by_rel
    assert links_by_rel["data"][0]["href"] == "http://localhost/stac/catalogs/fao/collections"

    # No per-collection child links were minted.
    assert "child" not in links_by_rel
