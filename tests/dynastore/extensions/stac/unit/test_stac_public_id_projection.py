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

"""Regression coverage for the STAC internal-id leak / lenient-resolution bug (#2354).

Two coupled fixes:

A) Serialization always projects a stored/echoed internal catalog or
   collection id to its public external label before it reaches the wire —
   ``item.collection``, ``Collection.id`` and every self/parent/items link.
B) The public STAC REST handlers reject a path param shaped like an internal
   storage id outright (404) before any lenient internal-service resolution
   further down the stack can accept it.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from starlette.requests import Request as StarletteRequest

from dynastore.models.ogc import Feature
from dynastore.modules.stac.stac_config import StacPluginConfig


_INTERNAL_CATALOG_ID = "c_r9h3q9ws4hukp"
_INTERNAL_COLLECTION_ID = "col_ce6uh8dxkv267"
_EXTERNAL_CATALOG_ID = "stac_harvester_test_catalog"
_EXTERNAL_COLLECTION_ID = "a-prod"


def _make_request(path: str = "/stac/catalogs/cat/collections/col/items") -> StarletteRequest:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": b"",
        "headers": [],
        "server": ("localhost", 80),
    }
    return StarletteRequest(scope)


def _fake_catalogs_service(
    catalog_resolution: str | None = _EXTERNAL_CATALOG_ID,
    collection_resolution: str | None = _EXTERNAL_COLLECTION_ID,
) -> Any:
    """A minimal stand-in for CatalogsProtocol exposing only the reverse
    (internal→external) resolvers exercised by the generator helpers."""
    resolve_catalog_external_id = AsyncMock(return_value=catalog_resolution)
    resolve_collection_external_id = AsyncMock(return_value=collection_resolution)
    collections = SimpleNamespace(resolve_collection_external_id=resolve_collection_external_id)
    return SimpleNamespace(
        resolve_catalog_external_id=resolve_catalog_external_id,
        collections=collections,
    )


# ---------------------------------------------------------------------------
# A) _resolve_public_catalog_id / _resolve_public_collection_id
# ---------------------------------------------------------------------------


class TestResolvePublicIdHelpers:
    @pytest.mark.asyncio
    async def test_external_catalog_id_passthrough_no_lookup(self, monkeypatch):
        import dynastore.extensions.stac.stac_generator as gen

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(gen, "get_protocol", lambda _p: catalogs_svc)

        result = await gen._resolve_public_catalog_id(_EXTERNAL_CATALOG_ID)

        assert result == _EXTERNAL_CATALOG_ID
        catalogs_svc.resolve_catalog_external_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_internal_catalog_id_is_projected(self, monkeypatch):
        import dynastore.extensions.stac.stac_generator as gen

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(gen, "get_protocol", lambda _p: catalogs_svc)

        result = await gen._resolve_public_catalog_id(_INTERNAL_CATALOG_ID)

        assert result == _EXTERNAL_CATALOG_ID
        catalogs_svc.resolve_catalog_external_id.assert_awaited_once_with(
            _INTERNAL_CATALOG_ID, allow_missing=True
        )

    @pytest.mark.asyncio
    async def test_internal_catalog_id_fails_open_when_unresolvable(self, monkeypatch):
        import dynastore.extensions.stac.stac_generator as gen

        catalogs_svc = _fake_catalogs_service(catalog_resolution=None)
        monkeypatch.setattr(gen, "get_protocol", lambda _p: catalogs_svc)

        result = await gen._resolve_public_catalog_id(_INTERNAL_CATALOG_ID)

        assert result == _INTERNAL_CATALOG_ID

    @pytest.mark.asyncio
    async def test_internal_collection_id_is_projected(self, monkeypatch):
        import dynastore.extensions.stac.stac_generator as gen

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(gen, "get_protocol", lambda _p: catalogs_svc)

        result = await gen._resolve_public_collection_id(
            _EXTERNAL_CATALOG_ID, _INTERNAL_COLLECTION_ID
        )

        assert result == _EXTERNAL_COLLECTION_ID
        catalogs_svc.collections.resolve_collection_external_id.assert_awaited_once_with(
            _EXTERNAL_CATALOG_ID, _INTERNAL_COLLECTION_ID, allow_missing=True
        )

    @pytest.mark.asyncio
    async def test_external_collection_id_passthrough_no_lookup(self, monkeypatch):
        import dynastore.extensions.stac.stac_generator as gen

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(gen, "get_protocol", lambda _p: catalogs_svc)

        result = await gen._resolve_public_collection_id(
            _EXTERNAL_CATALOG_ID, _EXTERNAL_COLLECTION_ID
        )

        assert result == _EXTERNAL_COLLECTION_ID
        catalogs_svc.collections.resolve_collection_external_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_catalogs_protocol_fails_open(self, monkeypatch):
        import dynastore.extensions.stac.stac_generator as gen

        monkeypatch.setattr(gen, "get_protocol", lambda _p: None)

        assert await gen._resolve_public_catalog_id(_INTERNAL_CATALOG_ID) == _INTERNAL_CATALOG_ID
        assert (
            await gen._resolve_public_collection_id(_INTERNAL_CATALOG_ID, _INTERNAL_COLLECTION_ID)
            == _INTERNAL_COLLECTION_ID
        )


# ---------------------------------------------------------------------------
# A) create_item_from_feature emits the external collection id on the wire
# ---------------------------------------------------------------------------


class TestCreateItemFromFeatureProjectsExternalId:
    @pytest.mark.asyncio
    async def test_internal_collection_id_projected_to_external_on_wire(self, monkeypatch):
        """Requesting by (or storing) an internal collection id must never echo
        it back — ``item.collection`` and every link are the external label."""
        import dynastore.extensions.stac.stac_generator as gen

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(gen, "get_protocol", lambda _p: catalogs_svc)

        feature = Feature(
            type="Feature",
            id="item-1",
            geometry=None,
            properties={"datetime": "2024-01-01T00:00:00Z"},
        )

        item = await gen.create_item_from_feature(
            request=_make_request(),
            catalog_id=_INTERNAL_CATALOG_ID,
            collection_id=_INTERNAL_COLLECTION_ID,
            feature=feature,
            stac_config=StacPluginConfig(),
        )

        assert item is not None
        item_dict = item.to_dict()
        assert item_dict["collection"] == _EXTERNAL_COLLECTION_ID
        assert item.get_self_href() == (
            f"http://localhost/stac/catalogs/{_EXTERNAL_CATALOG_ID}"
            f"/collections/{_EXTERNAL_COLLECTION_ID}/items/item-1"
        )
        for link in item_dict.get("links", []):
            href = link.get("href", "")
            assert _INTERNAL_CATALOG_ID not in href
            assert _INTERNAL_COLLECTION_ID not in href

    @pytest.mark.asyncio
    async def test_external_ids_are_unchanged_and_no_lookup_performed(self, monkeypatch):
        """The common case (client already used external ids): no resolution
        round-trip, id echoed straight through."""
        import dynastore.extensions.stac.stac_generator as gen

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(gen, "get_protocol", lambda _p: catalogs_svc)

        feature = Feature(
            type="Feature",
            id="item-1",
            geometry=None,
            properties={"datetime": "2024-01-01T00:00:00Z"},
        )

        item = await gen.create_item_from_feature(
            request=_make_request(),
            catalog_id=_EXTERNAL_CATALOG_ID,
            collection_id=_EXTERNAL_COLLECTION_ID,
            feature=feature,
            stac_config=StacPluginConfig(),
        )

        assert item.to_dict()["collection"] == _EXTERNAL_COLLECTION_ID
        catalogs_svc.resolve_catalog_external_id.assert_not_awaited()
        catalogs_svc.collections.resolve_collection_external_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pre_resolved_ids_skip_inline_resolution(self, monkeypatch):
        """``create_item_collection`` pre-resolves once per page and passes the
        external ids straight in — no per-item lookup."""
        import dynastore.extensions.stac.stac_generator as gen

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(gen, "get_protocol", lambda _p: catalogs_svc)

        feature = Feature(
            type="Feature",
            id="item-1",
            geometry=None,
            properties={"datetime": "2024-01-01T00:00:00Z"},
        )

        item = await gen.create_item_from_feature(
            request=_make_request(),
            catalog_id=_INTERNAL_CATALOG_ID,
            collection_id=_INTERNAL_COLLECTION_ID,
            feature=feature,
            stac_config=StacPluginConfig(),
            external_catalog_id=_EXTERNAL_CATALOG_ID,
            external_collection_id=_EXTERNAL_COLLECTION_ID,
        )

        assert item.to_dict()["collection"] == _EXTERNAL_COLLECTION_ID
        catalogs_svc.resolve_catalog_external_id.assert_not_awaited()
        catalogs_svc.collections.resolve_collection_external_id.assert_not_awaited()


# ---------------------------------------------------------------------------
# A) create_item_collection resolves the page's ids ONCE, not once per item
# ---------------------------------------------------------------------------


class TestCreateItemCollectionResolvesIdsOncePerPage:
    @pytest.mark.asyncio
    async def test_multi_item_page_resolves_external_ids_once(self, monkeypatch):
        import dynastore.extensions.stac.stac_generator as gen
        import dynastore.extensions.stac.stac_db as stac_db_mod

        catalogs_svc = _fake_catalogs_service()
        monkeypatch.setattr(gen, "get_protocol", lambda _p: catalogs_svc)

        features = [
            Feature(
                type="Feature",
                id=f"item-{i}",
                geometry=None,
                properties={"datetime": "2024-01-01T00:00:00Z"},
            )
            for i in range(5)
        ]

        async def _fake_paginated(*args: Any, **kwargs: Any):
            return features, len(features)

        monkeypatch.setattr(stac_db_mod, "get_stac_items_paginated", _fake_paginated)

        result = await gen.create_item_collection(
            _make_request(),
            conn=None,
            schema=_INTERNAL_CATALOG_ID,
            table=_INTERNAL_COLLECTION_ID,
            limit=10,
            offset=0,
            stac_config=StacPluginConfig(),
        )

        assert len(result["features"]) == 5
        for rendered in result["features"]:
            assert rendered["collection"] == _EXTERNAL_COLLECTION_ID

        # One resolution call each, not one per item.
        catalogs_svc.resolve_catalog_external_id.assert_awaited_once_with(
            _INTERNAL_CATALOG_ID, allow_missing=True
        )
        catalogs_svc.collections.resolve_collection_external_id.assert_awaited_once_with(
            _INTERNAL_CATALOG_ID, _INTERNAL_COLLECTION_ID, allow_missing=True
        )


# ---------------------------------------------------------------------------
# B) Public STAC handlers reject a path param shaped like an internal id
# ---------------------------------------------------------------------------


class TestPublicBoundaryRejectsInternalIds:
    def _svc(self):
        from dynastore.extensions.stac.stac_service import STACService

        return STACService.__new__(STACService)

    def _forbid_catalogs_service(self, svc, monkeypatch):
        async def _fail(*_a: Any, **_kw: Any):
            raise AssertionError(
                "the catalogs service must not be reached for an internal-shaped id"
            )

        monkeypatch.setattr(svc, "_get_catalogs_service", _fail)

    @pytest.mark.asyncio
    async def test_get_stac_catalog_rejects_internal_id(self, monkeypatch):
        svc = self._svc()
        self._forbid_catalogs_service(svc, monkeypatch)

        with pytest.raises(HTTPException) as exc_info:
            await svc.get_stac_catalog(
                catalog_id=_INTERNAL_CATALOG_ID,
                request=_make_request(),
                language="en",
                request_hints=frozenset(),
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_get_stac_collection_rejects_internal_collection_id(self, monkeypatch):
        svc = self._svc()
        self._forbid_catalogs_service(svc, monkeypatch)

        with pytest.raises(HTTPException) as exc_info:
            await svc.get_stac_collection(
                catalog_id=_EXTERNAL_CATALOG_ID,
                collection_id=_INTERNAL_COLLECTION_ID,
                request=_make_request(),
                language="en",
                request_hints=frozenset(),
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_get_stac_collection_items_rejects_internal_collection_id(self, monkeypatch):
        svc = self._svc()
        self._forbid_catalogs_service(svc, monkeypatch)

        with pytest.raises(HTTPException) as exc_info:
            await svc.get_stac_collection_items(
                catalog_id=_EXTERNAL_CATALOG_ID,
                collection_id=_INTERNAL_COLLECTION_ID,
                request=_make_request(),
                engine=object(),
                limit=10,
                offset=0,
                filter=None,
                language="en",
                request_hints=frozenset(),
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_get_stac_item_rejects_internal_catalog_id(self, monkeypatch):
        svc = self._svc()

        async def _fail(*_a: Any, **_kw: Any):
            raise AssertionError("engine must not be touched for an internal-shaped id")

        monkeypatch.setattr(
            "dynastore.extensions.stac.stac_service.managed_transaction", _fail
        )

        with pytest.raises(HTTPException) as exc_info:
            await svc.get_stac_item(
                catalog_id=_INTERNAL_CATALOG_ID,
                collection_id=_EXTERNAL_COLLECTION_ID,
                item_id="item-1",
                request=_make_request(),
                engine=object(),
                language="en",
            )

        assert exc_info.value.status_code == 404
