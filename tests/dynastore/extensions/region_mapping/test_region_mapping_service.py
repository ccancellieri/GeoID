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

"""Unit tests for the region_mapping extension's CRUD + serving router
(dynastore#2821). The catalogs service and the SQL store layer are fully
stubbed -- no DB.
"""
from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


class _StubCatalogs:
    def __init__(self, known_collections: Optional[Dict[Any, Any]] = None) -> None:
        self._known = known_collections or {}

    async def get_collection(self, catalog_id: str, collection_id: str) -> Optional[MagicMock]:
        return self._known.get((catalog_id, collection_id))


def _app(monkeypatch: pytest.MonkeyPatch, catalogs: Any, engine: Any = object()) -> FastAPI:
    from dynastore.extensions.region_mapping import region_mapping_service as svc
    from dynastore.models.protocols.catalogs import CatalogsProtocol

    def _fake_get_protocol(protocol_type: Any) -> Any:
        if protocol_type is CatalogsProtocol:
            return catalogs
        return None

    monkeypatch.setattr(svc, "get_protocol", _fake_get_protocol)
    monkeypatch.setattr(svc, "get_engine", lambda: engine)

    app = FastAPI()
    app.include_router(svc.RegionMappingService.router)
    return app


# ---------------------------------------------------------------------------
# POST /region-mappings
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_mapping_returns_201_and_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    catalogs = _StubCatalogs({("fao", "countries"): MagicMock()})
    app = _app(monkeypatch, catalogs)

    async def _apply_mapping(engine: Any, **kwargs: Any):
        return "fao_countries", [
            {
                "claim_ci": "country", "claim": "country", "mapping_id": "fao_countries",
                "role": "primary", "src_catalog": "fao", "src_collection": "countries",
                "region_prop": "adm0_code", "alias": "country", "title": "Countries",
            },
        ]

    monkeypatch.setattr(svc._store, "apply_mapping", _apply_mapping)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/region-mappings",
            json={"catalog": "fao", "collection": "countries", "column": "adm0_code", "alias": "country"},
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["mapping_id"] == "fao_countries"
    assert body["claims"][0]["claim"] == "country"


@pytest.mark.asyncio
async def test_register_mapping_unknown_collection_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    catalogs = _StubCatalogs({})
    app = _app(monkeypatch, catalogs)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/region-mappings",
            json={
                "catalog": "fao", "collection": "does-not-exist",
                "column": "adm0_code", "alias": "country",
            },
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_register_mapping_missing_alias_returns_422(monkeypatch: pytest.MonkeyPatch) -> None:
    """``alias`` is required -- TerriaJS always declares one, so there is no
    safe column-name default to fall back to."""
    catalogs = _StubCatalogs({("fao", "countries"): MagicMock()})
    app = _app(monkeypatch, catalogs)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/region-mappings",
            json={"catalog": "fao", "collection": "countries", "column": "adm0_code"},
        )

    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_register_mapping_unknown_column_returns_400(monkeypatch: pytest.MonkeyPatch) -> None:
    """The claimed column must be a real queryable property of the source
    collection -- checked once, on the write path, before any claim is
    persisted."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    catalogs = _StubCatalogs({("fao", "countries"): MagicMock()})
    app = _app(monkeypatch, catalogs)
    monkeypatch.setattr(
        svc, "resolve_queryable_property_names",
        AsyncMock(return_value={"iso3", "title"}),
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/region-mappings",
            json={
                "catalog": "fao", "collection": "countries",
                "column": "not_a_real_column", "alias": "country",
            },
        )

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_register_mapping_regex_metacharacter_claim_returns_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    catalogs = _StubCatalogs({("fao", "countries"): MagicMock()})
    app = _app(monkeypatch, catalogs)

    async def _apply_mapping(engine: Any, **kwargs: Any):
        from dynastore.extensions.region_mapping.claims import compute_claim_set
        compute_claim_set(
            catalog_id=kwargs["catalog_id"], collection_id=kwargs["collection_id"],
            column=kwargs["column"], alias=kwargs["alias"], extra_aliases=kwargs["extra_aliases"],
        )
        raise AssertionError("unreachable -- compute_claim_set should have raised")

    monkeypatch.setattr(svc._store, "apply_mapping", _apply_mapping)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/region-mappings",
            json={
                "catalog": "fao", "collection": "countries",
                "column": "adm0.code", "alias": "country",
            },
        )

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_register_mapping_no_engine_returns_503(monkeypatch: pytest.MonkeyPatch) -> None:
    catalogs = _StubCatalogs({("fao", "countries"): MagicMock()})
    app = _app(monkeypatch, catalogs, engine=None)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/region-mappings",
            json={
                "catalog": "fao", "collection": "countries",
                "column": "adm0_code", "alias": "country",
            },
        )

    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# DELETE /region-mappings/{mapping_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revoke_mapping_returns_deleted_count(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(svc._store, "delete_mapping", AsyncMock(return_value=3))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.delete("/region-mappings/fao_countries")

    assert resp.status_code == 200
    assert resp.json() == {"mapping_id": "fao_countries", "deleted_claims": 3}


@pytest.mark.asyncio
async def test_revoke_mapping_unknown_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))

    async def _delete_mapping(engine: Any, mapping_id: str) -> int:
        raise svc._store.MappingNotFoundError(mapping_id)

    monkeypatch.setattr(svc._store, "delete_mapping", _delete_mapping)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.delete("/region-mappings/does-not-exist")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /region-mappings
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_mappings_returns_items(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    row = {
        "claim_ci": "country", "claim": "country", "mapping_id": "fao_countries",
        "role": "primary", "src_catalog": "fao", "src_collection": "countries",
        "region_prop": "adm0_code", "alias": "country", "title": "Countries",
    }
    monkeypatch.setattr(svc._store, "list_claims", AsyncMock(return_value=[row]))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings", params={"catalog": "fao"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["items"][0]["claim"] == "country"
    assert body["limit"] == 200
    assert body["offset"] == 0


@pytest.mark.asyncio
async def test_list_mappings_invalid_cql_filter_returns_400(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app(monkeypatch, _StubCatalogs({}))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings", params={"filter": "not_a_column = 1"})

    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GET /region-mappings/region.json
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_definitions_shape_and_prefixed_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))

    primary = {
        "mapping_id": "fao_countries", "claim": "country", "src_catalog": "fao",
        "src_collection": "countries", "region_prop": "adm0_code", "title": "Countries",
    }
    monkeypatch.setattr(svc._store, "fetch_primary_records", AsyncMock(return_value=[primary]))
    monkeypatch.setattr(
        svc._store, "fetch_claims_for_mapping",
        AsyncMock(return_value=[
            {"claim": "country"}, {"claim": "adm0_code"}, {"claim": "fao_country"},
        ]),
    )
    monkeypatch.setattr(svc, "fetch_collection_bbox", AsyncMock(return_value=[10.0, 20.0, 30.0, 40.0]))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/region.json")

    assert resp.status_code == 200
    body = resp.json()
    entry = body["regionWmsMap"]["FAO_COUNTRIES"]

    assert entry["layerName"] == "default"
    assert entry["serverType"] == "MVT"
    assert entry["serverMinZoom"] == 0
    assert entry["serverMaxNativeZoom"] == 12
    assert entry["serverMaxZoom"] == 28
    assert entry["digits"] == 255
    assert entry["regionProp"] == "adm0_code"
    assert entry["uniqueIdProp"] == "adm0_code"
    assert set(entry["aliases"]) == {"country", "adm0_code", "fao_country"}
    assert entry["bbox"] == [10.0, 20.0, 30.0, 40.0]
    assert entry["regionIdsFile"].endswith("/region-mappings/fao_countries/regionIds")
    assert "{z}/{x}/{y}.mvt" in entry["server"]
    assert "collections=countries" in entry["server"]


@pytest.mark.asyncio
async def test_definitions_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    records = [
        {
            "mapping_id": f"cat_coll{i}", "claim": f"claim{i}", "src_catalog": "cat",
            "src_collection": f"coll{i}", "region_prop": "id", "title": f"Coll {i}",
        }
        for i in range(5)
    ]
    monkeypatch.setattr(svc._store, "fetch_primary_records", AsyncMock(return_value=records))
    monkeypatch.setattr(svc._store, "fetch_claims_for_mapping", AsyncMock(return_value=[]))
    monkeypatch.setattr(svc, "fetch_collection_bbox", AsyncMock(return_value=[1.0, 1.0, 2.0, 2.0]))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/region.json", params={"limit": 2, "offset": 1})

    body = resp.json()["regionWmsMap"]
    assert len(body) == 2


@pytest.mark.asyncio
async def test_definitions_with_cql_filter_bypasses_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    cached_fetch = AsyncMock(return_value=[])
    monkeypatch.setattr(svc._store, "fetch_primary_records", cached_fetch)

    uncached = AsyncMock(return_value=[])
    monkeypatch.setattr(svc._store, "list_claims", uncached)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/region-mappings/region.json", params={"filter": "src_catalog = 'fao'"},
        )

    assert resp.status_code == 200
    cached_fetch.assert_not_called()
    uncached.assert_awaited_once()


# ---------------------------------------------------------------------------
# GET /region-mappings/{mapping_id}/regionIds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_region_ids_returns_sorted_distinct_values(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_mapping_primary",
        AsyncMock(return_value={"src_catalog": "fao", "src_collection": "countries", "region_prop": "adm0_code"}),
    )
    monkeypatch.setattr(svc, "fetch_distinct_region_ids", AsyncMock(return_value=["DEU", "FRA", "ITA"]))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/fao_countries/regionIds")

    assert resp.status_code == 200
    body = resp.json()
    assert body["layer"] == "default"
    assert body["property"] == "adm0_code"
    assert body["values"] == ["DEU", "FRA", "ITA"]


@pytest.mark.asyncio
async def test_region_ids_unknown_mapping_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(svc._store, "fetch_mapping_primary", AsyncMock(return_value=None))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/does-not-exist/regionIds")

    assert resp.status_code == 404
