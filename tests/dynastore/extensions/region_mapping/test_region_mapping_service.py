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

"""Unit tests for the region_mapping extension's read-only serving router
(dynastore#443 Phase 1). The catalogs service is fully stubbed — no DB.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


def _feature(properties: Dict[str, Any]) -> MagicMock:
    f = MagicMock()
    f.properties = properties
    return f


class _StubCatalogs:
    """Minimal CatalogsProtocol stand-in for router tests.

    ``search_items`` dispatches on ``(catalog_id, collection_id)`` to one of
    two canned behaviors: registry claim records, or source-collection
    region-id feature rows (for the group_by DISTINCT loop).
    """

    def __init__(
        self,
        claim_records: List[Dict[str, Any]],
        region_id_pages: Optional[List[List[str]]] = None,
        collection_extent_bbox: Optional[List[float]] = None,
    ) -> None:
        self._claim_records = claim_records
        self._region_id_pages = region_id_pages or []
        self._collection_extent_bbox = collection_extent_bbox

    async def search_items(self, catalog_id: str, collection_id: str, request: Any) -> List[MagicMock]:
        from dynastore.extensions.region_mapping.registry_data import REGISTRY_CATALOG_ID

        if catalog_id == REGISTRY_CATALOG_ID:
            return self._filter_registry(request)

        # Source-collection region-id page fetch (group_by loop).
        offset = request.offset or 0
        page_index = offset // (request.limit or 1)
        if page_index >= len(self._region_id_pages):
            return []
        return [_feature({request.select[0].field: v}) for v in self._region_id_pages[page_index]]

    def _filter_registry(self, request: Any) -> List[MagicMock]:
        def _matches(rec: Dict[str, Any]) -> bool:
            for f in request.filters:
                if rec.get(f.field) != f.value:
                    return False
            return True

        matched = [r for r in self._claim_records if _matches(r)]
        limit = request.limit or len(matched)
        return [_feature(r) for r in matched[:limit]]

    async def get_collection(self, catalog_id: str, collection_id: str) -> Optional[MagicMock]:
        collection = MagicMock()
        if self._collection_extent_bbox is None:
            collection.extent = None
        else:
            collection.extent.spatial.bbox = [self._collection_extent_bbox]
        return collection


class _StubConfigs:
    """Minimal ConfigsProtocol stand-in — backs ``is_catalog_public``'s
    ``CatalogLookupAudience`` read with a fixed per-catalog opt-in set."""

    def __init__(self, public_catalogs: "set[str]") -> None:
        self._public_catalogs = public_catalogs

    async def get_config(self, config_cls: Any, catalog_id: Optional[str] = None, **_kw: Any) -> Any:
        from dynastore.modules.iam.audience_configs import CatalogLookupAudience

        if config_cls is CatalogLookupAudience:
            return CatalogLookupAudience(is_public=catalog_id in self._public_catalogs)
        return config_cls()


def _claim(mapping_id: str, claim: str, role: str, **extra: Any) -> Dict[str, Any]:
    base = {
        "claim": claim,
        "claim_ci": claim.casefold(),
        "mapping_id": mapping_id,
        "role": role,
        "src_catalog": extra.pop("src_catalog", "fao"),
        "src_collection": extra.pop("src_collection", "countries"),
        "region_prop": extra.pop("region_prop", "adm0_code"),
        "alias": extra.pop("alias", claim),
        "title": extra.pop("title", "Countries"),
    }
    base.update(extra)
    return base


@pytest.fixture(autouse=True)
def _reset_caches():
    """Every @cached read helper must start each test with an empty cache."""
    from dynastore.extensions.region_mapping.registry_data import (
        invalidate_serving_caches,
        is_catalog_public,
    )
    from dynastore.tools.cache import cache_clear

    invalidate_serving_caches()
    cache_clear(is_catalog_public)
    yield
    invalidate_serving_caches()
    cache_clear(is_catalog_public)


# Default visibility posture for tests that don't care about FINDING-1
# gating: every source catalog these existing fixtures reference ("fao",
# "who") is public, so pre-existing definitions/regionIds assertions keep
# their original meaning. Tests targeting the gate itself pass an explicit
# ``public_catalogs`` set (e.g. ``set()`` for an all-private scenario).
_DEFAULT_PUBLIC_CATALOGS = {"fao", "who"}


def _app_with_catalogs(
    monkeypatch: pytest.MonkeyPatch,
    catalogs: _StubCatalogs,
    *,
    public_catalogs: Optional["set[str]"] = None,
) -> FastAPI:
    from dynastore.extensions.region_mapping.region_mapping_service import RegionMappingService
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    from dynastore.models.protocols.configs import ConfigsProtocol

    configs = _StubConfigs(
        public_catalogs if public_catalogs is not None else set(_DEFAULT_PUBLIC_CATALOGS)
    )

    def _fake_get_protocol(protocol_type: Any) -> Any:
        if protocol_type is CatalogsProtocol:
            return catalogs
        if protocol_type is ConfigsProtocol:
            return configs
        return None

    monkeypatch.setattr(
        "dynastore.extensions.region_mapping.region_mapping_service.get_protocol",
        _fake_get_protocol,
    )
    monkeypatch.setattr(
        "dynastore.extensions.region_mapping.registry_data.get_protocol",
        _fake_get_protocol,
    )

    app = FastAPI()
    app.include_router(RegionMappingService.router)
    return app


# ---------------------------------------------------------------------------
# GET /region-mappings/definitions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_definitions_shape_and_prefixed_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [
        _claim("fao_countries", "country", "primary"),
        _claim("fao_countries", "adm0_code", "alias"),
        _claim("fao_countries", "fao_country", "alias"),
    ]
    catalogs = _StubCatalogs(records, collection_extent_bbox=[10.0, 20.0, 30.0, 40.0])
    app = _app_with_catalogs(monkeypatch, catalogs)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/definitions")

    assert resp.status_code == 200
    body = resp.json()
    entry = body["regionWmsMap"]["FAO_COUNTRIES"]

    assert entry["layerName"] == "default"
    assert entry["serverType"] == "MVT"
    assert entry["regionProp"] == "adm0_code"
    assert entry["uniqueIdProp"] == "adm0_code"
    assert set(entry["aliases"]) == {"country", "adm0_code", "fao_country"}
    assert "fao_country" in entry["aliases"], "catalog-prefixed alias variant must be present"
    assert entry["bbox"] == [10.0, 20.0, 30.0, 40.0]
    assert entry["regionIdsFile"].endswith("/region-mappings/fao_countries/regionIds")
    assert "{z}/{x}/{y}.mvt" in entry["server"]
    assert "collections=countries" in entry["server"]


@pytest.mark.asyncio
async def test_definitions_world_bounds_fallback_for_degenerate_extent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [_claim("fao_countries", "country", "primary")]
    catalogs = _StubCatalogs(records, collection_extent_bbox=[0.0, 0.0, 0.0, 0.0])
    app = _app_with_catalogs(monkeypatch, catalogs)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/definitions")

    entry = resp.json()["regionWmsMap"]["FAO_COUNTRIES"]
    assert entry["bbox"] == [-180.0, -90.0, 180.0, 90.0]


@pytest.mark.asyncio
async def test_definitions_no_extent_falls_back_to_world_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [_claim("fao_countries", "country", "primary")]
    catalogs = _StubCatalogs(records, collection_extent_bbox=None)
    app = _app_with_catalogs(monkeypatch, catalogs)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/definitions")

    entry = resp.json()["regionWmsMap"]["FAO_COUNTRIES"]
    assert entry["bbox"] == [-180.0, -90.0, 180.0, 90.0]


@pytest.mark.asyncio
async def test_definitions_alias_exact_match_filters_to_one_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [
        _claim("fao_countries", "country", "primary"),
        _claim("who_regions", "region", "primary", src_catalog="who", src_collection="regions"),
    ]
    catalogs = _StubCatalogs(records, collection_extent_bbox=[-10.0, -10.0, 10.0, 10.0])
    app = _app_with_catalogs(monkeypatch, catalogs)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/definitions", params={"alias": "COUNTRY"})

    body = resp.json()["regionWmsMap"]
    assert list(body.keys()) == ["FAO_COUNTRIES"]


@pytest.mark.asyncio
async def test_definitions_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [
        _claim(f"cat_coll{i}", f"claim{i}", "primary", src_collection=f"coll{i}")
        for i in range(5)
    ]
    catalogs = _StubCatalogs(records, collection_extent_bbox=[1.0, 1.0, 2.0, 2.0])
    app = _app_with_catalogs(monkeypatch, catalogs)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/definitions", params={"limit": 2, "offset": 1})

    body = resp.json()["regionWmsMap"]
    assert len(body) == 2


# ---------------------------------------------------------------------------
# GET /region-mappings/{mapping_id}/regionIds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_region_ids_returns_sorted_distinct_values(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [_claim("fao_countries", "country", "primary")]
    catalogs = _StubCatalogs(
        records,
        region_id_pages=[["ITA", "FRA", "ITA", "DEU"]],
    )
    app = _app_with_catalogs(monkeypatch, catalogs)

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
    catalogs = _StubCatalogs([])
    app = _app_with_catalogs(monkeypatch, catalogs)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/does-not-exist/regionIds")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Serve-time visibility gate (dynastore#443 review finding 1) — a mapping
# whose SOURCE catalog has not opted into CatalogLookupAudience.is_public
# must not leak data anonymously, even though /region-mappings/* itself is
# publicly reachable.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_definitions_excludes_mapping_from_private_source_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [
        _claim("fao_countries", "country", "primary", src_catalog="fao"),
        _claim("secret_regions", "region", "primary", src_catalog="secret", src_collection="regions"),
    ]
    catalogs = _StubCatalogs(records, collection_extent_bbox=[1.0, 1.0, 2.0, 2.0])
    # Only "fao" opts in; "secret" is not in the public set.
    app = _app_with_catalogs(monkeypatch, catalogs, public_catalogs={"fao"})

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/definitions")

    body = resp.json()["regionWmsMap"]
    assert list(body.keys()) == ["FAO_COUNTRIES"], (
        "a mapping sourced from a non-public catalog must not appear in "
        "/definitions"
    )


@pytest.mark.asyncio
async def test_definitions_all_private_yields_empty_map(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [_claim("fao_countries", "country", "primary")]
    catalogs = _StubCatalogs(records, collection_extent_bbox=[1.0, 1.0, 2.0, 2.0])
    app = _app_with_catalogs(monkeypatch, catalogs, public_catalogs=set())

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/definitions")

    assert resp.json() == {"regionWmsMap": {}}


@pytest.mark.asyncio
async def test_definitions_pagination_counts_only_visible_mappings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """limit/offset must apply to the VISIBLE set, not the raw claim rows —
    a private mapping sitting inside the requested page must not consume a
    page slot nor shift a later public mapping out of range."""
    records = [
        _claim("fao_a", "a", "primary", src_catalog="fao", src_collection="a"),
        _claim("secret_b", "b", "primary", src_catalog="secret", src_collection="b"),
        _claim("fao_c", "c", "primary", src_catalog="fao", src_collection="c"),
    ]
    catalogs = _StubCatalogs(records, collection_extent_bbox=[1.0, 1.0, 2.0, 2.0])
    app = _app_with_catalogs(monkeypatch, catalogs, public_catalogs={"fao"})

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/definitions", params={"limit": 2, "offset": 0})

    body = resp.json()["regionWmsMap"]
    assert set(body.keys()) == {"FAO_A", "FAO_C"}


@pytest.mark.asyncio
async def test_region_ids_returns_404_for_private_source_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [_claim("secret_regions", "region", "primary", src_catalog="secret", src_collection="regions")]
    catalogs = _StubCatalogs(
        records,
        region_id_pages=[["ITA", "FRA"]],
    )
    app = _app_with_catalogs(monkeypatch, catalogs, public_catalogs=set())

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/secret_regions/regionIds")

    assert resp.status_code == 404
    # Same message shape as a genuinely unknown mapping — no existence
    # disclosure via a distinct 403/error body.
    assert "not found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_region_ids_public_source_catalog_still_served(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [_claim("fao_countries", "country", "primary")]
    catalogs = _StubCatalogs(records, region_id_pages=[["ITA", "FRA"]])
    app = _app_with_catalogs(monkeypatch, catalogs, public_catalogs={"fao"})

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/fao_countries/regionIds")

    assert resp.status_code == 200
    assert resp.json()["values"] == ["FRA", "ITA"]
