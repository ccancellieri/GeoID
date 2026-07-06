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


@pytest.fixture(autouse=True)
def _default_sound_mapping_cardinality(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every ``region.json``/``_build_definitions`` test predates the
    per-mapping cardinality check -- default it to "sound" (one feature per
    code) so existing tests keep exercising what they were written for.
    Tests of the exclusion behavior itself override this per-test."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    monkeypatch.setattr(
        svc, "fetch_region_mapping_cardinality",
        AsyncMock(return_value={
            "feature_count": 1, "distinct_region_count": 1, "distinct_unique_id_count": 1,
        }),
    )


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
    entry = body["regionWmsMap"]["fao_countries"]

    assert entry["layerName"] == "default"
    assert entry["serverType"] == "MVT"
    assert entry["serverMinZoom"] == 0
    assert entry["serverMaxNativeZoom"] == 12
    assert entry["serverMaxZoom"] == 28
    assert entry["digits"] == 255
    assert entry["regionProp"] == "adm0_code"
    # No unique_id_prop registered -- falls back to FID (TerriaJS's own
    # client-side default), never regionProp: TerriaJS requires uniqueIdProp
    # to be a numeric, zero-based, sequential feature index, so defaulting
    # it to a string region code breaks matching.
    assert entry["uniqueIdProp"] == "FID"
    # regionProp's own value is never repeated inside aliases -- TerriaJS
    # already matches it via the "regionProp" field.
    assert set(entry["aliases"]) == {"country", "fao_country"}
    assert entry["bbox"] == [10.0, 20.0, 30.0, 40.0]
    assert entry["regionIdsFile"].endswith("/region-mappings/fao_countries/regionIds")
    assert "{z}/{x}/{y}.mvt" in entry["server"]
    assert "collections=countries" in entry["server"]


@pytest.mark.asyncio
async def test_definitions_excludes_mapping_with_duplicate_region_codes(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """A mapping whose regionProp repeats across features (e.g. an ISO3
    claim on an admin-1 collection) never reaches region.json -- TerriaJS
    can't tell its features apart by that code, so publishing it there is
    actively misleading. GET .../validate is how to see why."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))

    primary = {
        "mapping_id": "gaul_demo_gaul_level_1", "claim": "iso3", "src_catalog": "gaul_demo",
        "src_collection": "gaul_level_1", "region_prop": "ISO3_CODE", "title": "GAUL level 1",
    }
    monkeypatch.setattr(svc._store, "fetch_primary_records", AsyncMock(return_value=[primary]))
    monkeypatch.setattr(
        svc, "fetch_region_mapping_cardinality",
        AsyncMock(return_value={
            "feature_count": 3102, "distinct_region_count": 200, "distinct_unique_id_count": 3102,
        }),
    )

    transport = ASGITransport(app=app)
    with caplog.at_level("WARNING"):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/region-mappings/region.json")

    assert resp.status_code == 200
    assert resp.json() == {"regionWmsMap": {}}
    assert "gaul_demo_gaul_level_1" in caplog.text
    assert "regionProp is not unique per feature" in caplog.text


@pytest.mark.asyncio
async def test_definitions_keeps_sound_mappings_alongside_excluded_ones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One unsound mapping among several must not take down the rest of
    region.json -- each mapping's cardinality is judged independently."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))

    sound = {
        "mapping_id": "fao_countries", "claim": "country", "src_catalog": "fao",
        "src_collection": "countries", "region_prop": "adm0_code", "title": "Countries",
    }
    unsound = {
        "mapping_id": "gaul_demo_gaul_level_1", "claim": "iso3", "src_catalog": "gaul_demo",
        "src_collection": "gaul_level_1", "region_prop": "ISO3_CODE", "title": "GAUL level 1",
    }
    monkeypatch.setattr(
        svc._store, "fetch_primary_records", AsyncMock(return_value=[sound, unsound]),
    )
    monkeypatch.setattr(svc._store, "fetch_claims_for_mapping", AsyncMock(return_value=[]))
    monkeypatch.setattr(svc, "fetch_collection_bbox", AsyncMock(return_value=[0.0, 0.0, 1.0, 1.0]))

    async def _cardinality(src_catalog: str, *_args: Any, **_kwargs: Any) -> Dict[str, int]:
        if src_catalog == "gaul_demo":
            return {
                "feature_count": 3102, "distinct_region_count": 200,
                "distinct_unique_id_count": 3102,
            }
        return {"feature_count": 1, "distinct_region_count": 1, "distinct_unique_id_count": 1}

    monkeypatch.setattr(svc, "fetch_region_mapping_cardinality", _cardinality)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/region.json")

    assert resp.status_code == 200
    assert list(resp.json()["regionWmsMap"].keys()) == ["fao_countries"]


def test_default_maps_base_url_swaps_last_path_segment_for_maps() -> None:
    """The maps machine is a sibling gateway path, not nested under this
    service's own root_path -- e.g. '.../api/catalog' -> '.../api/maps',
    never '.../api/catalog/maps'."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    assert svc._default_maps_base_url(
        "https://data.review.fao.org/geospatial/dev/api/catalog"
    ) == "https://data.review.fao.org/geospatial/dev/api/maps"


@pytest.mark.asyncio
async def test_get_maps_base_url_honours_configured_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicitly configured ``maps_base_url`` wins over the sibling-path
    fallback -- required for deployments that don't follow that convention."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc
    from dynastore.extensions.region_mapping.config import RegionMappingConfig
    from dynastore.models.protocols.configs import ConfigsProtocol

    configs = MagicMock()
    configs.get_config = AsyncMock(
        return_value=RegionMappingConfig(maps_base_url="https://tiles.example.org")
    )

    def _fake_get_protocol(protocol_type: Any) -> Any:
        return configs if protocol_type is ConfigsProtocol else None

    monkeypatch.setattr(svc, "get_protocol", _fake_get_protocol)

    result = await svc._get_maps_base_url("https://data.review.fao.org/geospatial/dev/api/catalog")

    assert result == "https://tiles.example.org"


@pytest.mark.asyncio
async def test_definitions_server_url_is_sibling_to_catalog_not_nested_under_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression test: the 'server' tile URL must point at the sibling maps
    machine (e.g. '.../api/maps/tiles/...'), not '.../api/catalog/maps/tiles/...'."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_primary_records",
        AsyncMock(return_value=[{
            "mapping_id": "gaul_demo_gaul_level_1", "claim": "iso3", "src_catalog": "gaul_demo",
            "src_collection": "gaul_level_1", "region_prop": "ISO3_CODE", "title": "GAUL level 1",
        }]),
    )
    monkeypatch.setattr(svc._store, "fetch_claims_for_mapping", AsyncMock(return_value=[{"claim": "iso3"}]))
    monkeypatch.setattr(svc, "fetch_collection_bbox", AsyncMock(return_value=[0.0, 0.0, 1.0, 1.0]))

    transport = ASGITransport(app=app, root_path="/geospatial/dev/api/catalog")
    async with AsyncClient(transport=transport, base_url="http://data.review.fao.org") as client:
        resp = await client.get("/geospatial/dev/api/catalog/region-mappings/region.json")

    entry = resp.json()["regionWmsMap"]["gaul_demo_gaul_level_1"]
    assert entry["server"].startswith("http://data.review.fao.org/geospatial/dev/api/maps/")
    assert "/api/catalog/maps/" not in entry["server"]


@pytest.mark.asyncio
async def test_definitions_key_case_matches_server_and_region_ids_file_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression test: the regionWmsMap key must be lowercase like the tile
    'server' URL and 'region_ids_file' URL, so it can be copy-pasted as-is."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_primary_records",
        AsyncMock(return_value=[{
            "mapping_id": "gaul_demo_gaul_level_1", "claim": "iso3", "src_catalog": "gaul_demo",
            "src_collection": "gaul_level_1", "region_prop": "ISO3_CODE", "title": "GAUL level 1",
        }]),
    )
    monkeypatch.setattr(svc._store, "fetch_claims_for_mapping", AsyncMock(return_value=[{"claim": "iso3"}]))
    monkeypatch.setattr(svc, "fetch_collection_bbox", AsyncMock(return_value=[0.0, 0.0, 1.0, 1.0]))

    transport = ASGITransport(app=app, root_path="/geospatial/dev/api/catalog")
    async with AsyncClient(transport=transport, base_url="http://data.review.fao.org") as client:
        resp = await client.get("/geospatial/dev/api/catalog/region-mappings/region.json")

    body = resp.json()
    assert "gaul_demo_gaul_level_1" in body["regionWmsMap"]
    assert "GAUL_DEMO_GAUL_LEVEL_1" not in body["regionWmsMap"]
    entry = body["regionWmsMap"]["gaul_demo_gaul_level_1"]
    assert "gaul_demo" in entry["server"]
    assert "gaul_demo" in entry["regionIdsFile"]


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
# GET /region-mappings/{mapping_id}/validate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_mapping_sound_returns_valid_true_and_no_reasons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_mapping_primary",
        AsyncMock(return_value={
            "src_catalog": "fao", "src_collection": "countries",
            "region_prop": "adm0_code", "unique_id_prop": "FID",
        }),
    )
    monkeypatch.setattr(
        svc, "fetch_region_mapping_cardinality",
        AsyncMock(return_value={
            "feature_count": 200, "distinct_region_count": 200, "distinct_unique_id_count": 200,
        }),
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/fao_countries/validate")

    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "mapping_id": "fao_countries", "valid": True, "reasons": [],
        "feature_count": 200, "distinct_region_count": 200, "distinct_unique_id_count": 200,
    }


@pytest.mark.asyncio
async def test_validate_mapping_unsound_returns_valid_false_with_reasons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_mapping_primary",
        AsyncMock(return_value={
            "src_catalog": "gaul_demo", "src_collection": "gaul_level_1",
            "region_prop": "ISO3_CODE", "unique_id_prop": "FID",
        }),
    )
    monkeypatch.setattr(
        svc, "fetch_region_mapping_cardinality",
        AsyncMock(return_value={
            "feature_count": 3102, "distinct_region_count": 200, "distinct_unique_id_count": 3102,
        }),
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/gaul_demo_gaul_level_1/validate")

    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert len(body["reasons"]) == 1
    assert "regionProp is not unique per feature" in body["reasons"][0]


@pytest.mark.asyncio
async def test_validate_mapping_unknown_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(svc._store, "fetch_mapping_primary", AsyncMock(return_value=None))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/does-not-exist/validate")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /region-mappings/{mapping_id}/regionIds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_region_ids_returns_fid_ordered_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """TerriaJS positionally matches ``values[i]`` against a feature whose
    ``uniqueIdProp`` (FID) equals ``i`` -- so this endpoint must fetch via
    ``fetch_region_ids_by_unique_id`` (FID-ordered), never the
    deduplicated/alphabetically-sorted ``fetch_distinct_region_ids`` used
    by the CSV template endpoint."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_mapping_primary",
        AsyncMock(return_value={"src_catalog": "fao", "src_collection": "countries", "region_prop": "adm0_code"}),
    )
    captured: Dict[str, Any] = {}

    async def _fetch_region_ids_by_unique_id(*args: Any) -> list:
        captured["args"] = args
        return ["FRA", "FRA", "DEU"]

    monkeypatch.setattr(svc, "fetch_region_ids_by_unique_id", _fetch_region_ids_by_unique_id)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/fao_countries/regionIds")

    assert resp.status_code == 200
    body = resp.json()
    assert body["layer"] == "default"
    assert body["property"] == "adm0_code"
    assert body["values"] == ["FRA", "FRA", "DEU"]
    # unique_id_prop defaults to FID (TerriaJS's own client-side default)
    # when the mapping never explicitly registered one.
    assert captured["args"] == ("fao", "countries", "adm0_code", "FID")


@pytest.mark.asyncio
async def test_region_ids_uses_registered_unique_id_prop_when_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_mapping_primary",
        AsyncMock(return_value={
            "src_catalog": "fao", "src_collection": "countries",
            "region_prop": "adm0_code", "unique_id_prop": "row_index",
        }),
    )
    captured: Dict[str, Any] = {}

    async def _fetch_region_ids_by_unique_id(*args: Any) -> list:
        captured["args"] = args
        return ["DEU"]

    monkeypatch.setattr(svc, "fetch_region_ids_by_unique_id", _fetch_region_ids_by_unique_id)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.get("/region-mappings/fao_countries/regionIds")

    assert captured["args"] == ("fao", "countries", "adm0_code", "row_index")


@pytest.mark.asyncio
async def test_region_ids_unknown_mapping_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(svc._store, "fetch_mapping_primary", AsyncMock(return_value=None))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/does-not-exist/regionIds")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /region-mappings/{mapping_id}/regionIds.csv
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_region_ids_csv_headed_by_alias_one_value_per_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_mapping_primary",
        AsyncMock(return_value={
            "src_catalog": "fao", "src_collection": "countries",
            "region_prop": "adm0_code", "alias": "iso3",
        }),
    )
    monkeypatch.setattr(svc, "fetch_distinct_region_ids", AsyncMock(return_value=["DEU", "FRA", "ITA"]))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/fao_countries/regionIds.csv")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert 'filename="fao_countries.csv"' in resp.headers["content-disposition"]
    assert resp.text.splitlines() == ["iso3", "DEU", "FRA", "ITA"]


@pytest.mark.asyncio
async def test_region_ids_csv_falls_back_to_region_prop_without_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No registered alias on this row -- header falls back to the region
    property name rather than an empty column header."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_mapping_primary",
        AsyncMock(return_value={
            "src_catalog": "fao", "src_collection": "countries", "region_prop": "adm0_code",
        }),
    )
    monkeypatch.setattr(svc, "fetch_distinct_region_ids", AsyncMock(return_value=["DEU"]))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/fao_countries/regionIds.csv")

    assert resp.text.splitlines() == ["adm0_code", "DEU"]


@pytest.mark.asyncio
async def test_region_ids_csv_unknown_mapping_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(svc._store, "fetch_mapping_primary", AsyncMock(return_value=None))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/does-not-exist/regionIds.csv")

    assert resp.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("dangerous_value", ["=SUM(A1:A9)", "+1+1", "-123", "@cmd"])
async def test_region_ids_csv_escapes_formula_injection_in_values(
    monkeypatch: pytest.MonkeyPatch, dangerous_value: str,
) -> None:
    """Values starting with =, +, -, @ come back quoted so Excel/Sheets
    won't interpret them as a formula (OWASP CSV injection)."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_mapping_primary",
        AsyncMock(return_value={
            "src_catalog": "fao", "src_collection": "countries",
            "region_prop": "adm0_code", "alias": "iso3",
        }),
    )
    monkeypatch.setattr(svc, "fetch_distinct_region_ids", AsyncMock(return_value=[dangerous_value, "DEU"]))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/fao_countries/regionIds.csv")

    assert resp.text.splitlines() == ["iso3", f"'{dangerous_value}", "DEU"]


@pytest.mark.asyncio
async def test_region_ids_csv_escapes_formula_injection_in_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The header cell (mapping alias) is just as attacker-influenceable
    as the values and gets the same escaping."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_mapping_primary",
        AsyncMock(return_value={
            "src_catalog": "fao", "src_collection": "countries",
            "region_prop": "adm0_code", "alias": "=HYPERLINK(evil)",
        }),
    )
    monkeypatch.setattr(svc, "fetch_distinct_region_ids", AsyncMock(return_value=["DEU"]))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/fao_countries/regionIds.csv")

    assert resp.text.splitlines() == ["'=HYPERLINK(evil)", "DEU"]


@pytest.mark.asyncio
async def test_region_ids_csv_leaves_benign_values_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    monkeypatch.setattr(
        svc._store, "fetch_mapping_primary",
        AsyncMock(return_value={
            "src_catalog": "fao", "src_collection": "countries",
            "region_prop": "adm0_code", "alias": "iso3",
        }),
    )
    monkeypatch.setattr(svc, "fetch_distinct_region_ids", AsyncMock(return_value=["DEU", "FRA", "ITA"]))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/fao_countries/regionIds.csv")

    assert resp.text.splitlines() == ["iso3", "DEU", "FRA", "ITA"]


# ---------------------------------------------------------------------------
# TerriaJS parameters + multilanguage description (dynastore#443 follow-up)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_mapping_passes_terria_params_and_lang_to_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every new TerriaJS field on the request body, plus the resolved
    `lang`, must reach ``registry_store.apply_mapping`` unchanged."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    catalogs = _StubCatalogs({("fao", "countries"): MagicMock()})
    app = _app(monkeypatch, catalogs)

    captured: Dict[str, Any] = {}

    async def _apply_mapping(engine: Any, **kwargs: Any):
        captured.update(kwargs)
        return "fao_countries", []

    monkeypatch.setattr(svc._store, "apply_mapping", _apply_mapping)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/region-mappings?lang=it",
            json={
                "catalog": "fao", "collection": "countries", "column": "adm0_code",
                "alias": "country", "title": "Paesi",
                "layer_name": "gaul_layer", "server_type": "WMS",
                "server_subdomains": ["a", "b"], "server_min_zoom": 2,
                "server_max_native_zoom": 8, "server_max_zoom": 20,
                "unique_id_prop": "internal_id", "digits": 4,
            },
        )

    assert resp.status_code == 201, resp.text
    assert captured["lang"] == "it"
    assert captured["title"] == "Paesi"
    assert captured["layer_name"] == "gaul_layer"
    assert captured["server_type"] == "WMS"
    assert captured["server_subdomains"] == ["a", "b"]
    assert captured["server_min_zoom"] == 2
    assert captured["server_max_native_zoom"] == 8
    assert captured["server_max_zoom"] == 20
    assert captured["unique_id_prop"] == "internal_id"
    assert captured["digits"] == 4


@pytest.mark.asyncio
async def test_definitions_honours_per_mapping_terria_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-default layerName/serverType/zoom/digits/uniqueIdProp stored on a
    claim row all render through to region.json -- proves they are
    genuinely per-mapping now, not the old hardcoded template constants."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    primary = {
        "mapping_id": "fao_countries", "claim": "country", "src_catalog": "fao",
        "src_collection": "countries", "region_prop": "adm0_code", "title": "Countries",
        "layer_name": "gaul_layer", "server_type": "WMS", "server_subdomains": ["a", "b"],
        "server_min_zoom": 2, "server_max_native_zoom": 8, "server_max_zoom": 20,
        "unique_id_prop": "internal_id", "digits": 4,
    }
    monkeypatch.setattr(svc._store, "fetch_primary_records", AsyncMock(return_value=[primary]))
    monkeypatch.setattr(svc._store, "fetch_claims_for_mapping", AsyncMock(return_value=[]))
    monkeypatch.setattr(svc, "fetch_collection_bbox", AsyncMock(return_value=[0.0, 0.0, 1.0, 1.0]))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/region-mappings/region.json")

    entry = resp.json()["regionWmsMap"]["fao_countries"]
    assert entry["layerName"] == "gaul_layer"
    assert entry["serverType"] == "WMS"
    assert entry["serverSubdomains"] == ["a", "b"]
    assert entry["serverMinZoom"] == 2
    assert entry["serverMaxNativeZoom"] == 8
    assert entry["serverMaxZoom"] == 20
    assert entry["uniqueIdProp"] == "internal_id"
    assert entry["digits"] == 4


@pytest.mark.asyncio
async def test_definitions_resolves_description_to_requested_language(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A multilanguage ``title`` resolves to the caller's ``?lang=``, falling
    back to English when the requested language is missing."""
    from dynastore.extensions.region_mapping import region_mapping_service as svc

    app = _app(monkeypatch, _StubCatalogs({}))
    primary = {
        "mapping_id": "fao_countries", "claim": "country", "src_catalog": "fao",
        "src_collection": "countries", "region_prop": "adm0_code",
        "title": {"en": "Countries", "it": "Paesi"},
    }
    monkeypatch.setattr(svc._store, "fetch_primary_records", AsyncMock(return_value=[primary]))
    monkeypatch.setattr(svc._store, "fetch_claims_for_mapping", AsyncMock(return_value=[]))
    monkeypatch.setattr(svc, "fetch_collection_bbox", AsyncMock(return_value=[0.0, 0.0, 1.0, 1.0]))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp_it = await client.get("/region-mappings/region.json", params={"lang": "it"})
        resp_default = await client.get("/region-mappings/region.json")
        resp_missing = await client.get("/region-mappings/region.json", params={"lang": "fr"})

    entry_it = resp_it.json()["regionWmsMap"]["fao_countries"]
    entry_default = resp_default.json()["regionWmsMap"]["fao_countries"]
    entry_missing = resp_missing.json()["regionWmsMap"]["fao_countries"]

    assert entry_it["description"] == "Paesi"
    assert entry_default["description"] == "Countries"
    assert entry_missing["description"] == "Countries"  # falls back to 'en'
