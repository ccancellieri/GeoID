"""Unit tests for the DGGSService extension.

Tests focus on pure functions and request parsing — no live DB or FastAPI
server required.  Protocol interactions are covered by integration tests.
"""

import pytest

h3 = pytest.importorskip("h3", reason="h3 package not installed")

from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException


def test_dggs_service_importable():
    from dynastore.extensions.dggs.dggs_service import DGGSService
    assert DGGSService is not None


def test_dggs_service_conformance_uris():
    from dynastore.extensions.dggs.dggs_service import OGC_API_DGGS_URIS
    assert any("conf/core" in uri for uri in OGC_API_DGGS_URIS)
    assert any("conf/data-retrieval" in uri for uri in OGC_API_DGGS_URIS)
    assert any("conf/zone-query" in uri for uri in OGC_API_DGGS_URIS)
    assert all(uri.startswith("https://") for uri in OGC_API_DGGS_URIS)


def test_dggs_config_defaults():
    from dynastore.extensions.dggs.config import DGGSConfig
    cfg = DGGSConfig()
    assert cfg.enabled is True
    assert 0 <= cfg.default_resolution <= 15
    assert cfg.max_resolution >= cfg.default_resolution
    assert cfg.max_features_per_request > 0


def test_dggs_service_attributes():
    from dynastore.extensions.dggs.dggs_service import DGGSService
    assert DGGSService.prefix == "/dggs"
    assert DGGSService.priority == 170
    assert DGGSService.conformance_uris


def test_dggrs_info_h3():
    from dynastore.extensions.dggs.dggs_service import _H3_DGGRS
    assert _H3_DGGRS.id == "H3"
    assert _H3_DGGRS.maxRefinementLevel == 15
    assert _H3_DGGRS.defaultRefinementLevel >= 0


def test_dggrs_info_s2():
    from dynastore.extensions.dggs.dggs_service import _S2_DGGRS
    assert _S2_DGGRS.id == "S2"
    assert _S2_DGGRS.maxRefinementLevel == 30
    assert _S2_DGGRS.defaultRefinementLevel >= 0


def test_supported_dggrs_contains_h3_and_s2():
    from dynastore.extensions.dggs.dggs_service import _SUPPORTED_DGGRS
    assert "H3" in _SUPPORTED_DGGRS
    assert "S2" in _SUPPORTED_DGGRS


@pytest.mark.asyncio
async def test_get_dggrs_not_found():
    from dynastore.extensions.dggs.dggs_service import DGGSService
    svc = DGGSService.__new__(DGGSService)
    request = MagicMock()
    with pytest.raises(HTTPException) as exc_info:
        await svc.get_dggrs("UNKNOWN_GRID", request)
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_get_dggrs_data_invalid_bbox():
    from dynastore.extensions.dggs.dggs_service import DGGSService
    svc = DGGSService.__new__(DGGSService)
    request = MagicMock()
    with pytest.raises(HTTPException) as exc_info:
        await svc.get_dggs_data(
            catalog_id="cat1",
            collection_id="col1",
            request=request,
            zone_level=5,
            bbox="bad,bbox,string",
            datetime=None,
            parameter_name=None,
            dggs_id="H3",
        )
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_get_dggs_zone_invalid_h3_id():
    from dynastore.extensions.dggs.dggs_service import DGGSService
    svc = DGGSService.__new__(DGGSService)
    request = MagicMock()
    with pytest.raises(HTTPException) as exc_info:
        await svc.get_dggs_zone(
            catalog_id="cat1",
            collection_id="col1",
            zoneId="not-valid-h3",
            request=request,
            datetime=None,
            parameter_name=None,
            dggs_id="H3",
        )
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_get_dggs_zone_invalid_s2_id():
    from dynastore.extensions.dggs.dggs_service import DGGSService
    svc = DGGSService.__new__(DGGSService)
    request = MagicMock()
    with pytest.raises(HTTPException) as exc_info:
        await svc.get_dggs_zone(
            catalog_id="cat1",
            collection_id="col1",
            zoneId="not-valid-s2-token",
            request=request,
            datetime=None,
            parameter_name=None,
            dggs_id="S2",
        )
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_get_dggs_data_unsupported_dggs():
    from dynastore.extensions.dggs.dggs_service import DGGSService
    svc = DGGSService.__new__(DGGSService)
    request = MagicMock()
    with pytest.raises(HTTPException) as exc_info:
        await svc.get_dggs_data(
            catalog_id="cat1",
            collection_id="col1",
            request=request,
            zone_level=5,
            bbox=None,
            datetime=None,
            parameter_name=None,
            dggs_id="UNSUPPORTED_GRID",
        )
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_get_dggs_data_calls_fetch_and_aggregates_h3():
    from dynastore.extensions.dggs.dggs_service import DGGSService

    svc = DGGSService.__new__(DGGSService)
    svc._ogc_configs_protocol = None
    svc._ogc_catalogs_protocol = None

    mock_feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [12.48, 41.88]},
        "properties": {"value": 42.0},
    }

    fake_config = type("C", (), {
        "default_resolution": 5,
        "max_resolution": 10,
        "max_features_per_request": 1000,
    })()

    with patch.object(svc, "_get_plugin_config", new_callable=AsyncMock, return_value=fake_config), \
         patch.object(svc, "_fetch_features", new_callable=AsyncMock, return_value=[mock_feature]):
        request = MagicMock()
        result = await svc.get_dggs_data(
            catalog_id="cat1",
            collection_id="col1",
            request=request,
            zone_level=5,
            bbox=None,
            datetime=None,
            parameter_name=None,
            dggs_id="H3",
        )

    assert result.dggsId == "H3"
    assert result.zoneLevel == 5
    assert len(result.features) == 1


s2sphere = pytest.importorskip("s2sphere", reason="s2sphere package not installed")


@pytest.mark.asyncio
async def test_get_dggs_data_calls_fetch_and_aggregates_s2():
    from dynastore.extensions.dggs.dggs_service import DGGSService

    svc = DGGSService.__new__(DGGSService)
    svc._ogc_configs_protocol = None
    svc._ogc_catalogs_protocol = None

    mock_feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [12.48, 41.88]},
        "properties": {"value": 7.0},
    }

    fake_config = type("C", (), {
        "default_resolution": 10,
        "max_resolution": 20,
        "max_features_per_request": 1000,
    })()

    with patch.object(svc, "_get_plugin_config", new_callable=AsyncMock, return_value=fake_config), \
         patch.object(svc, "_fetch_features", new_callable=AsyncMock, return_value=[mock_feature]):
        request = MagicMock()
        result = await svc.get_dggs_data(
            catalog_id="cat1",
            collection_id="col1",
            request=request,
            zone_level=10,
            bbox=None,
            datetime=None,
            parameter_name=None,
            dggs_id="S2",
        )

    assert result.dggsId == "S2"
    assert result.zoneLevel == 10
    assert len(result.features) == 1
