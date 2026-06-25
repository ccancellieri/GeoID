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

"""Unit tests for OGC API - Connected Systems extension.

Covers:
- Service structure: OGCServiceMixin membership, conformance URIs, prefix
- Route registration: all required endpoints present
- Protocol compliance: isinstance(svc, ConnectedSystemsProtocol)
- DB helpers: _system_from_row, _datastream_from_row, _observation_from_row
- DDL constants: expected table/column names present
"""
import inspect
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Service structure
# ---------------------------------------------------------------------------

def test_service_inherits_ogc_mixin():
    from dynastore.extensions.connected_systems.consys_service import ConnectedSystemsService
    from dynastore.extensions.ogc_base import OGCServiceMixin

    assert issubclass(ConnectedSystemsService, OGCServiceMixin)


def test_service_protocol_compliance():
    from dynastore.extensions.connected_systems.consys_service import ConnectedSystemsService
    from dynastore.models.protocols import ConnectedSystemsProtocol
    from fastapi import FastAPI

    svc = ConnectedSystemsService(app=FastAPI())
    assert isinstance(svc, ConnectedSystemsProtocol)


def test_service_conformance_uris():
    from dynastore.extensions.connected_systems.consys_service import (
        ConnectedSystemsService,
        OGC_CONNECTED_SYSTEMS_URIS,
    )

    assert ConnectedSystemsService.conformance_uris == OGC_CONNECTED_SYSTEMS_URIS
    assert any("connectedsystems" in u for u in ConnectedSystemsService.conformance_uris)
    expected = [
        "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/core",
        "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/system-features",
        "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/datastreams",
        "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/observations",
    ]
    for uri in expected:
        assert uri in ConnectedSystemsService.conformance_uris, f"Missing conformance URI: {uri}"


def test_service_prefix():
    from dynastore.extensions.connected_systems.consys_service import ConnectedSystemsService

    assert ConnectedSystemsService.prefix == "/consys"


def test_service_init_assigns_app():
    from fastapi import FastAPI
    from dynastore.extensions.connected_systems.consys_service import ConnectedSystemsService

    app = FastAPI()
    svc = ConnectedSystemsService(app=app)
    assert svc.app is app


def test_service_init_app_none():
    from dynastore.extensions.connected_systems.consys_service import ConnectedSystemsService

    svc = ConnectedSystemsService()
    assert svc.app is None


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------

def test_router_has_landing_page():
    from dynastore.extensions.connected_systems.consys_service import ConnectedSystemsService

    svc = ConnectedSystemsService()
    routes = {r.path for r in svc.router.routes}
    assert "/consys/" in routes


def test_router_has_conformance():
    from dynastore.extensions.connected_systems.consys_service import ConnectedSystemsService

    svc = ConnectedSystemsService()
    routes = {r.path for r in svc.router.routes}
    assert "/consys/conformance" in routes


def test_router_has_systems_endpoints():
    from dynastore.extensions.connected_systems.consys_service import ConnectedSystemsService

    svc = ConnectedSystemsService()
    routes = {r.path for r in svc.router.routes}
    assert "/consys/systems" in routes
    assert "/consys/systems/{system_id}" in routes
    assert "/consys/systems/{system_id}/deployments" in routes
    assert "/consys/systems/{system_id}/datastreams" in routes


def test_router_has_datastreams_endpoints():
    from dynastore.extensions.connected_systems.consys_service import ConnectedSystemsService

    svc = ConnectedSystemsService()
    routes = {r.path for r in svc.router.routes}
    assert "/consys/datastreams" in routes
    assert "/consys/datastreams/{datastream_id}" in routes
    assert "/consys/datastreams/{datastream_id}/observations" in routes


def test_router_post_systems():
    from dynastore.extensions.connected_systems.consys_service import ConnectedSystemsService

    svc = ConnectedSystemsService()
    post_routes = {r.path for r in svc.router.routes if "POST" in getattr(r, "methods", set())}
    assert "/consys/systems" in post_routes


def test_router_post_observations():
    from dynastore.extensions.connected_systems.consys_service import ConnectedSystemsService

    svc = ConnectedSystemsService()
    post_routes = {r.path for r in svc.router.routes if "POST" in getattr(r, "methods", set())}
    assert "/consys/datastreams/{datastream_id}/observations" in post_routes


# ---------------------------------------------------------------------------
# DB row helpers
# ---------------------------------------------------------------------------

def _make_system_row():
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "catalog_id": "cat1",
        "system_id": "station-001",
        "name": "Weather Station 001",
        "description": "Agricultural weather station",
        "type": "Sensor",
        "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
        "properties": {"manufacturer": "Davis"},
        "stac_collection_id": None,
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }


def _make_datastream_row():
    return {
        "id": "00000000-0000-0000-0000-000000000002",
        "catalog_id": "cat1",
        "datastream_id": "ds-temp-001",
        "system_id": "00000000-0000-0000-0000-000000000001",
        "name": "Temperature",
        "description": "Air temperature sensor",
        "observed_property": "temperature",
        "unit_of_measurement": "degC",
        "properties": {},
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }


def _make_observation_row():
    return {
        "id": "00000000-0000-0000-0000-000000000003",
        "catalog_id": "cat1",
        "datastream_id": "00000000-0000-0000-0000-000000000002",
        "phenomenon_time": "2025-01-15T12:00:00Z",
        "result_time": "2025-01-15T12:00:01Z",
        "result_value": 23.5,
        "result_quality": 0.98,
        "parameters": {},
    }


def test_system_from_row_basic():
    from dynastore.modules.connected_systems.db import _system_from_row

    system = _system_from_row(_make_system_row())
    assert system is not None
    assert system.system_id == "station-001"
    assert system.catalog_id == "cat1"
    assert system.name == "Weather Station 001"
    assert system.type == "Sensor"


def test_system_from_row_self_link():
    from dynastore.modules.connected_systems.db import _system_from_row

    system = _system_from_row(_make_system_row(), root_url="http://example.com")
    assert any(
        lnk.rel == "self" and "station-001" in lnk.href
        for lnk in system.links
    )


def test_system_from_row_none_on_empty():
    from dynastore.modules.connected_systems.db import _system_from_row

    assert _system_from_row({}) is None
    assert _system_from_row(None) is None  # type: ignore[arg-type]


def test_datastream_from_row_basic():
    from dynastore.modules.connected_systems.db import _datastream_from_row

    ds = _datastream_from_row(_make_datastream_row())
    assert ds is not None
    assert ds.datastream_id == "ds-temp-001"
    assert ds.observed_property == "temperature"
    assert ds.unit_of_measurement == "degC"


def test_datastream_from_row_self_link():
    from dynastore.modules.connected_systems.db import _datastream_from_row

    ds = _datastream_from_row(_make_datastream_row(), root_url="http://ex")
    assert any("ds-temp-001" in lnk.href for lnk in ds.links)


def test_observation_from_row_basic():
    from dynastore.modules.connected_systems.db import _observation_from_row

    obs = _observation_from_row(_make_observation_row())
    assert obs is not None
    assert obs.result_value == 23.5
    assert obs.result_quality == pytest.approx(0.98)


# ---------------------------------------------------------------------------
# DDL constants
# ---------------------------------------------------------------------------

def test_ddl_contains_systems_table():
    from dynastore.modules.connected_systems.ddl import CONSYS_SYSTEMS_DDL

    assert "consys.systems" in CONSYS_SYSTEMS_DDL
    assert "catalog_id" in CONSYS_SYSTEMS_DDL
    assert "system_id" in CONSYS_SYSTEMS_DDL
    assert "PARTITION BY LIST" in CONSYS_SYSTEMS_DDL
    assert "GEOMETRY" in CONSYS_SYSTEMS_DDL


def test_ddl_contains_deployments_table():
    from dynastore.modules.connected_systems.ddl import CONSYS_DEPLOYMENTS_DDL

    assert "consys.deployments" in CONSYS_DEPLOYMENTS_DDL
    assert "system_id" in CONSYS_DEPLOYMENTS_DDL
    assert "time_start" in CONSYS_DEPLOYMENTS_DDL


def test_ddl_contains_datastreams_table():
    from dynastore.modules.connected_systems.ddl import CONSYS_DATASTREAMS_DDL

    assert "consys.datastreams" in CONSYS_DATASTREAMS_DDL
    assert "observed_property" in CONSYS_DATASTREAMS_DDL
    assert "unit_of_measurement" in CONSYS_DATASTREAMS_DDL


def test_ddl_contains_observations_table():
    from dynastore.modules.connected_systems.ddl import CONSYS_OBSERVATIONS_DDL

    assert "consys.observations" in CONSYS_OBSERVATIONS_DDL
    assert "phenomenon_time" in CONSYS_OBSERVATIONS_DDL
    assert "result_value" in CONSYS_OBSERVATIONS_DDL


def test_observations_brin_index_ddl():
    from dynastore.modules.connected_systems.ddl import CONSYS_OBSERVATIONS_IDX_DDL

    assert "BRIN" in CONSYS_OBSERVATIONS_IDX_DDL
    assert "phenomenon_time" in CONSYS_OBSERVATIONS_IDX_DDL


def test_systems_geometry_gist_index_ddl():
    from dynastore.modules.connected_systems.ddl import CONSYS_SYSTEMS_GEOM_IDX_DDL

    assert "systems_geometry_idx" in CONSYS_SYSTEMS_GEOM_IDX_DDL
    assert "GIST" in CONSYS_SYSTEMS_GEOM_IDX_DDL
    assert "geometry" in CONSYS_SYSTEMS_GEOM_IDX_DDL


def test_deployments_geometry_gist_index_ddl():
    from dynastore.modules.connected_systems.ddl import CONSYS_DEPLOYMENTS_GEOM_IDX_DDL

    assert "deployments_geometry_idx" in CONSYS_DEPLOYMENTS_GEOM_IDX_DDL
    assert "GIST" in CONSYS_DEPLOYMENTS_GEOM_IDX_DDL
    assert "geometry" in CONSYS_DEPLOYMENTS_GEOM_IDX_DDL


# ---------------------------------------------------------------------------
# DB functions — async stubs (no live DB)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_systems_calls_query():
    from dynastore.modules.connected_systems import db as consys_db

    conn = AsyncMock()
    with patch.object(
        consys_db._list_systems_query, "execute", new_callable=AsyncMock, return_value=[]
    ):
        result = await consys_db.list_systems(conn, "cat1", limit=10, offset=0)
    assert result == []


@pytest.mark.asyncio
async def test_list_datastreams_calls_query():
    from dynastore.modules.connected_systems import db as consys_db

    conn = AsyncMock()
    with patch.object(
        consys_db._list_datastreams_query, "execute", new_callable=AsyncMock, return_value=[]
    ):
        result = await consys_db.list_datastreams(conn, "cat1", limit=10, offset=0)
    assert result == []


@pytest.mark.asyncio
async def test_list_observations_calls_query():
    from dynastore.modules.connected_systems import db as consys_db

    conn = AsyncMock()
    with patch(
        "dynastore.modules.connected_systems.db.DQLQuery.from_builder"
    ) as mock_builder:
        mock_executor = AsyncMock()
        mock_executor.execute = AsyncMock(return_value=[])
        mock_builder.return_value = mock_executor
        
        result = await consys_db.list_observations(conn, "cat1", "ds-001", limit=10, offset=0)
        assert result == []
        mock_builder.assert_called_once()


@pytest.mark.asyncio
async def test_list_observations_with_datetime_filter():
    from dynastore.modules.connected_systems import db as consys_db
    from datetime import datetime

    conn = AsyncMock()
    with patch(
        "dynastore.modules.connected_systems.db.DQLQuery.from_builder"
    ) as mock_builder:
        mock_executor = AsyncMock()
        mock_executor.execute = AsyncMock(return_value=[])
        mock_builder.return_value = mock_executor
        
        result = await consys_db.list_observations(
            conn, "cat1", "ds-001", limit=10, offset=0, datetime="2025-06-01/2025-06-25"
        )
        assert result == []
        mock_builder.assert_called_once()


@pytest.mark.asyncio
async def test_list_observations_with_open_interval():
    from dynastore.modules.connected_systems import db as consys_db

    conn = AsyncMock()
    with patch(
        "dynastore.modules.connected_systems.db.DQLQuery.from_builder"
    ) as mock_builder:
        mock_executor = AsyncMock()
        mock_executor.execute = AsyncMock(return_value=[])
        mock_builder.return_value = mock_executor
        
        result = await consys_db.list_observations(
            conn, "cat1", "ds-001", limit=10, offset=0, datetime="../2025-06-25"
        )
        assert result == []
        mock_builder.assert_called_once()


@pytest.mark.asyncio
async def test_list_systems_with_bbox():
    from dynastore.modules.connected_systems import db as consys_db
    from dynastore.modules.db_config.query_executor import DQLQuery

    conn = AsyncMock()
    bbox = (10.0, 40.0, 15.0, 45.0)
    
    with patch.object(
        DQLQuery, "__init__", return_value=None
    ) as mock_init, patch.object(
        DQLQuery, "execute", new_callable=AsyncMock, return_value=[]
    ) as mock_execute:
        result = await consys_db.list_systems(conn, "cat1", limit=10, offset=0, bbox=bbox)
        
        mock_init.assert_called_once()
        call_args = str(mock_init.call_args)
        assert "ST_Intersects" in call_args
        assert "ST_MakeEnvelope" in call_args
        mock_execute.assert_called_once()
    
    assert result == []


@pytest.mark.asyncio
async def test_list_observations_with_bbox():
    from dynastore.modules.connected_systems import db as consys_db
    from dynastore.modules.db_config.query_executor import DQLQuery

    conn = AsyncMock()
    bbox = (10.0, 40.0, 15.0, 45.0)
    
    with patch.object(
        DQLQuery, "__init__", return_value=None
    ) as mock_init, patch.object(
        DQLQuery, "execute", new_callable=AsyncMock, return_value=[]
    ) as mock_execute:
        result = await consys_db.list_observations(
            conn, "cat1", "ds-001", limit=10, offset=0, bbox=bbox
        )
        
        mock_init.assert_called_once()
        call_args = str(mock_init.call_args)
        assert "ST_Intersects" in call_args
        assert "ST_MakeEnvelope" in call_args
        assert "consys.systems" in call_args
        mock_execute.assert_called_once()
    
    assert result == []


@pytest.mark.asyncio
async def test_get_system_returns_none_on_miss():
    from dynastore.modules.connected_systems import db as consys_db

    conn = AsyncMock()
    with patch.object(
        consys_db._get_system_query, "execute", new_callable=AsyncMock, return_value=None
    ):
        result = await consys_db.get_system(conn, "cat1", "nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_get_datastream_returns_none_on_miss():
    from dynastore.modules.connected_systems import db as consys_db

    conn = AsyncMock()
    with patch.object(
        consys_db._get_datastream_query, "execute", new_callable=AsyncMock, return_value=None
    ):
        result = await consys_db.get_datastream(conn, "cat1", "nonexistent")
    assert result is None


# ---------------------------------------------------------------------------
# Models — basic Pydantic validation
# ---------------------------------------------------------------------------

def test_system_create_model():
    from dynastore.modules.connected_systems.models import SystemCreate

    s = SystemCreate(system_id="s1", name="Station 1")
    assert s.type == "Sensor"
    assert s.geometry is None


def test_system_create_with_geometry():
    from dynastore.modules.connected_systems.models import SystemCreate

    s = SystemCreate(
        system_id="s1",
        name="Station 1",
        geometry={"type": "Point", "coordinates": [12.0, 41.0]},
    )
    assert s.geometry["type"] == "Point"


def test_observation_create_quality_bounds():
    from dynastore.modules.connected_systems.models import ObservationCreate
    from datetime import datetime, timezone

    obs = ObservationCreate(
        phenomenon_time=datetime.now(timezone.utc),
        result_value=25.0,
        result_quality=0.95,
    )
    assert obs.result_quality == pytest.approx(0.95)


def test_observation_create_invalid_quality():
    from dynastore.modules.connected_systems.models import ObservationCreate
    from datetime import datetime, timezone
    import pydantic

    with pytest.raises(pydantic.ValidationError):
        ObservationCreate(
            phenomenon_time=datetime.now(timezone.utc),
            result_value=25.0,
            result_quality=1.5,  # > 1.0 is invalid
        )


def test_datastream_create_model():
    from dynastore.modules.connected_systems.models import DataStreamCreate
    import uuid

    ds = DataStreamCreate(
        datastream_id="ds-rain-001",
        name="Rainfall",
        observed_property="precipitation",
        unit_of_measurement="mm",
        system_id=uuid.uuid4(),
    )
    assert ds.observed_property == "precipitation"
