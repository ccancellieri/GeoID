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

"""CRUD operations for OGC API - Connected Systems resources.

All public functions accept a ``DbResource`` (AsyncConnection | AsyncEngine)
and return typed Pydantic models. Follows the DQLQuery singleton pattern
from modules/styles/db.py.
"""

import json
import logging
import uuid
from typing import List, Optional, Tuple

from dateutil.parser import isoparse
from sqlalchemy import text

from dynastore.models.shared_models import Link
from dynastore.modules.db_config.query_executor import DbResource, DQLQuery, ResultHandler
from dynastore.modules.db_config.shared_queries import list_page_with_count

from .models import (
    DataStream,
    DataStreamCreate,
    Deployment,
    DeploymentCreate,
    Observation,
    ObservationCreate,
    System,
    SystemCreate,
    SystemUpdate,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Query singletons — Systems
# ---------------------------------------------------------------------------

_create_system_query = DQLQuery(
    """
    INSERT INTO consys.systems
        (catalog_id, system_id, name, description, type, geometry, properties, stac_collection_id)
    VALUES
        (:catalog_id, :system_id, :name, :description, :type,
         ST_GeomFromGeoJSON(:geometry),
         CAST(:properties AS jsonb), :stac_collection_id)
    RETURNING id, catalog_id, system_id, name, description, type,
              ST_AsGeoJSON(geometry)::jsonb AS geometry,
              properties, stac_collection_id, created_at, updated_at;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_get_system_query = DQLQuery(
    """
    SELECT id, catalog_id, system_id, name, description, type,
           ST_AsGeoJSON(geometry)::jsonb AS geometry,
           properties, stac_collection_id, created_at, updated_at
    FROM consys.systems
    WHERE catalog_id = :catalog_id AND system_id = :system_id;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_LIST_SYSTEMS_SQL = """
    SELECT COUNT(*) OVER() AS total_count,
           id, catalog_id, system_id, name, description, type,
           ST_AsGeoJSON(geometry)::jsonb AS geometry,
           properties, stac_collection_id, created_at, updated_at
    FROM consys.systems
    WHERE catalog_id = :catalog_id
    ORDER BY system_id
    LIMIT :limit OFFSET :offset;
    """

_LIST_SYSTEMS_BY_BBOX_SQL = """
    SELECT COUNT(*) OVER() AS total_count,
           id, catalog_id, system_id, name, description, type,
           ST_AsGeoJSON(geometry)::jsonb AS geometry,
           properties, stac_collection_id, created_at, updated_at
    FROM consys.systems
    WHERE catalog_id = :catalog_id
      AND geometry IS NOT NULL
      AND ST_Intersects(geometry, ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326))
    ORDER BY system_id
    LIMIT :limit OFFSET :offset;
    """

_delete_system_query = DQLQuery(
    "DELETE FROM consys.systems WHERE catalog_id = :catalog_id AND id = :system_uuid;",
    result_handler=ResultHandler.ROWCOUNT,
)

# ---------------------------------------------------------------------------
# Query singletons — Deployments
# ---------------------------------------------------------------------------

_create_deployment_query = DQLQuery(
    """
    INSERT INTO consys.deployments
        (catalog_id, system_id, name, description, time_start, time_end, geometry, properties)
    VALUES
        (:catalog_id, :system_id, :name, :description,
         :time_start, :time_end,
         ST_GeomFromGeoJSON(:geometry),
         CAST(:properties AS jsonb))
    RETURNING id, catalog_id, system_id, name, description, time_start, time_end,
              ST_AsGeoJSON(geometry)::jsonb AS geometry,
              properties, created_at;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_list_deployments_by_system_query = DQLQuery(
    """
    SELECT d.id, d.catalog_id, d.system_id, d.name, d.description,
           d.time_start, d.time_end,
           ST_AsGeoJSON(d.geometry)::jsonb AS geometry,
           d.properties, d.created_at
    FROM consys.deployments d
    JOIN consys.systems s ON s.id = d.system_id AND s.catalog_id = d.catalog_id
    WHERE s.catalog_id = :catalog_id AND s.system_id = :system_id
    ORDER BY d.time_start DESC
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

# ---------------------------------------------------------------------------
# Query singletons — DataStreams
# ---------------------------------------------------------------------------

_create_datastream_query = DQLQuery(
    """
    INSERT INTO consys.datastreams
        (catalog_id, datastream_id, system_id, name, description,
         observed_property, unit_of_measurement, properties)
    VALUES
        (:catalog_id, :datastream_id, :system_id, :name, :description,
         :observed_property, :unit_of_measurement, CAST(:properties AS jsonb))
    RETURNING id, catalog_id, datastream_id, system_id, name, description,
              observed_property, unit_of_measurement, properties, created_at, updated_at;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_get_datastream_query = DQLQuery(
    """
    SELECT id, catalog_id, datastream_id, system_id, name, description,
           observed_property, unit_of_measurement, properties, created_at, updated_at
    FROM consys.datastreams
    WHERE catalog_id = :catalog_id AND datastream_id = :datastream_id;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_list_datastreams_query = DQLQuery(
    """
    SELECT id, catalog_id, datastream_id, system_id, name, description,
           observed_property, unit_of_measurement, properties, created_at, updated_at
    FROM consys.datastreams
    WHERE catalog_id = :catalog_id
    ORDER BY datastream_id
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

_list_datastreams_by_system_query = DQLQuery(
    """
    SELECT ds.id, ds.catalog_id, ds.datastream_id, ds.system_id, ds.name, ds.description,
           ds.observed_property, ds.unit_of_measurement, ds.properties,
           ds.created_at, ds.updated_at
    FROM consys.datastreams ds
    JOIN consys.systems s ON s.id = ds.system_id AND s.catalog_id = ds.catalog_id
    WHERE s.catalog_id = :catalog_id AND s.system_id = :system_id
    ORDER BY ds.datastream_id
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

# ---------------------------------------------------------------------------
# Query singletons — Observations
# ---------------------------------------------------------------------------

_create_observation_query = DQLQuery(
    """
    INSERT INTO consys.observations
        (catalog_id, datastream_id, phenomenon_time, result_value, result_quality, parameters)
    VALUES
        (:catalog_id, :datastream_id, :phenomenon_time,
         :result_value, :result_quality, CAST(:parameters AS jsonb))
    RETURNING id, catalog_id, datastream_id, phenomenon_time, result_time,
              result_value, result_quality, parameters;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

def _build_observations_sql(
    datetime_str: Optional[str],
    bbox: Optional[Tuple[float, float, float, float]],
) -> Tuple[str, dict]:
    """Build the WHERE clause and extra bind params for an observations list query.

    Returns ``(sql_string, extra_params)`` where the SQL string embeds all
    optional filter clauses so it can be passed directly to ``DQLQuery``.
    The base params (catalog_id, datastream_id, limit, offset) are NOT included
    in ``extra_params`` — the caller merges them before calling ``execute``.
    """
    conditions: List[str] = [
        "ds.catalog_id = :catalog_id",
        "ds.datastream_id = :datastream_id",
    ]
    extra: dict = {}
    needs_systems_join = False

    if datetime_str:
        if "/" in datetime_str:
            start_str, end_str = datetime_str.split("/", 1)
            start_dt = isoparse(start_str) if start_str != ".." else None
            end_dt = isoparse(end_str) if end_str != ".." else None
            if start_dt and end_dt:
                conditions.append("o.phenomenon_time >= :start_dt")
                conditions.append("o.phenomenon_time <= :end_dt")
                extra["start_dt"] = start_dt
                extra["end_dt"] = end_dt
            elif start_dt:
                conditions.append("o.phenomenon_time >= :start_dt")
                extra["start_dt"] = start_dt
            elif end_dt:
                conditions.append("o.phenomenon_time <= :end_dt")
                extra["end_dt"] = end_dt
        else:
            dt = isoparse(datetime_str)
            conditions.append("o.phenomenon_time = :dt")
            extra["dt"] = dt

    if bbox is not None:
        xmin, ymin, xmax, ymax = bbox
        conditions.append("s.geometry IS NOT NULL")
        conditions.append(
            "ST_Intersects(s.geometry, ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326))"
        )
        extra["xmin"] = xmin
        extra["ymin"] = ymin
        extra["xmax"] = xmax
        extra["ymax"] = ymax
        needs_systems_join = True

    where_clause = " AND ".join(conditions)
    systems_join = (
        "JOIN consys.systems s ON s.id = ds.system_id AND s.catalog_id = ds.catalog_id"
        if needs_systems_join
        else ""
    )

    sql = (
        f"""
        SELECT o.id, o.catalog_id, o.datastream_id, o.phenomenon_time, o.result_time,
               o.result_value, o.result_quality, o.parameters
        FROM consys.observations o
        JOIN consys.datastreams ds ON ds.id = o.datastream_id AND ds.catalog_id = o.catalog_id
        {systems_join}
        WHERE {where_clause}
        ORDER BY o.phenomenon_time DESC
        LIMIT :limit OFFSET :offset;
        """
    )
    return sql, extra


# ---------------------------------------------------------------------------
# Row-to-model helpers
# ---------------------------------------------------------------------------

def _system_from_row(row: dict, root_url: str = "") -> Optional[System]:
    if not row:
        return None
    row = dict(row)
    row["links"] = [
        Link(
            href=f"{root_url}/consys/systems/{row['system_id']}",
            rel="self",
            type="application/json",
        )
    ]
    return System.model_validate(row)


def _deployment_from_row(row: dict, root_url: str = "") -> Optional[Deployment]:
    if not row:
        return None
    row = dict(row)
    row["links"] = []
    return Deployment.model_validate(row)


def _datastream_from_row(row: dict, root_url: str = "") -> Optional[DataStream]:
    if not row:
        return None
    row = dict(row)
    row["links"] = [
        Link(
            href=f"{root_url}/consys/datastreams/{row['datastream_id']}",
            rel="self",
            type="application/json",
        )
    ]
    return DataStream.model_validate(row)


def _observation_from_row(row: dict, root_url: str = "") -> Optional[Observation]:
    if not row:
        return None
    row = dict(row)
    row["links"] = []
    return Observation.model_validate(row)


# ---------------------------------------------------------------------------
# Public CRUD — Systems
# ---------------------------------------------------------------------------

async def create_system(
    conn: DbResource, catalog_id: str, data: SystemCreate
) -> Optional[System]:
    params = {
        "catalog_id": catalog_id,
        "system_id": data.system_id,
        "name": data.name,
        "description": data.description,
        "type": data.type,
        "geometry": json.dumps(data.geometry) if data.geometry else None,
        "properties": json.dumps(data.properties or {}),
        "stac_collection_id": data.stac_collection_id,
    }
    row = await _create_system_query.execute(conn, **params)
    return _system_from_row(row) if row else None


async def get_system(
    conn: DbResource, catalog_id: str, system_id: str
) -> Optional[System]:
    row = await _get_system_query.execute(
        conn, catalog_id=catalog_id, system_id=system_id
    )
    return _system_from_row(row) if row else None


async def list_systems(
    conn: DbResource,
    catalog_id: str,
    limit: int = 100,
    offset: int = 0,
    bbox: Optional[Tuple[float, float, float, float]] = None,
) -> Tuple[List[System], int]:
    """Page systems for a catalog. Returns ``(systems, total)``."""
    if bbox is not None:
        xmin, ymin, xmax, ymax = bbox
        rows, total = await list_page_with_count(
            conn,
            _LIST_SYSTEMS_BY_BBOX_SQL,
            {"catalog_id": catalog_id, "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax},
            limit=limit,
            offset=offset,
        )
    else:
        rows, total = await list_page_with_count(
            conn, _LIST_SYSTEMS_SQL, {"catalog_id": catalog_id}, limit=limit, offset=offset
        )
    systems = [s for r in rows if (s := _system_from_row(r)) is not None]
    return systems, total


async def update_system(
    conn: DbResource, catalog_id: str, system_id: str, data: SystemUpdate
) -> Optional[System]:
    update_values = data.model_dump(exclude_unset=True)
    if not update_values:
        return await get_system(conn, catalog_id, system_id)

    async def _builder(db_resource, raw_params):
        set_parts = []
        for k in raw_params:
            if k in ("catalog_id", "system_id"):
                continue
            if k == "geometry":
                set_parts.append("geometry = ST_GeomFromGeoJSON(:geometry)")
            elif k == "properties":
                set_parts.append('"properties" = CAST(:properties AS jsonb)')
            else:
                set_parts.append(f'"{k}" = :{k}')
        set_clause = ", ".join(set_parts)
        sql = text(
            f"""
            UPDATE consys.systems
            SET {set_clause}, updated_at = NOW()
            WHERE catalog_id = :catalog_id AND system_id = :system_id
            RETURNING id, catalog_id, system_id, name, description, type,
                      ST_AsGeoJSON(geometry)::jsonb AS geometry,
                      properties, stac_collection_id, created_at, updated_at;
            """
        )
        params = dict(raw_params)
        if "geometry" in params and params["geometry"] is not None:
            params["geometry"] = json.dumps(params["geometry"])
        if "properties" in params and params["properties"] is not None:
            params["properties"] = json.dumps(params["properties"])
        return sql, params

    executor = DQLQuery.from_builder(_builder, result_handler=ResultHandler.ONE_DICT)
    row = await executor.execute(
        conn, catalog_id=catalog_id, system_id=system_id, **update_values
    )
    return _system_from_row(row) if row else None


async def delete_system(
    conn: DbResource, catalog_id: str, system_uuid: uuid.UUID
) -> bool:
    affected = await _delete_system_query.execute(
        conn, catalog_id=catalog_id, system_uuid=system_uuid
    )
    return (affected or 0) > 0


# ---------------------------------------------------------------------------
# Public CRUD — Deployments
# ---------------------------------------------------------------------------

async def create_deployment(
    conn: DbResource,
    catalog_id: str,
    system_uuid: uuid.UUID,
    data: DeploymentCreate,
) -> Optional[Deployment]:

    params = {
        "catalog_id": catalog_id,
        "system_id": str(system_uuid),
        "name": data.name,
        "description": data.description,
        "time_start": data.time_start,
        "time_end": data.time_end,
        "geometry": json.dumps(data.geometry) if data.geometry else None,
        "properties": json.dumps(data.properties or {}),
    }
    row = await _create_deployment_query.execute(conn, **params)
    return _deployment_from_row(row) if row else None


async def list_deployments_for_system(
    conn: DbResource,
    catalog_id: str,
    system_id: str,
    limit: int = 100,
    offset: int = 0,
) -> List[Deployment]:
    rows = await _list_deployments_by_system_query.execute(
        conn, catalog_id=catalog_id, system_id=system_id, limit=limit, offset=offset
    )
    return [d for r in rows if (d := _deployment_from_row(r)) is not None]


# ---------------------------------------------------------------------------
# Public CRUD — DataStreams
# ---------------------------------------------------------------------------

async def create_datastream(
    conn: DbResource, catalog_id: str, data: DataStreamCreate
) -> Optional[DataStream]:
    params = {
        "catalog_id": catalog_id,
        "datastream_id": data.datastream_id,
        "system_id": str(data.system_id),
        "name": data.name,
        "description": data.description,
        "observed_property": data.observed_property,
        "unit_of_measurement": data.unit_of_measurement,
        "properties": json.dumps(data.properties or {}),
    }
    row = await _create_datastream_query.execute(conn, **params)
    return _datastream_from_row(row) if row else None


async def get_datastream(
    conn: DbResource, catalog_id: str, datastream_id: str
) -> Optional[DataStream]:
    row = await _get_datastream_query.execute(
        conn, catalog_id=catalog_id, datastream_id=datastream_id
    )
    return _datastream_from_row(row) if row else None


async def list_datastreams(
    conn: DbResource,
    catalog_id: str,
    limit: int = 100,
    offset: int = 0,
) -> List[DataStream]:
    rows = await _list_datastreams_query.execute(
        conn, catalog_id=catalog_id, limit=limit, offset=offset
    )
    return [d for r in rows if (d := _datastream_from_row(r)) is not None]


async def list_datastreams_for_system(
    conn: DbResource,
    catalog_id: str,
    system_id: str,
    limit: int = 100,
    offset: int = 0,
) -> List[DataStream]:
    rows = await _list_datastreams_by_system_query.execute(
        conn, catalog_id=catalog_id, system_id=system_id, limit=limit, offset=offset
    )
    return [d for r in rows if (d := _datastream_from_row(r)) is not None]


# ---------------------------------------------------------------------------
# Public CRUD — Observations
# ---------------------------------------------------------------------------

async def create_observation(
    conn: DbResource,
    catalog_id: str,
    datastream_uuid: uuid.UUID,
    data: ObservationCreate,
) -> Optional[Observation]:
    params = {
        "catalog_id": catalog_id,
        "datastream_id": str(datastream_uuid),
        "phenomenon_time": data.phenomenon_time,
        "result_value": data.result_value,
        "result_quality": data.result_quality,
        "parameters": json.dumps(data.parameters or {}),
    }
    row = await _create_observation_query.execute(conn, **params)
    return _observation_from_row(row) if row else None


async def list_observations(
    conn: DbResource,
    catalog_id: str,
    datastream_id: str,
    limit: int = 100,
    offset: int = 0,
    datetime: Optional[str] = None,
    bbox: Optional[Tuple[float, float, float, float]] = None,
) -> List[Observation]:
    sql, extra_params = _build_observations_sql(datetime, bbox)
    query = DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS)
    rows = await query.execute(
        conn,
        catalog_id=catalog_id,
        datastream_id=datastream_id,
        limit=limit,
        offset=offset,
        **extra_params,
    )
    return [o for r in rows if (o := _observation_from_row(r)) is not None]
