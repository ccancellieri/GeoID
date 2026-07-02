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

import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional, Tuple

from dynastore.modules.db_config.query_executor import DbResource, DQLQuery, ResultHandler
from dynastore.modules.db_config.shared_queries import list_page_with_count
from .models import MovingFeature, MovingFeatureCreate, MovingFeatureUpdate, TemporalGeometry, TemporalGeometryCreate, TemporalGeometryUpdate

logger = logging.getLogger(__name__)


def _compute_bbox_wkt(coordinates: List[List[float]]) -> Optional[str]:
    """Compute bounding box WKT from coordinate array.
    
    Args:
        coordinates: List of [lon, lat] or [lon, lat, elev] coordinates
    
    Returns:
        WKT POLYGON string or None if no valid coordinates
    """
    if not coordinates:
        return None
    
    lons = [c[0] for c in coordinates if len(c) >= 2]
    lats = [c[1] for c in coordinates if len(c) >= 2]
    
    if not lons or not lats:
        return None
    
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)
    
    return f"POLYGON(({min_lon} {min_lat},{min_lon} {max_lat},{max_lon} {max_lat},{max_lon} {min_lat},{min_lon} {min_lat}))"

# ---------------------------------------------------------------------------
# Moving feature queries
# ---------------------------------------------------------------------------

_create_mf_query = DQLQuery(
    """
    INSERT INTO moving_features.moving_features
        (catalog_id, collection_id, feature_type, properties)
    VALUES (:catalog_id, :collection_id, :feature_type, :properties)
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_get_mf_query = DQLQuery(
    """
    SELECT * FROM moving_features.moving_features
    WHERE catalog_id = :catalog_id AND id = :mf_id;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_LIST_MF_SQL = """
    SELECT COUNT(*) OVER() AS total_count, *
    FROM moving_features.moving_features
    WHERE catalog_id = :catalog_id AND collection_id = :collection_id
    ORDER BY created_at DESC
    LIMIT :limit OFFSET :offset;
    """

_delete_mf_query = DQLQuery(
    """
    DELETE FROM moving_features.moving_features
    WHERE catalog_id = :catalog_id AND id = :mf_id;
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

_update_mf_query = DQLQuery(
    """
    UPDATE moving_features.moving_features
    SET properties = :properties, updated_at = NOW()
    WHERE catalog_id = :catalog_id AND id = :mf_id
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

# ---------------------------------------------------------------------------
# Temporal geometry queries
# ---------------------------------------------------------------------------

_create_tg_query = DQLQuery(
    """
    INSERT INTO moving_features.temporal_geometries
        (mf_id, catalog_id, datetimes, coordinates, bbox_geom, crs, trs, interpolation, properties)
    VALUES (:mf_id, :catalog_id, :datetimes, :coordinates, ST_GeomFromText(:bbox_wkt, 4326), :crs, :trs, :interpolation, :properties)
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_list_tg_query = DQLQuery(
    """
    SELECT * FROM moving_features.temporal_geometries
    WHERE catalog_id = :catalog_id AND mf_id = :mf_id
    ORDER BY created_at ASC;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

_delete_tg_by_mf_query = DQLQuery(
    "DELETE FROM moving_features.temporal_geometries WHERE catalog_id = :catalog_id AND mf_id = :mf_id;",
    result_handler=ResultHandler.ROWCOUNT,
)

_get_tg_query = DQLQuery(
    """
    SELECT * FROM moving_features.temporal_geometries
    WHERE catalog_id = :catalog_id AND id = :tg_id;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_update_tg_query = DQLQuery(
    """
    UPDATE moving_features.temporal_geometries
    SET 
        datetimes = COALESCE(:datetimes, datetimes),
        coordinates = COALESCE(:coordinates, coordinates),
        bbox_geom = CASE 
            WHEN :coordinates IS NOT NULL THEN ST_GeomFromText(:bbox_wkt, 4326)
            ELSE bbox_geom
        END,
        crs = COALESCE(:crs, crs),
        trs = COALESCE(:trs, trs),
        interpolation = COALESCE(:interpolation, interpolation),
        properties = COALESCE(:properties, properties)
    WHERE catalog_id = :catalog_id AND id = :tg_id
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_list_tg_from_query = DQLQuery(
    """
    SELECT * FROM moving_features.temporal_geometries
    WHERE catalog_id = :catalog_id AND mf_id = :mf_id
      AND EXISTS (
            SELECT 1 FROM unnest(datetimes) AS t
            WHERE t >= CAST(:dt_start AS timestamptz)
          )
    ORDER BY created_at ASC;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

_list_tg_until_query = DQLQuery(
    """
    SELECT * FROM moving_features.temporal_geometries
    WHERE catalog_id = :catalog_id AND mf_id = :mf_id
      AND EXISTS (
            SELECT 1 FROM unnest(datetimes) AS t
            WHERE t <= CAST(:dt_end AS timestamptz)
          )
    ORDER BY created_at ASC;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

_list_tg_between_query = DQLQuery(
    """
    SELECT * FROM moving_features.temporal_geometries
    WHERE catalog_id = :catalog_id AND mf_id = :mf_id
      AND EXISTS (
            SELECT 1 FROM unnest(datetimes) AS t
            WHERE t >= CAST(:dt_start AS timestamptz) AND t <= CAST(:dt_end AS timestamptz)
          )
    ORDER BY created_at ASC;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

_LIST_MF_BY_BBOX_SQL = """
    SELECT COUNT(*) OVER() AS total_count, sub.* FROM (
        SELECT DISTINCT mf.* FROM moving_features.moving_features mf
        JOIN moving_features.temporal_geometries tg ON mf.id = tg.mf_id AND mf.catalog_id = tg.catalog_id
        WHERE mf.catalog_id = :catalog_id AND mf.collection_id = :collection_id
          AND tg.bbox_geom IS NOT NULL
          AND tg.bbox_geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
    ) sub
    ORDER BY sub.created_at DESC
    LIMIT :limit OFFSET :offset;
    """

_LIST_MF_BY_GEOMETRY_SQL = """
    SELECT COUNT(*) OVER() AS total_count, sub.* FROM (
        SELECT DISTINCT mf.* FROM moving_features.moving_features mf
        JOIN moving_features.temporal_geometries tg ON mf.id = tg.mf_id AND mf.catalog_id = tg.catalog_id
        WHERE mf.catalog_id = :catalog_id AND mf.collection_id = :collection_id
          AND tg.bbox_geom IS NOT NULL
          AND ST_Intersects(tg.bbox_geom, ST_GeomFromText(:geometry_wkt, 4326))
    ) sub
    ORDER BY sub.created_at DESC
    LIMIT :limit OFFSET :offset;
    """


# ---------------------------------------------------------------------------
# Row helpers
# ---------------------------------------------------------------------------

def _mf_from_row(row: dict) -> Optional[MovingFeature]:
    if not row:
        return None
    if isinstance(row.get("properties"), str):
        row["properties"] = json.loads(row["properties"])
    return MovingFeature.model_validate(row)


def _tg_from_row(row: dict) -> Optional[TemporalGeometry]:
    if not row:
        return None
    if isinstance(row.get("coordinates"), str):
        row["coordinates"] = json.loads(row["coordinates"])
    if isinstance(row.get("properties"), str):
        row["properties"] = json.loads(row["properties"])
    # datetimes come back from asyncpg as a list of datetime objects; keep as-is.
    return TemporalGeometry.model_validate(row)


# ---------------------------------------------------------------------------
# Moving feature CRUD
# ---------------------------------------------------------------------------

async def create_moving_feature(
    conn: DbResource,
    catalog_id: str,
    collection_id: str,
    mf: MovingFeatureCreate,
) -> Optional[MovingFeature]:
    row = await _create_mf_query.execute(
        conn,
        catalog_id=catalog_id,
        collection_id=collection_id,
        feature_type=mf.feature_type,
        properties=json.dumps(mf.properties or {}),
    )
    return _mf_from_row(row) if row else None


async def get_moving_feature(
    conn: DbResource,
    catalog_id: str,
    mf_id: uuid.UUID,
) -> Optional[MovingFeature]:
    row = await _get_mf_query.execute(conn, catalog_id=catalog_id, mf_id=str(mf_id))
    return _mf_from_row(row) if row else None


async def list_moving_features(
    conn: DbResource,
    catalog_id: str,
    collection_id: str,
    limit: int = 100,
    offset: int = 0,
) -> Tuple[List[MovingFeature], int]:
    """Page moving features in a collection. Returns ``(features, total)``."""
    rows, total = await list_page_with_count(
        conn,
        _LIST_MF_SQL,
        {"catalog_id": catalog_id, "collection_id": collection_id},
        limit=limit,
        offset=offset,
    )
    features = [mf for mf in (_mf_from_row(r) for r in rows if r) if mf is not None]
    return features, total


async def list_moving_features_by_bbox(
    conn: DbResource,
    catalog_id: str,
    collection_id: str,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    limit: int = 100,
    offset: int = 0,
) -> Tuple[List[MovingFeature], int]:
    """Page moving features intersecting a bbox. Returns ``(features, total)``."""
    rows, total = await list_page_with_count(
        conn,
        _LIST_MF_BY_BBOX_SQL,
        {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "min_lon": min_lon,
            "min_lat": min_lat,
            "max_lon": max_lon,
            "max_lat": max_lat,
        },
        limit=limit,
        offset=offset,
    )
    features = [mf for mf in (_mf_from_row(r) for r in rows if r) if mf is not None]
    return features, total


async def list_moving_features_by_geometry(
    conn: DbResource,
    catalog_id: str,
    collection_id: str,
    geometry_wkt: str,
    limit: int = 100,
    offset: int = 0,
) -> Tuple[List[MovingFeature], int]:
    """Page moving features intersecting a geometry. Returns ``(features, total)``."""
    rows, total = await list_page_with_count(
        conn,
        _LIST_MF_BY_GEOMETRY_SQL,
        {"catalog_id": catalog_id, "collection_id": collection_id, "geometry_wkt": geometry_wkt},
        limit=limit,
        offset=offset,
    )
    features = [mf for mf in (_mf_from_row(r) for r in rows if r) if mf is not None]
    return features, total


async def delete_moving_feature(
    conn: DbResource,
    catalog_id: str,
    mf_id: uuid.UUID,
) -> bool:
    count = await _delete_mf_query.execute(conn, catalog_id=catalog_id, mf_id=str(mf_id))
    return count > 0


async def update_moving_feature(
    conn: DbResource,
    catalog_id: str,
    mf_id: uuid.UUID,
    mf_update: MovingFeatureUpdate,
) -> Optional[MovingFeature]:
    row = await _update_mf_query.execute(
        conn,
        catalog_id=catalog_id,
        mf_id=str(mf_id),
        properties=json.dumps(mf_update.properties),
    )
    return _mf_from_row(row) if row else None


# ---------------------------------------------------------------------------
# Temporal geometry CRUD
# ---------------------------------------------------------------------------

async def create_temporal_geometry(
    conn: DbResource,
    catalog_id: str,
    mf_id: uuid.UUID,
    tg: TemporalGeometryCreate,
) -> Optional[TemporalGeometry]:
    bbox_wkt = _compute_bbox_wkt(tg.coordinates)
    row = await _create_tg_query.execute(
        conn,
        mf_id=str(mf_id),
        catalog_id=catalog_id,
        datetimes=tg.datetimes,
        coordinates=json.dumps(tg.coordinates),
        bbox_wkt=bbox_wkt,
        crs=tg.crs,
        trs=tg.trs,
        interpolation=tg.interpolation.value,
        properties=json.dumps(tg.properties or {}),
    )
    return _tg_from_row(row) if row else None


async def list_temporal_geometries(
    conn: DbResource,
    catalog_id: str,
    mf_id: uuid.UUID,
    dt_start: Optional[datetime] = None,
    dt_end: Optional[datetime] = None,
) -> List[TemporalGeometry]:
    if dt_start is not None and dt_end is not None:
        rows = await _list_tg_between_query.execute(
            conn, catalog_id=catalog_id, mf_id=str(mf_id), dt_start=dt_start, dt_end=dt_end
        )
    elif dt_start is not None:
        rows = await _list_tg_from_query.execute(
            conn, catalog_id=catalog_id, mf_id=str(mf_id), dt_start=dt_start
        )
    elif dt_end is not None:
        rows = await _list_tg_until_query.execute(
            conn, catalog_id=catalog_id, mf_id=str(mf_id), dt_end=dt_end
        )
    else:
        rows = await _list_tg_query.execute(conn, catalog_id=catalog_id, mf_id=str(mf_id))
    return [tg for tg in (_tg_from_row(r) for r in rows if r) if tg is not None]


async def delete_temporal_geometries_by_mf(
    conn: DbResource,
    catalog_id: str,
    mf_id: uuid.UUID,
) -> None:
    await _delete_tg_by_mf_query.execute(conn, catalog_id=catalog_id, mf_id=str(mf_id))


async def get_temporal_geometry(
    conn: DbResource,
    catalog_id: str,
    tg_id: uuid.UUID,
) -> Optional[TemporalGeometry]:
    row = await _get_tg_query.execute(conn, catalog_id=catalog_id, tg_id=str(tg_id))
    return _tg_from_row(row) if row else None


async def update_temporal_geometry(
    conn: DbResource,
    catalog_id: str,
    tg_id: uuid.UUID,
    tg_update: TemporalGeometryUpdate,
) -> Optional[TemporalGeometry]:
    coordinates_json = json.dumps(tg_update.coordinates) if tg_update.coordinates else None
    bbox_wkt = _compute_bbox_wkt(tg_update.coordinates) if tg_update.coordinates else None
    
    row = await _update_tg_query.execute(
        conn,
        catalog_id=catalog_id,
        tg_id=str(tg_id),
        datetimes=tg_update.datetimes,
        coordinates=coordinates_json,
        bbox_wkt=bbox_wkt,
        crs=tg_update.crs,
        trs=tg_update.trs,
        interpolation=tg_update.interpolation.value if tg_update.interpolation else None,
        properties=json.dumps(tg_update.properties) if tg_update.properties else None,
    )
    return _tg_from_row(row) if row else None
