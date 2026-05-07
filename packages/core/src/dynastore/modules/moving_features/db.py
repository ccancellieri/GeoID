#    Copyright 2025 FAO
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
from typing import List, Optional

from dynastore.modules.db_config.query_executor import DbResource, DQLQuery, ResultHandler
from .models import MovingFeature, MovingFeatureCreate, TemporalGeometry, TemporalGeometryCreate

logger = logging.getLogger(__name__)

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

_list_mf_query = DQLQuery(
    """
    SELECT * FROM moving_features.moving_features
    WHERE catalog_id = :catalog_id AND collection_id = :collection_id
    ORDER BY created_at DESC
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

_delete_mf_query = DQLQuery(
    """
    DELETE FROM moving_features.moving_features
    WHERE catalog_id = :catalog_id AND id = :mf_id;
    """,
    result_handler=ResultHandler.ROWCOUNT,
)

# ---------------------------------------------------------------------------
# Temporal geometry queries
# ---------------------------------------------------------------------------

_create_tg_query = DQLQuery(
    """
    INSERT INTO moving_features.temporal_geometries
        (mf_id, catalog_id, datetimes, coordinates, crs, trs, interpolation, properties)
    VALUES (:mf_id, :catalog_id, :datetimes, :coordinates, :crs, :trs, :interpolation, :properties)
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

_list_tg_from_query = DQLQuery(
    """
    SELECT * FROM moving_features.temporal_geometries
    WHERE catalog_id = :catalog_id AND mf_id = :mf_id
      AND EXISTS (
            SELECT 1 FROM unnest(datetimes) AS t
            WHERE t >= :dt_start::timestamptz
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
            WHERE t <= :dt_end::timestamptz
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
            WHERE t >= :dt_start::timestamptz AND t <= :dt_end::timestamptz
          )
    ORDER BY created_at ASC;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)


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
) -> List[MovingFeature]:
    rows = await _list_mf_query.execute(
        conn,
        catalog_id=catalog_id,
        collection_id=collection_id,
        limit=limit,
        offset=offset,
    )
    return [mf for mf in (_mf_from_row(r) for r in rows if r) if mf is not None]


async def delete_moving_feature(
    conn: DbResource,
    catalog_id: str,
    mf_id: uuid.UUID,
) -> bool:
    count = await _delete_mf_query.execute(conn, catalog_id=catalog_id, mf_id=str(mf_id))
    return count > 0


# ---------------------------------------------------------------------------
# Temporal geometry CRUD
# ---------------------------------------------------------------------------

async def create_temporal_geometry(
    conn: DbResource,
    catalog_id: str,
    mf_id: uuid.UUID,
    tg: TemporalGeometryCreate,
) -> Optional[TemporalGeometry]:
    row = await _create_tg_query.execute(
        conn,
        mf_id=str(mf_id),
        catalog_id=catalog_id,
        datetimes=tg.datetimes,
        coordinates=json.dumps(tg.coordinates),
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
