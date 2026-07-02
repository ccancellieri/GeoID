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

# dynastore/modules/db_config/shared_queries.py

import logging
from typing import Dict, Any, Optional, Tuple, Set, List
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    GeoDQLQuery,
    ResultHandler,
    DbResource,
)
from dateutil.parser import isoparse

logger = logging.getLogger(__name__)

# ==============================================================================
#  LOW-LEVEL, REUSABLE QUERY OBJECTS
# ==============================================================================

# --- Data Definition & Manipulation ---
delete_table_query = DDLQuery("DROP TABLE IF EXISTS {schema}.{table} CASCADE;")


# --- Data Query & Utility Objects ---
table_exists_query = DQLQuery(
    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table AND table_type = 'BASE TABLE');",
    result_handler=ResultHandler.SCALAR_ONE,
)

get_row_count_query = DQLQuery(
    "SELECT COUNT(*) FROM {schema}.{table} WHERE deleted_at IS NULL;",
    result_handler=ResultHandler.SCALAR_ONE,
)

get_table_column_names_query = DQLQuery(
    "SELECT column_name FROM information_schema.columns WHERE table_schema = :schema AND table_name = :table;",
    result_handler=ResultHandler.ALL,
)

get_items_paginated_query = GeoDQLQuery(
    "SELECT *, ST_AsEWKB(geom) as geom, ST_AsEWKB(bbox_geom) as bbox_geom FROM {schema}.{table} WHERE deleted_at IS NULL ORDER BY id LIMIT :limit OFFSET :offset;",
    result_handler=ResultHandler.ALL,
)

get_items_paginated_reprojected_query = GeoDQLQuery(
    "SELECT *, ST_AsEWKB(ST_Transform(geom, :target_crs_wkt)) as geom, ST_AsEWKB(ST_Transform(bbox_geom, :target_crs_wkt)) as bbox_geom FROM {schema}.{table} WHERE deleted_at IS NULL ORDER BY id LIMIT :limit OFFSET :offset;",
    result_handler=ResultHandler.ALL,
)

# --- Standalone Utility Functions ---


async def list_page_with_count(
    conn: DbResource,
    sql: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    limit: int,
    offset: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """Run a merged ``COUNT(*) OVER()`` + ``LIMIT/OFFSET`` page query and
    split the total from the page rows.

    ``sql`` must select ``COUNT(*) OVER() AS total_count`` alongside the page
    columns and end with ``ORDER BY ... LIMIT :limit OFFSET :offset`` — the
    caller owns the table/WHERE/ORDER BY (this is the SQL-side counterpart to
    ``extensions/tools/pagination.py``'s link builder, not a query builder).
    Count and page come back in a single round trip, so there is nothing to
    run concurrently and no shared-connection deadlock risk.

    Returns ``(rows, total)`` with ``total_count`` stripped from each row.
    An empty page returns ``([], 0)``.
    """
    query = DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS)
    rows = await query.execute(conn, limit=limit, offset=offset, **(params or {}))
    if not rows:
        return [], 0
    total = rows[0]["total_count"]
    return [{k: v for k, v in row.items() if k != "total_count"} for row in rows], total


async def get_table_column_names(conn: DbResource, schema: str, table: str) -> Set[str]:
    """Retrieves all column names for a table, used for building dynamic filters."""
    result = await get_table_column_names_query.execute(
        conn, schema=schema, table=table
    )
    return {row[0] for row in result}


def build_filter_clause(
    table_columns: Set[str],
    datetime_str: Optional[str] = None,
    subset_params: Optional[Dict[str, Any]] = None,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    intersects: Optional[Dict[str, Any]] = None,
    ids: Optional[List[str]] = None,
    param_suffix: str = "",
) -> Tuple[str, Dict[str, Any]]:
    """Builds a dynamic SQL WHERE clause and corresponding bind parameters."""
    conditions = ["deleted_at IS NULL"]
    bind_params = {}

    # BBOX Filter
    if bbox:
        xmin_p, ymin_p, xmax_p, ymax_p = (
            f"xmin{param_suffix}",
            f"ymin{param_suffix}",
            f"xmax{param_suffix}",
            f"ymax{param_suffix}",
        )
        conditions.append(
            f"geom && ST_Transform(ST_MakeEnvelope(:{xmin_p}, :{ymin_p}, :{xmax_p}, :{ymax_p}, 4326), ST_SRID(geom))"
        )
        bind_params[xmin_p] = bbox[0]
        bind_params[ymin_p] = bbox[1]
        bind_params[xmax_p] = bbox[2]
        bind_params[ymax_p] = bbox[3]

    # Datetime Filter
    if datetime_str:
        if "/" in datetime_str:
            start_str, end_str = datetime_str.split("/")
            # Handle open intervals '..'
            start_dt = isoparse(start_str) if start_str != ".." else None
            end_dt = isoparse(end_str) if end_str != ".." else None

            if start_dt and end_dt:
                start_dt_p, end_dt_p = (
                    f"start_dt{param_suffix}",
                    f"end_dt{param_suffix}",
                )
                conditions.append(
                    f"validity && tstzrange(:{start_dt_p}, :{end_dt_p}, '[]')"
                )  # Overlaps
                bind_params[start_dt_p] = start_dt
                bind_params[end_dt_p] = end_dt
            elif start_dt:
                start_dt_p = f"start_dt{param_suffix}"
                conditions.append(
                    f"(upper(validity) IS NULL OR upper(validity) >= :{start_dt_p})"
                )
                bind_params[start_dt_p] = start_dt
            elif end_dt:
                end_dt_p = f"end_dt{param_suffix}"
                conditions.append(f"lower(validity) <= :{end_dt_p}")
                bind_params[end_dt_p] = end_dt
        else:
            dt_p = f"dt{param_suffix}"
            conditions.append(f"validity @> :{dt_p}::timestamptz")  # Contains
            bind_params[dt_p] = isoparse(datetime_str)

    # Intersects Filter
    if intersects:
        intersects_p = f"intersects_geom{param_suffix}"
        conditions.append(
            f"ST_Intersects(geom, ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(:{intersects_p}), 4326), ST_SRID(geom)))"
        )
        bind_params[intersects_p] = str(
            intersects
        )  # Ensure it's a string for the driver

    # IDs Filter
    if ids:
        ids_p = f"ids{param_suffix}"
        conditions.append(f"external_id = ANY(:{ids_p})")
        bind_params[ids_p] = ids

    # Subset (Key-Value) Filter
    if subset_params:
        for key, value in subset_params.items():
            param = f"p_{key}{param_suffix}"
            if key in table_columns:
                conditions.append(f'"{key}" = :{param}')
            else:
                conditions.append(f"attributes->>'{key}' = :{param}")
            bind_params[param] = value
    return " AND ".join(conditions), bind_params
