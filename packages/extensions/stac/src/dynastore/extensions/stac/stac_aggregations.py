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

# dynastore/extensions/stac/stac_aggregations.py
import logging
from typing import Any, Dict, FrozenSet, List, Optional

from fastapi import HTTPException, status
from sqlalchemy import text

from dynastore.modules.stac.stac_config import AggregationRule, AggregationType
from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
from dynastore.tools.discovery import get_protocol
from dynastore.tools.cache import cached
from dynastore.models.protocols import CatalogsProtocol
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)


async def _resolve_physical_table(
    catalog_id: str, collection_id: str, *, db_resource=None
) -> Optional[str]:
    """Resolve physical table name via the storage driver config."""
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation

    try:
        driver = await get_driver(Operation.READ, catalog_id, collection_id)
        if hasattr(driver, "resolve_physical_table"):
            return await driver.resolve_physical_table(  # type: ignore[attr-defined]
                catalog_id, collection_id, db_resource=db_resource
            )
        return None
    except (ValueError, Exception):
        return None


@cached(
    maxsize=256,
    ttl=300,
    jitter=15,
    namespace="stac_agg",
    ignore=["conn"],
)
async def execute_aggregations(
    conn: Any,
    catalog_id: str,
    collection_ids: List[str],
    aggregation_rules: List[AggregationRule],
    where_sql: str = "TRUE",
    params: Optional[Dict[str, Any]] = None,
    filter_hints: Optional[FrozenSet[str]] = None,
) -> Dict[str, Any]:
    """
    Executes multiple aggregations and returns combined results.
    Follows OGC STAC Aggregation Extension format.

    Args:
        conn: Database connection
        catalog_id: Catalog identifier
        collection_ids: List of collection IDs to aggregate
        aggregation_rules: List of aggregation rules to execute
        where_sql: WHERE clause SQL (without WHERE keyword)
        params: Parameters for the WHERE clause
        filter_hints: Set of sidecars required by the filter ('attributes', 'geometry')

    Returns:
        Dictionary with aggregation results in OGC STAC format
    """
    if params is None:
        params = {}
    if filter_hints is None:
        filter_hints = frozenset()

    results = {}

    for agg_rule in aggregation_rules:
        try:
            if agg_rule.type == AggregationType.TERM:
                result = await _execute_term_aggregation(
                    conn,
                    catalog_id,
                    collection_ids,
                    agg_rule,
                    where_sql,
                    params,
                    filter_hints,
                )
            elif agg_rule.type == AggregationType.STATS:
                result = await _execute_stats_aggregation(
                    conn,
                    catalog_id,
                    collection_ids,
                    agg_rule,
                    where_sql,
                    params,
                    filter_hints,
                )
            elif agg_rule.type == AggregationType.GEOHASH:
                result = await _execute_geohash_aggregation(
                    conn,
                    catalog_id,
                    collection_ids,
                    agg_rule,
                    where_sql,
                    params,
                    filter_hints,
                )
            elif agg_rule.type == AggregationType.DATETIME:
                result = await _execute_datetime_aggregation(
                    conn,
                    catalog_id,
                    collection_ids,
                    agg_rule,
                    where_sql,
                    params,
                    filter_hints,
                )
            elif agg_rule.type == AggregationType.BBOX:
                result = await _execute_bbox_aggregation(
                    conn, catalog_id, collection_ids, where_sql, params, filter_hints
                )
            elif agg_rule.type == AggregationType.TEMPORAL_EXTENT:
                result = await _execute_temporal_extent_aggregation(
                    conn, catalog_id, collection_ids, where_sql, params, filter_hints
                )
            else:
                logger.warning(f"Unsupported aggregation type: {agg_rule.type}")
                continue

            results[agg_rule.name] = result

        except Exception as e:
            logger.error(
                f"Error executing aggregation '{agg_rule.name}': {e}", exc_info=True
            )
            results[agg_rule.name] = {"error": str(e)}

    return results


def _build_joins(phys_schema: str, phys_table: str, required_sidecars: FrozenSet[str]) -> str:
    joins = []
    if "attributes" in required_sidecars:
        table = f"{phys_table}_attributes"
        joins.append(f'JOIN "{phys_schema}"."{table}" s ON h.geoid = s.geoid')
    if "geometry" in required_sidecars:
        table = f"{phys_table}_geometries"
        joins.append(f'JOIN "{phys_schema}"."{table}" g ON h.geoid = g.geoid')
    return "\n".join(joins)


async def _execute_term_aggregation(
    conn: Any,
    catalog_id: str,
    collection_ids: List[str],
    agg_request: AggregationRule,
    where_sql: str,
    params: Dict[str, Any],
    filter_hints: FrozenSet[str],
) -> Dict[str, Any]:
    """Executes a STAC-style term aggregation (frequency counts)."""
    # The 'property' is like 'properties.asset_id'
    field_parts = agg_request.property.split(".")
    if len(field_parts) != 2 or field_parts[0] != "properties":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid aggregation property '{agg_request.property}'. Must be in 'properties.<name>' format.",
        )

    attribute_key = field_parts[1]

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    # Resolve physical schema
    phys_schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn))
    assert phys_schema is not None

    # Build the UNION ALL query for aggregation
    required = filter_hints | {"attributes"}
    select_fragments = []
    for collection_id in collection_ids:
        phys_table = await _resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if not phys_table:
            continue

        joins = _build_joins(phys_schema, phys_table, required)
        fragment = f"""
            SELECT s.attributes->>'{attribute_key}' AS val
            FROM "{phys_schema}"."{phys_table}" h
            {joins}
            WHERE {where_sql} AND s.attributes ? '{attribute_key}'
        """
        select_fragments.append(fragment)

    if not select_fragments:
        return {"buckets": []}

    full_union_query = " UNION ALL ".join(select_fragments)

    # Final aggregation query
    agg_sql = f"""
        SELECT val, COUNT(*) as doc_count
        FROM ({full_union_query}) as all_items
        GROUP BY val
        ORDER BY doc_count DESC
        LIMIT :limit;
    """

    final_params = {**params, "limit": agg_request.limit}
    query = DQLQuery(text(agg_sql), result_handler=ResultHandler.ALL_DICTS)
    rows = await query.execute(conn, **final_params)

    return {
        "buckets": [
            {"key": row["val"], "doc_count": row["doc_count"]}
            for row in (rows if rows else [])
        ]
    }


async def _execute_stats_aggregation(
    conn: Any,
    catalog_id: str,
    collection_ids: List[str],
    agg_request: AggregationRule,
    where_sql: str,
    params: Dict[str, Any],
    filter_hints: FrozenSet[str],
) -> Dict[str, Any]:
    """Executes statistical aggregation (min, max, avg, sum, count)."""
    field_parts = agg_request.property.split(".")
    if len(field_parts) != 2 or field_parts[0] != "properties":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid aggregation property '{agg_request.property}'. Must be in 'properties.<name>' format.",
        )

    attribute_key = field_parts[1]

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    # Resolve physical schema
    phys_schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn))
    assert phys_schema is not None

    # Build UNION ALL query
    required = filter_hints | {"attributes"}
    select_fragments = []
    for collection_id in collection_ids:
        phys_table = await _resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if not phys_table:
            continue

        joins = _build_joins(phys_schema, phys_table, required)
        fragment = f"""
            SELECT CAST(s.attributes->>'{attribute_key}' AS NUMERIC) AS val
            FROM "{phys_schema}"."{phys_table}" h
            {joins}
            WHERE {where_sql} 
              AND s.attributes ? '{attribute_key}'
              AND s.attributes->>'{attribute_key}' ~ '^-?[0-9]+(\\.[0-9]+)?$'
        """
        select_fragments.append(fragment)

    if not select_fragments:
        return {"min": None, "max": None, "avg": None, "sum": None, "count": 0}

    full_union_query = " UNION ALL ".join(select_fragments)

    # Stats query
    stats_sql = f"""
        SELECT 
            MIN(val) as result_min,
            MAX(val) as result_max,
            AVG(val) as result_avg,
            SUM(val) as result_sum,
            COUNT(val) as result_count
        FROM ({full_union_query}) as all_values;
    """

    query = DQLQuery(text(stats_sql), result_handler=ResultHandler.ONE_DICT)
    result_dict = await query.execute(conn, **params)

    if not result_dict:
        return {"min": None, "max": None, "avg": None, "sum": None, "count": 0}

    return {
        "min": float(result_dict["result_min"])
        if result_dict["result_min"] is not None
        else None,
        "max": float(result_dict["result_max"])
        if result_dict["result_max"] is not None
        else None,
        "avg": float(result_dict["result_avg"])
        if result_dict["result_avg"] is not None
        else None,
        "sum": float(result_dict["result_sum"])
        if result_dict["result_sum"] is not None
        else None,
        "count": int(result_dict["result_count"]),
    }


async def _execute_geohash_aggregation(
    conn: Any,
    catalog_id: str,
    collection_ids: List[str],
    agg_request: AggregationRule,
    where_sql: str,
    params: Dict[str, Any],
    filter_hints: FrozenSet[str],
) -> Dict[str, Any]:
    """Executes geohash-based spatial aggregation."""
    precision = agg_request.precision or 5  # Default precision

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    # Resolve physical schema
    phys_schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn))
    assert phys_schema is not None

    # Build UNION ALL query
    required = filter_hints | {"geometry"}
    select_fragments = []
    for collection_id in collection_ids:
        phys_table = await _resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if not phys_table:
            continue

        joins = _build_joins(phys_schema, phys_table, required)
        fragment = f"""
            SELECT ST_GeoHash(ST_Centroid(g.geom), {precision}) AS geohash_val
            FROM "{phys_schema}"."{phys_table}" h
            {joins}
            WHERE {where_sql} AND g.geom IS NOT NULL
        """
        select_fragments.append(fragment)

    if not select_fragments:
        return {"buckets": []}

    full_union_query = " UNION ALL ".join(select_fragments)

    # Aggregation query
    agg_sql = f"""
        SELECT geohash_val, COUNT(*) as doc_count
        FROM ({full_union_query}) as all_geohashes
        GROUP BY geohash_val
        ORDER BY doc_count DESC
        LIMIT :limit;
    """

    final_params = {**params, "limit": agg_request.limit}
    query = DQLQuery(text(agg_sql), result_handler=ResultHandler.ALL_DICTS)
    rows = await query.execute(conn, **final_params)

    return {
        "buckets": [
            {"key": row["geohash_val"], "doc_count": row["doc_count"]}
            for row in (rows if rows else [])
        ]
    }


async def _execute_datetime_aggregation(
    conn: Any,
    catalog_id: str,
    collection_ids: List[str],
    agg_request: AggregationRule,
    where_sql: str,
    params: Dict[str, Any],
    filter_hints: FrozenSet[str],
) -> Dict[str, Any]:
    """Executes temporal histogram aggregation."""
    interval = agg_request.interval or "1 day"  # Default interval

    # Parse property (could be 'properties.datetime' or direct column)
    if agg_request.property.startswith("properties."):
        field_parts = agg_request.property.split(".")
        attribute_key = field_parts[1]
        time_expr = f"(s.attributes->>'{attribute_key}')::timestamptz"
    else:
        # Assume it's a direct column like 'created_at' (or generic 'timestamp')
        time_expr = f'"{agg_request.property}"'

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    # Resolve physical schema
    phys_schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn))
    assert phys_schema is not None

    # Build UNION ALL query
    required = filter_hints | {"attributes"}
    select_fragments = []
    for collection_id in collection_ids:
        phys_table = await _resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if not phys_table:
            continue

        joins = _build_joins(phys_schema, phys_table, required)
        fragment = f"""
            SELECT {time_expr} AS ts_val
            FROM "{phys_schema}"."{phys_table}" h
            {joins}
            WHERE {where_sql} AND {time_expr} IS NOT NULL
        """
        select_fragments.append(fragment)

    if not select_fragments:
        return {"buckets": []}

    full_union_query = " UNION ALL ".join(select_fragments)

    # Histogram query using date_trunc
    agg_sql = f"""
        SELECT 
            date_trunc('{interval}', ts_val) AS bucket_val,
            COUNT(*) as doc_count
        FROM ({full_union_query}) as all_timestamps
        GROUP BY bucket_val
        ORDER BY bucket_val
        LIMIT :limit;
    """

    final_params = {**params, "limit": agg_request.limit}
    query = DQLQuery(text(agg_sql), result_handler=ResultHandler.ALL_DICTS)
    rows = await query.execute(conn, **final_params)

    return {
        "buckets": [
            {
                "key": row["bucket_val"].isoformat() if row["bucket_val"] else None,
                "doc_count": row["doc_count"],
            }
            for row in (rows if rows else [])
        ]
    }


async def _execute_bbox_aggregation(
    conn: Any,
    catalog_id: str,
    collection_ids: List[str],
    where_sql: str,
    params: Dict[str, Any],
    filter_hints: FrozenSet[str],
) -> Dict[str, Any]:
    """Calculates the combined bounding box for all matching items."""

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    # Resolve physical schema
    phys_schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn))
    assert phys_schema is not None

    # Build UNION ALL query
    required = filter_hints | {"geometry"}
    select_fragments = []
    for collection_id in collection_ids:
        phys_table = await _resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if not phys_table:
            continue

        joins = _build_joins(phys_schema, phys_table, required)
        fragment = f"""
            SELECT g.geom
            FROM "{phys_schema}"."{phys_table}" h
            {joins}
            WHERE {where_sql} AND g.geom IS NOT NULL
        """
        select_fragments.append(fragment)

    if not select_fragments:
        return {"bbox": None}

    full_union_query = " UNION ALL ".join(select_fragments)

    # Extent query
    bbox_sql = f"""
        SELECT 
            ST_XMin(ST_Extent(geom)) as minx,
            ST_YMin(ST_Extent(geom)) as miny,
            ST_XMax(ST_Extent(geom)) as maxx,
            ST_YMax(ST_Extent(geom)) as maxy
        FROM ({full_union_query}) as all_geoms;
    """

    query = DQLQuery(text(bbox_sql), result_handler=ResultHandler.ONE_DICT)
    result_dict = await query.execute(conn, **params)

    if not result_dict or result_dict.get("minx") is None:
        return {"bbox": None}

    return {
        "bbox": [
            float(result_dict["minx"]),
            float(result_dict["miny"]),
            float(result_dict["maxx"]),
            float(result_dict["maxy"]),
        ]
    }


async def _execute_temporal_extent_aggregation(
    conn: Any,
    catalog_id: str,
    collection_ids: List[str],
    where_sql: str,
    params: Dict[str, Any],
    filter_hints: FrozenSet[str],
) -> Dict[str, Any]:
    """Calculates the temporal extent (min/max datetime) for all matching items."""

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    # Resolve physical schema
    phys_schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn))
    assert phys_schema is not None

    # Build UNION ALL query
    required = filter_hints | {"attributes"}
    select_fragments = []
    for collection_id in collection_ids:
        phys_table = await _resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if not phys_table:
            continue

        joins = _build_joins(phys_schema, phys_table, required)
        fragment = f"""
            SELECT 
                (s.attributes->>'datetime')::timestamptz AS dt,
                (s.attributes->>'start_datetime')::timestamptz AS start_dt,
                (s.attributes->>'end_datetime')::timestamptz AS end_dt
            FROM "{phys_schema}"."{phys_table}" h
            {joins}
            WHERE {where_sql}
        """
        select_fragments.append(fragment)

    if not select_fragments:
        return {"interval": [[None, None]]}

    full_union_query = " UNION ALL ".join(select_fragments)

    # Temporal extent query
    temporal_sql = f"""
        SELECT 
            MIN(COALESCE(dt, start_dt)) as min_dt,
            MAX(COALESCE(dt, end_dt)) as max_dt
        FROM ({full_union_query}) as all_times;
    """

    query = DQLQuery(text(temporal_sql), result_handler=ResultHandler.ONE_DICT)
    result_dict = await query.execute(conn, **params)

    if not result_dict:
        return {"interval": [[None, None]]}

    start_time = result_dict.get("min_dt")
    end_time = result_dict.get("max_dt")

    return {
        "interval": [
            [
                start_time.isoformat() if start_time else None,
                end_time.isoformat() if end_time else None,
            ]
        ]
    }
