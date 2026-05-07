"""PG-backed lookup helpers for the geoid extension.

Replaces the ES-backed SearchService.search_by_geoid implementation.

Design:
- Items are stored per-collection in the catalog's PG schema. There is no
  cross-collection hub table — each collection has its own physical table.
- Hub table has only: geoid (UUID PK), transaction_time, deleted_at.
  All other fields (external_id, geometry, bbox, properties) are in sidecars.

Cross-collection geoid lookup uses a two-pass strategy:
  1. Read ``{schema}.collection_configs`` to enumerate (collection_id,
     physical_table) pairs for the catalog's PG driver.
  2. For each hub table, issue a lightweight SELECT on just the hub (no sidecar
     JOIN) to find which collection owns each requested geoid.
  3. For hits, issue a full fetch through ``ItemsProtocol.search_items``
     (which runs the QueryOptimizer and JOINs all required sidecars) to
     populate the GeoidResult fields.

Per-collection external_id lookup goes directly through
``ItemsProtocol.search_items`` with a CQL2 filter.
"""

import logging
from typing import Any, Dict, List, Tuple

from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def _feature_to_dict(feature: Any, catalog_id: str, collection_id: str) -> Dict[str, Any]:
    """Convert a Feature object returned by ItemsProtocol into a GeoidResult-shaped dict."""
    props: Dict[str, Any] = {}
    if hasattr(feature, "properties") and feature.properties:
        props = dict(feature.properties)
    elif isinstance(feature, dict):
        props = dict(feature.get("properties") or {})

    # geoid: prefer property field; fall back to feature.id (may be external_id
    # when AttributesSidecar is configured as the id provider).
    geoid = props.pop("geoid", None)
    if not geoid:
        geoid = (
            getattr(feature, "id", None)
            or (feature.get("id") if isinstance(feature, dict) else None)
        )

    external_id = props.pop("external_id", None)
    if not external_id and isinstance(feature, dict):
        external_id = feature.get("external_id")

    geometry = getattr(feature, "geometry", None)
    if geometry is None and isinstance(feature, dict):
        geometry = feature.get("geometry")

    bbox = getattr(feature, "bbox", None)
    if bbox is None and isinstance(feature, dict):
        bbox = feature.get("bbox")
    if bbox is not None:
        bbox = list(bbox)

    return {
        "geoid": str(geoid) if geoid else None,
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "external_id": external_id,
        "geometry": geometry,
        "bbox": bbox,
        "properties": props if props else None,
    }


async def _get_pg_collection_tables(
    schema: str,
    db_resource: Any,
) -> List[Tuple[str, str]]:
    """Return [(collection_id, physical_table), ...] for all PG-driver collections.

    Reads ``{schema}.collection_configs`` — the table the PG driver writes when
    ``ensure_storage`` runs for a collection.  Rows where ``physical_table`` is
    null or absent are skipped (non-PG driver configs share the same table but
    don't carry a physical_table).
    """
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

    sql = f"""
        SELECT collection_id,
               config_data->>'physical_table' AS physical_table
        FROM   "{schema}".collection_configs
        WHERE  class_key = 'ItemsPostgresqlDriver'
          AND  config_data->>'physical_table' IS NOT NULL
    """
    try:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(db_resource)
    except Exception as exc:
        logger.debug("_get_pg_collection_tables: query failed for schema %s: %s", schema, exc)
        return []
    if not rows:
        return []
    return [(r["collection_id"], r["physical_table"]) for r in rows]


async def _hub_geoid_lookup(
    schema: str,
    physical_table: str,
    geoids: List[str],
    db_resource: Any,
) -> List[str]:
    """Return the subset of ``geoids`` that exist in ``{schema}.{physical_table}``.

    Queries only the hub (no sidecar JOIN) so this is a single indexed PK scan.
    """
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
    from dynastore.tools.db import validate_sql_identifier

    try:
        validate_sql_identifier(schema)
        validate_sql_identifier(physical_table)
    except Exception:
        return []

    sql = f"""
        SELECT geoid::text AS geoid
        FROM   "{schema}"."{physical_table}"
        WHERE  geoid = ANY(:geoids::uuid[])
          AND  deleted_at IS NULL
    """
    try:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            db_resource, geoids=list(geoids)
        )
    except Exception as exc:
        logger.debug(
            "_hub_geoid_lookup: failed for %s.%s: %s", schema, physical_table, exc
        )
        return []
    return [r["geoid"] for r in (rows or [])]


async def lookup_by_geoids(
    catalog_id: str,
    geoids: List[str],
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Cross-collection geoid lookup within one catalog (PG-backed, two-pass).

    Pass 1 – hub-only scan (no sidecar JOIN):
      For each collection's physical table, issue
      ``SELECT geoid FROM "{schema}"."{table}" WHERE geoid = ANY(:geoids) AND deleted_at IS NULL``.
      This uses the PK index and is extremely fast.

    Pass 2 – full fetch via ItemsProtocol.search_items:
      For each (collection_id, [matching geoids]) pair found in pass 1, call
      ``ItemsProtocol.search_items`` with ``raw_where="h.geoid = ANY(:geoids)"``
      so the QueryOptimizer JOINs all required sidecars and populates geometry,
      external_id, and properties.

    Returns rows shaped like ``GeoidResult``.  Results may span multiple
    collections.  Missing geoids are silently skipped.
    """
    if not geoids:
        return []

    from dynastore.models.protocols import CatalogsProtocol, DatabaseProtocol
    from dynastore.models.query_builder import QueryRequest
    from dynastore.modules.db_config.query_executor import managed_transaction

    catalogs = get_protocol(CatalogsProtocol)
    db = get_protocol(DatabaseProtocol)
    if not catalogs or not db:
        logger.warning("lookup_by_geoids: protocols not available (catalogs=%s db=%s)", catalogs, db)
        return []

    schema = await catalogs.resolve_physical_schema(catalog_id)
    if not schema:
        logger.warning("lookup_by_geoids: cannot resolve schema for %s", catalog_id)
        return []

    engine = db.engine if hasattr(db, "engine") else db

    # --- Pass 1: resolve (collection_id → [geoids]) mapping via hub-only scans ---
    async with managed_transaction(engine) as conn:
        collection_tables = await _get_pg_collection_tables(schema, conn)
        if not collection_tables:
            logger.debug("lookup_by_geoids: no PG collection tables found in %s", schema)
            return []

        remaining_geoids = set(geoids)
        hits: Dict[str, List[str]] = {}  # collection_id → list of geoids

        for collection_id, physical_table in collection_tables:
            if not remaining_geoids or len([v for vs in hits.values() for v in vs]) >= limit:
                break
            found = await _hub_geoid_lookup(schema, physical_table, list(remaining_geoids), conn)
            if found:
                hits[collection_id] = found
                remaining_geoids -= set(found)

    # --- Pass 2: full fetch per collection via ItemsProtocol.search_items ---
    out: List[Dict[str, Any]] = []

    for collection_id, matched_geoids in hits.items():
        if len(out) >= limit:
            break
        request = QueryRequest(
            raw_where="h.geoid = ANY(:bulk_geoids)",
            raw_params={"bulk_geoids": list(matched_geoids)},
            limit=limit - len(out),
        )
        try:
            features = await catalogs.items.search_items(
                catalog_id=catalog_id,
                collection_id=collection_id,
                request=request,
            )
        except Exception as exc:
            logger.warning(
                "lookup_by_geoids: full fetch failed for %s/%s: %s",
                catalog_id, collection_id, exc,
            )
            continue

        for f in features or []:
            row = _feature_to_dict(f, catalog_id, collection_id)
            if row.get("geoid"):
                out.append(row)

    return out[:limit]


async def lookup_by_external_id(
    catalog_id: str,
    collection_id: str,
    external_id: str,
    limit: int = 1,
) -> List[Dict[str, Any]]:
    """Per-collection lookup by external_id (PG-backed).

    Routes through the existing PG query path (``ItemsProtocol.search_items``
    with a CQL2 filter on ``external_id``).  The CQL2 translator in
    ``item_query.py`` converts ``external_id = 'X'`` to the appropriate
    sidecar column expression.

    Returns at most ``limit`` rows shaped like ``GeoidResult``.
    """
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.models.query_builder import QueryRequest

    catalogs = get_protocol(CatalogsProtocol)
    if not catalogs:
        logger.warning("lookup_by_external_id: CatalogsProtocol not available")
        return []

    request = QueryRequest(
        cql_filter=f"external_id = '{external_id}'",
        limit=limit,
    )
    try:
        features = await catalogs.items.search_items(
            catalog_id=catalog_id,
            collection_id=collection_id,
            request=request,
        )
    except Exception as exc:
        logger.warning(
            "lookup_by_external_id: search_items failed for %s/%s ext=%s: %s",
            catalog_id, collection_id, external_id, exc,
        )
        return []

    out: List[Dict[str, Any]] = []
    for f in features or []:
        row = _feature_to_dict(f, catalog_id, collection_id)
        if not row.get("external_id"):
            row["external_id"] = external_id
        out.append(row)
    return out
