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

"""Routing-aware cross-collection geoid lookup for the geoid extension.

Replaces the ES-backed SearchService.search_by_geoid implementation.

Driver resolution (issue #989) — the catalog's ``ItemsRoutingConfig`` decides
how a geoid lookup is served, instead of hardcoding the PostgreSQL driver:

  1. If the catalog pins the **private ES items driver**
     (``items_elasticsearch_private_driver``, #966/#1047) it queries the
     per-tenant private index directly (the GeoID private-catalog use case).
  2. Else the driver routed to **SEARCH** is used (ES, PG, or any other).
  3. Else the first **READ** driver advertising a search backend is used.
     A READ-capable driver advertises SEARCH via
     ``derive_supported_operations`` (Capability.READ → {READ, SEARCH}).
  4. Else the lookup falls back to the PostgreSQL two-pass hub scan — which is
     the special case of "first READ driver" when that driver is the
     query-fallback PG store (``Capability.QUERY_FALLBACK_SOURCE``).

Index-backed path (steps 1-3 when the resolved driver is a real search index):
- List the catalog's collections and ask the driver to fetch the requested
  geoids by id (``read_entities(entity_ids=...)``). The private/public ES
  drivers fetch directly by document id from the per-tenant / public index.

PostgreSQL fallback path (step 4) — the two-pass strategy:
- Items are stored per-collection in the catalog's PG schema. There is no
  cross-collection hub table — each collection has its own physical table.
- Hub table has only: geoid (UUID PK), transaction_time, deleted_at.
  All other fields (external_id, geometry, bbox, properties) are in sidecars.
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
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

# Driver ref of the tenant-scoped private ES items driver (#966/#1047). Kept
# in sync with ``routing_config._PRIVATE_ITEMS_DRIVER_ID``.
_PRIVATE_ITEMS_DRIVER_REF = "items_elasticsearch_private_driver"


def _partition_uuid_inputs(geoids: List[str]) -> Tuple[List[str], List[str]]:
    """Split caller-supplied geoid strings into (valid_uuid, invalid) lists.

    The hub query casts ``ANY(CAST(:geoids AS uuid[]))`` and asyncpg raises
    ``InvalidTextRepresentation`` for any non-UUID element — which the
    surrounding ``try/except`` swallows, producing a silent empty result
    for the entire batch (#975). Pre-filtering here lets valid inputs
    succeed while malformed inputs surface in a single WARN log.
    """
    valid: List[str] = []
    invalid: List[str] = []
    for g in geoids:
        if g is None:
            invalid.append("")
            continue
        s = str(g).strip()
        try:
            UUID(s)
        except (ValueError, AttributeError, TypeError):
            invalid.append(s)
            continue
        valid.append(s)
    return valid, invalid


def _normalize_geometry(geometry: Any) -> Optional[Dict[str, Any]]:
    """Coerce a feature geometry into a plain GeoJSON dict.

    ``ItemsProtocol.search_items`` returns Feature objects whose ``geometry`` is
    a ``geojson_pydantic`` model (e.g. ``Polygon``), but ``GeoidResult.geometry``
    is typed ``Dict[str, Any]``. Passing the model through unchanged makes
    pydantic raise ``dict_type``. Normalize the common shapes:

    * ``None``                       → ``None``
    * plain ``dict``                 → unchanged
    * pydantic model (``model_dump``) → ``model_dump(mode="json", exclude_none=True)``
    * shapely geom (``__geo_interface__``) → its mapping

    Anything else is returned as-is so the contract validator surfaces it.
    """
    if geometry is None or isinstance(geometry, dict):
        return geometry
    if hasattr(geometry, "model_dump"):  # pydantic model (e.g. geojson_pydantic)
        return geometry.model_dump(mode="json", exclude_none=True)
    if hasattr(geometry, "__geo_interface__"):  # shapely / GeoJSON-like object
        return dict(geometry.__geo_interface__)
    return geometry


def _feature_to_dict(feature: Any, catalog_id: str, collection_id: str) -> Dict[str, Any]:
    """Convert a Feature object returned by ItemsProtocol into a GeoidResult-shaped dict.

    ``collection_id`` is the *default* collection — the one the caller is
    iterating. A per-catalog (not per-collection) index, such as the private ES
    driver, resolves a by-id read regardless of the scoped collection, so the
    matched feature may belong to a different collection than the one being
    scanned. When the feature carries its own collection membership that
    membership wins, so a cross-collection geoid lookup reports the collection
    the item actually belongs to rather than the first one iterated (#1327).
    """
    props: Dict[str, Any] = {}
    if hasattr(feature, "properties") and feature.properties:
        props = dict(feature.properties)
    elif isinstance(feature, dict):
        props = dict(feature.get("properties") or {})

    # collection_id: resolve the matched item's TRUE membership, preferring (in
    # order): the STAC top-level ``collection`` member (public/base ES feature),
    # the canonical/flavoured ``collection_id``/``collection`` property surfaced
    # by the private ES doc, then the caller-supplied default (PG path /
    # fallback). The top-level read is string-guarded so non-string attribute
    # stand-ins are ignored; consumed property keys are popped so the envelope
    # field is not echoed back inside ``properties``.
    top_collection = getattr(feature, "collection", None)
    if not isinstance(top_collection, str) and isinstance(feature, dict):
        top_collection = feature.get("collection")
    prop_collection = props.pop("collection_id", None)
    prop_collection_alt = props.pop("collection", None)
    prop_collection = prop_collection or prop_collection_alt
    resolved_collection = (
        (top_collection if isinstance(top_collection, str) and top_collection else None)
        or (prop_collection if isinstance(prop_collection, str) and prop_collection else None)
        or collection_id
    )

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
    geometry = _normalize_geometry(geometry)

    bbox = getattr(feature, "bbox", None)
    if bbox is None and isinstance(feature, dict):
        bbox = feature.get("bbox")
    if bbox is not None:
        bbox = list(bbox)

    return {
        "geoid": str(geoid) if geoid else None,
        "catalog_id": catalog_id,
        "collection_id": resolved_collection,
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
        WHERE  geoid = ANY(CAST(:geoids AS uuid[]))
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


async def _resolve_lookup_driver(catalog_id: str) -> Optional[Any]:
    """Resolve the items storage driver that should serve a geoid lookup.

    Implements the issue #989 resolution chain against the catalog's
    ``ItemsRoutingConfig``:

    1. Private ES items driver if it is pinned in any operation of the
       catalog's routing config (tenant-scoped lookup) — this takes
       precedence so a private catalog never leaks through a public path.
    2. Otherwise the driver resolved for SEARCH, falling back to the first
       READ driver (``get_items_search_driver``). A READ-capable driver
       advertises search via ``derive_supported_operations``.

    Returns the resolved driver instance, or ``None`` when no routing config
    / driver can be resolved (the caller then uses the PG hub-scan fallback).
    Resolution failures are logged deterministically so an operator can see
    why a lookup landed on the fallback path.
    """
    # --- Step 1: private ES items driver wins if the catalog pins it ---
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.routing_config import (
            ItemsRoutingConfig,
            _items_routing_has_private_driver,
        )

        configs = get_protocol(ConfigsProtocol)
        if configs is not None:
            routing = await configs.get_config(ItemsRoutingConfig, catalog_id=catalog_id)
            if isinstance(routing, ItemsRoutingConfig) and _items_routing_has_private_driver(routing):
                from dynastore.modules.storage.driver_registry import DriverRegistry

                driver = DriverRegistry.collection_index().get(_PRIVATE_ITEMS_DRIVER_REF)
                if driver is not None:
                    logger.debug(
                        "lookup driver-resolve: catalog '%s' pins the private ES "
                        "items driver — using tenant-scoped private index.",
                        catalog_id,
                    )
                    return driver
                logger.warning(
                    "lookup driver-resolve: catalog '%s' routing pins '%s' but "
                    "the driver is not registered in this scope; falling through "
                    "to SEARCH/READ resolution.",
                    catalog_id, _PRIVATE_ITEMS_DRIVER_REF,
                )
    except Exception as exc:
        logger.debug(
            "lookup driver-resolve: private-driver probe failed for '%s': %s; "
            "falling through to SEARCH/READ resolution.",
            catalog_id, exc,
        )

    # --- Steps 2-3: SEARCH driver, else first READ driver ---
    try:
        from dynastore.modules.storage.router import get_items_search_driver

        resolved = await get_items_search_driver(catalog_id)
        return resolved.driver
    except Exception as exc:
        logger.warning(
            "lookup driver-resolve: no SEARCH/READ driver resolved for catalog "
            "'%s' (%s); using the PostgreSQL hub-scan fallback.",
            catalog_id, exc,
        )
        return None


def _driver_is_pg_fallback(driver: Any) -> bool:
    """True when the resolved driver is the query-fallback PostgreSQL store.

    The PG items driver declares ``Capability.QUERY_FALLBACK_SOURCE`` — the
    same marker ``item_query._try_driver_dispatch`` keys on to mean "this is
    the PG path". When the routing chain lands on it (step 4 of #989) the
    geoid lookup uses the PG two-pass hub scan rather than a driver dispatch.
    """
    try:
        from dynastore.models.protocols.storage_driver import Capability

        return Capability.QUERY_FALLBACK_SOURCE in getattr(driver, "capabilities", frozenset())
    except Exception:
        return False


async def _driver_geoid_lookup(
    driver: Any,
    catalog_id: str,
    geoids: List[str],
    limit: int,
) -> List[Dict[str, Any]]:
    """Cross-collection geoid lookup via an index-backed storage driver.

    Lists the catalog's collections and asks the driver to fetch the requested
    geoids by document id (``read_entities(entity_ids=...)``). The private and
    public ES drivers resolve geoids directly from their per-tenant / public
    index, so this serves the routing-config-pinned search path of #989.

    Returns rows shaped like ``GeoidResult``. Missing geoids are skipped.
    """
    from dynastore.models.protocols import CatalogsProtocol

    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        logger.warning("_driver_geoid_lookup: CatalogsProtocol not available")
        return []

    out: List[Dict[str, Any]] = []
    remaining = set(geoids)
    offset, batch = 0, 100
    while remaining and len(out) < limit:
        try:
            collections = await catalogs.list_collections(catalog_id, limit=batch, offset=offset)
        except Exception as exc:
            logger.warning(
                "_driver_geoid_lookup: list_collections failed for '%s': %s",
                catalog_id, exc,
            )
            break
        if not collections:
            break

        for col in collections:
            if not remaining or len(out) >= limit:
                break
            collection_id = getattr(col, "id", None) or (
                col.get("id") if isinstance(col, dict) else None
            )
            if not collection_id:
                continue
            try:
                features = driver.read_entities(
                    catalog_id,
                    collection_id,
                    entity_ids=list(remaining),
                    limit=limit - len(out),
                )
                async for f in features:
                    row = _feature_to_dict(f, catalog_id, collection_id)
                    gid = row.get("geoid")
                    if gid:
                        out.append(row)
                        remaining.discard(str(gid))
                    if len(out) >= limit:
                        break
            except Exception as exc:
                logger.warning(
                    "_driver_geoid_lookup: read_entities failed for %s/%s via %s: %s",
                    catalog_id, collection_id, type(driver).__name__, exc,
                )
                continue

        if len(collections) < batch:
            break
        offset += batch

    return out[:limit]


async def lookup_by_geoids(
    catalog_id: str,
    geoids: List[str],
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Cross-collection geoid lookup within one catalog (routing-aware, #989).

    The catalog's ``ItemsRoutingConfig`` decides how the lookup is served
    (see the module docstring for the full resolution chain):

    - **Index-backed path** — when the routing config pins the private ES
      driver, or routes SEARCH/READ to a real search index: list the catalog's
      collections and fetch the requested geoids by id through the resolved
      driver's ``read_entities``.
    - **PostgreSQL fallback path** — when the resolved driver is the
      query-fallback PG store (or no driver resolves): the two-pass hub scan.

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

    valid_geoids, invalid_geoids = _partition_uuid_inputs(list(geoids))
    if invalid_geoids:
        logger.warning(
            "lookup_by_geoids: %d/%d input(s) are not valid UUIDs and will be skipped: %r",
            len(invalid_geoids), len(geoids), invalid_geoids,
        )
    if not valid_geoids:
        return []

    # --- Driver resolution (#989): index-backed path vs PG fallback ---
    driver = await _resolve_lookup_driver(catalog_id)
    if driver is not None and not _driver_is_pg_fallback(driver):
        logger.debug(
            "lookup_by_geoids: serving catalog '%s' via index driver '%s'.",
            catalog_id, type(driver).__name__,
        )
        return await _driver_geoid_lookup(driver, catalog_id, valid_geoids, limit)

    # --- PostgreSQL fallback path: two-pass hub scan ---
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

        remaining_geoids = set(valid_geoids)
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
    """Per-collection lookup by external_id.

    Routes through ``ItemsProtocol.search_items`` with a typed
    ``FilterCondition(field="external_id")`` — not a raw CQL2 string — so the
    predicate survives ES dispatch.  The ES driver remaps ``external_id`` to the
    correct envelope field name for whichever index is active (``_external_id``
    on the public index, ``external_id`` on the private/envelope index).

    Returns at most ``limit`` rows shaped like ``GeoidResult``.
    """
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.models.query_builder import FilterCondition, QueryRequest

    catalogs = get_protocol(CatalogsProtocol)
    if not catalogs:
        logger.warning("lookup_by_external_id: CatalogsProtocol not available")
        return []

    request = QueryRequest(
        filters=[FilterCondition(field="external_id", operator="=", value=external_id)],
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


