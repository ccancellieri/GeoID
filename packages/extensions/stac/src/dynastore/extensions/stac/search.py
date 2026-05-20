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

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field

from dynastore.models.protocols import CatalogsProtocol
from dynastore.models.query_builder import (
    FieldSelection,
    FilterCondition,
    FilterOperator,
    QueryRequest,
)
from dynastore.modules import get_protocol
from dynastore.modules.catalog.models import Collection
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.db_config.query_executor import (
    DbResource,
    DQLQuery,
    ResultHandler,
)
from dynastore.modules.db_config.shared_queries import build_filter_clause
from dynastore.modules.stac.stac_config import AggregationRule, StacPluginConfig
from dynastore.models.driver_context import DriverContext


class AttributeFilter(BaseModel):
    field: str
    operator: FilterOperator
    value: Any


class QueryFilter(BaseModel):
    op: str = Field("and")
    args: List[Union["QueryFilter", AttributeFilter]]


class ItemSearchRequest(BaseModel):
    catalog_id: Optional[str] = None
    collections: Optional[List[str]] = None
    ids: Optional[List[str]] = None
    bbox: Optional[Tuple[float, float, float, float]] = None
    datetime: Optional[str] = None
    intersects: Optional[Dict[str, Any]] = None
    filter: Optional[Union[AttributeFilter, QueryFilter]] = None
    filter_lang: str = "cql2-json"
    limit: int = Field(10, ge=1, le=1000)
    offset: int = Field(0, ge=0)
    aggregations: Optional[List[AggregationRule]] = Field(None, alias="aggregate")


class CollectionSearchRequest(BaseModel):
    catalog_id: Optional[str] = None
    catalog_ids: Optional[List[str]] = None
    ids: Optional[List[str]] = None
    bbox: Optional[Tuple[float, float, float, float]] = None
    datetime: Optional[str] = None
    keywords: Optional[List[str]] = None
    limit: int = Field(10, ge=1, le=1000)
    offset: int = Field(0, ge=0)
    sortby: Optional[str] = Field(
        None,
        description=(
            "Sort field. Prefix with '+' for ascending, '-' for descending. "
            "Aliases: 'code'=id, 'label'=title. E.g. '+code', '-label'."
        ),
    )
    lang: Optional[str] = Field(
        None,
        description="Language code for multilingual sort (e.g. 'en', 'fr'). Default: 'en'.",
    )


logger = logging.getLogger(__name__)


def _get_simplification_sql(config: Optional[StacPluginConfig]) -> str:
    """Builds the dynamic simplification SQL CASE statement from configuration."""
    if not config or not config.simplification:
        return "0.0001"

    simp = config.simplification
    case_sql = "CASE "
    for points, tolerance in sorted(
        simp.vertex_thresholds.items(), key=lambda item: item[0], reverse=True
    ):
        case_sql += f"WHEN ST_NPoints(geom) > {points} THEN {tolerance} "
    case_sql += f"ELSE {simp.default_tolerance} END"
    return f"({case_sql})"


_SAFE_ATTRIBUTE_FIELD_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

# CQL2-JSON operator mapping from FilterOperator values
_FILTER_OP_TO_CQL2: Dict[str, str] = {
    "eq": "=", "ne": "<>", "neq": "<>",
    "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
    "like": "like", "ilike": "like",  # pygeofilter uses "like"
    "in": "in", "nin": "not in",
}


def _filter_to_cql2_json(
    filt: Union[AttributeFilter, QueryFilter],
) -> Dict[str, Any]:
    """Convert STAC AttributeFilter/QueryFilter to a CQL2-JSON dict.

    This bridges the STAC custom filter model to the shared pygeofilter
    pipeline so all protocols use the same filter compilation path.
    """
    if isinstance(filt, QueryFilter):
        return {
            "op": filt.op.lower(),
            "args": [_filter_to_cql2_json(arg) for arg in filt.args],
        }
    # AttributeFilter
    op_str = filt.operator.value if isinstance(filt.operator, FilterOperator) else str(filt.operator)
    cql_op = _FILTER_OP_TO_CQL2.get(op_str)
    if cql_op is None:
        raise ValueError(f"Cannot convert operator '{op_str}' to CQL2-JSON")
    return {
        "op": cql_op,
        "args": [{"property": filt.field}, filt.value],
    }


def _build_cql2_field_mapping(table_columns: set) -> Dict[str, Any]:
    """Build a field_mapping dict for pygeofilter from JSONB attributes columns.

    Maps property names to ``attributes->>'field'`` SQL text expressions,
    which is the standard JSONB accessor pattern for STAC attributes.
    """
    from sqlalchemy import text
    return {
        col: text(f"attributes->>'{col}'")
        for col in table_columns
        if _SAFE_ATTRIBUTE_FIELD_RE.match(col)
    }

_COLLECTION_SORT_ALIASES: dict = {"code": "id", "label": "title"}
_COLLECTION_DIRECT_SORT_COLUMNS = frozenset({"id", "catalog_id"})


def _parse_collection_sort_sql(sortby: Optional[str], lang: Optional[str] = None) -> str:
    """
    Convert a sortby string to a safe SQL ORDER BY clause for collection search.

    Aliases: code → id, label → title
    Multilingual: title → (metadata->'title'->>'{lang}')
    Custom fields: (metadata->>'field') validated against _SAFE_ATTRIBUTE_FIELD_RE
    """
    if not sortby:
        return "catalog_id ASC, id ASC"
    direction = "DESC" if sortby.startswith("-") else "ASC"
    raw_field = sortby.lstrip("+-")
    field = _COLLECTION_SORT_ALIASES.get(raw_field, raw_field)
    if field in _COLLECTION_DIRECT_SORT_COLUMNS:
        return f"{field} {direction}"
    if field == "title":
        safe_lang = lang if (lang and re.match(r"^[a-z]{2,3}$", lang)) else "en"
        return f"(title->>'{safe_lang}') {direction}"
    if _SAFE_ATTRIBUTE_FIELD_RE.match(field):
        return f"(extra_metadata->>'{field}') {direction}"
    raise ValueError(f"Invalid sort field: {field!r}")


def _build_attribute_filter_sql(filt: AttributeFilter, params: dict) -> str:
    if not _SAFE_ATTRIBUTE_FIELD_RE.match(filt.field):
        raise ValueError(
            f"Invalid attribute field name '{filt.field}': "
            "must start with a letter or underscore and contain only alphanumeric characters and underscores."
        )
    op: FilterOperator = (
        filt.operator if isinstance(filt.operator, FilterOperator)
        else FilterOperator.from_str(str(filt.operator))
    )
    sql_op = op.to_sql()
    type_cast = "::numeric" if op.needs_numeric_cast else ""
    param_name = f"filter_{filt.field}_{op.value}"
    field_accessor = f"attributes->>'{filt.field}'"
    params[param_name] = filt.value
    return f"({field_accessor}{type_cast} {sql_op} :{param_name})"


def _build_complex_filter_sql(query_filter: QueryFilter, params: dict) -> str:
    if not query_filter.args:
        return ""
    op_sql = f" {query_filter.op.upper()} "
    conditions = []
    for arg in query_filter.args:
        if isinstance(arg, QueryFilter):
            conditions.append(_build_complex_filter_sql(arg, params))
        else:
            conditions.append(_build_attribute_filter_sql(arg, params))
    return f"({op_sql.join(filter(None, conditions))})"


async def _maybe_dispatch_to_es_search(
    cat_id: str,
    search_request: "ItemSearchRequest",
) -> Optional[Tuple[list, int, Optional[Dict[str, Any]]]]:
    """If the target collections route reads through
    ``items_elasticsearch_driver``, run the ES path via
    :class:`SearchService` and return STAC-shaped results.  Returns
    ``None`` when this dispatch is not applicable (no ES driver, CQL2
    filter present, multi-collection mixed-driver, …) so the caller
    falls back to the PG path.

    Closes the user-visible part of issue #222 — basic STAC search on
    ES-routed collections returns 0 items today because every search
    unconditionally builds raw PG SQL.
    """
    # Only basic structural filters routed through ES today.
    if search_request.filter is not None:
        return None

    # Resolve the primary READ driver for each requested collection.
    # When mixed (some ES, some PG) we don't dispatch — fall back to
    # PG and let the QueryOptimizer per-collection logic handle it.
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation

    cids = search_request.collections or []
    if not cids:
        return None
    es_indexer_id = "items_elasticsearch_driver"
    for cid in cids:
        try:
            driver = await get_driver(Operation.READ, cat_id, cid)
        except Exception:
            return None
        driver_id = type(driver).__name__
        # Snake-case match — same convention used in routing_config.
        from dynastore.modules.storage.routing_config import _to_snake
        if _to_snake(driver_id) != es_indexer_id:
            return None  # at least one collection isn't ES — bail

    # All target collections route through ES. Dispatch via the
    # backend-agnostic ItemSearchProtocol — this avoids importing
    # search-extension internals (search_models / search_service) and
    # keeps stac/search.py free of cross-extension coupling (#234).
    from dynastore.models.protocols.item_search import ItemSearchProtocol

    search_svc = get_protocol(ItemSearchProtocol)
    if search_svc is None:
        return None  # no ItemSearchProtocol provider registered

    try:
        result = await search_svc.search_items_struct(
            catalog_id=cat_id,
            collections=cids,
            ids=search_request.ids,
            bbox=list(search_request.bbox) if search_request.bbox else None,
            intersects=search_request.intersects,
            datetime=search_request.datetime,
            limit=search_request.limit,
        )
    except Exception as exc:
        logger.warning(
            "STAC search → ES dispatch failed (catalog=%s, collections=%s): %s",
            cat_id, cids, exc,
        )
        return None  # fall back to PG path on error

    # ItemSearchProtocol returns plain dicts (STAC Item JSON shape from ES
    # _source). The downstream serializer (stac_generator.create_search_results_collection)
    # operates on Feature pydantic instances and reads `feature.properties["_catalog_id"]` /
    # `["_collection_id"]` — injected by the PG path at search.py:882-883. Mirror that
    # shape here so the ES fast path returns the same contract as PG.
    from dynastore.models.shared_models import Feature
    features: list = []
    for raw in result.features:
        if isinstance(raw, Feature):
            features.append(raw)
            continue
        try:
            feat = Feature.model_validate(raw)
        except Exception as exc:
            logger.warning(
                "STAC search → ES dispatch: skipping malformed hit (catalog=%s): %s",
                cat_id, exc,
            )
            continue
        if feat.properties is None:
            feat.properties = {}
        feat.properties.setdefault("_catalog_id", cat_id)
        # ES indexed doc carries top-level `collection`; fall back to first
        # requested collection when only one is in scope.
        coll_id = raw.get("collection") if isinstance(raw, dict) else None
        if not coll_id and len(cids) == 1:
            coll_id = cids[0]
        feat.properties.setdefault("_collection_id", coll_id or "")
        features.append(feat)
    return features, result.total, None


async def search_items(
    db_resource: DbResource,
    search_request: ItemSearchRequest,
    stac_config: StacPluginConfig,
    hierarchy_sql: Optional[str] = None,
    hierarchy_params: Optional[Dict[str, Any]] = None,
) -> Tuple[list, int, Optional[Dict[str, Any]]]:
    from sqlalchemy.ext.asyncio import AsyncEngine

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None, "CatalogsProtocol not registered"
    assert search_request.catalog_id is not None, "search_request.catalog_id required"
    cat_id: str = search_request.catalog_id

    # ── ES dispatch fast path (issue #222) ────────────────────────────
    # When the resolved primary READ driver is ``items_elasticsearch_driver``
    # and the request is structural-filter only (no CQL2 ``filter``),
    # delegate to the platform :class:`SearchService` which already
    # implements the ES query path against ``{prefix}-items-{cat}`` /
    # the public alias.  CQL2 + ES is intentionally NOT routed here yet
    # because no CQL2→ES translator exists; those queries fall through
    # to the PG path (operator's responsibility to ensure their data
    # is in PG, or pin the collection's READ to PG via routing config).
    es_dispatch_result = await _maybe_dispatch_to_es_search(
        cat_id, search_request,
    )
    if es_dispatch_result is not None:
        return es_dispatch_result

    # Helper to execute with a connection (reusing if present, connecting if engine)
    _catalogs = catalogs

    async def get_columns_with_conn(resource):
        if isinstance(resource, AsyncEngine):
            async with resource.connect() as conn:
                return await _catalogs.get_collection_column_names(
                    cat_id, target_collections[0], ctx=DriverContext(db_resource=conn)
                )
        else:
            return await _catalogs.get_collection_column_names(
                cat_id, target_collections[0], ctx=DriverContext(db_resource=resource)
            )

    # Resolve layer config in parallel
    initial_collection_ids = search_request.collections or [
        c.id
        for c in await catalogs.list_collections(
            cat_id, limit=1000, ctx=DriverContext(db_resource=db_resource)
        )
    ]

    async def _check_layer_def(cid):
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation
        driver = await get_driver(Operation.READ, cat_id, cid)
        return await driver.get_driver_config(
            cat_id, cid, db_resource=db_resource
        )

    # Sequential — every check forwards the SAME `db_resource` (a live
    # asyncpg Connection); concurrent SELECTs over a single wire deadlock
    # asyncpg's single-stream protocol (regression observed in PRs #28,
    # #32, #43).
    results = [await _check_layer_def(cid) for cid in initial_collection_ids]

    # Store configs map
    collection_configs = {
        initial_collection_ids[i]: config
        for i, config in enumerate(results)
        if config is not None
    }
    target_collections = list(collection_configs.keys())

    if not target_collections:
        return [], 0, None

    # Get column names for filtering
    table_columns = await get_columns_with_conn(db_resource)

    where_sql, params = build_filter_clause(
        table_columns=table_columns,
        datetime_str=search_request.datetime,
        bbox=None,  # Handled via raw_where in loop
        intersects=None,  # Handled via raw_where in loop
        ids=None,  # Handled via QueryOptimizer filters
    )

    if search_request.filter:
        cql_filter_sql = None
        try:
            from dynastore.modules.tools.cql import parse_cql2_json_filter
            cql2_json = _filter_to_cql2_json(search_request.filter)
            field_mapping = _build_cql2_field_mapping(table_columns)
            cql_where, cql_params = parse_cql2_json_filter(
                cql2_json, field_mapping=field_mapping,
            )
            if cql_where:
                cql_filter_sql = f"({cql_where})"
                params.update(cql_params)
        except Exception as e:
            logger.warning(f"CQL2-JSON filter path failed, falling back to legacy: {e}")
            if isinstance(search_request.filter, QueryFilter):
                cql_filter_sql = _build_complex_filter_sql(search_request.filter, params)
            elif isinstance(search_request.filter, AttributeFilter):
                cql_filter_sql = _build_attribute_filter_sql(search_request.filter, params)
        if cql_filter_sql:
            where_sql += f" AND {cql_filter_sql}"

    if hierarchy_sql:
        where_sql += f" AND {hierarchy_sql}"
    if hierarchy_params:
        params.update(hierarchy_params)

    # Resolve physical schema via routing
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation

    catalogs2 = get_protocol(CatalogsProtocol)
    assert catalogs2 is not None, "CatalogsProtocol not registered"
    phys_schema = await catalogs2.resolve_physical_schema(
        cat_id, ctx=DriverContext(db_resource=db_resource)
    )

    # --- Query Planner: Dependency Analysis ---
    # Detect which sidecars are strictly required by the filter criteria

    req_geometry = bool(
        search_request.bbox
        or search_request.intersects
        or (hierarchy_sql and "geom" in hierarchy_sql)
        or "geom" in where_sql
    )

    req_stac = bool(
        "stac_" in where_sql
        or "external_extensions" in where_sql
        or "external_assets" in where_sql
    )

    # "Attributes" sidecar is needed if we filter on properties, IDs, or datetime (validity)
    req_attributes = bool(
        search_request.ids
        or search_request.datetime
        or search_request.filter
        or "attributes" in where_sql
        or "external_id" in where_sql
    )

    # --- Query Planner: Sort Strategy Optimization ---
    # If we are in "Spatial Only" mode (Geometry query, no attribute filters),
    # we can optimize by sorting on Hub columns (transaction_time) instead of joining Attributes (validity).
    # Otherwise, we default to standard STAC sorting (validity, external_id) which requires Attributes.

    if req_geometry and not req_attributes and not req_stac:
        # Optimization: Spatial Only Mode
        # Sort by Hub columns to avoid extra join
        sort_mode = "hub"
        sort_col = "h.transaction_time"
        sort_id = "h.geoid::text"
    else:
        # Standard Mode
        # Sort by Attributes (validity)
        sort_mode = "attributes"
        sort_col = "lower(s.validity)"  # Assumes 's' alias for attributes
        sort_id = "s.external_id"
        req_attributes = True  # Force attributes join for sorting

    candidate_fragments = []

    # Store table mapping for hydration: collection_id -> (phys_table, sidecar_table, geom_table, item_meta_table, stac_meta_table, has_attr, has_geom, has_item_meta, has_stac_meta)
    collection_table_map = {}
    # Per-collection flag: does the attributes sidecar persist a ``validity``
    # column? Mirrors ``ItemsWritePolicy.enable_validity`` (#974). When False,
    # the column does not exist and the hydration SELECT must not reference it.
    collection_validity_enabled: dict = {}

    for collection_id in target_collections:
        config = collection_configs[collection_id]

        # Instantiate QueryOptimizer for this collection — STAC search needs
        # the stac_metadata sidecar in the SELECT/JOIN.
        from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
        optimizer = QueryOptimizer(config, consumer=ConsumerType.STAC)

        # --- Query Request Construction ---

        query_selects = []
        raw_selects = [
            f"'{cat_id}' as catalog_id",
            f"'{collection_id}' as collection_id",
        ]

        # We always need geoid
        query_selects.append(FieldSelection(field="geoid"))

        # --- Filter Logic Mapping ---
        query_filters = []
        raw_where_clauses = []

        # Start with global params copy to avoid pollution
        query_params = params.copy()

        # Spatial Filters -> Geometry Sidecar (via QueryOptimizer)
        # We use raw_where to handle spatial operators cleanly (avoiding casting issues in QueryOptimizer '&&')
        # BUT QueryOptimizer only joins sidecars if fields are used in SELECT/FILTER/SORT/GROUP.
        # So we MUST force Geometry sidecar usage if we have spatial filters.
        # We do this by selecting a cheap property: ST_SRID(geometry).

        has_spatial_filter = False

        if search_request.bbox:
            # Construct WKT Polygon for BBOX (if needed) but here we use ST_MakeEnvelope
            # Use 'geom' instead of 'geometry' to avoid ambiguity with attributes sidecar
            raw_where_clauses.append(
                "ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326) && geom"
            )
            query_params.update(
                {
                    "xmin": search_request.bbox[0],
                    "ymin": search_request.bbox[1],
                    "xmax": search_request.bbox[2],
                    "ymax": search_request.bbox[3],
                }
            )
            has_spatial_filter = True

        if search_request.intersects:
            # Use ST_Intersects(ST_GeomFromGeoJSON(:intersects), geom)
            raw_where_clauses.append(
                "ST_Intersects(ST_GeomFromGeoJSON(:intersects), geom)"
            )
            query_params["intersects"] = json.dumps(search_request.intersects)
            has_spatial_filter = True

        # Force Geometry Sidecar if needed
        if has_spatial_filter:
            # We use ST_SRID as a transformation to trigger sidecar join in QueryOptimizer
            # alias starts with _ to indicate internal use
            # Use 'geom' to avoid ambiguity with attributes.geometry JSONB
            query_selects.append(
                FieldSelection(field="geom", transformation="ST_SRID", alias="_srid")
            )

        # Hierarchy SQL (recursive CTE usually) -> Handled via raw_where below if present

        # Attribute Filters -> Attributes Sidecar
        if search_request.ids:
            # Use "id" field (aliased external_id)
            # Operator "IN" with tuple.
            query_filters.append(
                FilterCondition(
                    field="id",
                    operator=FilterOperator.IN,
                    value=list(search_request.ids),
                )
            )

        # Datetime is usually handled in where_sql (from build_filter_clause).
        # We added where_sql to raw_where.
        # But we need to ensure Attributes sidecar is joined if datetime filter exists.

        # Use the pre-computed where_sql from build_filter_clause as raw_where
        # This includes datetime, cql_filter logic.

        combined_raw_where = []
        raw_where_clauses_str = " AND ".join(raw_where_clauses)
        if raw_where_clauses_str:
            combined_raw_where.append(raw_where_clauses_str)

        if where_sql:
            # Prefixes deleted_at to avoid ambiguity
            where_sql = where_sql.replace("deleted_at IS NULL", "h.deleted_at IS NULL")
            combined_raw_where.append(where_sql)

        # --- Optimization Mode: Spatial Only ---

        # Check if we need Attributes Sidecar based on filters
        req_attributes = bool(
            search_request.ids
            or search_request.datetime
            or search_request.filter
            or "attributes" in where_sql
            or "external_id" in where_sql
            or "validity" in where_sql
        )

        req_stac = bool(
            "stac_" in where_sql
            or "external_extensions" in where_sql
            or "external_assets" in where_sql
        )

        # Per-collection validity flag (#1083): when the attributes sidecar
        # does not materialise a ``validity`` column (``enable_validity=False``,
        # the post-#974 default), selecting ``validity`` mis-resolves to the
        # JSONB text accessor ``attributes->>'validity'`` and the datetime
        # filter 500s with a ``text >= timestamptz`` type error. Sort by the
        # item's own ``datetime`` instead, and let the bare-``validity`` token
        # in the WHERE rewrite to ``NULL::tstzrange`` (match-all).
        from dynastore.modules.storage.drivers.pg_sidecars import (
            driver_sidecars as _driver_sidecars,
        )
        coll_has_validity = any(
            getattr(sc, "enable_validity", False)
            for sc in _driver_sidecars(config)
            if sc.enabled
            and getattr(sc, "sidecar_type", "") in ("attributes", "feature_attributes")
        )

        # Select & Sort Strategy
        if req_attributes:
            if coll_has_validity:
                # Standard Mode: Sort by Attributes (validity)
                query_selects.append(
                    FieldSelection(field="validity", alias="valid_from")
                )
            else:
                # No validity column → sort by the item's datetime.
                raw_selects.append(
                    "(sc_attributes.attributes->>'datetime')::timestamptz as valid_from"
                )
            # Use raw select for ID coalescence
            # QueryOptimizer uses sc_attributes for the attributes sidecar
            raw_selects.append(
                "COALESCE(sc_attributes.external_id, h.geoid::text) as id"
            )

        elif not req_stac:
            # Optimization: Spatial Only Mode (and no STAC)
            # Sort by Hub columns
            query_selects.append(
                FieldSelection(field="transaction_time", alias="valid_from")
            )  # Hub field
            raw_selects.append("h.geoid::text as id")  # Hub field
        else:
            # Fallback
            query_selects.append(
                FieldSelection(field="transaction_time", alias="valid_from")
            )
            raw_selects.append("h.geoid::text as id")

        # Construct QueryRequest
        query_req = QueryRequest(
            select=query_selects,
            raw_selects=raw_selects,
            filters=query_filters,
            raw_where=" AND ".join(combined_raw_where) if combined_raw_where else None,
            raw_params=query_params,
            limit=None,
            offset=None,
        )

        try:
            _driver = await get_driver(Operation.READ, cat_id, collection_id)
            _location = await _driver.location(cat_id, collection_id)
            phys_table = _location.identifiers.get("table")
        except (ValueError, Exception):
            phys_table = None
        if not phys_table or not phys_schema:
            continue

        # Build SQL
        # Use collection_id as param_prefix to avoid collisions in UNION ALL
        # We sanitize the prefix because it's used in SQL param names (must be alphanumeric)
        safe_prefix = re.sub(r"[^a-zA-Z0-9]", "_", collection_id)

        try:
            sql_fragment, fragment_params = optimizer.build_optimized_query(
                query_req,
                schema=phys_schema,
                table=phys_table,
                param_prefix=safe_prefix,
            )

            # Correct any hardcoded 's.' aliases in the fragment if they survived (should be sc_attributes)
            # Actually we fixed them in raw_selects above.

            candidate_fragments.append(sql_fragment)

            # Merge fragment params into the main execution params
            if fragment_params:
                params.update(fragment_params)

            # Update mapping for hydration
            sidecar_table = f"{phys_table}_attributes"
            geom_table = f"{phys_table}_geometries"
            item_meta_table = f"{phys_table}_item_metadata"
            stac_meta_table = f"{phys_table}_stac_metadata"

            has_attr_sidecar = False
            has_geom_sidecar = False
            has_item_meta_sidecar = False
            has_stac_meta_sidecar = False
            # Sidecars are a PG-driver-internal concept; for non-PG
            # driver configs (Elasticsearch, DuckDB, Iceberg, …) the
            # helper returns [] and the sidecar-shaped hydration is
            # simply skipped.
            from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars
            for sc in driver_sidecars(config):
                if not sc.enabled:
                    continue
                stype = getattr(sc, "sidecar_type", "")
                if stype in ["attributes", "feature_attributes"]:
                    has_attr_sidecar = True
                    collection_validity_enabled[collection_id] = bool(
                        getattr(sc, "enable_validity", False)
                    )
                elif stype in ["geometries", "geometry"]:
                    has_geom_sidecar = True
                elif stype == "item_metadata":
                    has_item_meta_sidecar = True
                elif stype == "stac_metadata":
                    has_stac_meta_sidecar = True

            collection_table_map[collection_id] = (
                phys_table,
                sidecar_table,
                geom_table,
                item_meta_table,
                stac_meta_table,
                has_attr_sidecar,
                has_geom_sidecar,
                has_item_meta_sidecar,
                has_stac_meta_sidecar,
            )

        except Exception as e:
            logger.error(
                f"Failed to build optimized query for collection {collection_id}: {e}"
            )
            import traceback

            logger.error(traceback.format_exc())
            continue

    if not candidate_fragments:
        return [], 0, None

    full_union_query = " UNION ALL ".join(candidate_fragments)

    # Merged paging + count using a window function (1 query instead of 2)
    paging_query = f"""
        WITH candidate_matches AS ({full_union_query})
        SELECT catalog_id, collection_id, geoid, valid_from, id,
               COUNT(*) OVER() AS _total_count
        FROM candidate_matches
        ORDER BY valid_from DESC, id
        LIMIT :limit OFFSET :offset
    """

    final_params = {
        "limit": search_request.limit,
        "offset": search_request.offset,
        **params,
    }

    logger.debug(f"EXECUTE PAGING+COUNT QUERY: {paging_query}")
    page_rows = await DQLQuery(
        paging_query, result_handler=ResultHandler.ALL_DICTS
    ).execute(db_resource, **final_params)

    if not page_rows:
        return [], 0, None

    total = int(page_rows[0]["_total_count"])

    # Hydration Phase: Single UNION ALL across all matching collections
    # Build one fragment per collection, combined into one query.
    simplification_sql = _get_simplification_sql(stac_config)

    from collections import defaultdict

    ids_by_collection: dict = defaultdict(list)
    for row in page_rows:
        ids_by_collection[row["collection_id"]].append(row["geoid"])

    hydration_fragments = []
    hydration_params: dict = {}

    for coll_idx, (coll_id, geoids) in enumerate(ids_by_collection.items()):
        if coll_id not in collection_table_map:
            continue

        (
            p_tab,
            s_tab,
            g_tab,
            im_tab,
            sm_tab,
            has_attr,
            has_geom,
            has_item_meta,
            has_stac_meta,
        ) = collection_table_map[coll_id]

        geoid_param = f"geoids_{coll_idx}"
        hydration_params[geoid_param] = geoids

        # Issue #220: geometry_hash now lives on the geometries sidecar.
        # Project ``g.geometry_hash`` when the geometries sidecar is
        # joined below (``has_geom``); otherwise emit NULL to keep the
        # column shape stable.
        select_parts = [
            f"SELECT '{cat_id}' as catalog_id,"
            f" '{coll_id}' as collection_id,"
            f" h.geoid, h.transaction_time"
        ]
        joins = [f'FROM "{phys_schema}"."{p_tab}" h']

        if has_attr:
            # #974: ``validity`` only exists on the attributes sidecar when
            # ``ItemsWritePolicy.enable_validity`` is True. When disabled the
            # column is absent, so fall back to the hub transaction_time for
            # ``valid_from`` and NULL bounds rather than emitting ``s.validity``.
            if collection_validity_enabled.get(coll_id, False):
                validity_select = (
                    ", lower(s.validity) as valid_from"
                    ", upper(s.validity) as valid_to"
                )
            else:
                validity_select = (
                    ", h.transaction_time as valid_from"
                    ", null::timestamptz as valid_to"
                )
            select_parts.append(
                ", COALESCE(s.external_id, h.geoid::text) as id"
                ", s.external_id"
                f"{validity_select}"
                ", s.attributes, s.asset_id"
            )
            joins.append(f'LEFT JOIN "{phys_schema}"."{s_tab}" s ON h.geoid = s.geoid')
        else:
            select_parts.append(
                ", h.geoid::text as id, h.geoid::text as external_id"
                ", h.transaction_time as valid_from, null as valid_to"
                ", '{}'::jsonb as attributes, null as asset_id"
            )

        if has_geom:
            select_parts.append(
                f", g.geometry_hash"
                f", ST_AsEWKB(ST_SimplifyPreserveTopology(g.geom, {simplification_sql})) AS simplified_geom"
                f", ST_XMin(g.geom) as bbox_xmin, ST_YMin(g.geom) as bbox_ymin"
                f", ST_XMax(g.geom) as bbox_xmax, ST_YMax(g.geom) as bbox_ymax"
            )
            joins.append(f'LEFT JOIN "{phys_schema}"."{g_tab}" g ON h.geoid = g.geoid')
        else:
            select_parts.append(
                ", null as geometry_hash"
                ", null as simplified_geom"
                ", null as bbox_xmin, null as bbox_ymin"
                ", null as bbox_xmax, null as bbox_ymax"
            )

        if has_item_meta:
            select_parts.append(
                ", im.title as stac_title, im.description as stac_description"
                ", im.keywords as stac_keywords"
            )
            joins.append(f'LEFT JOIN "{phys_schema}"."{im_tab}" im ON h.geoid = im.geoid')
        else:
            select_parts.append(
                ", null as stac_title, null as stac_description"
                ", null as stac_keywords"
            )

        if has_stac_meta:
            select_parts.append(
                ", sm.external_extensions, sm.external_assets"
                ", sm.extra_fields as stac_extra_fields"
            )
            joins.append(f'LEFT JOIN "{phys_schema}"."{sm_tab}" sm ON h.geoid = sm.geoid')
        else:
            select_parts.append(
                ", null as external_extensions, null as external_assets"
                ", null as stac_extra_fields"
            )

        fragment = (
            "".join(select_parts)
            + " " + " ".join(joins)
            + f" WHERE h.geoid = ANY(CAST(:{geoid_param} AS UUID[]))"
        )
        hydration_fragments.append(fragment)

    hydrated_items = {}
    if hydration_fragments:
        hydration_sql = " UNION ALL ".join(hydration_fragments)
        logger.debug(f"EXECUTE HYDRATION QUERY: {hydration_sql}")
        all_results = await DQLQuery(
            hydration_sql, result_handler=ResultHandler.ALL_DICTS
        ).execute(db_resource, **hydration_params)
        for item in all_results:
            hydrated_items[(item["collection_id"], item["geoid"])] = item


    # Reconstruct sorted list
    rows = []
    
    from dynastore.models.protocols import ItemsProtocol
    items_svc = get_protocol(ItemsProtocol)
    
    for row in page_rows:
        key = (row["collection_id"], row["geoid"])
        if key in hydrated_items:
            item_data = hydrated_items[key]
            if item_data.get("simplified_geom"):
                item_data["geom"] = item_data.pop("simplified_geom")
            else:
                item_data["geom"] = None
            
            if items_svc:
                col_config = collection_configs.get(item_data["collection_id"])
                # We need to preserve original `item_data` values for sidecars (e.g., stac_title/stac_description)
                # ItemsProtocol will place these nicely into `feature.properties` or `feature.stac_extensions` etc.
                feature = items_svc.map_row_to_feature(item_data, col_config=col_config)
                if feature:
                    if feature.properties is None:
                        feature.properties = {}
                    feature.properties["_catalog_id"] = item_data["catalog_id"]
                    feature.properties["_collection_id"] = item_data["collection_id"]
                    rows.append(feature)
            else:
                rows.append(item_data)

    # Aggregations
    aggregation_results = None
    if search_request.aggregations:
        from dynastore.extensions.stac.stac_aggregations import execute_aggregations

        # Hints for joins in aggregation queries (frozenset for deterministic cache keys)
        _hints: set = set()
        # If we have attribute filters, we likely need attributes sidecar
        if "attributes" in where_sql or "properties" in str(search_request.filter):
            _hints.add("attributes")
        # If we have spatial filters, we likely need geometry sidecar
        if "geom" in where_sql or search_request.bbox or search_request.intersects:
            _hints.add("geometry")
        hints = frozenset(_hints)

        if isinstance(db_resource, AsyncEngine):
            async with db_resource.connect() as agg_conn:
                aggregation_results = await execute_aggregations(
                    agg_conn,
                    cat_id,
                    target_collections,
                    search_request.aggregations,
                    where_sql,
                    params,
                    filter_hints=hints,
                )
        else:
            aggregation_results = await execute_aggregations(
                db_resource,
                cat_id,
                target_collections,
                search_request.aggregations,
                where_sql,
                params,
                filter_hints=hints,
            )

    return rows, total, aggregation_results


async def search_collections(
    db_resource: DbResource, search_request: CollectionSearchRequest
) -> Tuple[List[Collection], int]:
    # Determine which schemas to search
    target_schemas = []

    # Resolve effective catalog filter (catalog_ids list takes precedence when set)
    effective_catalog_ids = []
    if search_request.catalog_id:
        effective_catalog_ids = [search_request.catalog_id]
    elif search_request.catalog_ids:
        effective_catalog_ids = search_request.catalog_ids

    if effective_catalog_ids:
        from dynastore.models.protocols import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        assert catalogs is not None, "CatalogsProtocol not registered"
        for cid in effective_catalog_ids:
            schema = await catalogs.resolve_physical_schema(cid, ctx=DriverContext(db_resource=db_resource))
            if schema:
                target_schemas.append(schema)
        # If none of the catalog_ids resolved, implies no results
    else:
        # Search all catalogs
        # Retrieve all physical schemas from catalog.catalogs
        # We query the catalog registry directly to find all active physical schemas
        catalog_query = (
            "SELECT physical_schema FROM catalog.catalogs WHERE deleted_at IS NULL"
        )
        try:
            target_schemas = await DQLQuery(
                catalog_query, result_handler=ResultHandler.ALL_SCALARS
            ).execute(db_resource)
        except Exception as e:
            logger.warning(f"Failed to retrieve catalog schemas for global search: {e}")
            return [], 0

    if not target_schemas:
        return [], 0

    where_clauses = ["deleted_at IS NULL"]
    params = {}

    # Common filters — catalog_id / catalog_ids filtering is handled via schema UNION above

    if search_request.ids:
        where_clauses.append("id = ANY(:ids)")
        params["ids"] = search_request.ids

    if search_request.keywords:
        where_clauses.append("mc.keywords @> CAST(:keywords AS jsonb)")
        params["keywords"] = str(search_request.keywords).replace("'", '"')

    if search_request.bbox:
        where_clauses.append(
            """EXISTS (SELECT 1 FROM jsonb_array_elements(ms.extent->'spatial'->'bbox') AS bbox WHERE ST_Intersects(ST_MakeEnvelope((bbox->>0)::float, (bbox->>1)::float, (bbox->>2)::float, (bbox->>3)::float, 4326), ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326)))"""
        )
        params.update(
            {
                "xmin": search_request.bbox[0],
                "ymin": search_request.bbox[1],
                "xmax": search_request.bbox[2],
                "ymax": search_request.bbox[3],
            }
        )

    where_sql = " AND ".join(where_clauses)

    # Each subquery JOINs both domain-scoped metadata tables (CORE + STAC).
    # The collection-tier M2.5 hard cut replaced the legacy monolithic
    # ``{schema}.collection_metadata`` with ``collection_core``
    # (title / description / keywords / license / extra_metadata) and
    # ``collection_stac`` (links / assets / extent / providers /
    # summaries / item_assets).  Text-match filters land on the CORE
    # alias (``mc``), spatial filters on the STAC alias (``ms``).
    _meta_cols = (
        "c.id, c.catalog_id, "
        "mc.title, mc.description, mc.keywords, mc.license, "
        "ms.links, ms.assets, ms.extent, ms.providers, ms.summaries, "
        "ms.item_assets, mc.extra_metadata"
    )
    union_queries = []
    for schema in target_schemas:
        union_queries.append(
            f'SELECT {_meta_cols} '
            f'FROM "{schema}".collections c '
            f'LEFT JOIN "{schema}".collection_core mc ON mc.collection_id = c.id '
            f'LEFT JOIN "{schema}".collection_stac ms ON ms.collection_id = c.id '
            f'WHERE {where_sql}'
        )

    full_union_query = " UNION ALL ".join(union_queries)

    # use CTE to encapsulate the union for COUNT and paging
    base_query_cte = f"WITH all_matches AS ({full_union_query})"

    count_query = f"{base_query_cte} SELECT count(*) FROM all_matches"

    order_by = _parse_collection_sort_sql(search_request.sortby, search_request.lang)
    data_query = (
        f"{base_query_cte} SELECT * FROM all_matches ORDER BY {order_by} LIMIT :limit OFFSET :offset"
    )

    final_params = {
        "limit": search_request.limit,
        "offset": search_request.offset,
        **params,
    }

    # Execute sequentially
    try:
        count_result = await DQLQuery(
            count_query, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
        ).execute(db_resource, **params)

        if not count_result:
            return [], 0

        rows_result = await DQLQuery(
            data_query, result_handler=ResultHandler.ALL_DICTS
        ).execute(db_resource, **final_params)

        total_count = count_result
        collections = [Collection.model_validate(row) for row in rows_result]
        return collections, total_count
    except Exception as e:
        logger.error(f"Error executing collection search: {e}")
        raise
