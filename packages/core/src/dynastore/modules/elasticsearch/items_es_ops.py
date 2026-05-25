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

"""
Shared async helpers for items-tier Elasticsearch drivers.

Both the public ``ItemsElasticsearchDriver`` (per-tenant
``{prefix}-{cat}-items``) and the private ``ItemsElasticsearchPrivateDriver``
(per-tenant ``{prefix}-geoid-{cat}``) implement the same capability surface
(COUNT, STATISTICS, AGGREGATION, INTROSPECTION). The implementations only
differ in the index name and the routing key — the ES query shape is
identical. This module hosts the four data-side operations as pure
functions so each driver can delegate without duplicating the query DSL.

These helpers use the shared async ES client from
:mod:`dynastore.modules.elasticsearch.client`.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from dynastore.models.protocols.field_definition import FieldCapability

logger = logging.getLogger(__name__)


def _scope_query(
    query: Optional[Dict[str, Any]] = None,
    *,
    collection: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a bool query that ANDs the caller's optional query with a
    ``term: collection`` filter when a collection scope is given.

    Both items drivers store the parent collection id under the
    ``collection`` field (STAC-native; matches what bulk_reindex writes
    and what the live driver writes via the event-listener path).
    """
    base = query or {"match_all": {}}
    if not collection:
        return base
    return {
        "bool": {
            "must": [base],
            "filter": [{"term": {"collection": collection}}],
        }
    }


async def es_count_items(
    es: Any,
    index_name: str,
    *,
    query: Optional[Dict[str, Any]] = None,
    collection: Optional[str] = None,
    routing: Optional[str] = None,
) -> int:
    """Return the doc count for ``index_name`` matching ``query``.

    Returns 0 if the index does not exist or the request fails — same
    contract as the Protocol's ``count_entities``.
    """
    body = {"query": _scope_query(query, collection=collection)}
    params: Dict[str, Any] = {"ignore_unavailable": "true"}
    if routing:
        params["routing"] = routing
    try:
        resp = await es.count(index=index_name, body=body, params=params)
        return int(resp.get("count", 0))
    except Exception as exc:
        logger.warning(
            "es_count_items: count failed on %s (collection=%s): %s",
            index_name, collection, exc,
        )
        return 0


async def es_extents(
    es: Any,
    index_name: str,
    *,
    collection: Optional[str] = None,
    routing: Optional[str] = None,
    geometry_field: str = "geometry",
    datetime_field: str = "properties.datetime",
) -> Optional[Dict[str, Any]]:
    """Compute spatial + temporal extents via ES aggregations.

    Returns ``{"spatial": {"bbox": [[w, s, e, n]]},
              "temporal": {"interval": [[start, end]]}}``
    or ``None`` when no documents match the scope.

    Spatial bounds use ``geo_bounds`` aggregation on ``geometry``;
    temporal bounds use ``min``/``max`` on ``properties.datetime``.
    Either part can be missing — fields the index doesn't carry simply
    don't contribute to the result.
    """
    body: Dict[str, Any] = {
        "size": 0,
        "query": _scope_query(None, collection=collection),
        "aggs": {
            "_bbox": {"geo_bounds": {"field": geometry_field}},
            "_dt_min": {"min": {"field": datetime_field}},
            "_dt_max": {"max": {"field": datetime_field}},
        },
    }
    params: Dict[str, Any] = {"ignore_unavailable": "true"}
    if routing:
        params["routing"] = routing
    try:
        resp = await es.search(index=index_name, body=body, params=params)
    except Exception as exc:
        logger.warning(
            "es_extents: aggregation failed on %s (collection=%s): %s",
            index_name, collection, exc,
        )
        return None

    aggs = resp.get("aggregations", {}) or {}
    out: Dict[str, Any] = {}

    bbox_agg = aggs.get("_bbox", {}).get("bounds")
    if bbox_agg:
        tl = bbox_agg.get("top_left") or {}
        br = bbox_agg.get("bottom_right") or {}
        if tl and br:
            out["spatial"] = {
                "bbox": [[tl.get("lon"), br.get("lat"),
                          br.get("lon"), tl.get("lat")]]
            }

    dt_min = aggs.get("_dt_min", {}).get("value_as_string") or aggs.get("_dt_min", {}).get("value")
    dt_max = aggs.get("_dt_max", {}).get("value_as_string") or aggs.get("_dt_max", {}).get("value")
    if dt_min or dt_max:
        out["temporal"] = {"interval": [[dt_min, dt_max]]}

    return out or None


async def es_aggregate(
    es: Any,
    index_name: str,
    *,
    aggregation_type: str,
    field: Optional[str] = None,
    query: Optional[Dict[str, Any]] = None,
    collection: Optional[str] = None,
    routing: Optional[str] = None,
    size: int = 50,
) -> Any:
    """Run a single-bucket aggregation over the items index.

    Supports the aggregation_type values documented on
    ``CollectionItemsStore.aggregate``:

      - ``terms`` — top-N unique values + counts (requires ``field``)
      - ``stats`` — min/max/avg/sum/count over a numeric field (requires ``field``)
      - ``cardinality`` — approximate distinct count (requires ``field``)
      - ``histogram`` — fixed-interval histogram (requires ``field``)

    Other types fall through with ``ValueError``.
    """
    if aggregation_type in {"terms", "stats", "cardinality", "histogram"} and not field:
        raise ValueError(
            f"es_aggregate: aggregation_type={aggregation_type!r} requires a 'field' argument."
        )

    if aggregation_type == "terms":
        agg: Dict[str, Any] = {"terms": {"field": field, "size": size}}
    elif aggregation_type == "stats":
        agg = {"stats": {"field": field}}
    elif aggregation_type == "cardinality":
        agg = {"cardinality": {"field": field}}
    elif aggregation_type == "histogram":
        agg = {"histogram": {"field": field, "interval": 1}}
    else:
        raise ValueError(
            f"es_aggregate: unsupported aggregation_type={aggregation_type!r}; "
            "expected one of terms, stats, cardinality, histogram."
        )

    body: Dict[str, Any] = {
        "size": 0,
        "query": _scope_query(query, collection=collection),
        "aggs": {"_agg": agg},
    }
    params: Dict[str, Any] = {"ignore_unavailable": "true"}
    if routing:
        params["routing"] = routing
    try:
        resp = await es.search(index=index_name, body=body, params=params)
    except Exception as exc:
        logger.warning(
            "es_aggregate: %s on %s.%s (collection=%s) failed: %s",
            aggregation_type, index_name, field, collection, exc,
        )
        return None
    return resp.get("aggregations", {}).get("_agg")


# ES type → canonical data_type (see ``dynastore.models.field_types``). ES has
# no date-only type — its ``date`` is an instant → ``timestamp``; ``object`` /
# ``nested`` map to ``jsonb`` (no canonical object type).
_ES_TYPE_TO_DATA_TYPE = {
    "text": "string", "keyword": "string", "ip": "string",
    "long": "bigint", "unsigned_long": "bigint",
    "integer": "integer", "short": "integer", "byte": "integer",
    "float": "double", "double": "double", "half_float": "double", "scaled_float": "double",
    "date": "timestamp", "date_nanos": "timestamp",
    "boolean": "boolean", "binary": "binary",
    "geo_point": "geometry", "geo_shape": "geometry",
    "object": "jsonb", "nested": "jsonb",
}

# ES type → queryable capabilities, co-located with the canonical-type table so
# the two can never drift apart (they must share the same key set — see the SSOT
# test). ``unsigned_long`` is aggregatable like the other numerics; ``binary``
# carries no query caps (you cannot filter/sort/aggregate a blob).
_NUMERIC_CAPS = [
    FieldCapability.FILTERABLE,
    FieldCapability.SORTABLE,
    FieldCapability.AGGREGATABLE,
]
_ES_TYPE_TO_CAPS: Dict[str, List[FieldCapability]] = {
    "text": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
    "keyword": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.GROUPABLE],
    "ip": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
    "long": _NUMERIC_CAPS,
    "unsigned_long": _NUMERIC_CAPS,
    "integer": _NUMERIC_CAPS,
    "short": _NUMERIC_CAPS,
    "byte": _NUMERIC_CAPS,
    "float": _NUMERIC_CAPS,
    "double": _NUMERIC_CAPS,
    "half_float": _NUMERIC_CAPS,
    "scaled_float": _NUMERIC_CAPS,
    "date": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
    "date_nanos": [FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
    "boolean": [FieldCapability.FILTERABLE],
    "binary": [],
    "geo_point": [FieldCapability.SPATIAL],
    "geo_shape": [FieldCapability.SPATIAL],
    "object": [FieldCapability.FILTERABLE],
    "nested": [FieldCapability.FILTERABLE],
}


async def es_introspect_mapping(
    es: Any,
    index_name: str,
    *,
    skip_internal_prefix: str = "_",
) -> List[Any]:
    """Introspect the index mapping into ``FieldDefinition`` instances.

    Returns an empty list if the index does not exist or the mapping
    cannot be read. Internal dynastore-prefixed fields (``_asset_id``,
    ``_external_id``, ``_valid_from``, ``_valid_to`` …) are skipped so
    they don't surface to OGC queryables consumers.
    """
    from dynastore.models.protocols.field_definition import (
        FieldDefinition as ProtocolFieldDefinition,
    )

    try:
        mapping = await es.indices.get_mapping(index=index_name)
    except Exception as exc:
        logger.warning(
            "es_introspect_mapping: get_mapping failed on %s: %s",
            index_name, exc,
        )
        return []

    properties: Dict[str, Any] = {}
    for _, idx_data in mapping.items():
        properties = idx_data.get("mappings", {}).get("properties", {}) or {}
        break

    out: List[Any] = []
    for name, info in properties.items():
        if name.startswith(skip_internal_prefix):
            continue
        es_type = (info or {}).get("type", "object")
        caps = _ES_TYPE_TO_CAPS.get(es_type, [FieldCapability.FILTERABLE])
        out.append(
            ProtocolFieldDefinition(
                name=name,
                data_type=_ES_TYPE_TO_DATA_TYPE.get(es_type, "string"),
                capabilities=caps,
            )
        )
    return out
