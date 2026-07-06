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
import re
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field

from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
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

# Internal columns produced by the STAC search hydration query that must
# never surface in the serialized item properties.  These are SQL aliases
# for JOIN-derived values (bbox envelope components, temporal sort anchor,
# STAC sidecar columns, geometry hash) that the sidecar exclusion lists do
# not cover because the hydration query generates them outside the normal
# per-sidecar get_internal_columns() contract.
_HYDRATION_INTERNAL_FIELDS: frozenset = frozenset({
    "valid_from",
    "valid_to",
    "bbox_xmin",
    "bbox_ymin",
    "bbox_xmax",
    "bbox_ymax",
    "stac_title",
    "stac_description",
    "stac_keywords",
    "stac_extra_fields",
    "external_assets",
    "external_extensions",
    "geometry_hash",
    "_total_count",
})


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
    # ``None`` means "use the configured default"; any supplied value is
    # clamped to the configured maximum rather than rejected (OGC API -
    # Features Part 1 Core /req/core/fc-limit-response-1). Resolved in
    # ``search_items()`` against ``StacPluginConfig.default_limit`` /
    # ``max_limit``.
    limit: Optional[int] = Field(None, ge=1)
    offset: int = Field(0, ge=0)
    aggregations: Optional[List[AggregationRule]] = Field(None, alias="aggregate")
    sortby: Optional[List[str]] = Field(
        None,
        description=(
            "Sort fields. Each entry is a field name optionally prefixed with "
            "'+' for ascending (default) or '-' for descending. "
            "GET form also accepts a single comma-separated string. "
            "Example POST: [\"+datetime\", \"-id\"]. "
            "Example GET: \"+datetime,-id\"."
        ),
    )


class CollectionSearchRequest(BaseModel):
    catalog_id: Optional[str] = None
    catalog_ids: Optional[List[str]] = None
    ids: Optional[List[str]] = None
    bbox: Optional[Tuple[float, float, float, float]] = None
    datetime: Optional[str] = None
    keywords: Optional[List[str]] = None
    # STAC Collection Search extension: free-text query param `q`
    q: Optional[List[str]] = Field(
        None,
        description=(
            "Free-text search terms. Terms are matched case-insensitively against "
            "collection id, title, description, and keywords. "
            "GET form accepts a comma-separated string; POST accepts a list."
        ),
    )
    # See ``ItemSearchRequest.limit`` — same "clamp, don't reject" contract,
    # resolved in ``search_collections()``.
    limit: Optional[int] = Field(None, ge=1)
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


def _build_cql2_field_mapping(optimizer: QueryOptimizer) -> Dict[str, Any]:
    """Build a storage-mode-aware field_mapping for the CQL2 parser.

    Mirrors the ``/items`` path (``item_query._apply_query_transformations``):
    each declared queryable resolves to ``literal_column(field_def.sql_expression)``
    so pygeofilter emits a real bound-parameter comparison against the correct
    physical column — ``sc_attributes."col"`` for COLUMNAR storage, the JSONB
    accessor for JSONB storage — instead of a hard-coded ``attributes->>'col'``
    that is wrong for COLUMNAR collections (declared queryables are physical
    columns post #1065/#1074). A ``text()`` TextClause has no comparison
    operators and silently collapses every predicate to an always-false ``1=0``;
    ``literal_column`` does not.

    Property names are validated against this mapping's keys by ``parse_cql_filter``
    (unknown → ``ValueError`` → HTTP 400) and values are bound as parameters.
    """
    from sqlalchemy import literal_column
    return {
        name: literal_column(field_def.sql_expression)
        for name, field_def in optimizer.get_all_queryable_fields().items()
    }


def _attributes_hydration_projection(attr_sidecar_config: Any) -> Tuple[str, bool]:
    """Return ``(sql, is_columnar)`` for the hydration ``attributes`` projection.

    Takes the attributes sidecar **config** (as produced by ``driver_sidecars``)
    and wraps it in the runtime sidecar to reuse its storage-mode resolution.

    The hydration is a ``UNION ALL`` across collections, so every fragment must
    expose the same ``attributes`` column shape. JSONB sidecars keep all
    properties in a single blob column (``jsonb_column_name``, default
    ``attributes``), projected directly. COLUMNAR sidecars store each declared
    property as its own physical column and have **no** ``attributes`` column —
    referencing ``s.attributes`` there raises ``UndefinedColumnError`` — so the
    blob is rebuilt with ``jsonb_build_object`` over the declared property
    columns (identity/stat columns are excluded by ``get_property_field_names``),
    keeping the UNION-ALL shape uniform. A COLUMNAR sidecar with no declared
    properties yields an empty object, matching the JSONB empty-blob shape.

    ``is_columnar`` is returned so the row mapper can splat the rebuilt blob back
    into individual row keys: the COLUMNAR attribute mapper reads ``row[col]``,
    not the blob, so the reconstructed values must be exposed under their column
    names for properties to populate.
    """
    from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
        FeatureAttributeSidecar,
    )
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeStorageMode,
    )

    sidecar = FeatureAttributeSidecar(attr_sidecar_config)
    if sidecar.resolved_storage_mode == AttributeStorageMode.COLUMNAR:
        names = sidecar.get_property_field_names()
        if not names:
            return "'{}'::jsonb", True
        pairs = ", ".join(f"'{n}', s.\"{n}\"" for n in names)
        return f"jsonb_build_object({pairs})", True
    return f's."{attr_sidecar_config.jsonb_column_name}"', False

async def _translate_filter_to_es(
    cat_id: str,
    cids: List[str],
    filt: Union[AttributeFilter, QueryFilter],
    driver: Any,
) -> Optional[Dict[str, Any]]:
    """Translate a STAC search ``filter`` to an Elasticsearch Query DSL clause.

    Reuses the same queryables SSOT the PostgreSQL path uses
    (``QueryOptimizer.get_all_queryable_fields`` via the collection's
    ``ItemsPostgresqlDriverConfig`` — the config waterfall is driver-agnostic),
    builds the ES field mapping (``build_es_field_mapping``; private flat doc vs
    public extras-bucket selected from the driver's ``TENANT_ISOLATED``
    capability), converts the filter to CQL2-JSON, parses it to a pygeofilter
    AST, and emits a plain ES clause via the shared translator.

    Returns ``None`` (→ caller falls back to the PG path, unchanged behaviour)
    on ANY failure: missing config/protocols, an unknown queryable property, or
    an unsupported operator. The first scoped collection drives the mapping,
    consistent with the PG path (``collection_configs[target_collections[0]]``).
    """
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.models.protocols.storage_driver import Capability
        from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
        from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
        from dynastore.modules.storage.drivers.es_common import (
            build_es_field_mapping,
            build_es_fulltext_mapping,
            cql_ast_to_es_query,
        )
        from dynastore.models.driver_context import DriverContext
        from pygeofilter.parsers.cql2_json import parse as parse_cql2_json

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return None
        col_config = await configs.get_config(
            ItemsPostgresqlDriverConfig,
            catalog_id=cat_id,
            collection_id=cids[0],
            ctx=DriverContext(db_resource=None),
        )
        if col_config is None:
            return None

        optimizer = QueryOptimizer(col_config, consumer=ConsumerType.STAC)
        private = Capability.TENANT_ISOLATED in getattr(
            driver, "capabilities", frozenset()
        )
        queryable_fields = optimizer.get_all_queryable_fields()
        field_mapping = build_es_field_mapping(queryable_fields, private=private)
        fulltext_mapping = build_es_fulltext_mapping(queryable_fields, private=private)

        cql2_json = _filter_to_cql2_json(filt)
        ast_node = parse_cql2_json(cql2_json)
        return cql_ast_to_es_query(ast_node, field_mapping, fulltext_mapping)
    except Exception as exc:
        logger.debug(
            "STAC search → CQL2-ES translation skipped (catalog=%s, "
            "collections=%s): %s — falling back to PG path.",
            cat_id, cids, exc,
        )
        return None


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


# Safe item sort fields available in the candidate_matches CTE columns:
# valid_from (timestamptz), id (text), catalog_id (text), collection_id (text)
_ITEM_DIRECT_SORT_COLUMNS = frozenset({"valid_from", "id", "catalog_id", "collection_id"})
_ITEM_SORT_ALIASES: dict = {"datetime": "valid_from"}

# Allowed STAC property key characters: letters, digits, underscore, hyphen, colon.
# Colon is required for extension-namespaced keys such as ``eo:cloud_cover``.
# This regex is intentionally more restrictive than the full JSON key spec to
# block SQL-injection attempts while covering the realistic STAC namespace corpus.
_SAFE_STAC_PROPERTY_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_:\-]*$')


class _ItemSortEntry:
    """One parsed sortby entry for item search, with PG and ES resolution."""

    __slots__ = ("order_by_expr", "pg_alias", "pg_raw_select_template", "direction", "raw_field")

    def __init__(
        self,
        order_by_expr: str,
        direction: str,
        raw_field: str,
        pg_alias: Optional[str] = None,
        pg_raw_select_template: Optional[str] = None,
    ) -> None:
        self.order_by_expr = order_by_expr
        self.direction = direction
        self.raw_field = raw_field
        self.pg_alias = pg_alias
        self.pg_raw_select_template = pg_raw_select_template


def _parse_item_sort_entries(sortby: Optional[List[str]]) -> List["_ItemSortEntry"]:
    """Parse the STAC sortby list into resolved sort entries.

    Handles three kinds of sort fields:

    * **Direct CTE columns** — ``valid_from`` (alias ``datetime``), ``id``,
      ``catalog_id``, ``collection_id``: resolved to the bare column name so
      the ``ORDER BY`` clause can reference the ``candidate_matches`` CTE
      projection directly.
    * **``properties.<field>``** — STAC item properties stored as JSONB in the
      attributes sidecar.  ``<field>`` is validated against
      ``_SAFE_STAC_PROPERTY_RE`` (alphanumeric + ``_``, ``-``, ``:``).  A
      deterministic alias ``_sort{i}`` is assigned; the per-collection fragment
      builder must project ``sc_attributes.attributes->>'<field>' AS _sort{i}``
      into each candidate fragment so the outer CTE can reference it.
    * Any other field — rejected with ``ValueError`` (→ HTTP 400).

    Returns an empty list when *sortby* is ``None`` or all entries are empty.
    The caller must fall back to the default order (``valid_from DESC, id``)
    when the list is empty.
    """
    if not sortby:
        return []
    entries: List[_ItemSortEntry] = []
    prop_counter = 0
    for raw_entry in sortby:
        if not raw_entry:
            continue
        direction = "DESC" if raw_entry.startswith("-") else "ASC"
        raw_field = raw_entry.lstrip("+-")
        mapped = _ITEM_SORT_ALIASES.get(raw_field, raw_field)

        if mapped in _ITEM_DIRECT_SORT_COLUMNS:
            entries.append(_ItemSortEntry(
                order_by_expr=f"{mapped} {direction}",
                direction=direction,
                raw_field=raw_field,
            ))
        elif raw_field.startswith("properties."):
            tail = raw_field[len("properties."):]
            if not _SAFE_STAC_PROPERTY_RE.match(tail):
                raise ValueError(
                    f"Invalid item sort field {raw_field!r}: property key "
                    f"{tail!r} contains unsafe characters."
                )
            alias = f"_sort{prop_counter}"
            prop_counter += 1
            # The pg_raw_select_template uses a {sidecar} placeholder replaced
            # per-fragment so callers can substitute the correct table alias.
            entries.append(_ItemSortEntry(
                order_by_expr=f"{alias} {direction}",
                direction=direction,
                raw_field=raw_field,
                pg_alias=alias,
                pg_raw_select_template=(
                    f"{{sidecar}}.attributes->>'{tail}' AS {alias}"
                ),
            ))
        else:
            raise ValueError(
                f"Invalid item sort field {raw_field!r}. "
                f"Supported: {sorted(_ITEM_DIRECT_SORT_COLUMNS | set(_ITEM_SORT_ALIASES))}"
                f" or 'properties.<key>' for STAC item properties."
            )
    return entries


def _parse_item_sort_order_by(sortby: Optional[List[str]]) -> str:
    """Convert the STAC sortby list to a safe SQL ORDER BY clause for item search.

    Each entry in the list is a field name with an optional '+' (ascending, default)
    or '-' (descending) prefix.  The fields are validated against the columns
    projected by the candidate_matches CTE — valid_from, id, catalog_id, collection_id
    — plus the alias 'datetime' which maps to valid_from.  A ``properties.<field>``
    entry resolves to a ``_sort{i}`` alias that each per-collection fragment must
    project (see :func:`_parse_item_sort_entries` and the fragment loop in
    :func:`search_items`).

    Returns the default ``valid_from DESC, id`` order when sortby is empty/None.
    """
    entries = _parse_item_sort_entries(sortby)
    if not entries:
        return "valid_from DESC, id"
    return ", ".join(e.order_by_expr for e in entries)


def _inject_search_hints(
    features: list,
    cat_id: str,
    cids: list,
    coll_ext_id_map: Optional[Dict[str, str]] = None,
) -> list:
    """Inject the catalog/collection hints the STAC search serializer reads.

    ``read_entities`` already returns read-contract Features (extras
    un-nested, empty geometry nulled, internal/echo keys stripped — the
    reconstruction the driver owns), so this only mirrors the
    ``feature.properties["_catalog_id"]`` / ``["_collection_id"]`` hints the
    platform search serializer (``stac_generator.create_search_results_collection``)
    reads — the same hints the PostgreSQL path injects. The collection hint
    prefers the item's own ``collection`` and falls back to the sole requested
    collection when only one is in scope.

    ``coll_ext_id_map`` maps internal collection ids to their public external
    labels; when provided, both the per-feature ``collection`` attribute and
    the ``cids`` fallback are translated before being stored as the hint so the
    STAC item serializer emits the public id on every search result.
    """
    def _ext(internal: str) -> str:
        if coll_ext_id_map:
            return coll_ext_id_map.get(internal, internal)
        return internal

    for feat in features:
        if feat.properties is None:
            feat.properties = {}
        feat.properties.setdefault("_catalog_id", cat_id)
        coll_id = getattr(feat, "collection", None)
        if not coll_id and len(cids) == 1:
            coll_id = cids[0]
        feat.properties.setdefault("_collection_id", _ext(coll_id or ""))
    return features


async def _maybe_dispatch_to_es_search(
    cat_id: str,
    search_request: "ItemSearchRequest",
    *,
    principals: Optional[List[str]] = None,
    principal: Optional[Any] = None,
    coll_ext_id_map: Optional[Dict[str, str]] = None,
    had_explicit_scope: bool = True,
) -> Optional[Tuple[list, int, Optional[Dict[str, Any]]]]:
    """Dispatch a structural search to the catalog's routing-pinned items
    SEARCH driver, when one is configured and search-capable.

    Resolution is fully routing-driven (issue #989): each requested
    collection's items SEARCH driver is resolved via
    :func:`router.get_items_search_driver` (``Operation.SEARCH`` then a
    ``READ`` fallback). The structural query is then dispatched to **that
    driver** through its streaming ``read_entities`` + ``count_entities``
    contract — so a public-ES catalog, a GEOID-style catalog routing SEARCH
    to its tenant-private ES index, or any future ES items driver are all
    served the same way, with no hardcoded driver class.

    Returns ``None`` (caller falls back to the PostgreSQL ``QueryOptimizer``
    path) when the dispatch is not applicable:

    * no collections are scoped;
    * the collections resolve to mixed drivers (can't serve in one query);
    * the resolved driver is a read-primary fallback (PostgreSQL, advertising
      ``Capability.QUERY_FALLBACK_SOURCE``) — i.e. the catalog has no
      dedicated search backend, the routing-aware "fall back to READ" case;
    * the resolved driver is not an ES items driver (no ``is_es_items_driver``);
    * a CQL2 ``filter`` is present but the resolved driver does not advertise
      ``supports_cql_es``, or the filter cannot be translated to ES Query DSL
      (unknown queryable property, unsupported operator) — the CQL path then
      defers to the PG ``QueryOptimizer`` exactly as before.

    Closes the user-visible part of #222 (ES-routed collections returned 0
    items because every search unconditionally built raw PG SQL) and the
    driver-coupling reported on #989 (the fast path hardcoded the public ES
    class and resolved ``Operation.READ`` instead of ``SEARCH``). A CQL2
    ``filter`` is now translated to ES Query DSL (shared CQL2→ES translator,
    queryables SSOT field mapping) and served by the ES driver too, instead of
    forcing the PG path.

    ``had_explicit_scope`` distinguishes a caller-supplied ``collections=``
    filter (bounded by what the caller asked for — the per-collection
    verification loop below is cheap) from an auto-expanded, catalog-wide
    scope (``_expand_collections_for_search`` rewriting a bare ``/search``
    to every collection of the catalog — bounded only by the catalog's
    collection count). For the latter, resolving the driver once per
    collection turned a 1,971-collection catalog's unscoped search into a
    45s+ hang: each resolution is a cache-keyed-per-collection config lookup
    (``router._resolve_driver_ids_cached``), so a cold cache after a large
    harvest meant ~2,000 sequential config round trips before the ES query
    even ran (#2865). An auto-expanded scope instead resolves the driver
    ONCE at catalog scope (``collection_id=None``, the documented
    catalog-level resolution) — the same result a per-collection
    resolution would give in the common case (no per-collection routing
    override), matching the config waterfall's own shape where the
    collection tier is a rare override over the catalog default, not the
    default itself.
    """
    cids = search_request.collections or []
    if not cids:
        return None

    from dynastore.modules.storage.router import get_items_search_driver
    from dynastore.modules.storage.hints import Hint
    from dynastore.models.protocols.storage_driver import Capability
    from dynastore.models.query_builder import QueryRequest

    has_filter = search_request.filter is not None
    # Prefer the attribute-capable ES driver when a filter is present so the
    # router resolves the driver that advertises ``ATTRIBUTE_FILTER``.
    search_hints = frozenset({Hint.ATTRIBUTE_FILTER}) if has_filter else frozenset()

    driver: Any = None
    if had_explicit_scope:
        # Explicit, caller-bounded scope: resolve every collection so a
        # genuinely mixed-driver selection still falls through to PG.
        for cid in cids:
            try:
                resolved = await get_items_search_driver(cat_id, cid, hints=search_hints)
            except Exception:
                return None
            candidate = resolved.driver
            if driver is None:
                driver = candidate
            elif type(candidate) is not type(driver):
                return None  # mixed drivers — PG path
    else:
        # Auto-expanded, catalog-wide scope: one catalog-level resolution
        # instead of one per collection — see the ``had_explicit_scope``
        # note above.
        try:
            resolved = await get_items_search_driver(cat_id, None, hints=search_hints)
        except Exception:
            return None
        driver = resolved.driver

    if driver is None:
        return None

    # A read-primary fallback (PostgreSQL) advertises QUERY_FALLBACK_SOURCE:
    # no dedicated search backend, so defer to the QueryOptimizer SQL path.
    if Capability.QUERY_FALLBACK_SOURCE in getattr(driver, "capabilities", frozenset()):
        return None

    # Only ES items drivers honor the structural QueryRequest dimensions on
    # their streaming read/count path today; anything else → PG path.
    if not getattr(driver, "is_es_items_driver", False):
        return None

    # Routing-honouring fallback (#2894): the ES driver is the configured
    # search primary, but if its per-tenant items index does not exist (a
    # recreated catalog whose index has not been (re)provisioned, or one
    # that never ran the ES-secondary drain) it cannot serve. ``read_entities``
    # / ``count_entities`` both degrade an index-missing failure to an empty
    # result (``ignore_unavailable`` / a swallowed ``NotFoundError``) rather
    # than raising, so the ``try/except`` below never observes it — without
    # this check STAC search would return a confident ``numberMatched: 0``
    # for data that is safely persisted in PostgreSQL. Mirrors the same guard
    # ``item_query._try_driver_dispatch`` and
    # ``extensions.tools.query.maybe_dispatch_items_to_search_driver`` apply.
    _index_check = getattr(driver, "index_available", None)
    if _index_check is not None and not await _index_check(cat_id):
        return None

    # CQL2 filter → ES Query DSL. Only proceed when the driver advertises
    # CQL-ES support AND the filter translates; otherwise PG fallback. The
    # field mapping reuses the same ``QueryOptimizer`` queryables SSOT the PG
    # path uses (``_build_cql2_field_mapping``).
    es_filter: Optional[Dict[str, Any]] = None
    if search_request.filter is not None:
        if not getattr(driver, "supports_cql_es", False):
            return None
        es_filter = await _translate_filter_to_es(
            cat_id, cids, search_request.filter, driver,
        )
        if es_filter is None:
            return None  # untranslatable → PG fallback

    # Multi-collection search: carry the collection set on the request so the
    # driver scopes via its terms filter and queries all shards. cids[0] is the
    # representative positional (the index is per-catalog, not per-collection).
    request = QueryRequest(
        item_ids=search_request.ids,
        collections=cids,
        bbox=list(search_request.bbox) if search_request.bbox else None,
        intersects=search_request.intersects,
        datetime=search_request.datetime,
        limit=search_request.limit,
        offset=search_request.offset,
        es_filter=es_filter,
        sortby=getattr(search_request, "sortby", None) or None,
    )

    # Row-level ABAC: when the resolved driver opts in (the standardized
    # envelope driver, ``applies_access_filter=True``), compile the caller's
    # read scope and put it on the request so the driver AND-s it into the
    # query. The driver itself fails closed when this is absent, so forgetting
    # it here can only under-return, never leak; setting it makes the search
    # return the documents the principal may actually read.
    if getattr(driver, "applies_access_filter", False):
        from dynastore.modules.storage.access_scope import (
            compile_read_access_filter,
        )

        request.access_filter = await compile_read_access_filter(
            catalog_id=cat_id,
            collections=cids,
            principals=principals,
            principal=principal,
        )

    try:
        # read_entities streams read-contract Features (O(1) memory); the page
        # is bounded by ``limit`` so materializing it for the response is fine.
        items = driver.read_entities(
            cat_id, cids[0], request=request,
            limit=search_request.limit, offset=search_request.offset,
        )
        features = [feat async for feat in items]
        total = await driver.count_entities(cat_id, cids[0], request=request)
    except Exception as exc:
        logger.warning(
            "STAC search → SEARCH-driver dispatch failed "
            "(catalog=%s, collections=%s, driver=%s): %s",
            cat_id, cids, type(driver).__name__, exc,
        )
        return None  # fall back to PG path on error

    # read_entities already rebuilt the read contract; only inject the
    # serializer's catalog/collection hints — see :func:`_inject_search_hints`.
    _inject_search_hints(features, cat_id, cids, coll_ext_id_map=coll_ext_id_map)
    return features, total, None


async def _resolve_scoped_collection_ids(
    catalogs: Any, cat_id: str, external_ids: List[str],
) -> Tuple[List[str], Dict[str, str]]:
    """Resolve an explicit ``collections=`` filter list to internal ids.

    Both the ES canonical docs and the PG-backed hydration path key
    ``collection_id`` on the immutable INTERNAL id (see #2653), so a
    caller-supplied EXTERNAL id must be resolved before it reaches the
    search dispatch or it silently matches zero documents (#2786).

    Mirrors ``_ItemsElasticsearchBase._resolve_internal_ids``'s convention
    (elasticsearch.py): resolve the catalog id first, then each collection id
    scoped to it, via ``allow_missing=True``.

    An id shaped like an internal id (``is_internal_physical_name``) is
    rejected outright rather than resolved — passing it through would let it
    match its own internal-keyed documents directly, leaking the internal-id
    namespace onto the REST filter surface (same class of leak as #2325). An
    id that simply fails to resolve as an external label is dropped the same
    way the PG fallback below already treats an unknown collection (excluded
    from ``target_collections``, contributing zero matches rather than
    raising) — so a mixed list of known/unknown ids still serves the known
    ones.

    Returns ``(internal_ids, coll_ext_id_map)`` where ``coll_ext_id_map`` maps
    each resolved internal id back to the external id the caller supplied, for
    response-side restoration.
    """
    from dynastore.modules.catalog.catalog_service import is_internal_physical_name

    internal_cat = await catalogs.resolve_catalog_id(cat_id, allow_missing=True)
    resolved_cat_id = internal_cat if internal_cat is not None else cat_id

    internal_ids: List[str] = []
    coll_ext_id_map: Dict[str, str] = {}
    for external_id in external_ids:
        if is_internal_physical_name(external_id, "col"):
            continue
        internal_id = await catalogs.collections.resolve_collection_id(
            resolved_cat_id, external_id, allow_missing=True
        )
        if internal_id is None:
            continue
        internal_ids.append(internal_id)
        coll_ext_id_map[internal_id] = external_id
    return internal_ids, coll_ext_id_map


async def _expand_collections_for_search(
    catalogs: Any,
    cat_id: str,
    search_request: ItemSearchRequest,
    db_resource: DbResource,
) -> Tuple[ItemSearchRequest, Dict[str, str]]:
    """Scope an unscoped search to ALL collections of the catalog.

    The search-driver dispatch requires explicit collection scoping to
    resolve a (uniform) SEARCH driver, so an unscoped request used to skip
    it and land on the PostgreSQL fallback — which drops every collection
    whose READ driver carries no PG layer config. For a catalog whose items
    routing pins an ES search driver that meant ``GET /search`` (and
    ``ids``-only lookups) answered ``numberMatched: 0`` while the same query
    scoped with ``collections=`` matched.

    Returns a tuple of (request, coll_ext_id_map) where ``coll_ext_id_map``
    maps each internal collection id to its public external label, needed by
    both the ES dispatch's response-side hint injection and the PG hydration
    path's ``feature.properties["_collection_id"]`` restoration.

    Returns the request unchanged when the catalog has no collections;
    otherwise returns a copy scoped to every collection id. An unscoped
    request is expanded to every collection of the catalog (one
    ``list_collection_id_pairs`` round-trip — id/external_id pairs only, no
    per-collection metadata hydration). An explicitly-scoped request has its
    external collection ids resolved to internal ids (see
    :func:`_resolve_scoped_collection_ids`) — unresolvable/internal-shaped
    ids are dropped rather than passed through.
    """
    if search_request.collections:
        internal_ids, coll_ext_id_map = await _resolve_scoped_collection_ids(
            catalogs, cat_id, search_request.collections
        )
        return (
            search_request.model_copy(update={"collections": internal_ids}),
            coll_ext_id_map,
        )
    pairs = await catalogs.list_collection_id_pairs(
        cat_id, ctx=DriverContext(db_resource=db_resource)
    )
    coll_ext_id_map: Dict[str, str] = {
        cid: (ext_id or cid) for cid, ext_id in (pairs or [])
    }
    all_cids = list(coll_ext_id_map.keys())
    if not all_cids:
        return search_request, coll_ext_id_map
    return search_request.model_copy(update={"collections": all_cids}), coll_ext_id_map


async def search_items(
    db_resource: DbResource,
    search_request: ItemSearchRequest,
    stac_config: StacPluginConfig,
    hierarchy_sql: Optional[str] = None,
    hierarchy_params: Optional[Dict[str, Any]] = None,
    *,
    principals: Optional[List[str]] = None,
    principal: Optional[Any] = None,
) -> Tuple[list, int, Optional[Dict[str, Any]]]:
    from sqlalchemy.ext.asyncio import AsyncEngine

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None, "CatalogsProtocol not registered"
    assert search_request.catalog_id is not None, "search_request.catalog_id required"
    cat_id: str = search_request.catalog_id

    # Resolve default/clamp the page size against the configured policy
    # (OGC API - Features Part 1 Core /req/core/fc-limit-response-1): an
    # over-max ``limit`` is capped, never rejected. Must run before any
    # downstream use of ``search_request.limit`` (dispatch, SQL LIMIT, links).
    from dynastore.extensions.tools.pagination import resolve_page_limit
    search_request.limit = resolve_page_limit(
        search_request.limit,
        default_limit=stac_config.default_limit,
        max_limit=stac_config.max_limit,
    )

    # An unscoped request is rewritten to explicitly scope all collections of
    # the catalog so the routing-aware dispatch below can serve it; an
    # explicitly-scoped request has its external collection ids resolved to
    # internal ids (#2786). See :func:`_expand_collections_for_search`.
    # The function also returns an internal→external collection id map used
    # throughout the search response to emit public ids, not internal tokens.
    had_explicit_scope = bool(search_request.collections)
    search_request, _coll_ext_id_map = await _expand_collections_for_search(
        catalogs, cat_id, search_request, db_resource
    )
    if had_explicit_scope and not search_request.collections:
        # Every explicitly-requested collection failed to resolve as a live
        # external id (unknown, or internal-id-shaped and rejected outright).
        # Zero matches — same convention the PG fallback's "no
        # target_collections" branch below applies — never fall through to
        # the unscoped-expansion path, which would silently search every
        # collection in the catalog instead.
        return [], 0, None

    # ── Routing-aware search-driver dispatch (issues #222, #989) ──────
    # For structural-filter-only requests (no CQL2 ``filter``), dispatch to
    # the items SEARCH driver the catalog's routing pins — public ES, the
    # tenant-private ES index, or any future search-capable driver — resolved
    # via ``router.get_items_search_driver``. CQL2 and read-primary
    # (PostgreSQL ``QUERY_FALLBACK_SOURCE``) catalogs fall through to the
    # QueryOptimizer SQL path below. See :func:`_maybe_dispatch_to_es_search`.
    search_dispatch_result = await _maybe_dispatch_to_es_search(
        cat_id, search_request, principals=principals, principal=principal,
        coll_ext_id_map=_coll_ext_id_map, had_explicit_scope=had_explicit_scope,
    )
    if search_dispatch_result is not None:
        return search_dispatch_result

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

    # Layer-config resolution (#2865 PG-fallback fix). `initial_collection_ids`
    # can be the full auto-expanded catalog (a bare `/search`), so resolving
    # the READ driver AND its layer config once PER COLLECTION reproduced the
    # same O(collections) shape the merged ES-dispatch fix addressed for
    # driver resolution alone (#2873) — thousands of sequential round trips on
    # the single `db_resource` connection right after a large harvest.
    #
    # Mirrors that fix here, split the same way ``_maybe_dispatch_to_es_search``
    # is: an explicitly-scoped request (``had_explicit_scope`` — bounded by
    # what the caller asked for, never the whole catalog) keeps the original
    # per-collection driver + config resolution unchanged. An auto-expanded
    # scope resolves the READ driver ONCE at catalog level
    # (``collection_id=None``); when that driver is the PostgreSQL items
    # driver (the case this fix targets) every collection's layer config is
    # fetched in ONE batched query (``ConfigsProtocol.get_configs_batch``)
    # instead of one ``get_config`` round trip per collection. Any other
    # driver class, or a missing ``ConfigsProtocol``, falls back to the
    # original per-collection loop — ``get_driver_config`` is polymorphic
    # per driver class, so the PG-shaped batch call would be wrong there.
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation

    # Sequential — every check forwards the SAME `db_resource` (a live
    # asyncpg Connection); concurrent SELECTs over a single wire deadlock
    # asyncpg's single-stream protocol (regression observed in PRs #28,
    # #32, #43). `get_driver_config` is polymorphic per driver class
    # (postgresql/bigquery/iceberg/duckdb each return their own config
    # type), so this per-collection resolution is the only correct path
    # for a non-PostgreSQL READ driver.
    async def _check_layer_def(cid: str):
        driver = await get_driver(Operation.READ, cat_id, cid)
        return await driver.get_driver_config(cat_id, cid, db_resource=db_resource)

    async def _per_collection_layer_defs() -> Dict[str, Any]:
        results = [await _check_layer_def(cid) for cid in initial_collection_ids]
        return {
            initial_collection_ids[i]: config
            for i, config in enumerate(results)
            if config is not None
        }

    collection_configs: Dict[str, Any] = {}
    if had_explicit_scope:
        collection_configs = await _per_collection_layer_defs()
    else:
        # Auto-expanded (bare `/search`) scope: one catalog-level driver
        # resolution — raises, same as the old per-collection loop, when no
        # READ driver is configured for the catalog at all.
        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver

        catalog_driver = await get_driver(Operation.READ, cat_id, None)
        configs = get_protocol(ConfigsProtocol)
        if isinstance(catalog_driver, ItemsPostgresqlDriver) and configs is not None:
            # The common case this fix targets: batch the config fetch
            # instead of one round trip per collection.
            from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
            collection_configs = await configs.get_configs_batch(
                ItemsPostgresqlDriverConfig, cat_id, initial_collection_ids,
                ctx=DriverContext(db_resource=db_resource),
            )
        else:
            # A non-PostgreSQL READ driver (BigQuery/Iceberg/DuckDB) — the
            # batched fetch above assumes ItemsPostgresqlDriverConfig, which
            # would be the wrong config type here — or ConfigsProtocol is
            # unavailable. Fall back to the original per-collection loop
            # rather than silently returning no results.
            collection_configs = await _per_collection_layer_defs()

    target_collections = list(collection_configs.keys())

    if not target_collections:
        # Every scoped collection resolved a READ driver with no PG layer
        # config — typically a search-driver catalog whose request could not
        # be served by the dispatch above (mixed drivers, untranslatable
        # filter). Returning empty is the only safe answer here, but it must
        # never be silent: the caller sees numberMatched=0 for data that
        # exists.
        logger.warning(
            "search_items: PG fallback has no layer config for any of the "
            "%d scoped collection(s) in catalog '%s' — returning empty. "
            "The catalog's search driver could not serve this request "
            "(mixed drivers or untranslatable filter?).",
            len(initial_collection_ids), cat_id,
        )
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
        from dynastore.modules.tools.cql import parse_cql2_json_filter
        from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType

        cql2_json = _filter_to_cql2_json(search_request.filter)
        # Resolve queryables storage-mode-aware via the QueryOptimizer, exactly
        # as the ``/items`` path does, so an attribute filter binds to the
        # correct physical column — ``sc_attributes."col"`` for COLUMNAR storage
        # or the JSONB accessor for JSONB storage. The first target collection's
        # config drives the mapping, consistent with the column resolution above
        # (``get_columns_with_conn`` also resolves ``target_collections[0]``).
        # An unknown property raises ``ValueError`` here, surfaced as HTTP 400 by
        # the route handler — never silently ignored, never matched against a
        # non-existent JSONB accessor.
        filter_optimizer = QueryOptimizer(
            collection_configs[target_collections[0]], consumer=ConsumerType.STAC
        )
        field_mapping = _build_cql2_field_mapping(filter_optimizer)
        cql_where, cql_params = parse_cql2_json_filter(
            cql2_json, field_mapping=field_mapping,
        )
        if cql_where:
            where_sql += f" AND ({cql_where})"
            params.update(cql_params)

    if hierarchy_sql:
        where_sql += f" AND {hierarchy_sql}"
    if hierarchy_params:
        params.update(hierarchy_params)

    # Resolve physical schema via routing
    catalogs2 = get_protocol(CatalogsProtocol)
    assert catalogs2 is not None, "CatalogsProtocol not registered"
    phys_schema = await catalogs2.resolve_physical_schema(
        cat_id, ctx=DriverContext(db_resource=db_resource)
    )

    # Sidecar requirements and sort strategy are resolved per-collection inside
    # the loop below (``req_attributes``/``req_stac`` recomputed from each
    # collection's WHERE), and the final ORDER BY is built by
    # ``_parse_item_sort_order_by`` over the unioned ``candidate_matches`` CTE.

    # Pre-parse sortby once so every fragment loop iteration can inject the
    # same property-sort alias expressions.  Raises ValueError (→ HTTP 400)
    # here — before any fragment work — if a sort field is invalid.
    _sort_entries = _parse_item_sort_entries(search_request.sortby)
    # Property-sort entries need ``sc_attributes`` to be joined in each fragment
    # and their value projected as a stable alias (``_sort{i}``) into the CTE.
    _prop_sort_entries = [e for e in _sort_entries if e.pg_alias is not None]

    candidate_fragments = []

    # Store table mapping for hydration: collection_id -> (phys_table, sidecar_table, geom_table, item_meta_table, stac_meta_table, has_attr, has_geom, has_item_meta, has_stac_meta)
    collection_table_map = {}
    # Per-collection flag: does the attributes sidecar persist a ``validity``
    # column? Mirrors ``ItemsWritePolicy.enable_validity`` (#974). When False,
    # the column does not exist and the hydration SELECT must not reference it.
    collection_validity_enabled: dict = {}
    # Per-collection SQL projection that yields the feature-properties blob as
    # ``attributes``. JSONB sidecars project the blob column directly; COLUMNAR
    # sidecars have no ``attributes`` column, so the blob is reconstructed from
    # the declared property columns. Defaults to the JSONB shape.
    collection_attr_projection: dict = {}
    # Collections whose attributes sidecar is COLUMNAR: their rebuilt ``attributes``
    # blob must be splatted into individual row keys before mapping, because the
    # COLUMNAR attribute mapper reads ``row[col]`` rather than the blob.
    collection_attr_columnar: set = set()

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

        # Check if we need Attributes Sidecar based on filters or sort.
        # ``properties.*`` sort entries also require the attributes sidecar join
        # so their JSONB accessor expression is available in the fragment SELECT.
        req_attributes = bool(
            search_request.ids
            or search_request.datetime
            or search_request.filter
            or "attributes" in where_sql
            or "external_id" in where_sql
            or "validity" in where_sql
            or _prop_sort_entries
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
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            AttributeStorageMode,
        )
        coll_has_validity = False
        # None  → no attributes sidecar present for this collection
        # True  → JSONB sidecar
        # False → COLUMNAR sidecar (attributes blob column absent)
        _coll_attr_is_jsonb: Optional[bool] = None
        for _sc in _driver_sidecars(config):
            if not _sc.enabled:
                continue
            if getattr(_sc, "sidecar_type", "") in ("attributes", "feature_attributes"):
                coll_has_validity = bool(getattr(_sc, "enable_validity", False))
                # Import here to avoid circular at module level; mirrors
                # ``_attributes_hydration_projection`` which does the same import.
                from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
                    FeatureAttributeSidecar,
                )
                _coll_attr_is_jsonb = (
                    FeatureAttributeSidecar(_sc).resolved_storage_mode  # type: ignore[arg-type]
                    == AttributeStorageMode.JSONB
                )
                break  # only the first enabled attributes sidecar is relevant

        # ``properties.*`` sort injects ``sc_attributes.attributes->>'<key>'``
        # into each fragment. That accessor is only valid when the attributes
        # sidecar is in JSONB storage mode. COLUMNAR sidecars store each
        # declared property as its own physical column and have no ``attributes``
        # blob — the expression is invalid SQL and produces a 500.  Raise a
        # clear 400 here instead, before any fragment work is attempted.
        if _prop_sort_entries:
            if _coll_attr_is_jsonb is None:
                raise ValueError(
                    f"Sorting by properties.{_prop_sort_entries[0].raw_field!r} "
                    f"is not supported for collection {collection_id!r}: "
                    "no attributes sidecar is configured for this collection."
                )
            if not _coll_attr_is_jsonb:
                raise ValueError(
                    f"Sorting by properties is not supported for collection "
                    f"{collection_id!r} (COLUMNAR attribute storage)."
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

        # Inject a raw SELECT expression for every ``properties.*`` sort alias
        # so each fragment projects the value under the stable ``_sort{i}`` name
        # and the outer CTE can reference it in ORDER BY.
        # The sidecar table alias used by QueryOptimizer for attributes is
        # ``sc_attributes``; we use that directly in the JSONB accessor.
        for _se in _prop_sort_entries:
            if _se.pg_raw_select_template is not None:
                raw_selects.append(
                    _se.pg_raw_select_template.format(sidecar="sc_attributes")
                )

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

        # Physical table for this collection. Resolve from the per-collection
        # driver config already loaded above (``collection_configs``, fetched
        # with the live ``db_resource``). ``driver.location()`` must NOT be used
        # here: it takes no ``db_resource`` and re-resolves the config with
        # none, yielding a default config whose ``physical_table`` is ``None``,
        # so ``location()`` raises and the collection is silently skipped —
        # COLUMNAR attribute search then returns an empty page (#1641).
        # ``phys_schema`` above is likewise resolved with the live resource.
        phys_table = getattr(config, "physical_table", None)
        if phys_table:
            from dynastore.tools.db import validate_sql_identifier

            phys_table = validate_sql_identifier(phys_table)
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
                    _attr_proj, _attr_is_columnar = (
                        _attributes_hydration_projection(sc)
                    )
                    collection_attr_projection[collection_id] = _attr_proj
                    if _attr_is_columnar:
                        collection_attr_columnar.add(collection_id)
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

    # Build ORDER BY from optional sortby; default to valid_from DESC, id.
    # Uses the pre-parsed _sort_entries (already validated above); calling
    # _parse_item_sort_order_by a second time would re-parse but is safe.
    order_by_clause = (
        ", ".join(e.order_by_expr for e in _sort_entries)
        if _sort_entries
        else "valid_from DESC, id"
    )

    # When ``properties.*`` sort is requested, the CTE outer SELECT must also
    # carry the ``_sort{i}`` aliases so ``ORDER BY`` can reference them.
    _prop_sort_select_extras = (
        ", " + ", ".join(e.pg_alias for e in _prop_sort_entries)  # type: ignore[misc]
        if _prop_sort_entries
        else ""
    )

    # Merged paging + count using a window function (1 query instead of 2)
    paging_query = f"""
        WITH candidate_matches AS ({full_union_query})
        SELECT catalog_id, collection_id, geoid, valid_from, id{_prop_sort_select_extras},
               COUNT(*) OVER() AS _total_count
        FROM candidate_matches
        ORDER BY {order_by_clause}
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
            # COLUMNAR attribute sidecars have no ``attributes`` blob column;
            # reconstruct it from the declared property columns (#1253 follow-up).
            attr_select = collection_attr_projection.get(coll_id, "s.attributes")
            select_parts.append(
                ", COALESCE(s.external_id, h.geoid::text) as id"
                ", s.external_id"
                f"{validity_select}"
                f", {attr_select} as attributes, s.asset_id"
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
    from dynastore.extensions.tools.query import resolve_items_read_policy
    items_svc = get_protocol(ItemsProtocol)

    # Resolve the read policy once per collection (STAC search may span
    # several), so the row mapper surfaces ``feature_type.expose`` computed
    # values onto ``feature.properties`` for the STAC item generator to read.
    # NOTE: STAC search keeps its own native id projection — the hydration
    # SQL above aliases ``COALESCE(s.external_id, h.geoid::text) AS id`` for
    # every row, which is the STAC Item convention (items key on external_id).
    # ``ItemsReadPolicy.feature_type.external_id_as_feature_id`` therefore does
    # NOT override the STAC item id on this path by design; only the expose
    # merge is honoured here.
    _read_policy_by_collection: Dict[str, Any] = {}

    async def _read_policy_for(collection_id: str) -> Any:
        if collection_id not in _read_policy_by_collection:
            _read_policy_by_collection[collection_id] = (
                await resolve_items_read_policy(cat_id, collection_id)
            )
        return _read_policy_by_collection[collection_id]

    for row in page_rows:
        key = (row["collection_id"], row["geoid"])
        if key in hydrated_items:
            item_data = hydrated_items[key]
            if item_data.get("simplified_geom"):
                item_data["geom"] = item_data.pop("simplified_geom")
            else:
                item_data["geom"] = None

            # COLUMNAR collections: the attribute mapper reads ``row[col]`` per
            # declared column, not the rebuilt ``attributes`` blob. Splat the
            # blob's keys back onto the row so properties populate (#1253 follow-up).
            if item_data["collection_id"] in collection_attr_columnar:
                _attrs = item_data.get("attributes")
                if isinstance(_attrs, dict):
                    for _k, _v in _attrs.items():
                        item_data.setdefault(_k, _v)

            if items_svc:
                col_config = collection_configs.get(item_data["collection_id"])
                read_policy = await _read_policy_for(item_data["collection_id"])
                # We need to preserve original `item_data` values for sidecars (e.g., stac_title/stac_description)
                # ItemsProtocol will place these nicely into `feature.properties` or `feature.stac_extensions` etc.
                feature = items_svc.map_row_to_feature(item_data, col_config=col_config, read_policy=read_policy)
                if feature:
                    if feature.properties is None:
                        feature.properties = {}
                    # Strip hydration-internal columns that leaked into properties
                    # because they are not covered by any sidecar get_internal_columns()
                    # contract (they are SQL aliases from the hydration SELECT).
                    for _hk in _HYDRATION_INTERNAL_FIELDS:
                        feature.properties.pop(_hk, None)
                    feature.properties["_catalog_id"] = item_data["catalog_id"]
                    _internal_coll_id = item_data["collection_id"]
                    feature.properties["_collection_id"] = (
                        _coll_ext_id_map.get(_internal_coll_id, _internal_coll_id)
                    )
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
    db_resource: DbResource,
    search_request: CollectionSearchRequest,
    *,
    default_limit: int = 10,
    max_limit: int = 1000,
) -> Tuple[List[Collection], int]:
    # Resolve default/clamp the page size against the configured policy
    # (OGC API - Features Part 1 Core /req/core/fc-limit-response-1): an
    # over-max ``limit`` is capped, never rejected. Callers pass the
    # ``StacPluginConfig`` policy; the literal fallbacks above only apply to
    # (test) callers that don't.
    from dynastore.extensions.tools.pagination import resolve_page_limit
    search_request.limit = resolve_page_limit(
        search_request.limit, default_limit=default_limit, max_limit=max_limit,
    )

    # Listing visibility: this search builds its own SQL (it does not go
    # through the routed collection drivers), so it translates the
    # request's constraints itself — the catalog-level constraint narrows
    # which schemas are UNIONed at all, and each schema's subquery carries
    # that catalog's visible-collection predicate. No constraint published
    # (background work, or no authorization layer) ⟹ unfiltered.
    from dynastore.models.protocols.visibility import (
        resolve_catalog_listing_ids,
        resolve_collection_listing_ids,
    )

    visible_catalogs = await resolve_catalog_listing_ids()

    # Determine which (catalog_id, schema) pairs to search
    target_pairs: List[Tuple[str, str]] = []

    # Resolve effective catalog filter (catalog_ids list takes precedence when set)
    effective_catalog_ids = []
    if search_request.catalog_id:
        effective_catalog_ids = [search_request.catalog_id]
    elif search_request.catalog_ids:
        effective_catalog_ids = search_request.catalog_ids

    if effective_catalog_ids:
        if visible_catalogs is not None:
            effective_catalog_ids = [
                c for c in effective_catalog_ids if c in visible_catalogs
            ]
        from dynastore.models.protocols import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)
        assert catalogs is not None, "CatalogsProtocol not registered"
        for cid in effective_catalog_ids:
            schema = await catalogs.resolve_physical_schema(cid, ctx=DriverContext(db_resource=db_resource))
            if schema:
                target_pairs.append((cid, schema))
        # If none of the catalog_ids resolved, implies no results
    else:
        # Search all catalogs the caller may see.
        # We query the catalog registry directly to find all active physical schemas
        catalog_query = (
            "SELECT id FROM catalog.catalogs WHERE deleted_at IS NULL"
        )
        try:
            rows = await DQLQuery(
                catalog_query, result_handler=ResultHandler.ALL_DICTS
            ).execute(db_resource)
        except Exception as e:
            logger.warning(f"Failed to retrieve catalog schemas for global search: {e}")
            return [], 0
        for row in rows or []:
            cid, schema = row.get("id"), row.get("id")
            if not cid or not schema:
                continue
            if visible_catalogs is not None and cid not in visible_catalogs:
                continue
            target_pairs.append((cid, schema))

    if not target_pairs:
        return [], 0

    # Hide transient collections (#2194): a non-null ``lifecycle_status`` overlay
    # (#2066) marks a collection mid-provisioning or mid-hard-delete — write-gated
    # and not a live catalog member — so it must not surface in STAC collection
    # search. Only ACTIVE rows (overlay NULL) are returned; a direct GET-by-id
    # still resolves it. Folded into the per-catalog subquery WHERE so the CTE
    # COUNT and the paged data query stay consistent.
    where_clauses = ["deleted_at IS NULL", "c.lifecycle_status IS NULL"]
    params = {}

    # Common filters — catalog_id / catalog_ids filtering is handled via schema UNION above

    if search_request.ids:
        where_clauses.append("id = ANY(:ids)")
        params["ids"] = search_request.ids

    if search_request.keywords:
        where_clauses.append("mc.keywords @> CAST(:keywords AS jsonb)")
        params["keywords"] = str(search_request.keywords).replace("'", '"')

    # STAC Collection Search `q` — free-text across id, title (English), description (English).
    # `title` and `description` are JSONB localized objects ({"en": "...", "fr": "..."}).
    # Applying lower() directly to a jsonb column raises UndefinedFunctionError in Postgres
    # because there is no lower(jsonb) overload.  Extract the English text value with the
    # ->>'en' accessor (returns text, NULL when the key is absent) before calling lower().
    if search_request.q:
        q_conditions = []
        for idx, term in enumerate(search_request.q):
            p = f"_q_term_{idx}"
            params[p] = f"%{term.lower()}%"
            q_conditions.append(
                f"(lower(c.id) LIKE :{p} "
                f"OR lower(mc.description->>'en') LIKE :{p} "
                f"OR lower(mc.title->>'en') LIKE :{p})"
            )
        if q_conditions:
            where_clauses.append("(" + " AND ".join(q_conditions) + ")")

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
    # ``external_id`` must be selected alongside the internal ``id`` so
    # ``Collection.model_validate(row)`` below populates it — without it the
    # model's ``external_id`` field stays None and
    # ``BaseMetadata._serialize_public_id`` (shared_models.py) has nothing to
    # swap onto the wire ``id``, leaking the internal ``col_...`` token on
    # every collection this query serves (Collection Search branch and the
    # PG plain-listing fallback both consume it via
    # ``_pg_collections_to_stac_dicts`` in stac_service.py). Refs #2853.
    _meta_cols = (
        "c.id, c.external_id, c.catalog_id, "
        "mc.title, mc.description, mc.keywords, mc.license, "
        "ms.links, ms.assets, ms.extent, ms.providers, ms.summaries, "
        "ms.item_assets, mc.extra_metadata"
    )
    union_queries = []
    for idx, (cid, schema) in enumerate(target_pairs):
        # Per-catalog visible-collection predicate (listing visibility).
        per_where = where_sql
        visible_collections = await resolve_collection_listing_ids(cid)
        if visible_collections is not None:
            if not visible_collections:
                continue
            vis_param = f"_vis_cols_{idx}"
            params[vis_param] = list(visible_collections)
            per_where = f"{where_sql} AND c.id = ANY(:{vis_param})"
        union_queries.append(
            f'SELECT {_meta_cols} '
            f'FROM "{schema}".collections c '
            f'LEFT JOIN "{schema}".collection_core mc ON mc.collection_id = c.id '
            f'LEFT JOIN "{schema}".collection_stac ms ON ms.collection_id = c.id '
            f'WHERE {per_where}'
        )

    if not union_queries:
        return [], 0

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
