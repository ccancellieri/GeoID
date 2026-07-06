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

"""Claim-set computation kernel + source-collection read helpers for the
region_mapping extension (dynastore#443/#448/#2821).

Holds no persistence dependency of its own — ``mapping_id``/``claim_ci``
computation is pure, and the two ``fetch_*`` helpers below read the
*source* collection (the collection being claimed, e.g. a country-boundary
layer), never the registry itself. Registry persistence (the
``region_mapping.mappings`` table) lives in ``registry_queries.py`` /
``registry_store.py``.

``fetch_distinct_region_ids`` deliberately bypasses ``CatalogsProtocol``'s
item-query surface (``search_items``/``stream_items``) and the
storage-driver routing it triggers: the registry and its reads are
detached by design, and PostgreSQL is the system of record for every
collection's rows regardless of which driver serves live traffic. It
issues a dedicated SQL query straight at the source collection's physical
attributes table instead.
"""
from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Optional, Sequence, Tuple

from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    _read_live_fg_acquire_timeout,
    managed_transaction,
)
from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
    ItemsWritePolicy,
)
from dynastore.modules.storage.drivers.pg_sidecars import (
    FeatureAttributeSidecar,
    FeatureAttributeSidecarConfig,
    SidecarRegistry,
    driver_sidecars,
    sidecar_table_name,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
)
from dynastore.tools.cache import cached
from dynastore.tools.db import validate_column_identifier, validate_sql_identifier
from dynastore.tools.discovery import get_protocol
from dynastore.tools.protocol_helpers import get_engine

ROLE_PRIMARY = "primary"
ROLE_ALIAS = "alias"

WORLD_BBOX: Tuple[float, float, float, float] = (-180.0, -90.0, 180.0, 90.0)

_REGEX_METACHARS = re.compile(r"[.^$*+?{}\[\]\\|()]")
_SLUG_INVALID = re.compile(r"[^a-z0-9_]+")
_SLUG_COLLAPSE = re.compile(r"_+")


def slugify(value: str) -> str:
    slug = _SLUG_INVALID.sub("_", value.strip().lower())
    return _SLUG_COLLAPSE.sub("_", slug).strip("_")


def mapping_id_for(catalog_id: str, collection_id: str) -> str:
    return slugify(f"{catalog_id}_{collection_id}")


def validate_claim_text(claim: str) -> None:
    """Reject a claim string containing regex metacharacters.

    TerriaJS compiles each alias into a ``^alias$`` case-insensitive regex
    (dynastore#443) — an unescaped metacharacter would silently change
    matching semantics instead of matching the literal string.
    """
    if _REGEX_METACHARS.search(claim):
        raise ValueError(
            f"region_mapping: claim {claim!r} contains regex metacharacters "
            "('.^$*+?{}[]\\|()') — TerriaJS compiles aliases into literal "
            "^alias$ (case-insensitive) regexes, so claim text must not "
            "contain them."
        )


def compute_claim_set(
    *,
    region_prop: str,
    aliases: Sequence[str],
) -> "OrderedDict[str, Tuple[str, str]]":
    """Return ``{claim_ci: (claim, role)}`` for one ``region_mapping`` apply.

    The claim set is exactly the identifiers TerriaJS may match a CSV column
    header against for this layer: ``{region_prop, *aliases}``. Every token
    is registered as its own row keyed by ``claim_ci`` (its ``casefold()``),
    and that column is the table's PRIMARY KEY -- so the same ``regionProp``
    or ``alias`` can never be claimed by two different mappings (a second
    claim hits PG ``23505`` -> HTTP 409). This is the storage-level guarantee
    behind the API's "alias and regionProp are unique across all mappings"
    rule.

    ``region_prop``'s token is ``role="primary"`` (it is both the tile
    property TerriaJS reads and a matchable CSV header); every alias is
    ``role="alias"``. Deduplicated case-insensitively, so an alias that only
    differs from ``region_prop`` by case collapses into the single primary.
    """
    region_ci = region_prop.casefold()
    candidates = [region_prop, *aliases]

    claims: "OrderedDict[str, Tuple[str, str]]" = OrderedDict()
    for candidate in candidates:
        validate_claim_text(candidate)
        claim_ci = candidate.casefold()
        if claim_ci in claims:
            continue
        role = ROLE_PRIMARY if claim_ci == region_ci else ROLE_ALIAS
        claims[claim_ci] = (candidate, role)
    return claims


def is_degenerate_bbox(bbox: Optional[Sequence[float]]) -> bool:
    """True when ``bbox`` is missing or its extent collapses to zero/negative area."""
    if not bbox or len(bbox) < 4:
        return True
    minx, miny, maxx, maxy = bbox[0], bbox[1], bbox[2], bbox[3]
    return maxx <= minx or maxy <= miny


# ---------------------------------------------------------------------------
# Source-collection reads — unrelated to registry persistence.
# ---------------------------------------------------------------------------


@cached(maxsize=128, ttl=300, namespace="region_mapping_collection_bbox")
async def fetch_collection_bbox(catalog_id: str, collection_id: str) -> List[float]:
    """Source collection's spatial extent bbox, falling back to world bounds
    when missing or degenerate (``xmax<=xmin`` or ``ymax<=ymin``)."""
    catalogs = get_protocol(CatalogsProtocol)
    bbox: Optional[Sequence[float]] = None
    if catalogs is not None:
        try:
            collection = await catalogs.get_collection(catalog_id, collection_id)
        except Exception:
            collection = None
        if collection is not None and collection.extent and collection.extent.spatial:
            boxes = collection.extent.spatial.bbox or []
            if boxes:
                bbox = boxes[0]
    if is_degenerate_bbox(bbox):
        return list(WORLD_BBOX)
    return list(bbox)  # type: ignore[arg-type]


def _find_attributes_sidecar(
    col_config: ItemsPostgresqlDriverConfig,
) -> Optional[FeatureAttributeSidecarConfig]:
    return next(
        (
            sc for sc in driver_sidecars(col_config)
            if isinstance(sc, FeatureAttributeSidecarConfig)
        ),
        None,
    )


# Default TerriaJS uniqueIdProp when a collection declares no external_id and
# the caller gives none: the numeric feature-index column shapefile ingestion
# commonly materialises.
FALLBACK_UNIQUE_ID_PROP = "FID"


@dataclass(frozen=True)
class CollectionColumns:
    """The subset of a source collection's physical shape region_mapping needs
    to decide whether a column can back a region mapping.

    ``declared`` is the columnar ``attribute_schema`` (user-declared columns
    materialised as real PG columns). ``external_id_field`` is the system
    identity column the PG driver adds outside ``attribute_schema`` when the
    collection's write policy carries an EXTERNAL_ID rule -- a real column all
    the same, so :meth:`has_column` treats it as present.

    ``external_id_path`` is the *source column* the external_id VALUE is
    extracted from (``ItemsWritePolicy.derive.external_id`` -- e.g. ``"CODE"``),
    NOT the ``external_id`` storage column. It is ``None`` when the collection
    configures no external_id extraction path (external_id then falls back to
    the feature's STAC id, and there is no source column to key on). This is
    the property TerriaJS's MVT tiles actually carry, so it -- not the internal
    ``external_id`` storage column -- is what a uniqueIdProp defaults to.
    """

    is_columnar: bool
    declared: FrozenSet[str]
    external_id_field: Optional[str]
    external_id_path: Optional[str]
    validity_column: Optional[str]

    def has_column(self, name: str) -> bool:
        """True when ``name`` is a real physical column: a declared columnar
        attribute, or the driver-managed ``external_id`` identity column."""
        if name in self.declared:
            return True
        return self.external_id_field is not None and name == self.external_id_field

    @property
    def enable_external_id(self) -> bool:
        return self.external_id_field is not None


async def resolve_collection_columns(
    src_catalog: str, src_collection: str,
) -> Optional[CollectionColumns]:
    """Resolve a source collection's columnar shape from its persisted driver
    config, or ``None`` when the collection has no attributes sidecar (nothing
    region-mappable). Reads config only -- issues no query against the
    collection's data.
    """
    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        return None
    col_config = await configs.get_config(
        ItemsPostgresqlDriverConfig,
        catalog_id=src_catalog,
        collection_id=src_collection,
    )
    attrs_config = _find_attributes_sidecar(col_config)
    if attrs_config is None:
        return None
    attrs_sidecar = SidecarRegistry.get_sidecar(attrs_config, lenient=True)
    if not isinstance(attrs_sidecar, FeatureAttributeSidecar):
        return None
    is_columnar = attrs_sidecar.resolved_storage_mode == AttributeStorageMode.COLUMNAR
    declared = frozenset(attr.name for attr in (attrs_config.attribute_schema or []))
    return CollectionColumns(
        is_columnar=is_columnar,
        declared=declared,
        external_id_field=attrs_config.external_id_field,
        external_id_path=await _resolve_external_id_path(src_catalog, src_collection),
        validity_column=attrs_config.validity_column,
    )


async def _resolve_external_id_path(
    src_catalog: str, src_collection: str,
) -> Optional[str]:
    """The source column the external_id VALUE is extracted from, read off the
    collection's ``ItemsWritePolicy`` (``derive.external_id`` -- e.g. ``"CODE"``).

    ``None`` when no extraction path is configured (external_id then defaults to
    the STAC id and no source column exists to key a mapping on). Mirrors
    ``item_service._resolve_external_id_path``; swallows config errors to
    ``None`` -- a missing/unreadable policy just means "no external_id path".
    """
    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        return None
    try:
        policy = await configs.get_config(
            ItemsWritePolicy, catalog_id=src_catalog, collection_id=src_collection,
        )
        getter = getattr(policy, "external_id_path", None)
        path = getter() if callable(getter) else None
    except Exception:
        return None
    return str(path) if path else None


def _columnar_columns(attrs_config: FeatureAttributeSidecarConfig) -> set:
    """Names of every real column a columnar collection exposes to a
    region-mapping read: the declared ``attribute_schema`` columns plus the
    driver-managed ``external_id`` identity column (materialised outside
    ``attribute_schema`` but a genuine column, so a mapping may key on it)."""
    columns = {attr.name for attr in (attrs_config.attribute_schema or [])}
    if attrs_config.external_id_field:
        columns.add(attrs_config.external_id_field)
    return columns


def resolve_unique_id_prop(
    supplied: Optional[str], external_id_path: Optional[str], has_fid: bool,
) -> Optional[str]:
    """Resolve TerriaJS's ``uniqueIdProp`` for a mapping, or ``None`` when it
    cannot be resolved (the caller must then reject the mapping).

    Precedence:

    1. An explicitly supplied value always wins.
    2. Else the collection's ``external_id`` *source column* when external_id
       extraction is configured (``external_id_path`` -- e.g. ``"CODE"``). This
       is the column TerriaJS's tiles actually carry, and its values survive a
       feature's versions, letting /regionIds pick the latest. NOT the internal
       ``external_id`` storage column, which the tiles never expose.
    3. Else the :data:`FALLBACK_UNIQUE_ID_PROP` numeric feature index, but only
       when the collection declares it as a column.
    4. Else ``None`` -- no usable per-feature index exists.
    """
    if supplied:
        return supplied
    if external_id_path:
        return external_id_path
    if has_fid:
        return FALLBACK_UNIQUE_ID_PROP
    return None


@cached(maxsize=128, ttl=120, namespace="region_mapping_region_ids")
async def fetch_distinct_region_ids(
    src_catalog: str, src_collection: str, region_prop: str,
) -> List[str]:
    """Sorted distinct values of ``region_prop`` in the source collection.

    Issues one server-side ``SELECT DISTINCT`` straight against the source
    collection's physical attributes table, resolving ``region_prop`` to
    either a promoted columnar column or a JSONB-document key from the
    collection's persisted driver config (collections are all-columnar or
    all-JSONB, never mixed — see ``AttributeStorageMode``). Soft-deleted
    rows are excluded via the hub's ``deleted_at`` column, mirroring the
    item read path's lifecycle predicate.
    """
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    if catalogs is None or configs is None:
        return []

    phys_schema = await catalogs.resolve_physical_schema(src_catalog, allow_missing=True)
    col_config = await configs.get_config(
        ItemsPostgresqlDriverConfig,
        catalog_id=src_catalog,
        collection_id=src_collection,
    )
    phys_table = col_config.physical_table
    if not phys_schema or not phys_table:
        return []

    attrs_config = _find_attributes_sidecar(col_config)
    if attrs_config is None:
        return []
    attrs_sidecar = SidecarRegistry.get_sidecar(attrs_config, lenient=True)
    if not isinstance(attrs_sidecar, FeatureAttributeSidecar):
        return []

    schema = validate_sql_identifier(phys_schema)
    hub_table = validate_sql_identifier(phys_table)
    attrs_table = validate_sql_identifier(
        sidecar_table_name(phys_table, attrs_config.sidecar_id)
    )

    params: Dict[str, Any] = {}
    if attrs_sidecar.resolved_storage_mode == AttributeStorageMode.COLUMNAR:
        declared = _columnar_columns(attrs_config)
        if region_prop not in declared:
            # Claimed property isn't a materialised column on this
            # columnar collection — nothing to read.
            return []
        col = validate_column_identifier(region_prop)
        value_expr = f's."{col}"'
    else:
        jsonb_col = validate_column_identifier(attrs_config.jsonb_column_name)
        value_expr = f's."{jsonb_col}" ->> :region_prop'
        params["region_prop"] = region_prop

    sql = (
        f'SELECT DISTINCT {value_expr} AS region_value '
        f'FROM "{schema}"."{hub_table}" h '
        f'JOIN "{schema}"."{attrs_table}" s ON s.geoid = h.geoid '
        f'WHERE h.deleted_at IS NULL AND {value_expr} IS NOT NULL '
        f'ORDER BY region_value'
    )

    engine = get_engine()
    if engine is None:
        return []
    # Bounded so a rebuild triggered under pool pressure times out into
    # PoolSaturationError -> 503 + Retry-After instead of holding the
    # connection for the full request timeout (dynastore#2902).
    async with managed_transaction(
        engine, acquire_timeout=await _read_live_fg_acquire_timeout()
    ) as conn:
        values = await DQLQuery(
            sql, result_handler=ResultHandler.ALL_SCALARS,
        ).execute(conn, **params)
    return sorted(str(v) for v in (values or []))


def _fid_index(value: Any) -> Optional[int]:
    """Coerce a raw unique-id attribute value to a non-negative list index.

    Values arrive as int, float, or text depending on the storage lane and
    how the source dataset typed the field (JSONB ``->>`` always yields
    text; a float-typed source field yields "984.0"). Non-numeric,
    fractional, and negative values cannot be positional indexes — skip
    them (return ``None``) rather than erroring or corrupting the array
    (a negative index would silently overwrite entries from the end).
    """
    try:
        as_float = float(value)
        as_int = int(as_float)
    except (TypeError, ValueError, OverflowError):
        # int() raises on the floats float() happily parses: ValueError
        # for "nan", OverflowError for "inf"/"Infinity".
        return None
    if as_int != as_float or as_int < 0:
        return None
    return as_int


@cached(maxsize=128, ttl=120, namespace="region_mapping_region_ids_by_unique_id")
async def fetch_region_ids_by_unique_id(
    src_catalog: str, src_collection: str, region_prop: str, unique_id_prop: str,
) -> List[str]:
    """Per-feature ``region_prop`` values, positioned by ``unique_id_prop``.

    TerriaJS's MVT region matching (``RegionProvider.processRegionIds``)
    treats the returned array's *index* as a feature's ``uniqueIdProp``
    value: ``values[i]`` must be the region code of the feature whose
    ``uniqueIdProp`` equals ``i``. This is the opposite shape from
    ``fetch_distinct_region_ids`` (deduplicated, alphabetically sorted —
    for CSV templates): here every feature contributes one entry (region
    codes may repeat, e.g. many admin-1 features sharing one country
    code), indexed by its numeric unique id.

    ``unique_id_prop`` values are not guaranteed dense (e.g. a source
    shapefile's FID column can have permanent gaps from features dropped
    during ingestion, not just soft-deletes) — positions with no matching
    feature are filled with ``""`` rather than left absent or ``None``.
    TerriaJS's ``processRegionIds`` unconditionally calls ``.toLowerCase()``
    on every array entry while loading this file, before any per-feature
    MVT lookup happens, so a ``null`` entry crashes the load even though
    that index would never actually be dereferenced at render time. An
    empty string survives ``.toLowerCase()`` and can't collide with a real
    region code.
    """
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    if catalogs is None or configs is None:
        return []

    phys_schema = await catalogs.resolve_physical_schema(src_catalog, allow_missing=True)
    col_config = await configs.get_config(
        ItemsPostgresqlDriverConfig,
        catalog_id=src_catalog,
        collection_id=src_collection,
    )
    phys_table = col_config.physical_table
    if not phys_schema or not phys_table:
        return []

    attrs_config = _find_attributes_sidecar(col_config)
    if attrs_config is None:
        return []
    attrs_sidecar = SidecarRegistry.get_sidecar(attrs_config, lenient=True)
    if not isinstance(attrs_sidecar, FeatureAttributeSidecar):
        return []

    schema = validate_sql_identifier(phys_schema)
    hub_table = validate_sql_identifier(phys_table)
    attrs_table = validate_sql_identifier(
        sidecar_table_name(phys_table, attrs_config.sidecar_id)
    )

    params: Dict[str, Any] = {}
    if attrs_sidecar.resolved_storage_mode == AttributeStorageMode.COLUMNAR:
        declared = _columnar_columns(attrs_config)
        if region_prop not in declared or unique_id_prop not in declared:
            # Claimed property (or the unique-id column) isn't a
            # materialised column on this columnar collection.
            return []
        region_col = validate_column_identifier(region_prop)
        fid_col = validate_column_identifier(unique_id_prop)
        region_expr = f's."{region_col}"'
        fid_expr = f's."{fid_col}"'
    else:
        jsonb_col = validate_column_identifier(attrs_config.jsonb_column_name)
        region_expr = f's."{jsonb_col}" ->> :region_prop'
        # No ::int cast here: attribute values ingested as JSON floats
        # come back as "984.0", which the cast rejects with 22P02.
        # Coercion/validation happens in Python via _fid_index below;
        # DB-side ordering is irrelevant because rows are placed into the
        # result array positionally by fid.
        fid_expr = f's."{jsonb_col}" ->> :unique_id_prop'
        params["region_prop"] = region_prop
        params["unique_id_prop"] = unique_id_prop

    base_sql = (
        f'SELECT {region_expr} AS region_value, {fid_expr} AS fid, h.geoid AS _geoid '
        f'FROM "{schema}"."{hub_table}" h '
        f'JOIN "{schema}"."{attrs_table}" s ON s.geoid = h.geoid '
        f'WHERE h.deleted_at IS NULL AND {region_expr} IS NOT NULL '
        f'AND {fid_expr} IS NOT NULL'
    )
    if attrs_config.validity_column is not None:
        # With validity/versioning enabled a feature carried across versions
        # leaves several live (deleted_at NULL) rows sharing one uniqueIdProp
        # value. Collapse to one row per value, keeping the latest-written
        # version (highest geoid) -- the current feature -- so the positional
        # regionIds array reflects it and versions don't overwrite each other in
        # arbitrary order. A no-op for non-versioned collections (one row per
        # value already), so it is applied only when validity is configured.
        sql = (
            f'SELECT DISTINCT ON (fid) region_value, fid '
            f'FROM ({base_sql}) v ORDER BY fid, _geoid DESC'
        )
    else:
        sql = base_sql

    engine = get_engine()
    if engine is None:
        return []
    # Bounded for the same reason as fetch_distinct_region_ids above
    # (dynastore#2902).
    async with managed_transaction(
        engine, acquire_timeout=await _read_live_fg_acquire_timeout()
    ) as conn:
        rows = await DQLQuery(
            sql, result_handler=ResultHandler.ALL_DICTS,
        ).execute(conn, **params)

    indexed = []
    for row in rows or []:
        idx = _fid_index(row["fid"])
        if idx is not None:
            indexed.append((idx, str(row["region_value"])))
    if not indexed:
        return []

    ordered: List[str] = [""] * (max(i for i, _ in indexed) + 1)
    for idx, region_value in indexed:
        ordered[idx] = region_value
    return ordered


_EMPTY_CARDINALITY: Dict[str, int] = {
    "feature_count": 0, "distinct_region_count": 0, "distinct_unique_id_count": 0,
    "null_unique_id_count": 0,
}


@cached(maxsize=128, ttl=120, namespace="region_mapping_cardinality")
async def fetch_region_mapping_cardinality(
    src_catalog: str, src_collection: str, region_prop: str, unique_id_prop: str,
) -> Dict[str, int]:
    """Feature count vs. distinct-value counts of ``region_prop`` and
    ``unique_id_prop`` in the source collection.

    The cardinality signal :func:`validate_region_mapping_stats` uses to
    detect a misconfigured mapping: either column repeating a value across
    more features than there are distinct values means that column can't
    identify one feature per code. Returns all zeros when the source
    collection/columns can't be resolved.
    """
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    if catalogs is None or configs is None:
        return dict(_EMPTY_CARDINALITY)

    phys_schema = await catalogs.resolve_physical_schema(src_catalog, allow_missing=True)
    col_config = await configs.get_config(
        ItemsPostgresqlDriverConfig,
        catalog_id=src_catalog,
        collection_id=src_collection,
    )
    phys_table = col_config.physical_table
    if not phys_schema or not phys_table:
        return dict(_EMPTY_CARDINALITY)

    attrs_config = _find_attributes_sidecar(col_config)
    if attrs_config is None:
        return dict(_EMPTY_CARDINALITY)
    attrs_sidecar = SidecarRegistry.get_sidecar(attrs_config, lenient=True)
    if not isinstance(attrs_sidecar, FeatureAttributeSidecar):
        return dict(_EMPTY_CARDINALITY)

    schema = validate_sql_identifier(phys_schema)
    hub_table = validate_sql_identifier(phys_table)
    attrs_table = validate_sql_identifier(
        sidecar_table_name(phys_table, attrs_config.sidecar_id)
    )

    params: Dict[str, Any] = {}
    if attrs_sidecar.resolved_storage_mode == AttributeStorageMode.COLUMNAR:
        declared = _columnar_columns(attrs_config)
        if region_prop not in declared or unique_id_prop not in declared:
            return dict(_EMPTY_CARDINALITY)
        region_expr = f's."{validate_column_identifier(region_prop)}"'
        fid_expr = f's."{validate_column_identifier(unique_id_prop)}"'
    else:
        jsonb_col = validate_column_identifier(attrs_config.jsonb_column_name)
        region_expr = f's."{jsonb_col}" ->> :region_prop'
        fid_expr = f's."{jsonb_col}" ->> :unique_id_prop'
        params["region_prop"] = region_prop
        params["unique_id_prop"] = unique_id_prop

    from_where = (
        f'FROM "{schema}"."{hub_table}" h '
        f'JOIN "{schema}"."{attrs_table}" s ON s.geoid = h.geoid '
        f'WHERE h.deleted_at IS NULL AND {region_expr} IS NOT NULL AND {fid_expr} IS NOT NULL'
    )
    if attrs_config.validity_column is not None:
        # Count over one row per uniqueIdProp value (its latest version) --
        # exactly the set /regionIds serves. Without this, a feature carried
        # across validity windows inflates feature_count above the distinct id
        # count and the mapping reads as broken when it is soundly versioned.
        # Applied only when validity is configured (a no-op otherwise).
        latest = (
            f'SELECT DISTINCT ON ({fid_expr}) {region_expr} AS region_value, {fid_expr} AS fid '
            f'{from_where} ORDER BY {fid_expr}, h.geoid DESC'
        )
        sql = (
            f'SELECT COUNT(*) AS feature_count, '
            f'COUNT(DISTINCT region_value) AS distinct_region_count, '
            f'COUNT(DISTINCT fid) AS distinct_unique_id_count FROM ({latest}) v'
        )
    else:
        sql = (
            f'SELECT COUNT(*) AS feature_count, '
            f'COUNT(DISTINCT {region_expr}) AS distinct_region_count, '
            f'COUNT(DISTINCT {fid_expr}) AS distinct_unique_id_count '
            f'{from_where}'
        )

    # Live features whose uniqueIdProp value is NULL -- they cannot be
    # positioned in the regionIds array at all. Counted over the raw live set
    # (before any non-null filter or version-dedup) so the mapping can be
    # refused when the required per-feature index is not populated everywhere.
    null_where = (
        f'FROM "{schema}"."{hub_table}" h '
        f'JOIN "{schema}"."{attrs_table}" s ON s.geoid = h.geoid '
        f'WHERE h.deleted_at IS NULL AND {fid_expr} IS NULL'
    )
    null_sql = f'SELECT COUNT(*) AS null_unique_id_count {null_where}'
    null_params = (
        {"unique_id_prop": unique_id_prop}
        if attrs_sidecar.resolved_storage_mode != AttributeStorageMode.COLUMNAR
        else {}
    )

    engine = get_engine()
    if engine is None:
        return dict(_EMPTY_CARDINALITY)
    async with managed_transaction(
        engine, acquire_timeout=await _read_live_fg_acquire_timeout()
    ) as conn:
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(conn, **params)
        null_row = await DQLQuery(
            null_sql, result_handler=ResultHandler.ONE_DICT,
        ).execute(conn, **null_params)
    if not row:
        return dict(_EMPTY_CARDINALITY)
    return {
        "feature_count": int(row["feature_count"] or 0),
        "distinct_region_count": int(row["distinct_region_count"] or 0),
        "distinct_unique_id_count": int(row["distinct_unique_id_count"] or 0),
        "null_unique_id_count": int((null_row or {}).get("null_unique_id_count") or 0),
    }


def validate_region_mapping_stats(stats: Dict[str, int]) -> List[str]:
    """Human-readable reasons ``stats`` (from
    :func:`fetch_region_mapping_cardinality`) describes a misconfigured
    mapping. Empty list means the mapping is sound."""
    feature_count = stats.get("feature_count", 0)
    reasons: List[str] = []
    null_unique_id_count = stats.get("null_unique_id_count", 0)
    if null_unique_id_count > 0:
        reasons.append(
            f"uniqueIdProp is not populated on every feature: {null_unique_id_count} "
            "live feature(s) have a NULL uniqueIdProp value. Every feature must carry "
            "a non-null per-feature index for the regionIds array to position it -- "
            "an external_id/index column with gaps cannot back a region mapping."
        )
    if feature_count == 0:
        reasons.append(
            "No features have non-null values for both regionProp and "
            "uniqueIdProp -- the source collection/columns may be wrong, "
            "or every value is missing."
        )
        return reasons
    distinct_region_count = stats.get("distinct_region_count", 0)
    if distinct_region_count < feature_count:
        reasons.append(
            f"regionProp is not unique per feature: {feature_count} features share only "
            f"{distinct_region_count} distinct values. TerriaJS will highlight every "
            "feature carrying a given code whenever that code appears in a CSV row, "
            "instead of exactly one -- register a column with one distinct value per "
            "feature (e.g. an admin-1 code on an admin-1 collection, not a country code)."
        )
    distinct_unique_id_count = stats.get("distinct_unique_id_count", 0)
    if distinct_unique_id_count < feature_count:
        reasons.append(
            f"uniqueIdProp is not unique per feature: {feature_count} features share only "
            f"{distinct_unique_id_count} distinct values. The regionIds array is positioned "
            "by this column, so features sharing a value silently overwrite each other's "
            "region code."
        )
    return reasons


NO_COLUMNAR_SCHEMA_REASON = (
    "Source collection has no columnar items_schema; region mapping requires "
    "declared physical columns (JSONB attributes cannot back a region layer)."
)


def uncached_if(fn: Any, no_cache: bool) -> Any:
    """Return ``fn``'s underlying uncached original when ``no_cache`` is set,
    else ``fn`` itself.

    The :func:`dynastore.tools.cache.cached` decorator wraps with
    ``functools.wraps``, so the raw (cache-free) coroutine is reachable as
    ``fn.__wrapped__``. This lets a ``?no_cache=true`` request read straight
    through to PostgreSQL for one call without clearing anyone's cache -- the
    diagnostic escape hatch for the per-pod read-cache lag on the serving
    endpoints.
    """
    return getattr(fn, "__wrapped__", fn) if no_cache else fn


async def evaluate_mapping_soundness(
    catalog: str, collection: str, region_prop: str, unique_id_prop: str,
    *, no_cache: bool = False,
) -> Tuple[List[str], Dict[str, int]]:
    """Every condition a registered mapping must still satisfy to be served,
    checked live against its source collection. Returns ``(reasons, stats)`` --
    an empty ``reasons`` means sound.

    The single soundness authority shared by ``GET /{id}/validate`` and the
    ``GET /region.json`` exclusion filter, so a mapping is served iff it would
    validate: source exists, still has a columnar items_schema, still declares
    both ``region_prop`` and ``unique_id_prop`` as columns, and their live
    cardinality (external_id checked against its latest-version-per-id set)
    identifies one feature per code with no NULL index gaps.
    """
    reasons: List[str] = []
    stats = dict(_EMPTY_CARDINALITY)

    catalogs = get_protocol(CatalogsProtocol)
    source = (
        await catalogs.get_collection(catalog, collection) if catalogs is not None else None
    )
    if source is None:
        reasons.append(f"Source collection {catalog!r}/{collection!r} no longer exists.")
        return reasons, stats

    cols = await resolve_collection_columns(catalog, collection)
    if cols is None or not cols.is_columnar or not cols.declared:
        reasons.append(NO_COLUMNAR_SCHEMA_REASON)
        return reasons, stats

    if not cols.has_column(region_prop):
        reasons.append(
            f"regionProp {region_prop!r} is no longer a declared column of the "
            "source collection's items_schema."
        )
    if not cols.has_column(unique_id_prop):
        reasons.append(
            f"uniqueIdProp {unique_id_prop!r} is no longer a declared column of the "
            "source collection's items_schema."
        )
    if reasons:
        return reasons, stats

    stats = await uncached_if(fetch_region_mapping_cardinality, no_cache)(
        catalog, collection, region_prop, unique_id_prop,
    )
    reasons.extend(validate_region_mapping_stats(stats))
    return reasons, stats
