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
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
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
    catalog_id: str,
    collection_id: str,
    column: str,
    alias: str,
    extra_aliases: Sequence[str],
) -> "OrderedDict[str, Tuple[str, str]]":
    """Return ``{claim_ci: (claim, role)}`` for one ``region_mapping`` apply.

    The claim set is ``{column, alias, *extra_aliases,
    "{catalog_id}_{alias}"}``. Deduplicated case-insensitively
    (``casefold``); the entry whose ``casefold()`` matches ``alias``'s is
    ``role="primary"``, every other member is ``role="alias"`` -- exactly
    one primary always results, even when ``column`` (or an
    ``extra_alias``) differs from ``alias`` only by case.
    """
    alias_ci = alias.casefold()
    candidates = [column, alias, *extra_aliases, f"{catalog_id}_{alias}"]

    claims: "OrderedDict[str, Tuple[str, str]]" = OrderedDict()
    for candidate in candidates:
        validate_claim_text(candidate)
        claim_ci = candidate.casefold()
        if claim_ci in claims:
            continue
        role = ROLE_PRIMARY if claim_ci == alias_ci else ROLE_ALIAS
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
        declared = {attr.name for attr in (attrs_config.attribute_schema or [])}
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
    async with managed_transaction(engine) as conn:
        values = await DQLQuery(
            sql, result_handler=ResultHandler.ALL_SCALARS,
        ).execute(conn, **params)
    return sorted(str(v) for v in (values or []))
