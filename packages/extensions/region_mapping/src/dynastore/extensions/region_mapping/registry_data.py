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

"""Shared data-access + claim-computation kernel for the region_mapping extension.

Holds no FastAPI / preset-framework dependency — both the read-only router
(``region_mapping_service.py``) and the two presets
(``presets/region_mappings_registry.py``, ``presets/region_mapping.py``)
import from here so the registry catalog/collection ids, the ``ItemsSchema``
shape, the claim-set computation, and the cached read helpers have a single
source of truth.

Uniqueness / conflict design
-----------------------------
Every claim record is stored with ``id = "{mapping_id}__{claim_ci}"`` — NOT
``id = claim_ci``. This matters: the ``claim_ci`` field carries a native
UNIQUE constraint (``FieldDefinition(unique=True)``; see Rule 1 in
``modules/storage/field_constraints.py``), but that constraint is only
useful if two *different* mappings claiming the same text produce two
*different* item ids — otherwise an upsert keyed on ``id`` would silently
overwrite the earlier claim instead of hitting the UNIQUE constraint and
raising ``23505`` (mapped to HTTP 409 by the global exception-handler
chain). Re-applying the *same* mapping's *same* claim is a legitimate
idempotent update (stable id), while a second mapping claiming the same
text gets a fresh id and correctly collides on the ``claim_ci`` column.
"""
from __future__ import annotations

import re
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.models.query_builder import (
    FieldSelection,
    FilterCondition,
    QueryRequest,
    SortOrder,
)
from dynastore.modules.iam.audience_configs import CatalogLookupAudience
from dynastore.modules.storage.driver_config import ItemsSchema
from dynastore.tools.cache import cache_clear, cached
from dynastore.tools.discovery import get_protocol

REGISTRY_CATALOG_ID = "_region_mappings_"
MAPPINGS_COLLECTION_ID = "mappings"

ROLE_PRIMARY = "primary"
ROLE_ALIAS = "alias"

WORLD_BBOX: Tuple[float, float, float, float] = (-180.0, -90.0, 180.0, 90.0)

# The registry is bounded by (registered collections x aliases per
# collection) — not by regionIds cardinality — so a single generous fetch
# followed by client-side grouping/pagination is simpler and cheap for
# /region-mappings/definitions. The GROUP-BY-in-Postgres DISTINCT trick is
# reserved for fetch_distinct_region_ids below, which must scale to
# real-world regionIds cardinality (thousands+ for admin-boundary layers).
DEFINITIONS_FETCH_CAP = 5000
REGION_IDS_PAGE_SIZE = 5000

_REGEX_METACHARS = re.compile(r"[.^$*+?{}\[\]\\|()]")
_SLUG_INVALID = re.compile(r"[^a-z0-9_]+")
_SLUG_COLLAPSE = re.compile(r"_+")


def item_id_for(mapping_id: str, claim_ci: str) -> str:
    """Stable item id for one claim record — see the module docstring."""
    return f"{mapping_id}__{claim_ci}"


def build_registry_items_schema() -> ItemsSchema:
    """``ItemsSchema`` for the ``mappings`` RECORDS collection.

    ``claim_ci`` is the only field that needs a physical UNIQUE constraint;
    ``unique=True`` materializes as a native columnar UNIQUE constraint
    regardless of ``default_access`` (Rule 1,
    ``modules/storage/field_constraints.py``).
    """
    return ItemsSchema(
        fields={
            "claim_ci": FieldDefinition(name="claim_ci", data_type="string", unique=True, required=True),
            "claim": FieldDefinition(name="claim", data_type="string", required=True),
            "mapping_id": FieldDefinition(name="mapping_id", data_type="string", required=True),
            "role": FieldDefinition(name="role", data_type="string", required=True),
            "src_catalog": FieldDefinition(name="src_catalog", data_type="string", required=True),
            "src_collection": FieldDefinition(name="src_collection", data_type="string", required=True),
            "region_prop": FieldDefinition(name="region_prop", data_type="string", required=True),
            "alias": FieldDefinition(name="alias", data_type="string"),
            "title": FieldDefinition(name="title", data_type="string"),
        },
    )


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
    alias: Optional[str],
    extra_aliases: Sequence[str],
) -> "OrderedDict[str, Tuple[str, str]]":
    """Return ``{claim_ci: (claim, role)}`` for one ``region_mapping`` apply.

    The claim set is ``{column, canonical_alias, *extra_aliases,
    "{catalog_id}_{canonical_alias}"}`` where ``canonical_alias`` is
    ``alias`` or, when unset, ``column`` itself. Deduplicated
    case-insensitively (``casefold``); the entry equal to
    ``canonical_alias`` is ``role="primary"``, every other member is
    ``role="alias"``.
    """
    canonical_alias = alias or column
    candidates = [column, canonical_alias, *extra_aliases, f"{catalog_id}_{canonical_alias}"]

    claims: "OrderedDict[str, Tuple[str, str]]" = OrderedDict()
    for candidate in candidates:
        validate_claim_text(candidate)
        claim_ci = candidate.casefold()
        if claim_ci in claims:
            continue
        role = ROLE_PRIMARY if candidate == canonical_alias else ROLE_ALIAS
        claims[claim_ci] = (candidate, role)
    return claims


def is_degenerate_bbox(bbox: Optional[Sequence[float]]) -> bool:
    """True when ``bbox`` is missing or its extent collapses to zero/negative area."""
    if not bbox or len(bbox) < 4:
        return True
    minx, miny, maxx, maxy = bbox[0], bbox[1], bbox[2], bbox[3]
    return maxx <= minx or maxy <= miny


# ---------------------------------------------------------------------------
# Read helpers — shared by the extension router and preset dry_run.
# ---------------------------------------------------------------------------


async def fetch_claim_by_ci(claim_ci: str) -> Optional[Dict[str, Any]]:
    """Uncached exact lookup — used by ``region_mapping`` dry_run's
    conflicting-claim check, which needs a fresh read, never a stale cache
    hit."""
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return None
    features = await catalogs.search_items(
        REGISTRY_CATALOG_ID,
        MAPPINGS_COLLECTION_ID,
        QueryRequest(
            filters=[FilterCondition(field="claim_ci", operator="eq", value=claim_ci)],
            limit=1,
        ),
    )
    if not features:
        return None
    return dict(features[0].properties or {})


@cached(maxsize=256, ttl=300, namespace="region_mapping_primary_records")
async def fetch_primary_records(
    catalog: Optional[str], collection: Optional[str], alias_ci: Optional[str],
) -> List[Dict[str, Any]]:
    """Fetch the primary-role (or, when ``alias_ci`` is given, the exact
    claim) records used to build ``/region-mappings/definitions``.

    Bounded by ``DEFINITIONS_FETCH_CAP`` — see the module docstring above.
    """
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return []
    filters: List[FilterCondition] = []
    if alias_ci:
        filters.append(FilterCondition(field="claim_ci", operator="eq", value=alias_ci))
    else:
        filters.append(FilterCondition(field="role", operator="eq", value=ROLE_PRIMARY))
    if catalog:
        filters.append(FilterCondition(field="src_catalog", operator="eq", value=catalog))
    if collection:
        filters.append(FilterCondition(field="src_collection", operator="eq", value=collection))
    features = await catalogs.search_items(
        REGISTRY_CATALOG_ID,
        MAPPINGS_COLLECTION_ID,
        QueryRequest(
            filters=filters,
            sort=[SortOrder(field="mapping_id")],
            limit=DEFINITIONS_FETCH_CAP,
        ),
    )
    return [dict(f.properties or {}) for f in features]


async def _fetch_claims_for_mapping_impl(mapping_id: str) -> List[Dict[str, Any]]:
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return []
    features = await catalogs.search_items(
        REGISTRY_CATALOG_ID,
        MAPPINGS_COLLECTION_ID,
        QueryRequest(
            filters=[FilterCondition(field="mapping_id", operator="eq", value=mapping_id)],
            limit=1000,
        ),
    )
    return [dict(f.properties or {}) for f in features]


@cached(maxsize=256, ttl=300, namespace="region_mapping_claims_for_mapping")
async def fetch_claims_for_mapping(mapping_id: str) -> List[Dict[str, Any]]:
    """All claim records (any role) sharing ``mapping_id`` — used to build the
    ``aliases`` array of one definitions entry."""
    return await _fetch_claims_for_mapping_impl(mapping_id)


async def fetch_claims_for_mapping_uncached(mapping_id: str) -> List[Dict[str, Any]]:
    """Uncached variant of :func:`fetch_claims_for_mapping`.

    Used by the ``region_mapping`` preset's ``apply()`` (stale-claim cleanup
    on a changed alias set) and ``revoke()`` (authoritative delete-by-
    mapping_id) — both need the current registry state, never a stale cache
    hit, to avoid orphaning or under-deleting claim rows.
    """
    return await _fetch_claims_for_mapping_impl(mapping_id)


@cached(maxsize=256, ttl=300, namespace="region_mapping_mapping_primary")
async def fetch_mapping_primary(mapping_id: str) -> Optional[Dict[str, Any]]:
    """The single primary-role record for ``mapping_id`` — used by
    ``/region-mappings/{mapping_id}/regionIds`` to resolve ``src_catalog`` /
    ``src_collection`` / ``region_prop``."""
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return None
    features = await catalogs.search_items(
        REGISTRY_CATALOG_ID,
        MAPPINGS_COLLECTION_ID,
        QueryRequest(
            filters=[
                FilterCondition(field="mapping_id", operator="eq", value=mapping_id),
                FilterCondition(field="role", operator="eq", value=ROLE_PRIMARY),
            ],
            limit=1,
        ),
    )
    if not features:
        return None
    return dict(features[0].properties or {})


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


@cached(maxsize=128, ttl=120, namespace="region_mapping_region_ids")
async def fetch_distinct_region_ids(
    src_catalog: str, src_collection: str, region_prop: str,
) -> List[str]:
    """Sorted distinct values of ``region_prop`` in the source collection.

    Uses ``GROUP BY`` (== DISTINCT), driver-agnostic, in a bounded internal
    offset loop so it structurally bypasses the HTTP 1000-item response cap
    — regionIds cardinality can run into the thousands for country/admin
    boundary layers.
    """
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        return []
    values: set = set()
    offset = 0
    while True:
        features = await catalogs.search_items(
            src_catalog,
            src_collection,
            QueryRequest(
                select=[FieldSelection(field=region_prop)],
                group_by=[region_prop],
                sort=[SortOrder(field=region_prop)],
                limit=REGION_IDS_PAGE_SIZE,
                offset=offset,
            ),
        )
        if not features:
            break
        for feature in features:
            value = (feature.properties or {}).get(region_prop)
            if value is not None:
                values.add(str(value))
        if len(features) < REGION_IDS_PAGE_SIZE:
            break
        offset += REGION_IDS_PAGE_SIZE
    return sorted(values)


@cached(maxsize=512, ttl=60, namespace="region_mapping_catalog_public")
async def is_catalog_public(catalog_id: str) -> bool:
    """True when ``catalog_id`` has opted into anonymous lookup via
    ``CatalogLookupAudience.is_public``.

    Serve-time visibility gate: registering a ``region_mapping`` claim does
    not itself make the *source* catalog's data public — the direct
    ``/region-mappings/.*`` policy only opens the registry-serving routes,
    it does not bypass the source catalog's own access posture. The router
    calls this before returning anything derived from the source collection
    (bbox, title, regionIds) so a claim registered against a private
    catalog is not readable anonymously. Checked per request — the short
    TTL bounds how quickly a revoked opt-in closes the leak, not just how
    quickly a new opt-in becomes visible.
    """
    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        return False
    try:
        audience = await configs.get_config(CatalogLookupAudience, catalog_id=catalog_id)
    except Exception:
        return False
    return isinstance(audience, CatalogLookupAudience) and bool(audience.is_public)


def invalidate_serving_caches() -> None:
    """Clear every ``@cached`` region-mapping read used by the extension router.

    Called by the ``region_mapping`` preset after apply/revoke so newly
    registered (or removed) claims are visible on the next request without
    waiting out the cache TTL.
    """
    cache_clear(fetch_primary_records)
    cache_clear(fetch_claims_for_mapping)
    cache_clear(fetch_mapping_primary)
    cache_clear(fetch_collection_bbox)
    cache_clear(fetch_distinct_region_ids)
