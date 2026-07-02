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
layer) via ``CatalogsProtocol``, never the registry itself. Registry
persistence (the ``region_mapping.mappings`` table) lives in
``registry_queries.py`` / ``registry_store.py``.
"""
from __future__ import annotations

import re
from collections import OrderedDict
from typing import List, Optional, Sequence, Tuple

from dynastore.models.query_builder import FieldSelection, QueryRequest, SortOrder
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.tools.cache import cached
from dynastore.tools.discovery import get_protocol

ROLE_PRIMARY = "primary"
ROLE_ALIAS = "alias"

WORLD_BBOX: Tuple[float, float, float, float] = (-180.0, -90.0, 180.0, 90.0)

# regionIds cardinality can run into the thousands for country/admin
# boundary layers — bounded offset loop below structurally bypasses the
# HTTP 1000-item response cap.
REGION_IDS_PAGE_SIZE = 5000

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
