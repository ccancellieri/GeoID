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

"""Execute a JoinRequest: stream primary features + merge secondary lookup.

Driver-agnostic: the executor takes a primary-stream callable and an
already-materialized secondary lookup dict, so callers can wire in any
data source (DynaStore items via ItemsProtocol, BigQuery via Phase 4a's
ItemsBigQueryDriver, ad-hoc test fixtures, etc.).

The core join loop (key extraction, O(1) dict lookup, merge-into-properties)
is shared with the dwh enrichment path via ``resolve_join_value`` from
``modules/tools/item_stream`` (#1835).  ``run_join`` adds OGC-joins-specific
concerns on top: ``enrichment=False`` pass-through, ``projection.attributes``
column filtering, geometry selection, and paging (offset + limit).  These
concerns require access to the pre-merge primary properties, so ``run_join``
keeps its own async-for loop rather than delegating wholesale to the async
generator ``stream_join_features``; it does use ``resolve_join_value`` so the
key-extraction logic is not duplicated.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Callable, Dict, Optional

from dynastore.models.ogc import Feature
from dynastore.modules.joins.models import JoinRequest
from dynastore.modules.tools.item_stream import normalize_feature_attributes, resolve_join_value

PrimaryStream = Callable[..., AsyncIterator[Feature]]


def _merge_properties(
    primary_props: Dict[str, Any],
    secondary_props: Optional[Dict[str, Any]],
    enrich: bool,
    join_col: str,
    proj_attrs: Optional[list[str]],
) -> Dict[str, Any]:
    """Merge primary and secondary properties with optional projection.
    
    Args:
        primary_props: Primary feature properties.
        secondary_props: Secondary properties (None for LEFT JOIN non-matches).
        enrich: If True, merge secondary properties into primary.
        join_col: Join key column (always preserved).
        proj_attrs: Optional attribute whitelist.
    
    Returns:
        Merged properties dict with secondary fields set to None for
        LEFT JOIN non-matches.
    """
    if secondary_props is not None and enrich:
        merged = {**primary_props, **secondary_props}
    elif secondary_props is not None and not enrich:
        merged = dict(primary_props)
    elif secondary_props is None and enrich:
        # LEFT JOIN with no match: preserve primary props, set secondary to None
        # We don't know which secondary columns exist, so we just pass through primary
        merged = dict(primary_props)
    else:
        merged = dict(primary_props)
    
    if proj_attrs is not None:
        # Drop primary attributes the caller didn't ask for, but ALWAYS
        # keep the join key so client output stays self-describing.
        keep = set(proj_attrs) | {join_col}
        merged = {k: v for k, v in merged.items() if k in keep}
    
    return merged


async def run_join(
    request: JoinRequest,
    *,
    primary_stream: AsyncIterator[Feature],
    secondary_index: Dict[Any, Dict[str, Any]],
) -> AsyncIterator[Feature]:
    """Execute the join.

    Uses ``resolve_join_value`` (from ``modules/tools/item_stream``) for key
    extraction so the resolution logic is shared with the dwh ``enrich_features``
    path (#1835).  OGC-joins-specific concerns (``enrichment=False`` props-only
    pass-through, ``projection.attributes`` filtering, geometry selection, paging)
    are applied in this loop because they require access to the pre-merge primary
    properties or must interleave with the offset counter.

    Args:
        request: Validated JoinRequest.
        primary_stream: Async iterator over primary collection features.
        secondary_index: ``{join_value: row_dict}`` mapping prepared by
            the caller (typically by exhausting the secondary driver's
            stream and indexing on ``request.join.secondary_column``).

    Yields features with secondary properties merged into
    ``feature.properties`` (when ``request.join.enrichment is True``);
    otherwise just yields matching features unchanged.
    """
    join_col = request.join.primary_column
    join_type = request.join.join_type
    enrich = request.join.enrichment
    proj = request.projection
    paging = request.paging
    yielded = 0
    skipped = 0

    async for feat in primary_stream:
        feat = normalize_feature_attributes(feat)
        props = feat.properties or {}
        # resolve_join_value handles the absent-vs-None distinction and the
        # feature.id fallback in one place, replacing the inline if/else that
        # was here before (#1835 unification).
        key = resolve_join_value(feat, join_col, "properties")
        if key is None:
            continue
        match = secondary_index.get(key)
        
        # LEFT JOIN: yield primary feature even without match
        # INNER JOIN: skip feature if no match
        if match is None and join_type == "INNER":
            continue

        if paging is not None and skipped < paging.offset:
            skipped += 1
            continue

        # For LEFT JOIN with no match, use empty dict for secondary properties
        secondary_props: Optional[Dict[str, Any]] = match if match is not None else None
        merged = _merge_properties(
            primary_props=props,
            secondary_props=secondary_props,
            enrich=enrich,
            join_col=join_col,
            proj_attrs=proj.attributes,
        )

        yield Feature(
            type="Feature",
            id=feat.id,
            geometry=feat.geometry if proj.with_geometry else None,
            properties=merged,
        )
        yielded += 1
        if paging is not None and yielded >= paging.limit:
            return


async def index_secondary(
    secondary_stream: AsyncIterator[Feature],
    *,
    secondary_column: str,
) -> Dict[Any, Dict[str, Any]]:
    """Drain a secondary feature stream into a {key: properties} dict.

    Used by the joins service to pre-materialize the secondary side
    before invoking ``run_join``. Streaming both sides simultaneously
    is a future optimization; the dict-lookup approach is proven by the
    existing /dwh path.
    """
    out: Dict[Any, Dict[str, Any]] = {}
    async for feat in secondary_stream:
        feat = normalize_feature_attributes(feat)
        props = feat.properties or {}
        # See run_join() above for the matching fallback rationale: BQ
        # (and any other driver that promotes the join column to feat.id)
        # would otherwise yield zero indexable rows. Distinguish absent
        # vs. explicit None so a NULL secondary value drops the row
        # (which is standard JOIN semantics) instead of being resurrected
        # under feat.id.
        if secondary_column in props:
            key = props[secondary_column]
        else:
            key = feat.id
        if key is not None:
            # Surface the join key inside properties too so downstream
            # `enrichment` merges round-trip the value into the joined
            # feature even when the secondary driver hoisted it out.
            row = dict(props)
            row.setdefault(secondary_column, key)
            out[key] = row
    return out
