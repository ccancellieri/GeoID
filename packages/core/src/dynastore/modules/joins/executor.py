"""Execute a JoinRequest: stream primary features + merge secondary lookup.

Driver-agnostic: the executor takes a primary-stream callable and an
already-materialized secondary lookup dict, so callers can wire in any
data source (DynaStore items via ItemsProtocol, BigQuery via Phase 4a's
ItemsBigQueryDriver, ad-hoc test fixtures, etc.).
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Callable, Dict

from dynastore.models.ogc import Feature
from dynastore.modules.joins.models import JoinRequest

PrimaryStream = Callable[..., AsyncIterator[Feature]]


async def run_join(
    request: JoinRequest,
    *,
    primary_stream: AsyncIterator[Feature],
    secondary_index: Dict[Any, Dict[str, Any]],
) -> AsyncIterator[Feature]:
    """Execute the join.

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
    enrich = request.join.enrichment
    proj = request.projection
    paging = request.paging
    yielded = 0
    skipped = 0

    async for feat in primary_stream:
        props = feat.properties or {}
        # Look up the join value: prefer the explicit property, but fall
        # back to feat.id only when the column is ABSENT from properties.
        # Drivers like ItemsBigQueryDriver promote the id_column out of
        # `properties` into `feat.id` (see bigquery_stream.row_to_feature),
        # so a join on the id column would otherwise drop every feature.
        # An explicit None in properties means the caller actively wants
        # to drop the row (NULL keys do not match) — distinguish absent
        # vs. None-valued so we don't resurrect intentionally-null rows.
        if join_col in props:
            key = props[join_col]
        else:
            key = feat.id
        if key is None:
            continue
        match = secondary_index.get(key)
        if match is None:
            continue  # inner-join semantics by default — no LEFT JOIN at PR-1

        if paging is not None and skipped < paging.offset:
            skipped += 1
            continue

        merged = {**props, **match} if enrich else dict(props)
        if proj.attributes is not None:
            # Drop primary attributes the caller didn't ask for, but ALWAYS
            # keep the join key so client output stays self-describing.
            keep = set(proj.attributes) | {join_col}
            merged = {k: v for k, v in merged.items() if k in keep}

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
