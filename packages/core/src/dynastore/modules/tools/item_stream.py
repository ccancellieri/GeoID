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

"""
Driver-agnostic item stream normalization and streaming-join utilities.

Historically the PG read path could echo columns selected outside the declared
attribute schema into the Feature's model_extra (extra='allow'), while the
Elasticsearch canonical path puts them in feature.properties directly. That
mismatch caused every feature to appear property-less on the PG path for
consumers that join on feature.properties, silently dropping all rows (#1818).

As of #1826 the PG producer (``item_service.map_row_to_feature``) guarantees
the contract directly — user attributes live only in ``feature.properties`` and
are no longer duplicated into model_extra — so ``normalize_feature_attributes``
is now a defense-in-depth safety net (a no-op on a well-formed PG/ES feature)
rather than the load-bearing fix. It is kept so any future driver that emits
model_extra-only attributes still reads uniformly here.

This module provides:
  normalize_feature_attributes — lift any stray model_extra keys into
                                  feature.properties, making the Feature layout
                                  uniform across drivers.
  resolve_join_value           — section-aware join key resolver (#1827).
  stream_normalized_items      — the shared stream boundary: call stream_items
                                  and normalize each Feature before yielding.
  stream_join_features         — the shared streaming-join primitive (#1835):
                                  key extraction via resolve_join_value, O(1)
                                  dict lookup, merge-into-properties, yield.
                                  Both enrich_features (dwh path) and run_join
                                  (OGC joins path) delegate here so join
                                  semantics live in exactly one place.
"""

from typing import Any, AsyncIterator, Dict, FrozenSet, Optional, TYPE_CHECKING

from dynastore.models.ogc import Feature

if TYPE_CHECKING:
    from dynastore.models.protocols import ItemsProtocol  # noqa: F401
    from dynastore.models.protocols.configs import ConfigsProtocol  # noqa: F401
    from dynastore.models.driver_context import DriverContext  # noqa: F401
    from dynastore.models.query_builder import QueryRequest  # noqa: F401
    from dynastore.modules.storage.hints import Hint  # noqa: F401
    from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType  # noqa: F401


def normalize_feature_attributes(feature: Feature) -> Feature:
    """Lift pydantic model_extra keys into feature.properties so consumers can
    join on feature.properties regardless of the backing driver.

    The PG read path leaves columns selected outside the declared attribute
    schema in the Feature's model_extra (extra='allow'); the Elasticsearch
    canonical path populates properties directly. This merges both into
    properties (existing properties wins on key collision) and clears the
    lifted extras so they are not also emitted at the top level. See #1818.

    Mutates and returns the same feature. Idempotent: a feature with no
    model_extra is returned unchanged.
    """
    extra = getattr(feature, "model_extra", None)
    if not extra:
        return feature
    props = feature.properties if feature.properties else {}
    feature.properties = {**extra, **props}
    if feature.__pydantic_extra__:
        feature.__pydantic_extra__.clear()
    return feature


# Sections where expose_all and external_id_as_feature_id place system/stats
# fields on the Feature (as foreign members or in model_extra). These are
# checked in order by resolve_join_value when the key is absent from flat
# feature.properties. See #1827.
_JOIN_SECTIONS = ("system", "stats")


def resolve_join_value(
    feature: Feature,
    join_column: str,
    join_source: str = "properties",
) -> Optional[Any]:
    """Resolve a join key from a Feature using an explicit section.

    join_source="properties" reads feature.properties[join_column] (falling back to
    feature.id when absent, preserving the catalog-tier id-join behavior).
    join_source="system"/"stats" reads ONLY that foreign-member section
    (feature.properties[section][join_column], or model_extra[section] pre-normalization),
    so a system identity like external_id or a stat like area can be joined without
    colliding with a same-named property. See #1827.

    When join_source="properties" and the key is present in feature.properties
    (even as None), returns that value directly — preserving the existing semantics
    where an explicit None in properties means the row should be dropped.
    """
    props = feature.properties or {}
    if join_source == "properties":
        if join_column in props:
            return props[join_column]
        extra = getattr(feature, "model_extra", None) or {}
        for src in (props, extra):
            for section in _JOIN_SECTIONS:
                sec = src.get(section)
                if isinstance(sec, dict) and join_column in sec:
                    return sec[join_column]
        return getattr(feature, "id", None)
    # Explicit section: read ONLY from the named section (properties or model_extra).
    extra = getattr(feature, "model_extra", None) or {}
    for src in (props, extra):
        sec = src.get(join_source)
        if isinstance(sec, dict) and join_column in sec:
            return sec[join_column]
    return None


async def stream_join_features(
    primary_stream: AsyncIterator[Feature],
    secondary_index: Dict[Any, Dict[str, Any]],
    join_column: str,
    *,
    join_source: str = "properties",
    inner_join: bool = True,
) -> AsyncIterator[Feature]:
    """Streaming O(1) per-feature dict-lookup join — the shared primitive.

    Iterates ``primary_stream`` and merges matching secondary properties from
    ``secondary_index`` (keyed by the join-column value) into each feature's
    ``properties``.  Both ``enrich_features`` (dwh export path) and ``run_join``
    (OGC joins path) delegate to this function so join semantics live in one
    place (#1835).

    Key resolution is handled by ``resolve_join_value`` so that
    ``join_source="system"`` or ``"stats"`` routes the lookup to the appropriate
    named section without colliding with a same-named user property (#1827).

    Args:
        primary_stream: Async iterator of primary ``Feature`` objects.
        secondary_index: ``{join_value: properties_dict}`` prepared by the
            caller (e.g. ``index_secondary`` or ``get_enrichment_data``).
        join_column: The property/column name used as the join key.
        join_source: Which section to read the join key from.  One of
            ``"properties"`` (default), ``"system"``, or ``"stats"``.
        inner_join: When ``True`` (default) only features with a matching
            secondary row are yielded.  When ``False`` (LEFT JOIN) all
            primary features are yielded; those without a match pass through
            with their properties unmodified.

    Yields:
        ``Feature`` instances.  Matched features have secondary properties
        merged into ``feature.properties`` (secondary wins on collision, so
        the enrichment data overrides the primary).  Unmatched features are
        yielded unchanged when ``inner_join=False``.
    """
    async for feature in primary_stream:
        props = feature.properties or {}
        key = resolve_join_value(feature, join_column, join_source)
        match = secondary_index.get(key) if key is not None else None
        if match is not None:
            yield Feature(
                type=feature.type,
                id=feature.id,
                geometry=feature.geometry,
                properties={**props, **match},
            )
        elif not inner_join:
            yield feature


async def stream_normalized_items(
    items_svc: "ItemsProtocol",
    catalog_id: str,
    collection_id: str,
    request: "QueryRequest",
    *,
    config: "ConfigsProtocol | None" = None,
    ctx: "DriverContext | None" = None,
    consumer: "ConsumerType | None" = None,
    hints: "FrozenSet[Hint]" = frozenset(),
) -> AsyncIterator[Feature]:
    """Stream via ItemsProtocol.stream_items and yield normalize_feature_attributes(f)
    for each feature. The single point where dwh join, OGC joins and feature
    export become blind to whether the store is PostgreSQL or Elasticsearch.

    See #1818: normalization ensures model_extra keys from the PG driver are
    visible in feature.properties for all downstream join and export consumers.
    """
    response = await items_svc.stream_items(
        catalog_id=catalog_id,
        collection_id=collection_id,
        request=request,
        config=config,
        ctx=ctx,
        consumer=consumer,
        hints=hints,
    )
    async for feature in response.items:
        yield normalize_feature_attributes(feature)
