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

"""
Cross-driver enrichment utilities.

Two functions:
  get_enrichment_data  — materialize enrichment data from any driver with
                         ENRICHMENT capability into a join-ready dict.
  enrich_features      — streaming O(1) join of a feature iterator against
                         pre-materialized enrichment data.

Typical usage pattern (replaces hardwired BQ join logic):

    # 1. Resolve enrichment driver
    enrichment_driver = await resolve_datasource(cat, coll, hints=frozenset({"enrichment"}))

    # 2. Materialize enrichment data (cached by caller when appropriate)
    enrichment = await get_enrichment_data(
        enrichment_driver, cat, coll,
        join_column="geoid",
        context={"query": bq_query, "project": "dwh-review"},
    )

    # 3. Optionally pre-filter feature query by join keys when set is small
    #    (existing QueryTransformProtocol pattern)

    # 4. Stream features from feature driver
    feature_driver = await resolve_datasource(cat, coll, hints=frozenset({Hint.FEATURES}))
    features = feature_driver.read_entities(cat, coll, request=query)

    # 5. Enrich (streaming, O(1) per feature)
    enriched = enrich_features(features, enrichment, join_column="geoid")
"""

from typing import Any, AsyncIterator, Dict, Optional

from dynastore.models.ogc import Feature
from dynastore.modules.tools.item_stream import resolve_join_value, stream_join_features


async def get_enrichment_data(
    driver: Any,
    catalog_id: str,
    collection_id: str,
    *,
    join_column: str,
    context: Optional[Dict[str, Any]] = None,
    limit: int = 10_000,
) -> Dict[Any, Dict[str, Any]]:
    """Materialize enrichment data from a driver into a join-ready dict.

    Reads all entities from ``driver.read_entities()`` and indexes them by
    ``join_column`` value.  The result is a plain ``dict`` suitable for
    O(1) per-feature lookup in ``enrich_features()``.

    Enrichment datasets are typically small (< 10 000 rows) so materializing
    them up-front avoids per-feature round-trips.  The caller is responsible
    for caching the result when appropriate.

    Args:
        driver: A ``CollectionItemsStore`` with ENRICHMENT capability.
        catalog_id: The catalog that owns the collection.
        collection_id: The collection to read enrichment data from.
        join_column: Feature property key used as join key.
        context: Driver-specific context (e.g. BigQuery query, project ID).
        limit: Safety cap on rows materialized. Defaults to 10 000.

    Returns:
        ``{join_key: properties_dict}`` — ready for ``enrich_features()``.
    """
    result: Dict[Any, Dict[str, Any]] = {}
    async for entity in driver.read_entities(
        catalog_id,
        collection_id,
        context=context,
        limit=limit,
    ):
        props = entity.properties or {}
        # resolve_join_value checks flat properties, system/stats sections,
        # and feature.id in order — covers all driver placements. See #1827.
        key = resolve_join_value(entity, join_column)
        if key is not None:
            row = dict(props)
            # Surface the join column as a property too so downstream
            # enrichment merges round-trip the value into the joined feature.
            row.setdefault(join_column, key)
            result[key] = row
    return result


async def enrich_features(
    feature_stream: AsyncIterator[Feature],
    enrichment_data: Dict[Any, Dict[str, Any]],
    join_column: str,
    *,
    inner_join: bool = True,
    join_source: str = "properties",
) -> AsyncIterator[Feature]:
    """Streaming O(1) per-feature dict lookup enrichment.

    Delegates to ``stream_join_features`` (the shared streaming-join primitive)
    so join semantics live in one place (#1835).

    Iterates ``feature_stream`` and merges matching enrichment properties
    from ``enrichment_data`` (keyed by ``join_column`` value).

    Args:
        feature_stream: Async iterator of ``Feature`` objects.
        enrichment_data: Pre-materialized join dict from ``get_enrichment_data()``.
        join_column: Feature property key to join on.
        inner_join: If ``True`` (default), only yield features that have a
            matching enrichment row.  If ``False``, yield all features
            (LEFT JOIN semantics — features without a match pass through
            unmodified).
        join_source: Which section to read the join key from. One of
            "properties" (default), "system", or "stats". When "system" or
            "stats", the key is resolved from the named foreign-member section
            only, so a system field like external_id can be used without
            colliding with a same-named user property. See #1827.

    Yields:
        ``Feature`` instances with enrichment properties merged into
        ``feature.properties``.
    """
    async for feature in stream_join_features(
        feature_stream,
        enrichment_data,
        join_column,
        join_source=join_source,
        inner_join=inner_join,
    ):
        yield feature
