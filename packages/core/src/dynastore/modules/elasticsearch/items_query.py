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

"""Single source of truth for the Elasticsearch geoid-search query DSL.

This module lives in ``core`` so that **both** the items storage drivers
(``ItemsElasticsearchDriver`` and ``ItemsElasticsearchPrivateDriver``) and the
search extension build the *same* ES query body. Routing-aware item search
(issue #989) dispatches structural queries through whichever driver the
derived search pool resolves for the catalog's ``ItemsRoutingConfig``
(INDEX lane preferred, READ lane fallback — see
``dynastore.modules.storage.router.get_items_search_driver``) — public ES,
the tenant-scoped private ES index, or any future search-capable driver — so
the query construction can no longer live inside one extension's service class.

The functions are intentionally free of any model dependency (plain kwargs in,
plain ``dict`` out) so the core driver layer never imports search-extension
models.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from dynastore.tools.ogc_common import parse_rfc3339_interval


@dataclass(frozen=True)
class EnvelopeFields:
    """Names of the system-envelope fields a structural items query filters on.

    The two ES item drivers write the same logical envelope under *different*
    field names: the public per-catalog index uses STAC-flavoured names
    (``collection``, ``id``) while the tenant-private index uses the canonical
    names (``collection_id``, ``geoid``). Both now carry ``external_id`` at the
    document root — the legacy public ``_external_id`` mirror was removed in
    #1285 identity convergence, so structural queries address the root field on
    every index shape.
    A structural query must address whichever shape the resolved index carries,
    so :func:`build_items_query` takes one of these mappings instead of
    hard-coding a single shape (the D1 "reconcile public/private envelope"
    divergence — until both shapes converge this keeps the SSOT honest for both
    indexes).

    ``geoid`` and ``item_id`` are distinct because publicly the STAC item ``id``
    *is* the geoid (one field), whereas the private doc keeps a dedicated
    ``geoid`` keyword.
    """

    collection: str = "collection"
    item_id: str = "id"
    geoid: str = "id"
    external_id: str = "external_id"


#: Public per-catalog items index field names (STAC-flavoured) — the default.
PUBLIC_ENVELOPE_FIELDS = EnvelopeFields()

#: Tenant-private items index field names (canonical envelope).
PRIVATE_ENVELOPE_FIELDS = EnvelopeFields(
    collection="collection_id",
    item_id="geoid",
    geoid="geoid",
    external_id="external_id",
)

#: Canonical envelope-driver field names. The standardized items index carries
#: the canonical identity envelope (``collection_id`` / ``geoid`` /
#: ``external_id``) at the document root — the same shape as the private index —
#: so structural queries address those names. Kept as a distinct constant from
#: ``PRIVATE_ENVELOPE_FIELDS`` because the envelope driver is its own index
#: shape (it adds a typed access envelope) and the two may diverge independently
#: as the standardized index evolves.
ENVELOPE_FIELDS = EnvelopeFields(
    collection="collection_id",
    item_id="geoid",
    geoid="geoid",
    external_id="external_id",
)


def parse_sort(
    sortby: Optional[str],
    known_fields: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Parse a STAC ``sortby`` string into an ES sort clause.

    Sort paths are resolved through
    :func:`dynastore.modules.elasticsearch.items_projection.resolve_es_field_path`
    so a request to sort on an unknown extension field
    (``properties.foo:bar``) targets the ``properties.extras.foo:bar`` bucket
    where the projection helper actually wrote it. Tier-1 paths (and Tier-2
    paths when ``known_fields`` carries the per-catalog overlay) pass through
    unchanged.

    ``known_fields`` defaults to Tier 1 only — used by cross-catalog
    ``/search`` (against the platform alias) where Tier-2 fields are not
    guaranteed to exist on every member; on non-declaring catalogs they sort
    as missing (graceful per-index miss, zero ES cost).
    """
    if not sortby:
        return [{"_score": {"order": "desc"}}]
    from dynastore.modules.elasticsearch.items_projection import (
        build_known_fields,
        resolve_es_field_path,
    )
    if known_fields is None:
        known_fields = build_known_fields()
    direction = "desc" if sortby.startswith("-") else "asc"
    field = sortby.lstrip("+-")
    field = resolve_es_field_path(field, known_fields)
    # Text fields sort on the .keyword sub-field
    text_fields = {"properties.title", "title", "description", "properties.description"}
    if field in text_fields:
        field = f"{field}.keyword"
    return [{field: {"order": direction}}, {"_score": {"order": "desc"}}]


def parse_datetime_filter(datetime_str: Optional[str]) -> Optional[Dict[str, Any]]:
    """Convert a STAC datetime string to an ES range filter.

    A bare instant (no ``"/"``) intentionally keeps its own term-or-lte-range
    "should" query below rather than going through the shared interval split
    — that behavior is specific to this driver. Only the ``"/"``-interval
    branch reuses the shared ``".."``-open-bound parsing.
    """
    if not datetime_str:
        return None
    if "/" in datetime_str:
        gte, lte = parse_rfc3339_interval(datetime_str)
        r: Dict[str, Any] = {}
        if gte:
            r["gte"] = gte
        if lte:
            r["lte"] = lte
        return {"range": {"properties.datetime": r}}
    return {
        "bool": {
            "should": [
                {"term": {"properties.datetime": datetime_str}},
                {"range": {"properties.datetime": {"lte": datetime_str}}},
            ]
        }
    }


def build_items_query(
    *,
    q: Optional[str] = None,
    ids: Optional[List[str]] = None,
    geoid: Optional[List[str]] = None,
    external_id: Optional[List[str]] = None,
    collections: Optional[List[str]] = None,
    bbox: Optional[Sequence[float]] = None,
    intersects: Optional[Dict[str, Any]] = None,
    datetime: Optional[str] = None,
    fields: EnvelopeFields = PUBLIC_ENVELOPE_FIELDS,
) -> Dict[str, Any]:
    """Build an Elasticsearch items query DSL from structural search params.

    Returns the inner query (``bool`` / ``match_all``); callers wrap it in the
    ``{"query": ...}`` envelope and add their own index/collection scoping.

    ``catalog_id`` scoping is enforced by index naming itself (the caller routes
    a scoped query to ``{prefix}-{catalog_id}-items`` / the tenant private
    index), so there is deliberately no ``catalog_id`` term filter here — the
    driver does not materialise a ``catalog_id`` keyword on each item doc, so a
    term filter on it would match zero docs and silently empty every
    catalog-scoped response (#819 comment 4).
    """
    must: List[Dict[str, Any]] = []
    filter_: List[Dict[str, Any]] = []

    if q:
        # ``lenient: true`` makes Elasticsearch skip fields that cannot honour
        # the query (e.g. ``properties.file:size`` is mapped as ``long`` but
        # the ``properties.*`` wildcard pulls it into the multi_match — without
        # lenient, ES rejects the whole search). The wildcard is load-bearing
        # for STAC extension keywords we don't list explicitly, so we keep it
        # and let ES ignore type mismatches.
        must.append({
            "multi_match": {
                "query": q,
                "type": "best_fields",
                "fields": [
                    f"{fields.item_id}^3",
                    "title.*^2",
                    "properties.title.*^2",
                    "description.*",
                    "properties.description.*",
                    "keywords.*",
                    "properties.keywords.*",
                    "properties.*",
                ],
                "fuzziness": "AUTO",
                "lenient": True,
                "minimum_should_match": "1",
            }
        })

    if ids:
        filter_.append({"terms": {fields.item_id: ids}})

    if geoid:
        # In the public index the STAC Item ``id`` IS the geoid — the canonical
        # identifier the platform mints and the only field reliably indexed at
        # the root for every item (``properties.geoid`` and root ``geoid`` are
        # reserved-but-unwritten in the public items mapping, #819). The private
        # index keeps a dedicated root ``geoid`` keyword, so ``fields.geoid``
        # selects the right one per index shape.
        filter_.append({"terms": {fields.geoid: geoid}})

    if external_id:
        # Both index shapes carry an un-prefixed ``external_id`` root keyword
        # (the canonical identity field). The legacy public ``_external_id``
        # mirror was removed in #1285 identity convergence.
        filter_.append({"terms": {fields.external_id: external_id}})

    if collections:
        filter_.append({"terms": {fields.collection: collections}})

    if bbox:
        lon_min, lat_min, lon_max, lat_max = bbox[:4]
        filter_.append({
            "geo_shape": {
                "geometry": {
                    "shape": {
                        "type": "envelope",
                        "coordinates": [[lon_min, lat_max], [lon_max, lat_min]],
                    },
                    "relation": "intersects",
                }
            }
        })
    elif intersects:
        filter_.append({
            "geo_shape": {
                "geometry": {
                    "shape": intersects,
                    "relation": "intersects",
                }
            }
        })

    dt_filter = parse_datetime_filter(datetime)
    if dt_filter:
        filter_.append(dt_filter)

    if not must and not filter_:
        return {"match_all": {}}
    return {
        "bool": {
            **({"must": must} if must else {}),
            **({"filter": filter_} if filter_ else {}),
        }
    }
