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

"""Canonical routing hints — the per-request preference axis.

Three-axis routing model
========================

DynaStore's driver dispatch separates three orthogonal concerns.  Each
axis answers a different question; every concept the routing layer
deals with belongs to exactly one of them.

::

    +---------------+--------------------------+--------------------------+
    | Axis          | Question it answers      | Mechanism                |
    +===============+==========================+==========================+
    | ``Operation`` | What KIND of work?       | ``Operation`` StrEnum    |
    |               | (verb)                   | (WRITE / READ / SEARCH / |
    |               |                          | UPLOAD)                  |
    |               |                          |                          |
    +---------------+--------------------------+--------------------------+
    | ``Capability``| What can this driver DO? | ``Capability`` enums on  |
    |               | (structural fact about   | each driver — used by    |
    |               | the driver)              | the discovery endpoint   |
    |               |                          | and apply-handler        |
    |               |                          | validation.              |
    +---------------+--------------------------+--------------------------+
    | ``Hint``      | Which entry inside       | ``Hint`` (this module).  |
    |               | ``operations[Op]`` does  | Caller passes            |
    |               | the caller want?         | ``hints=frozenset({...})``|
    |               | (per-request preference) | to ``get_driver``;       |
    |               |                          | drivers self-declare via |
    |               |                          | ``supported_hints``;     |
    |               |                          | operators pin via        |
    |               |                          | ``OperationDriverEntry   |
    |               |                          | .hints``.                |
    +---------------+--------------------------+--------------------------+

Rule of thumb
-------------

* ``Operation`` is a verb — adding one is rare and structural.
* ``Capability`` is a noun about the driver — adding one means a new
  thing the driver can structurally do (e.g. soft-delete, transactional
  writes).
* ``Hint`` is an adjective on a request — adding one is cheap and
  doesn't churn enums or migrations.  A new "kind of search" (e.g.
  ``aggregation``, ``count``, ``geocoding``) is a new hint, not a new
  Operation or Capability.

Why a single canonical ``Hint`` catalogue
-----------------------------------------

Hint strings were previously declared inline as bare ``"snake_case"``
literals in ``OperationDriverEntry.hints`` defaults and in each
driver's ``supported_hints`` ClassVar.  That meant:

* Operators couldn't discover the canonical vocabulary.
* Typos failed silently — a hint string nobody recognises just
  matches nothing.
* Read-variants like ``fulltext`` / ``aggregation`` / ``count`` /
  ``statistics`` lived in the ``Capability`` enum, conflating "what
  the driver can structurally do" with "what flavour of read the
  caller is asking for".

This module is the single source of truth.  The values mirror the
existing string literals so that ``Hint.GEOMETRY_EXACT == "geometry_exact"``
holds at runtime — equality and set membership both succeed against the
enum member, which keeps comparisons against legacy persisted string
hints (e.g. on ``OperationDriverEntry.hints``) cheap.  The read-variant
flavours (``FULLTEXT``, ``AGGREGATION``, ``COUNT``, ``STATISTICS``,
``SORT``, ``GROUP_BY``, ``ATTRIBUTE_FILTER``, ``SPATIAL_FILTER``) were
promoted from the ``Capability`` enum once the structural/per-request
axis split landed; drivers self-declare which flavours they serve via
``supported_hints``.
"""

from enum import StrEnum


class Hint(StrEnum):
    """Canonical per-request routing hints.

    A ``StrEnum`` so persisted ``OperationDriverEntry.hints`` strings and
    driver-class ``supported_hints`` members interoperate transparently:
    ``Hint.GEOMETRY_EXACT == "geometry_exact"`` holds, and the matcher
    consumes a typed ``FrozenSet[Hint]`` end-to-end.

    Members are grouped by concern below; group order is editorial only.
    """

    # ── Geometry rendering preferences ────────────────────────────────
    # Read-path entries declare which precision they serve.  Public ES
    # carries ``GEOMETRY_SIMPLIFIED`` (fast, lossy); PG carries
    # ``GEOMETRY_EXACT`` (full WKB).
    GEOMETRY_SIMPLIFIED = "geometry_simplified"
    GEOMETRY_EXACT = "geometry_exact"

    # Collection/catalog metadata read from the STAC-shaped search-backend
    # (ES) rather than the canonical PG sidecar.  The ES copy is the
    # lightweight envelope stored at index time; PG is the system of record
    # (full envelope with all sidecar columns).  Callers that only need the
    # ES-shaped representation (e.g. landing-page renders that mirror the
    # search index) pass this hint to route the metadata READ to ES first
    # and PG as a fall-open fallback.  Combined with GEOMETRY_SIMPLIFIED
    # on the same ES entry so operators can request both in one hint set.
    STAC_PREFERRED = "stac_preferred"

    # Driver can produce MVT-shaped geometry rows that the tile renderer
    # wraps in ``ST_AsMVT``.  Today only PG advertises this; future ES /
    # DuckDB drivers opt in by adding it to ``supported_hints`` and
    # implementing ``get_features_query(geom_format="MVT")``.  Tile reads
    # MUST pass ``hints=frozenset({Hint.TILES})`` so first-match routing
    # returns a tile-capable driver even when ES is listed first for READ.
    TILES = "tiles"

    # ── Search / read-variant flavours ────────────────────────────────
    # These are flavours of ``Operation.SEARCH`` (or ``Operation.READ``).
    # The caller asks for a specific shape; the dispatcher picks an
    # entry whose ``hints`` (or driver class ``supported_hints``)
    # contains the requested flavour.  Each member below was promoted
    # from ``Capability`` once the read-variants stopped being treated
    # as structural facts about the driver and became per-request
    # preferences — callers route via
    # ``get_driver(Operation.SEARCH, hints=frozenset({Hint.AGGREGATION}))``
    # and drivers self-declare which flavours they serve via
    # ``supported_hints``.
    SEARCH = "search"
    FULLTEXT = "fulltext"
    ATTRIBUTE_FILTER = "attribute_filter"
    SPATIAL_FILTER = "spatial_filter"
    AGGREGATION = "aggregation"
    COUNT = "count"
    STATISTICS = "statistics"
    SORT = "sort"
    GROUP_BY = "group_by"

    # ── Workload preference ───────────────────────────────────────────
    # Coarse "what is this query for" tags.  A caller doing batch
    # analytical work passes ``ANALYTICS`` and gets routed to DuckDB /
    # Iceberg; an interactive feature read passes ``FEATURES`` and gets
    # PG.  Less precise than the search-variant hints; useful when the
    # caller doesn't know the exact shape but wants the "right kind of
    # backend".
    ANALYTICS = "analytics"
    FEATURES = "features"
    WRITE = "write"
    METADATA = "metadata"
    ASSETS = "assets"

    # ── Cross-driver feature requests ─────────────────────────────────
    # Hints that signal participation in a specific feature surface.
    # ``JOIN`` opts the driver into OGC API - Joins dispatch (extensions/
    # joins).  Future cross-driver features add their own hint here
    # rather than minting a Capability.
    JOIN = "join"

    # ── Asset-tier defaults ───────────────────────────────────────────
    # Asset-tier drivers use ``DEFAULT`` as a "no special preference"
    # marker on their ``preferred_for``.  Documented here so the
    # vocabulary is one consolidated catalogue rather than scattered
    # asset-driver and storage-driver dialects.
    DEFAULT = "default"


# ---------------------------------------------------------------------------
# Named hint sets
# ---------------------------------------------------------------------------

# Protocol-level "request exact geometry" constant.  Pass this as the
# ``hints=`` argument wherever the caller must receive full-precision
# (non-simplified) geometry regardless of which concrete driver happens to
# carry the exact tier.  The routing layer selects the first registered
# driver whose ``supported_hints`` is a *superset* of this set — today PG,
# tomorrow any driver that declares ``Hint.GEOMETRY_EXACT``.  No driver
# class name appears here; adding a new exact-capable driver requires zero
# changes to any call site that uses this constant.
EXACT_READ_HINTS: frozenset = frozenset({Hint.GEOMETRY_EXACT})


def parse_request_hints(values) -> frozenset:
    """Parse caller-supplied hint tokens into a ``frozenset[Hint]``.

    Accepts the value of the ``?hints=`` query parameter in either repeated
    form (``?hints=geometry_exact&hints=tiles``) or comma-joined form
    (``?hints=geometry_exact,tiles``); ``values`` is therefore an iterable of
    strings (FastAPI hands a ``List[str]`` for a repeated param) or ``None``.

    Tokens are matched case-insensitively against the canonical :class:`Hint`
    vocabulary; unknown tokens are silently dropped so a typo or a hint this
    deployment doesn't recognise simply has no effect (same tolerance the
    routing matcher already applies to unrecognised ``supported_hints``).
    Returns an empty frozenset when nothing valid is supplied, which keeps the
    default (simplified / search-backend) read path byte-for-byte unchanged.
    """
    if not values:
        return frozenset()
    by_value = {h.value: h for h in Hint}
    parsed = set()
    for raw in values:
        if raw is None:
            continue
        for token in str(raw).split(","):
            member = by_value.get(token.strip().lower())
            if member is not None:
                parsed.add(member)
    return frozenset(parsed)
