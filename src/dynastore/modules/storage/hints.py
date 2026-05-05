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
    |               |                          | INDEX / BACKUP / UPLOAD  |
    |               |                          | / TRANSFORM)             |
    +---------------+--------------------------+--------------------------+
    | ``Capability``| What can this driver DO? | ``Capability`` enums on  |
    |               | (structural fact about   | each driver — used by    |
    |               | the driver)              | the discovery endpoint   |
    |               |                          | and apply-handler        |
    |               |                          | validation.              |
    +---------------+--------------------------+--------------------------+
    | ``Hint``      | Which entry inside       | ``Hint`` (this module).  |
    |               | ``operations[Op]`` does  | Caller passes ``hint=``  |
    |               | the caller want?         | to ``get_driver``;       |
    |               | (per-request preference) | drivers self-declare via |
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
holds at runtime — code that already passes the bare string keeps
working unchanged.  Subsequent PRs migrate driver field types from
``FrozenSet[str]`` to ``FrozenSet[Hint]`` and reclassify the
read-variant capabilities into hints.
"""

from enum import StrEnum


class Hint(StrEnum):
    """Canonical per-request routing hints.

    A ``StrEnum`` so existing call sites that pass bare strings
    (``hint="geometry_exact"``) keep working unchanged — equality and
    set membership both succeed against the enum member.  Migrations to
    typed ``FrozenSet[Hint]`` fields land in follow-up PRs.

    Members are grouped by concern below; group order is editorial only.
    """

    # ── Geometry rendering preferences ────────────────────────────────
    # Read-path entries declare which precision they serve.  Public ES
    # carries ``GEOMETRY_SIMPLIFIED`` (fast, lossy); PG carries
    # ``GEOMETRY_EXACT`` (full WKB).
    GEOMETRY_SIMPLIFIED = "geometry_simplified"
    GEOMETRY_EXACT = "geometry_exact"

    # ── Search / read-variant flavours ────────────────────────────────
    # These are flavours of ``Operation.SEARCH`` (or ``Operation.READ``).
    # The caller asks for a specific shape; the dispatcher picks an
    # entry whose ``hints`` (or driver class ``supported_hints``)
    # contains the requested flavour.  ``FULLTEXT``, ``AGGREGATION``,
    # ``COUNT``, ``STATISTICS``, ``SORT``, ``GROUP_BY``,
    # ``ATTRIBUTE_FILTER`` and ``SPATIAL_FILTER`` are present today as
    # ``Capability`` members; the migration to ``Hint`` happens in a
    # follow-up so callers can route via
    # ``get_driver(Operation.SEARCH, hint=Hint.AGGREGATION)`` instead
    # of the current capability-as-routing-key contortion.
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

    # ── Backend identity (legacy / debugging) ─────────────────────────
    # ``BIGQUERY`` was used as a self-identifying hint on the BQ
    # driver's ``supported_hints``.  Kept here for parity with the
    # current driver declarations so existing matches still hold;
    # likely deprecated once Phase 3 lands and read-variant hints are
    # the canonical mechanism.
    BIGQUERY = "bigquery"

    # ── Asset-tier defaults ───────────────────────────────────────────
    # Asset-tier drivers use ``DEFAULT`` as a "no special preference"
    # marker on their ``preferred_for``.  Documented here so the
    # vocabulary is one consolidated catalogue rather than scattered
    # asset-driver and storage-driver dialects.
    DEFAULT = "default"
