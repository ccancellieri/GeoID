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

"""Configuration for the Access Envelope sidecar (opt-in ABAC scaffolding).

This sidecar stores a compact JSONB access-envelope in a dedicated sub-table
``{schema}.{table}_access_envelope``.  It is the PG counterpart of the
Elasticsearch ``_attrs``-based envelope already landed in #1441.

Registration
------------
``SidecarConfigRegistry.register("access_envelope", AccessEnvelopeSidecarConfig)``
runs at module import so that config deserialization can instantiate this type
from a stored dict.  The *live implementation* registry
(``SidecarRegistry.register("access_envelope", AccessEnvelopeSidecar)``) is
called at the bottom of ``access_envelope.py`` and likewise runs at import;
operators still need to ensure the module is imported (e.g. via the consuming
service's SCOPE setup) for the registry entry to be active.

This sidecar is **opt-in only** — ``is_mandatory()`` returns ``False``.
"""

from typing import List, Literal

from dynastore.modules.storage.drivers.pg_sidecars.base import (
    SidecarConfig,
    SidecarConfigRegistry,
)


class AccessEnvelopeSidecarConfig(SidecarConfig):
    """PG sub-table sidecar for per-item ABAC access envelopes.

    Owns the ``{schema}.{table}_access_envelope`` sub-table.  Each row stores:

    * ``geoid`` — FK to the hub table.
    * ``access_envelope`` — JSONB blob with the following shape::

          {
              "visibility": "public" | "private" | ...,
              "owner": "<owner-principal-id>",
              "attrs": {"dept": "finance", ...}
          }

    The envelope is **never** projected into ``Feature.properties``; it is only
    used in the SQL ``WHERE`` clause by :meth:`AccessEnvelopeSidecar.apply_query_context`.

    ROW-LEVEL ABAC STATUS (tracking issue: #1457)
    ----------------------------------------------
    The PG access-envelope path is enforced end-to-end:

    * Writes stamp the envelope.  ``ItemService`` resolves ``_access_envelope``
      for any collection whose WRITE driver opts in to row-level ABAC, which now
      includes PG ``access_envelope`` sidecars — detected by
      ``_collection_uses_access_aware_driver`` (branch 2).  [#1457 G1/G4]

    * Reads enforce the envelope.  The PG read dispatch compiles the caller's
      read scope and sets ``QueryRequest.access_filter`` before the query
      optimiser runs; the optimiser force-includes this sidecar for ABAC
      collections, so an unset filter fails closed (zero rows) and never leaks.
      [#1457 G2]

    REMAINING LIMITATION
    --------------------
    ``access_filter_to_pg_clause`` does not translate the
    ``AccessFilter.union`` field (multi-collection differential ABAC).  A
    single-collection read is exact; a multi-collection PG read whose principal
    has *different* grants per collection under-returns — safe, matching the
    equal-or-stricter invariant in ``access_filter.py``.  The union node is
    honoured by ``AccessFilter.union_of`` and the ES path; only the PG-clause
    translation is pending.  [#1457 G3]
    """

    sidecar_type: Literal["access_envelope"] = "access_envelope"

    column_name: str = "access_envelope"
    """Column name for the JSONB envelope within the sub-table."""

    gin_index: bool = True
    """Create a GIN index on the JSONB envelope column for fast containment queries."""

    btree_attrs_index: bool = False
    """Emit a btree expression index per ``known_attrs_keys`` entry on the JSONB
    path ``access_envelope->'attrs'->>'<key>'`` (#1453). Accelerates the ABAC
    read filter's equality/range predicates without promoting attrs to real
    columns. No-op unless ``known_attrs_keys`` is also non-empty."""

    known_attrs_keys: List[str] = []
    """Attribute keys to index when ``btree_attrs_index`` is True. Each becomes a
    btree expression index matching the read filter's ``->'attrs'->>'<key>'``
    access pattern. Keys must be ``[A-Za-z0-9_]`` identifiers; others are skipped
    with a warning at DDL build time."""


SidecarConfigRegistry.register("access_envelope", AccessEnvelopeSidecarConfig)
