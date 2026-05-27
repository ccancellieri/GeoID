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

    KNOWN GAPS (see tracking issue: #1457)
    ----------------------------------------------
    G1: ``_access_envelope`` is NOT yet populated in ``item_context`` by
        ``ItemService.upsert_bulk`` for PG-sidecar collections — only
        ES-envelope collections trigger the async ``_resolve_access_envelope``.
        Until that wiring lands, sidecar writes are no-ops (sidecar receives
        ``None``, skips write).

    G2: ``QueryRequest.access_filter`` is NOT yet populated by the PG read
        dispatch path.  Until that lands, sidecar reads fail-closed (return
        zero rows).

    G3: ``access_filter_to_pg_clause`` does not handle the ``AccessFilter.union``
        field (multi-collection differential ABAC).  Single-collection reads are
        correct; multi-collection PG reads via the sidecar will under-return
        (safe — matches the equal-or-stricter invariant from ``access_filter.py``).

    G4: ``_collection_uses_access_aware_driver`` (at ``item_service.py``) does
        not detect PG sidecar presence — must be extended to also check for
        ``sidecar_type == "access_envelope"`` in any active PG driver config.
    """

    sidecar_type: Literal["access_envelope"] = "access_envelope"

    column_name: str = "access_envelope"
    """Column name for the JSONB envelope within the sub-table."""

    gin_index: bool = True
    """Create a GIN index on the JSONB envelope column for fast containment queries."""

    btree_attrs_index: bool = False
    """Create a btree index on individual top-level attrs keys (reserved for future use)."""

    known_attrs_keys: List[str] = []
    """Declared attribute keys for documentation / schema hints (not enforced at DB level)."""


SidecarConfigRegistry.register("access_envelope", AccessEnvelopeSidecarConfig)
