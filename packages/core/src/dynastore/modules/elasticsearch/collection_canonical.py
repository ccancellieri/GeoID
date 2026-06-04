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

"""Canonical ES envelope for COLLECTIONS (refs #1285/#1800).

Brings collections onto the same modular, pluggable envelope as items, via the
level-agnostic core in :mod:`dynastore.modules.elasticsearch.canonical_doc`:

* **system**     — internal identity/lifecycle (``catalog_id``/``collection_id``
  queryable axes, ``created``/``updated`` lifecycle); never on the STAC wire.
* **properties** — the attribute bag (``title``/``description``/``keywords``/
  ``license``/``language``/``cube:dimensions``/extension fields). Unknown keys
  route to ``properties.extras`` so the strict mapping never grows per-key.
* **reserved members** — protocol-structural keys surfaced verbatim
  (:data:`COLLECTION_RESERVED_MEMBERS`: ``id``/``type``/``extent``/``links``/
  ``assets``/``providers``/``summaries``/``stac_extensions`` …).
* **access**     — IAM authorization sidecar (plugged in by the ABAC layer).

A STAC Collection has **no** ``properties`` member — its attributes live at the
top level — so the read-time projector (:func:`unproject_collection_from_es`)
hoists ``properties.*`` back to the top level rather than nesting them. That is
the per-(entity_level, protocol) remapping the registry layers on the generic
structural normalisation.

``extent`` is treated here as an opaque reserved member: the
``CollectionElasticsearchDriver`` keeps its existing
``_enrich_doc``/``_unenrich_doc`` extent ⟷ ES ``geo_shape``/``date_range``
transforms around these calls.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from dynastore.modules.elasticsearch.metadata_canonical import (
    build_canonical_metadata_doc,
    unproject_metadata_from_es,
)

# STAC/structural Collection members surfaced verbatim on the wire. Anything
# NOT here and not an identity/lifecycle axis is an attribute → properties.
COLLECTION_RESERVED_MEMBERS: frozenset = frozenset({
    "id",
    "type",
    "stac_version",
    "stac_extensions",
    "links",
    "assets",
    "item_assets",
    "extent",
    "providers",
    "summaries",
    "conformsTo",
})


def build_canonical_collection_doc(
    metadata: Dict[str, Any],
    *,
    catalog_id: str,
    collection_id: str,
    known_fields: Dict[str, Any],
    access: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble the canonical collection ``_source`` from STAC collection metadata.

    Thin collection-level adapter over
    :func:`~dynastore.modules.elasticsearch.metadata_canonical.build_canonical_metadata_doc`.
    ``extent`` is carried opaquely as a reserved member (the driver
    enriches/unenriches it). Pure function — *metadata* is not mutated.
    """
    return build_canonical_metadata_doc(
        metadata,
        identity={
            "id": collection_id,
            "catalog_id": catalog_id,
            "collection_id": collection_id,
        },
        reserved_member_keys=COLLECTION_RESERVED_MEMBERS,
        known_fields=known_fields,
        access=access,
    )


def unproject_collection_from_es(source: Dict[str, Any]) -> Dict[str, Any]:
    """Reconstruct a STAC Collection dict from a canonical ``_source`` (#1285).

    Inverse of :func:`build_canonical_collection_doc`: structural members
    (:data:`COLLECTION_RESERVED_MEMBERS`) surface verbatim, the ``properties``
    attribute bag (with ``extras`` hoisted) spreads onto the **top level** (a
    STAC Collection has no ``properties`` member), lifecycle surfaces as Common
    Metadata, and internal identity / ``system`` / ``access`` are dropped.

    ``extent`` is returned as stored (opaque) — the driver's ``_unenrich_doc``
    restores the STAC extent shape afterwards. Pure function.
    """
    return unproject_metadata_from_es(
        source,
        reserved_member_keys=COLLECTION_RESERVED_MEMBERS,
        wire_type="Collection",
    )


__all__ = [
    "COLLECTION_RESERVED_MEMBERS",
    "build_canonical_collection_doc",
    "unproject_collection_from_es",
]
