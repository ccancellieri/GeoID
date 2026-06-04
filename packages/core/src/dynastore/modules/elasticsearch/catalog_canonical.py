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

"""Canonical ES envelope for CATALOGS (refs #1285/#1800).

A STAC Catalog is the thinnest metadata entity: identity + lifecycle, a few
attributes (``title``/``description``), and structural ``links`` —  no
``extent``/``providers``/``assets``. Shares the metadata-entity envelope and
read projection with collections; see :mod:`metadata_canonical`.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from dynastore.modules.elasticsearch.metadata_canonical import (
    build_canonical_metadata_doc,
    unproject_metadata_from_es,
)

# STAC/structural Catalog members surfaced verbatim on the wire.
CATALOG_RESERVED_MEMBERS: frozenset = frozenset({
    "id",
    "type",
    "stac_version",
    "stac_extensions",
    "links",
    "conformsTo",
})


def build_canonical_catalog_doc(
    metadata: Dict[str, Any],
    *,
    catalog_id: str,
    known_fields: Dict[str, Any],
    access: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble the canonical catalog ``_source`` from STAC catalog metadata.

    Thin catalog-level adapter over
    :func:`~dynastore.modules.elasticsearch.metadata_canonical.build_canonical_metadata_doc`.
    Pure function — *metadata* is not mutated.
    """
    return build_canonical_metadata_doc(
        metadata,
        identity={"id": catalog_id, "catalog_id": catalog_id},
        reserved_member_keys=CATALOG_RESERVED_MEMBERS,
        known_fields=known_fields,
        access=access,
    )


def unproject_catalog_from_es(source: Dict[str, Any]) -> Dict[str, Any]:
    """Reconstruct a STAC Catalog dict from a canonical ``_source`` (#1285).

    Inverse of :func:`build_canonical_catalog_doc`: structural members
    (:data:`CATALOG_RESERVED_MEMBERS`) surface verbatim, the ``properties``
    attribute bag (with ``extras`` hoisted) spreads onto the **top level** (a
    STAC Catalog has no ``properties`` member), lifecycle surfaces as Common
    Metadata, and internal identity / ``system`` / ``access`` are dropped.

    Pure function — *source* is not mutated.
    """
    return unproject_metadata_from_es(
        source,
        reserved_member_keys=CATALOG_RESERVED_MEMBERS,
        wire_type="Catalog",
    )


__all__ = [
    "CATALOG_RESERVED_MEMBERS",
    "build_canonical_catalog_doc",
    "unproject_catalog_from_es",
]
