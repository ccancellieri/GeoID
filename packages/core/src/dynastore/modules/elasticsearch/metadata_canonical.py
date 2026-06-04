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

"""Shared canonical envelope for metadata entities (collections, catalogs).

Both STAC Catalogs and Collections are "metadata entities": flat documents
with **no** ``properties`` member — their attributes live at the top level
alongside structural members (``links``/``extent``/``providers`` …). They share
one canonical shape and one read projection, parameterised only by the set of
structural members the level surfaces and the wire ``type``. Items differ
(``properties`` IS a wire member) and keep their own item-level adapters.

This module factors that shared logic so :mod:`collection_canonical` and
:mod:`catalog_canonical` are thin, level-specific wrappers (refs #1285/#1800).
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from dynastore.modules.elasticsearch.canonical_doc import build_canonical_envelope

# Lifecycle fields that live in ``system`` for storage but surface as STAC
# Common Metadata at the top level on read.
DEFAULT_LIFECYCLE_KEYS = ("created", "updated")


def build_canonical_metadata_doc(
    metadata: Dict[str, Any],
    *,
    identity: Dict[str, Any],
    reserved_member_keys: "frozenset[str]",
    known_fields: Dict[str, Any],
    lifecycle_keys: Sequence[str] = DEFAULT_LIFECYCLE_KEYS,
    access: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble a canonical metadata ``_source`` from a STAC metadata dict.

    Partitions *metadata* into the canonical sections and delegates to
    :func:`build_canonical_envelope`:

    * ``identity`` — internal flat axes (``id``/``catalog_id``/
      ``collection_id``) added at the top level; dropped from the wire on read.
    * lifecycle (``lifecycle_keys``) → the ``system`` container.
    * structural members (``reserved_member_keys``) → carried verbatim.
    * everything else → the ``properties`` attribute bag (unknown keys route to
      ``properties.extras``).

    Pure function — *metadata* is not mutated.
    """
    md = dict(metadata or {})

    system: Dict[str, Any] = {}
    for key in lifecycle_keys:
        val = md.pop(key, None)
        if val is not None:
            system[key] = val

    reserved: Dict[str, Any] = {}
    for key in reserved_member_keys:
        if key in md:
            reserved[key] = md.pop(key)

    # Drop any echoed internal identity; the remainder is the attribute bag.
    for key in identity:
        md.pop(key, None)

    return build_canonical_envelope(
        identity=dict(identity),
        properties=md,
        known_fields=known_fields,
        reserved_members=reserved,
        system=system or None,
        access=access or None,
    )


def unproject_metadata_from_es(
    source: Dict[str, Any],
    *,
    reserved_member_keys: "frozenset[str]",
    wire_type: str,
    lifecycle_keys: Sequence[str] = DEFAULT_LIFECYCLE_KEYS,
) -> Dict[str, Any]:
    """Reconstruct a STAC metadata-entity dict from a canonical ``_source``.

    Inverse of :func:`build_canonical_metadata_doc`:

    * structural members (``reserved_member_keys``) surface verbatim;
    * the ``properties`` attribute bag (with ``extras`` hoisted) spreads onto
      the **top level** — these entities have no ``properties`` member;
    * ``system`` lifecycle keys surface as top-level Common Metadata;
    * internal identity axes and the ``system``/``access`` containers are
      dropped from the wire.

    Pure function — *source* is not mutated.
    """
    if not isinstance(source, dict):
        return source

    out: Dict[str, Any] = {}
    for key in reserved_member_keys:
        if key in source:
            out[key] = source[key]
    out.setdefault("type", wire_type)

    props = source.get("properties")
    if isinstance(props, dict):
        flat = dict(props)
        extras = flat.pop("extras", None)
        if isinstance(extras, dict):
            for k, v in extras.items():
                flat.setdefault(k, v)
        for k, v in flat.items():
            out.setdefault(k, v)

    system = source.get("system")
    if isinstance(system, dict):
        for key in lifecycle_keys:
            if key in system:
                out.setdefault(key, system[key])

    return out


__all__ = [
    "DEFAULT_LIFECYCLE_KEYS",
    "build_canonical_metadata_doc",
    "unproject_metadata_from_es",
]
