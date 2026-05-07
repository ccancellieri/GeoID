#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
Generalized, consumer-agnostic asset-link contribution protocol.

Producers (Maps, Tiles, Features, …) implement `AssetContributor` and
declare what asset links they can emit for a given resource reference.
Consumers (STAC, Records, Coverages, Features) iterate
`get_protocols(AssetContributor)` and attach the yielded `AssetLink`s
to their own representation, without importing each other.

This is distinct from TRANSFORM-capable asset drivers (role-based driver
plan §Protocols), which mutate a single asset document at read time
e.g. resolve URIs, filter fields.  `AssetContributor` adds sibling links;
TRANSFORM rewrites the document.
"""

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional, Protocol, Tuple, runtime_checkable


@dataclass(frozen=True)
class ResourceRef:
    """Neutral reference to a geospatial resource.

    Consumers build this from their internal representation (a STAC Item,
    an OGC feature, a coverage) so producers can emit links without
    importing consumer-specific types.
    """

    catalog_id: str
    collection_id: str
    item_id: Optional[str] = None
    bbox: Optional[Tuple[float, float, float, float]] = None
    geometry: Optional[dict] = None
    base_url: str = ""
    style: Optional[str] = None
    extras: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AssetLink:
    """Neutral asset/link entry. Consumers map these into their schema
    (`pystac.Asset`, OGC `Link`, …)."""

    key: str
    href: str
    title: str
    media_type: str
    roles: Tuple[str, ...] = ()


@runtime_checkable
class AssetContributor(Protocol):
    """Producer of asset links for geospatial resources.

    Optional: if the producer module is not loaded, `get_protocols` returns
    an empty iteration and consumers render without the contribution.
    """

    priority: int

    def contribute(self, ref: ResourceRef) -> Iterable[AssetLink]: ...
