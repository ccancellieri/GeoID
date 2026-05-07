#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
Consumer-agnostic link-contribution protocol.

Producers (Styles, Tiles, Maps, …) emit AnchoredLink instances declaring
what links they can contribute for a given ResourceRef. Consumers (STAC,
Features, Coverages, Records) iterate get_protocols(LinkContributor)
and translate the anchor into their schema (nested on data_asset for
STAC, resource_root for OGC responses, etc.).

Distinct from AssetContributor: AssetContributor emits sibling assets;
LinkContributor emits links that may anchor inside an existing asset or
at response/collection root. OGC_STYLES.md explicitly recommends nested
`rel: "style"` links inside the data asset — that shape can't be
expressed through AssetContributor, which is why LinkContributor exists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncIterator,
    Literal,
    Mapping,
    Protocol,
    runtime_checkable,
)

from dynastore.models.protocols.asset_contrib import ResourceRef


Anchor = Literal["resource_root", "data_asset", "collection_root"]


@dataclass(frozen=True)
class AnchoredLink:
    """Neutral link entry. Consumers map anchor into their schema:

    | anchor            | STAC                         | OGC responses       |
    |-------------------|------------------------------|---------------------|
    | resource_root     | item.links[]                 | response.links[]    |
    | data_asset        | item.assets["data"].links[]  | response.links[]*   |
    | collection_root   | collection.links[] + merge   | collection.links[]  |

    * OGC responses without a data-asset concept fall back to resource_root.
    """

    anchor: Anchor
    rel: str
    href: str
    title: str
    media_type: str
    extras: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class LinkContributor(Protocol):
    """Producer of anchored links for geospatial resources.

    Optional producer: if the module isn't loaded, get_protocols() returns
    an empty iteration and consumers render without the contribution.
    """

    priority: int

    def contribute_links(self, ref: ResourceRef) -> AsyncIterator[AnchoredLink]: ...
