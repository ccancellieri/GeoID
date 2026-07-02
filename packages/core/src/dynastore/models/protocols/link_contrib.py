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
    Sequence,
    Tuple,
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


class _ResourceRootLinkContributor:
    """``LinkContributor`` for a collection-scoped ``resource_root`` link.

    Built by :func:`make_resource_root_contributor` — see there for the
    contract this class implements.
    """

    def __init__(
        self,
        *,
        rel: str,
        path_template: str,
        methods: Sequence[Tuple[str, str]],
        priority: int,
    ) -> None:
        self._rel = rel
        self._path_template = path_template
        self._methods = tuple(methods)
        self.priority = priority

    async def contribute_links(self, ref: ResourceRef) -> AsyncIterator[AnchoredLink]:
        if ref.item_id is not None:
            return  # resource_root links of this shape are collection-scoped
        href = self._path_template.format(
            base=ref.base, catalog_id=ref.catalog_id, collection_id=ref.collection_id,
        )
        for method, title in self._methods:
            yield AnchoredLink(
                anchor="resource_root",
                rel=self._rel,
                href=href,
                title=title,
                media_type="application/json",
                extras={"method": method},
            )


def make_resource_root_contributor(
    *,
    rel: str,
    path_template: str,
    methods: Sequence[Tuple[str, str]],
    priority: int,
) -> LinkContributor:
    """Build a ``LinkContributor`` for a simple, collection-scoped resource_root link.

    Several extensions (Joins, the legacy DWH join surface, …) advertise one
    endpoint per collection as a ``resource_root`` link and differ only in
    ``rel``, ``href``, per-method ``title``, and priority. This factory covers
    that shape so each extension only supplies its own values instead of
    re-implementing the ``item_id`` guard and href assembly.

    Args:
        rel: The link relation to emit for every yielded link.
        path_template: An ``str.format`` template rendered with ``base``
            (``ref.base``), ``catalog_id``, and ``collection_id``.
        methods: Ordered ``(http_method, title)`` pairs; one ``AnchoredLink``
            is yielded per pair, all sharing the same ``rel`` and ``href``,
            with ``extras={"method": http_method}``.
        priority: The contributor's ``priority`` (matches the owning
            extension's registration priority).
    """
    return _ResourceRootLinkContributor(
        rel=rel, path_template=path_template, methods=methods, priority=priority,
    )
