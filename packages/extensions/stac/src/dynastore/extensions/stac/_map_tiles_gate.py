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

"""Shared predicate for map-tile capability.

Determines whether a STAC item will receive a native dynastore map-tile link
from ``TilesStacContributor`` (tiles extension).  Both the tiles contributor
and the WMTS web-map-links contributor import from here so the condition is
evaluated consistently — only one native vs. external link, never both.

Housing this predicate in the ``stac`` extension avoids a circular import:
the tiles extension already imports ``StacContribution`` from
``dynastore.extensions.stac.stac_contributor``, so placing shared helpers
here keeps the import graph acyclic (stac → [nothing in tiles];
tiles → stac).

Public surface
--------------
``is_item_map_tileable(ref)`` — True when all three conditions hold:

1. The tiles service (``/tiles`` prefix) is registered in this process.
2. The item carries at least one COG asset.
3. A default style ID is resolvable from ``ref.extras``.
"""

from __future__ import annotations

from typing import Optional

from dynastore.models.protocols.asset_contrib import ResourceRef

# Route prefix used by the tiles service — must stay in sync with
# tiles_service.py.  Changes to the prefix there must be reflected here.
_TILES_PREFIX = "/tiles"

# COG media types the predicate recognises (subset matching GDAL / rio-tiler).
_COG_MEDIA_TYPES = frozenset(
    {
        "image/tiff; application=geotiff",
        "image/tiff; application=geotiff; profile=cloud-optimized",
        "image/tiff",
        "image/geotiff",
    }
)

# Asset roles that indicate a COG data file.
_COG_ROLES = frozenset({"data", "cloud-optimized", "overview"})


def _tiles_route_registered() -> bool:
    """Return True when the tiles extension is active in this process.

    Inspects the protocol registry for an ``ExtensionProtocol`` whose
    ``prefix`` attribute equals ``_TILES_PREFIX``.  Never raises.
    """
    try:
        from dynastore.tools.discovery import get_protocols
        from dynastore.extensions.protocols import ExtensionProtocol

        for ext in get_protocols(ExtensionProtocol):
            if getattr(ext, "prefix", None) == _TILES_PREFIX:
                return True
    except Exception:
        pass
    return False


def _first_cog_href(item_assets: dict) -> Optional[str]:
    """Return the href of the first COG asset in *item_assets*, or ``None``."""
    for _key, info in item_assets.items():
        mt = info.get("type", "")
        roles = set(info.get("roles") or [])
        href = info.get("href", "")
        if not href:
            continue
        if mt in _COG_MEDIA_TYPES or (roles & _COG_ROLES):
            return href
    return None


def _default_style_id(ref: ResourceRef) -> Optional[str]:
    """Return the resolved default style ID from ``ref.extras``, or ``None``."""
    return ref.extras.get("default_style_id") or None


def is_item_map_tileable(ref: ResourceRef) -> bool:
    """Return True when ``TilesStacContributor`` will emit a native tile link.

    The three conditions mirror ``TilesStacContributor.contribute`` exactly:
    tiles route registered **and** item has a COG asset **and** a default
    style ID is resolvable.  ``WmtsWebMapLinksContributor`` calls this to
    decide whether to suppress the external-WMTS fallback link.
    """
    if not _tiles_route_registered():
        return False
    item_assets: dict = ref.extras.get("item_assets") or {}
    if not item_assets:
        return False
    if _first_cog_href(item_assets) is None:
        return False
    if _default_style_id(ref) is None:
        return False
    return True
