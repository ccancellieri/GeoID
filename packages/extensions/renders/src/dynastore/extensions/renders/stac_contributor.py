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

"""STAC enrichment contributor for the renders extension.

For STAC items/collections whose assets include a COG (``image/tiff;
application=geotiff`` or ``image/tiff`` with ``cloud-optimized``/``data``
roles) and for which a default style can be resolved, this contributor adds:

1. An ``AssetLink`` (alternate render tile URL template) with roles
   ``("overview", "visual")``.
2. A ``StacContribution`` that declares the ``render:renders`` STAC extension
   URI and injects a ``renders`` map entry under ``properties``.
3. A ``rel=style`` link on the item pointing at the style endpoint.

Capability-gate: the contributor only emits enrichment when the renders route
is actually registered (``_renders_route_registered()``). A STAC assembly that
runs in a scope without the renders extension loaded sees nothing — no
orphan URLs, no false conformance claims.

Design constraints:
- No DB access: all inputs come through ``ResourceRef.extras``, populated by
  the STAC generator from already-loaded data.
- No inter-extension imports: the ``AssetContributor`` / ``StacContributor``
  protocol is neutral (lives in ``models/protocols``).
- Enrich-don't-rewrite: never mutate ``item_assets`` hrefs.
- External IDs in public URLs: catalog/collection IDs in tile URLs come from
  ``ResourceRef.catalog_id`` / ``.collection_id``, which the STAC generator
  already fills with external (public) IDs.
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional

from dynastore.models.protocols.asset_contrib import AssetLink, ResourceRef

logger = logging.getLogger(__name__)

# STAC render extension schema URI.
_RENDER_EXTENSION_URI = (
    "https://stac-extensions.github.io/render/v2.0.0/schema.json"
)

# COG media types the contributor recognises.
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

# Route prefix used by the renders service — must match renders_service.py.
_RENDERS_PREFIX = "/renders"


def _renders_route_registered() -> bool:
    """Return True when the renders extension is active in this process.

    Checks by looking for the renders service in the registered protocols.
    This is the same pattern the WMTS contributor uses (it inspects item_assets
    at call time; we inspect the protocol registry).
    """
    try:
        from dynastore.tools.discovery import get_protocols
        from dynastore.extensions.protocols import ExtensionProtocol
        for ext in get_protocols(ExtensionProtocol):
            prefix = getattr(ext, "prefix", None)
            if prefix == _RENDERS_PREFIX:
                return True
    except Exception:
        pass
    return False


def _first_cog_href(item_assets: dict) -> Optional[str]:
    """Return the href of the first COG asset, or None."""
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
    """Return the resolved default style ID from ``ref.extras``, or None."""
    return ref.extras.get("default_style_id") or None


class RendersStacContributor:
    """AssetContributor + StacContributor that adds render tile links for COG items.

    Registered in ``RendersService.lifespan`` so it is active wherever the
    STAC read path is served. A single instance satisfies both protocols
    structurally (``contribute`` + ``contribute_stac``).
    """

    priority: int = 60  # run after language (10) and WMTS (50)

    def contribute(self, ref: ResourceRef) -> Iterable[AssetLink]:
        """Emit a render tile XYZ-template asset for COG items."""
        if not _renders_route_registered():
            return

        item_assets: dict = ref.extras.get("item_assets") or {}
        if not item_assets:
            # Collection-level ref or no assets — nothing to emit.
            return

        cog_href = _first_cog_href(item_assets)
        if not cog_href:
            return

        style_id = _default_style_id(ref)
        if not style_id:
            return

        # Build the tile URL template using external (public) IDs.
        href = (
            f"{ref.base_url}{_RENDERS_PREFIX}/catalogs/{ref.catalog_id}"
            f"/collections/{ref.collection_id}"
            f"/styles/{style_id}/tiles/WebMercatorQuad"
            f"/{{z}}/{{x}}/{{y}}.png"
        )
        yield AssetLink(
            key="render_tiles",
            href=href,
            title="Styled Raster Tiles (PNG)",
            media_type="image/png",
            roles=("overview", "visual"),
        )

    def contribute_stac(self, ref: ResourceRef) -> Iterable:
        """Declare render:renders and inject the renders map when a COG+style exist."""
        from dynastore.extensions.stac.stac_contributor import StacContribution

        if not _renders_route_registered():
            return

        item_assets: dict = ref.extras.get("item_assets") or {}
        if not item_assets:
            return

        cog_href = _first_cog_href(item_assets)
        if not cog_href:
            return

        style_id = _default_style_id(ref)
        if not style_id:
            return

        tile_url_template = (
            f"{ref.base_url}{_RENDERS_PREFIX}/catalogs/{ref.catalog_id}"
            f"/collections/{ref.collection_id}"
            f"/styles/{style_id}/tiles/WebMercatorQuad"
            f"/{{z}}/{{x}}/{{y}}.png"
        )
        style_url = (
            f"{ref.base_url}/styles/catalogs/{ref.catalog_id}"
            f"/collections/{ref.collection_id}/styles/{style_id}"
        )

        renders_map = {
            style_id: {
                "href": tile_url_template,
                "type": "image/png",
                "title": f"Raster tiles styled with '{style_id}'",
                "roles": ["overview", "visual"],
                "links": [
                    {
                        "rel": "style",
                        "href": style_url,
                        "type": "application/vnd.ogc.sld+xml",
                        "title": f"Style '{style_id}'",
                    }
                ],
            }
        }

        yield StacContribution(
            stac_extensions=(_RENDER_EXTENSION_URI,),
            extra_fields={"renders": renders_map},
        )
