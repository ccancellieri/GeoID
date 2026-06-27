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

"""STAC enrichment contributor for the tiles extension (map-tile routes).

For STAC items/collections whose assets include a COG (``image/tiff;
application=geotiff`` or ``image/tiff`` with ``cloud-optimized``/``data``
roles) and for which a default style can be resolved, this contributor adds:

1. An ``AssetLink`` (alternate render tile URL template) with roles
   ``("overview", "visual")``.
2. A ``StacContribution`` that declares the ``render:renders`` STAC extension
   URI and injects a ``renders`` map entry under ``properties``.
3. A ``rel=style`` link on the item pointing at the style endpoint.

The tile URL template uses the map-tile route shape under ``/tiles``:
``{base}/tiles/catalogs/{cat}/collections/{coll}/map/tiles/WebMercatorQuad/{z}/{x}/{y}.png``

Capability-gate: the contributor only emits enrichment when the tiles route
is actually registered (``_tiles_route_registered()``). A STAC assembly
without the tiles extension loaded sees nothing.

The tileability predicate (_tiles_route_registered, _first_cog_href,
_default_style_id) is shared via ``dynastore.extensions.stac._map_tiles_gate``
so ``WmtsWebMapLinksContributor`` can gate on the same condition without
duplicating the logic.  The stac package is the right home for shared helpers
because tiles already imports StacContribution from stac; placing the helpers
there keeps the import graph acyclic.
"""

from __future__ import annotations

import logging
from typing import Iterable

from dynastore.models.protocols.asset_contrib import AssetLink, ResourceRef
from dynastore.extensions.stac._map_tiles_gate import (
    _TILES_PREFIX,
    _tiles_route_registered,
    _first_cog_href,
    _default_style_id,
)

logger = logging.getLogger(__name__)

# STAC render extension schema URI.
_RENDER_EXTENSION_URI = (
    "https://stac-extensions.github.io/render/v2.0.0/schema.json"
)


class TilesStacContributor:
    """AssetContributor + StacContributor that adds map-tile links for COG items.

    Registered in ``TilesService.lifespan`` so it is active wherever the
    STAC read path is served. A single instance satisfies both protocols
    structurally (``contribute`` + ``contribute_stac``).

    Emits tile URL templates using the default-style map-tile route:
    ``/tiles/catalogs/{cat}/collections/{coll}/map/tiles/WebMercatorQuad/{z}/{x}/{y}.png``
    """

    priority: int = 60  # run after language (10) and WMTS (50)

    def contribute(self, ref: ResourceRef) -> Iterable[AssetLink]:
        """Emit a render tile XYZ-template asset for COG items."""
        if not _tiles_route_registered():
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

        href = (
            f"{ref.base_url}{_TILES_PREFIX}/catalogs/{ref.catalog_id}"
            f"/collections/{ref.collection_id}"
            f"/map/tiles/WebMercatorQuad"
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

        if not _tiles_route_registered():
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
            f"{ref.base_url}{_TILES_PREFIX}/catalogs/{ref.catalog_id}"
            f"/collections/{ref.collection_id}"
            f"/map/tiles/WebMercatorQuad"
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
