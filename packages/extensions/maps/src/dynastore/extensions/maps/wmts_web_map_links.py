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

"""WMTS web-map-links contributor.

For raster STAC items that carry a WMTS KVP preview asset (i.e. an asset
whose href contains ``request=GetPreview`` and a ``layer=`` parameter),
this contributor derives a GetTile URL template conforming to the STAC
web-map-links extension (v1.2.0) and emits it as an AssetLink with
``rel="wmts"`` so standard map viewers can render the layer as a tiled
raster on a slippy map.

No tiling infrastructure is required — the contributor simply repoints
the existing source WMTS by swapping ``GetPreview`` for ``GetTile`` and
appending the ``{TileMatrix}/{TileRow}/{TileCol}`` placeholders.

Design constraints
------------------
- No inter-module imports; cross-module communication via protocols only.
- Defensive: malformed URLs or missing WMTS assets produce no output.
- Items with no WMTS preview asset (vectors, non-WMTS rasters) are left
  completely untouched.
"""

from __future__ import annotations

import logging
from typing import Iterable
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from dynastore.models.protocols.asset_contrib import AssetLink, ResourceRef

logger = logging.getLogger(__name__)

# STAC web-map-links extension schema URI (v1.2.0).
WEB_MAP_LINKS_EXTENSION_URI = (
    "https://stac-extensions.github.io/web-map-links/v1.2.0/schema.json"
)

# Preferred tile matrix set for slippy-map viewers.  Fall back to any
# web-mercator variant the source already carries; otherwise inject this.
# Default to "EPSG:3857" — the id legacy GeoServer/MapServer WMTS actually
# advertise (the FAO source WMTS advertises EPSG:3857 and GoogleMapsCompatible,
# not the OGC API "WebMercatorQuad" id, so emitting WebMercatorQuad would 404).
_PREFERRED_TMS = "EPSG:3857"
_EPSG3857_VARIANTS = frozenset(
    {"WebMercatorQuad", "EPSG:3857", "GoogleMapsCompatible", "GoogleCRS84Quad"}
)

# Parameters that are only meaningful for GetPreview and must not appear in
# the derived GetTile template URL.
_PREVIEW_ONLY_PARAMS = frozenset({"width", "height", "transparent", "bgcolor"})


def _derive_wmts_get_tile_href(preview_href: str) -> str | None:
    """Derive a WMTS GetTile KVP template URL from a GetPreview href.

    Returns ``None`` when the href cannot be parsed or does not look like
    a WMTS KVP GetPreview request.
    """
    try:
        parsed = urlparse(preview_href)
        params = parse_qs(parsed.query, keep_blank_values=False)
    except Exception:
        return None

    # Normalise keys to lower-case for case-insensitive matching.
    params_lower = {k.lower(): v for k, v in params.items()}

    # Must have request=GetPreview (case-insensitive) and a layer param.
    request_vals = [v.lower() for v in params_lower.get("request", [])]
    if "getpreview" not in request_vals:
        return None
    if "layer" not in params_lower:
        return None

    # Rebuild the parameter dict using the original casing for keys,
    # but with GetPreview overrides applied.
    new_params: dict[str, list[str]] = {}
    for k, v in params.items():
        k_lower = k.lower()
        if k_lower in _PREVIEW_ONLY_PARAMS:
            continue
        if k_lower == "request":
            new_params[k] = ["GetTile"]
        elif k_lower == "version":
            new_params[k] = ["1.0.0"]
        elif k_lower == "format":
            new_params[k] = ["image/png"]
        elif k_lower == "tilematrixset":
            # Keep as-is; prefer EPSG:3857 variants for slippy-map compat.
            existing = v[0] if v else ""
            if existing in _EPSG3857_VARIANTS:
                new_params[k] = [existing]
            else:
                new_params[k] = [_PREFERRED_TMS]
        else:
            new_params[k] = v

    # Ensure mandatory WMTS KVP params are present.
    new_params.setdefault("service", ["WMTS"])
    new_params.setdefault("version", ["1.0.0"])
    new_params.setdefault("format", ["image/png"])

    # If source had no tilematrixset param at all, inject the preferred one.
    has_tms = any(k.lower() == "tilematrixset" for k in new_params)
    if not has_tms:
        new_params["tilematrixset"] = [_PREFERRED_TMS]

    # Flatten single-value lists back to scalar strings.
    flat_params = {k: v[0] if len(v) == 1 else v for k, v in new_params.items()}

    # Append tile-coordinate template variables.
    query_string = urlencode(flat_params, doseq=True)
    query_string += "&tilematrix={TileMatrix}&tilerow={TileRow}&tilecol={TileCol}"

    return urlunparse(parsed._replace(query=query_string))


class WmtsWebMapLinksContributor:
    """AssetContributor that emits STAC web-map-links ``wmts`` typed assets.

    Activated automatically when the maps extension is loaded (registered via
    ``register_plugin`` in ``wmts_web_map_links`` module import side-effect).

    The contributor inspects ``ref.extras["item_assets"]`` (populated by
    ``asset_factory._to_resource_ref`` for items) and emits a GetTile
    template asset when a WMTS-style preview asset is found.  Items with no
    such asset are left untouched.
    """

    priority: int = 50  # run before the default-100 contributors

    def contribute(self, ref: ResourceRef) -> Iterable[AssetLink]:
        """Yield a ``wmts`` AssetLink for the first parseable WMTS preview asset."""
        item_assets: dict = ref.extras.get("item_assets") or {}
        if not item_assets:
            return

        for asset_key, asset_info in item_assets.items():
            href: str = asset_info.get("href", "")
            if not href:
                continue
            try:
                get_tile_href = _derive_wmts_get_tile_href(href)
            except Exception as exc:  # noqa: BLE001 — never fail a request
                logger.debug(
                    "WmtsWebMapLinksContributor: skipping asset %r — %s",
                    asset_key,
                    exc,
                )
                continue

            if get_tile_href is None:
                continue

            # Emit the WMTS GetTile template asset with web-map-links roles.
            yield AssetLink(
                key="wmts_tiles",
                href=get_tile_href,
                title="WMTS Tile Layer",
                media_type="image/png",
                roles=("visual", "tiles"),
            )
            # Only emit once — for the first parseable WMTS preview found.
            return

    def contribute_stac(self, ref: ResourceRef) -> Iterable:
        """StacContributor: declare the web-map-links extension URI when emitting."""
        from dynastore.extensions.stac.stac_contributor import StacContribution

        item_assets: dict = ref.extras.get("item_assets") or {}
        has_wmts = any(
            _derive_wmts_get_tile_href(info.get("href", "")) is not None
            for info in item_assets.values()
        )
        if has_wmts:
            yield StacContribution(
                stac_extensions=(WEB_MAP_LINKS_EXTENSION_URI,),
            )
