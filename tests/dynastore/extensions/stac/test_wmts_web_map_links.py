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

"""Unit tests for the WMTS web-map-links contributor.

Covers:
- A FAO-like raster item with a WMTS GetPreview asset produces a ``wmts``
  AssetLink whose href is a valid GetTile template containing the
  {TileMatrix}/{TileRow}/{TileCol} placeholders.
- The web-map-links extension URI is declared via contribute_stac.
- An item with no WMTS-style preview asset produces no link.
- An item with a malformed href produces no link and does not raise.
- GetPreview-only params (width, height) are stripped from the template.
- tilematrixset is forced to the preferred web-mercator id (EPSG:3857) when
  the source carries a non-slippy-map CRS (EPSG:4326).
"""

import pytest
from urllib.parse import parse_qs, urlparse

from dynastore.extensions.stac.wmts_web_map_links import (
    WEB_MAP_LINKS_EXTENSION_URI,
    WmtsWebMapLinksContributor,
    _derive_wmts_get_tile_href,
)
from dynastore.models.protocols.asset_contrib import AssetLink, ResourceRef


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAO_PREVIEW_HREF = (
    "https://data.apps.fao.org/map/wmts/wmts"
    "?layer=fao-gismgr/GAEZ-V5/maps/AEZ33/GAEZ-V5.AEZ33"
    "&request=GetPreview"
    "&tilematrixset=EPSG:4326"
    "&service=wmts"
    "&width=512&height=512"
)


def _make_ref(asset_hrefs: dict[str, str]) -> ResourceRef:
    """Build a ResourceRef with item_assets populated from a {key: href} dict."""
    return ResourceRef(
        catalog_id="cat",
        collection_id="col",
        item_id="item1",
        extras={
            "item_assets": {
                k: {"href": v, "type": "image/png", "roles": ["visual"]}
                for k, v in asset_hrefs.items()
            }
        },
    )


def _parse_template(href: str) -> dict:
    """Parse the non-template portion of a GetTile href into a params dict."""
    # Split off the template variables which are not valid percent-encoding.
    base, _, _ = href.partition("&tilematrix=")
    parsed = urlparse(base)
    return {k.lower(): v[0] if len(v) == 1 else v
            for k, v in parse_qs(parsed.query).items()}


# ---------------------------------------------------------------------------
# _derive_wmts_get_tile_href — unit-level
# ---------------------------------------------------------------------------

def test_derive_get_tile_basic():
    result = _derive_wmts_get_tile_href(_FAO_PREVIEW_HREF)
    assert result is not None, "Expected a derived href"
    assert "GetTile" in result
    assert "{TileMatrix}" in result
    assert "{TileRow}" in result
    assert "{TileCol}" in result


def test_derive_get_tile_layer_preserved():
    result = _derive_wmts_get_tile_href(_FAO_PREVIEW_HREF)
    assert result is not None
    params = _parse_template(result)
    assert "fao-gismgr/GAEZ-V5/maps/AEZ33/GAEZ-V5.AEZ33" in params.get("layer", "")


def test_derive_get_tile_strips_preview_only_params():
    result = _derive_wmts_get_tile_href(_FAO_PREVIEW_HREF)
    assert result is not None
    params = _parse_template(result)
    assert "width" not in params
    assert "height" not in params


def test_derive_get_tile_forces_web_mercator_for_epsg4326():
    """A non-mercator source TMS (EPSG:4326) is overridden with the preferred
    web-mercator id EPSG:3857 (the id legacy WMTS actually advertise)."""
    result = _derive_wmts_get_tile_href(_FAO_PREVIEW_HREF)
    assert result is not None
    params = _parse_template(result)
    assert params.get("tilematrixset") == "EPSG:3857"


def test_derive_get_tile_keeps_epsg3857():
    href = _FAO_PREVIEW_HREF.replace("EPSG:4326", "EPSG:3857")
    result = _derive_wmts_get_tile_href(href)
    assert result is not None
    params = _parse_template(result)
    assert params.get("tilematrixset") == "EPSG:3857"


def test_derive_get_tile_injects_tms_when_absent():
    href = (
        "https://example.org/wmts?service=WMTS&request=GetPreview"
        "&layer=my/layer&format=image/png"
    )
    result = _derive_wmts_get_tile_href(href)
    assert result is not None
    params = _parse_template(result)
    assert params.get("tilematrixset") == "EPSG:3857"


def test_derive_get_tile_version_fixed_to_1_0_0():
    href = _FAO_PREVIEW_HREF + "&version=0.9"
    result = _derive_wmts_get_tile_href(href)
    assert result is not None
    params = _parse_template(result)
    assert params.get("version") == "1.0.0"


def test_derive_get_tile_returns_none_for_non_wmts():
    """A plain HTTP href (no request=GetPreview) should return None."""
    assert _derive_wmts_get_tile_href("https://example.org/image.png") is None


def test_derive_get_tile_returns_none_for_get_capabilities():
    href = "https://example.org/wmts?service=WMTS&request=GetCapabilities"
    assert _derive_wmts_get_tile_href(href) is None


def test_derive_get_tile_returns_none_for_empty():
    assert _derive_wmts_get_tile_href("") is None


def test_derive_get_tile_returns_none_for_malformed():
    assert _derive_wmts_get_tile_href("not a url at all!!!") is None


# ---------------------------------------------------------------------------
# WmtsWebMapLinksContributor.contribute
# ---------------------------------------------------------------------------

def test_contribute_emits_wmts_link_for_fao_item():
    contributor = WmtsWebMapLinksContributor()
    ref = _make_ref({"preview": _FAO_PREVIEW_HREF})
    links = list(contributor.contribute(ref))
    assert len(links) == 1
    link = links[0]
    assert isinstance(link, AssetLink)
    assert link.key == "wmts_tiles"
    assert "{TileMatrix}" in link.href
    assert "{TileRow}" in link.href
    assert "{TileCol}" in link.href
    assert "GetTile" in link.href


def test_contribute_emits_correct_roles():
    contributor = WmtsWebMapLinksContributor()
    ref = _make_ref({"preview": _FAO_PREVIEW_HREF})
    links = list(contributor.contribute(ref))
    assert links[0].roles == ("visual", "tiles")


def test_contribute_emits_png_media_type():
    contributor = WmtsWebMapLinksContributor()
    ref = _make_ref({"preview": _FAO_PREVIEW_HREF})
    links = list(contributor.contribute(ref))
    assert links[0].media_type == "image/png"


def test_contribute_no_link_when_no_wmts_preview():
    contributor = WmtsWebMapLinksContributor()
    ref = _make_ref({"data": "https://storage.googleapis.com/bucket/file.tif"})
    links = list(contributor.contribute(ref))
    assert links == []


def test_contribute_no_link_when_item_assets_absent():
    contributor = WmtsWebMapLinksContributor()
    ref = ResourceRef(catalog_id="cat", collection_id="col", item_id="item1")
    links = list(contributor.contribute(ref))
    assert links == []


def test_contribute_no_link_for_collection_ref():
    """Collection refs carry no item_id and no item_assets."""
    contributor = WmtsWebMapLinksContributor()
    ref = ResourceRef(catalog_id="cat", collection_id="col")
    links = list(contributor.contribute(ref))
    assert links == []


def test_contribute_survives_malformed_href():
    """Malformed hrefs must not raise — just produce no link."""
    contributor = WmtsWebMapLinksContributor()
    ref = _make_ref({"preview": "!!!not-a-url!!!"})
    links = list(contributor.contribute(ref))
    assert links == []


def test_contribute_only_emits_once_when_multiple_wmts_assets():
    """Only the first parseable WMTS preview should produce a link."""
    second_preview = _FAO_PREVIEW_HREF.replace("AEZ33", "OTHER")
    contributor = WmtsWebMapLinksContributor()
    ref = _make_ref({"preview1": _FAO_PREVIEW_HREF, "preview2": second_preview})
    links = list(contributor.contribute(ref))
    assert len(links) == 1


# ---------------------------------------------------------------------------
# WmtsWebMapLinksContributor.contribute_stac (extension URI declaration)
# ---------------------------------------------------------------------------

def test_contribute_stac_declares_extension_uri_for_wmts_item():
    from dynastore.extensions.stac.stac_contributor import StacContribution

    contributor = WmtsWebMapLinksContributor()
    ref = _make_ref({"preview": _FAO_PREVIEW_HREF})
    contributions = list(contributor.contribute_stac(ref))
    assert len(contributions) == 1
    contrib = contributions[0]
    assert isinstance(contrib, StacContribution)
    assert WEB_MAP_LINKS_EXTENSION_URI in contrib.stac_extensions


def test_contribute_stac_no_uri_when_no_wmts_asset():
    contributor = WmtsWebMapLinksContributor()
    ref = _make_ref({"data": "https://bucket/file.tif"})
    contributions = list(contributor.contribute_stac(ref))
    assert contributions == []


# ---------------------------------------------------------------------------
# End-to-end via asset_factory.add_dynamic_assets
# ---------------------------------------------------------------------------

def test_add_dynamic_assets_attaches_wmts_link_to_item():
    """Smoke test: the contributor integrates with the STAC asset factory."""
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from unittest.mock import patch, MagicMock

    import pystac
    from dynastore.extensions.stac.asset_factory import add_dynamic_assets
    from dynastore.tools.discovery import register_plugin, unregister_plugin, get_protocols

    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    item = pystac.Item(
        id="aez33",
        geometry=None,
        bbox=[-180, -90, 180, 90],
        datetime=now,
        properties={},
    )
    item.add_asset(
        "preview",
        pystac.Asset(href=_FAO_PREVIEW_HREF, media_type="image/png", roles=["visual"]),
    )

    fake_request = SimpleNamespace(query_params={})
    context = SimpleNamespace(
        base_url="http://ex",
        catalog_id="cat",
        collection_id="col",
        request=fake_request,
        stac_config=MagicMock(),
        asset_id=None,
        default_style_id=None,
    )

    contributor = WmtsWebMapLinksContributor()
    register_plugin(contributor)
    get_protocols.cache_clear()
    try:
        add_dynamic_assets(item, context)  # type: ignore[arg-type]
    finally:
        unregister_plugin(contributor)
        get_protocols.cache_clear()

    assert "wmts_tiles" in item.assets
    wmts_asset = item.assets["wmts_tiles"]
    assert "{TileMatrix}" in wmts_asset.href
    assert "{TileRow}" in wmts_asset.href
    assert "{TileCol}" in wmts_asset.href
    assert "GetTile" in wmts_asset.href
