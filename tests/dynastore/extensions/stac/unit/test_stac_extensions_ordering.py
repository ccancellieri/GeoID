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

"""Read-path integration test: asset-derived stac_extensions are declared.

Regression coverage for the ordering bug where StacContributor.contribute_stac
fired BEFORE add_dynamic_assets_and_links populated item.assets, so any
contributor that gates its extension URI on ref.extras["item_assets"]
(e.g. WmtsWebMapLinksContributor) could never emit its URI on the single-item
GET path.

The fix adds a second apply_stac_contributions pass after
add_dynamic_assets_and_links so contributors see the fully-populated asset map.

This test asserts:
- An item carrying a WMTS GetPreview asset is served with the ``wmts_tiles``
  dynamic asset attached.
- The web-map-links extension URI is declared in item.stac_extensions.
- apply_stac_contributions remains idempotent: the URI appears exactly once.
"""

import pytest

from starlette.requests import Request as StarletteRequest

from dynastore.models.ogc import Feature
from dynastore.modules.stac.stac_config import StacPluginConfig
from dynastore.tools.discovery import get_protocols, register_plugin, unregister_plugin
from dynastore.extensions.stac.wmts_web_map_links import (
    WEB_MAP_LINKS_EXTENSION_URI,
    WmtsWebMapLinksContributor,
)


_WMTS_PREVIEW_HREF = (
    "https://data.apps.fao.org/map/wmts/wmts"
    "?layer=fao-gismgr/GAEZ-V5/maps/AEZ57/GAEZ-V5.AEZ57"
    "&request=GetPreview"
    "&tilematrixset=EPSG:4326"
    "&service=wmts"
    "&width=512&height=512"
)


def _make_request(base_url: str = "http://localhost") -> StarletteRequest:
    """Build a minimal starlette Request suitable for the STAC generator."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/stac/catalogs/cat/collections/col/items/item1",
        "query_string": b"",
        "headers": [],
        "server": ("localhost", 80),
    }
    return StarletteRequest(scope)


@pytest.mark.asyncio
async def test_wmts_asset_and_extension_uri_present_on_single_item_read():
    """The web-map-links URI must appear in stac_extensions when the item
    carries a WMTS GetPreview source asset (the preview → wmts_tiles derivation
    by WmtsWebMapLinksContributor).

    This is the exact failure mode of the ordering bug: the early
    apply_stac_contributions call ran before the preview asset was attached, so
    contribute_stac saw an empty item_assets dict and emitted nothing.
    """
    from dynastore.extensions.stac.stac_generator import create_item_from_feature

    feature = Feature(
        type="Feature",
        id="GAEZ-V5.AEZ57",
        geometry=None,
        bbox=[-180.0, -90.0, 180.0, 90.0],
        properties={
            "datetime": "2024-01-01T00:00:00Z",
            # External WMTS preview asset stored in properties (JSONB round-trip
            # path — no stac_metadata sidecar active).
            "assets": {
                "preview": {
                    "href": _WMTS_PREVIEW_HREF,
                    "type": "image/png",
                    "roles": ["visual"],
                    "title": "Preview",
                }
            },
        },
    )

    stac_config = StacPluginConfig()
    request = _make_request()

    contributor = WmtsWebMapLinksContributor()
    register_plugin(contributor)
    get_protocols.cache_clear()
    try:
        item = await create_item_from_feature(
            request=request,
            catalog_id="fao-gismgr",
            collection_id="aez57",
            feature=feature,
            stac_config=stac_config,
        )
    finally:
        unregister_plugin(contributor)
        get_protocols.cache_clear()

    assert item is not None, "create_item_from_feature returned None"

    # The WMTS GetPreview source asset must have been picked up and the derived
    # GetTile template emitted under the 'wmts_tiles' key.
    assert "wmts_tiles" in item.assets, (
        f"Expected 'wmts_tiles' in item.assets; got: {list(item.assets.keys())}"
    )
    wmts_href = item.assets["wmts_tiles"].href
    assert "{TileMatrix}" in wmts_href, f"GetTile template missing {{TileMatrix}}: {wmts_href}"
    assert "{TileRow}" in wmts_href, f"GetTile template missing {{TileRow}}: {wmts_href}"
    assert "{TileCol}" in wmts_href, f"GetTile template missing {{TileCol}}: {wmts_href}"

    # The web-map-links extension URI must be declared.
    assert WEB_MAP_LINKS_EXTENSION_URI in item.stac_extensions, (
        f"Expected web-map-links URI in stac_extensions; got: {item.stac_extensions}"
    )

    # Idempotency: the URI must appear exactly once.
    count = item.stac_extensions.count(WEB_MAP_LINKS_EXTENSION_URI)
    assert count == 1, (
        f"web-map-links URI duplicated in stac_extensions (found {count} times)"
    )


@pytest.mark.asyncio
async def test_no_extension_uri_when_no_wmts_preview_asset():
    """An item without a WMTS preview asset must NOT have the web-map-links URI."""
    from dynastore.extensions.stac.stac_generator import create_item_from_feature

    feature = Feature(
        type="Feature",
        id="plain-item",
        geometry=None,
        bbox=None,
        properties={
            "datetime": "2024-06-01T00:00:00Z",
        },
    )

    stac_config = StacPluginConfig()
    request = _make_request()

    contributor = WmtsWebMapLinksContributor()
    register_plugin(contributor)
    get_protocols.cache_clear()
    try:
        item = await create_item_from_feature(
            request=request,
            catalog_id="fao-gismgr",
            collection_id="plain",
            feature=feature,
            stac_config=stac_config,
        )
    finally:
        unregister_plugin(contributor)
        get_protocols.cache_clear()

    assert item is not None
    assert "wmts_tiles" not in item.assets
    assert WEB_MAP_LINKS_EXTENSION_URI not in item.stac_extensions
