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

"""Unit tests for RendersStacContributor.

Pure: no DB, no rio-tiler, no HTTP.  Mocks _renders_route_registered so
tests don't depend on a real protocol registry.
"""

from __future__ import annotations

from typing import Any, Iterable
from unittest.mock import patch

import pytest

from dynastore.models.protocols.asset_contrib import ResourceRef
from dynastore.extensions.renders.stac_contributor import (
    RendersStacContributor,
    _first_cog_href,
    _RENDER_EXTENSION_URI,
    _RENDERS_PREFIX,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ref(
    *,
    catalog_id: str = "my-catalog",
    collection_id: str = "my-collection",
    item_id: str = "item-1",
    base_url: str = "https://api.example.org",
    item_assets: dict | None = None,
    default_style_id: str | None = None,
) -> ResourceRef:
    extras: dict[str, Any] = {}
    if item_assets is not None:
        extras["item_assets"] = item_assets
    if default_style_id is not None:
        extras["default_style_id"] = default_style_id
    return ResourceRef(
        catalog_id=catalog_id,
        collection_id=collection_id,
        item_id=item_id,
        base_url=base_url,
        extras=extras,
    )


_COG_ASSET = {
    "cog_band1": {
        "href": "https://s3.example.com/data/file.tif",
        "type": "image/tiff; application=geotiff; profile=cloud-optimized",
        "roles": ["data"],
    }
}

_NON_COG_ASSET = {
    "thumbnail": {
        "href": "https://s3.example.com/thumb.jpg",
        "type": "image/jpeg",
        "roles": ["thumbnail"],
    }
}


# ---------------------------------------------------------------------------
# _first_cog_href
# ---------------------------------------------------------------------------


class TestFirstCogHref:
    def test_detects_cog_by_media_type(self):
        assets = {
            "data": {
                "href": "https://s3/file.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            }
        }
        assert _first_cog_href(assets) == "https://s3/file.tif"

    def test_detects_cog_by_roles(self):
        assets = {
            "data": {
                "href": "https://s3/file.tif",
                "type": "image/tiff",
                "roles": ["data", "cloud-optimized"],
            }
        }
        assert _first_cog_href(assets) is not None

    def test_returns_none_for_non_cog(self):
        assert _first_cog_href(_NON_COG_ASSET) is None

    def test_returns_none_for_empty(self):
        assert _first_cog_href({}) is None

    def test_skips_assets_without_href(self):
        assets = {
            "empty": {"type": "image/tiff; application=geotiff"}
        }
        assert _first_cog_href(assets) is None


# ---------------------------------------------------------------------------
# RendersStacContributor.contribute — AssetLink emission
# ---------------------------------------------------------------------------


class TestContribute:
    def test_emits_asset_link_when_route_registered(self):
        contributor = RendersStacContributor()
        ref = _ref(item_assets=_COG_ASSET, default_style_id="ndvi")

        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=True,
        ):
            links = list(contributor.contribute(ref))

        assert len(links) == 1
        link = links[0]
        assert link.key == "render_tiles"
        assert link.media_type == "image/png"
        assert "renders" in link.href
        assert "my-catalog" in link.href
        assert "my-collection" in link.href
        assert "ndvi" in link.href
        assert "{z}" in link.href
        assert "{x}" in link.href
        assert "{y}" in link.href

    def test_no_emission_when_route_not_registered(self):
        contributor = RendersStacContributor()
        ref = _ref(item_assets=_COG_ASSET, default_style_id="ndvi")

        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=False,
        ):
            links = list(contributor.contribute(ref))

        assert links == []

    def test_no_emission_when_no_cog_asset(self):
        contributor = RendersStacContributor()
        ref = _ref(item_assets=_NON_COG_ASSET, default_style_id="ndvi")

        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=True,
        ):
            links = list(contributor.contribute(ref))

        assert links == []

    def test_no_emission_when_no_default_style(self):
        contributor = RendersStacContributor()
        ref = _ref(item_assets=_COG_ASSET, default_style_id=None)

        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=True,
        ):
            links = list(contributor.contribute(ref))

        assert links == []

    def test_no_emission_when_no_item_assets(self):
        contributor = RendersStacContributor()
        ref = _ref(default_style_id="ndvi")

        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=True,
        ):
            links = list(contributor.contribute(ref))

        assert links == []

    def test_uses_external_ids_in_url(self):
        """The URL must contain the external (public) catalog/collection IDs."""
        contributor = RendersStacContributor()
        ref = _ref(
            catalog_id="public-cat-id",
            collection_id="public-col-id",
            item_assets=_COG_ASSET,
            default_style_id="fire",
        )
        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=True,
        ):
            links = list(contributor.contribute(ref))

        assert len(links) == 1
        assert "public-cat-id" in links[0].href
        assert "public-col-id" in links[0].href


# ---------------------------------------------------------------------------
# RendersStacContributor.contribute_stac — StacContribution emission
# ---------------------------------------------------------------------------


class TestContributeStac:
    def test_emits_render_extension_uri(self):
        contributor = RendersStacContributor()
        ref = _ref(item_assets=_COG_ASSET, default_style_id="ndvi")

        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=True,
        ):
            contributions = list(contributor.contribute_stac(ref))

        assert len(contributions) == 1
        contrib = contributions[0]
        assert _RENDER_EXTENSION_URI in contrib.stac_extensions

    def test_renders_map_key_is_style_id(self):
        contributor = RendersStacContributor()
        ref = _ref(item_assets=_COG_ASSET, default_style_id="my_style")

        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=True,
        ):
            contributions = list(contributor.contribute_stac(ref))

        renders = contributions[0].extra_fields.get("renders", {})
        assert "my_style" in renders

    def test_renders_map_contains_href_and_type(self):
        contributor = RendersStacContributor()
        ref = _ref(item_assets=_COG_ASSET, default_style_id="ndvi")

        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=True,
        ):
            contributions = list(contributor.contribute_stac(ref))

        entry = contributions[0].extra_fields["renders"]["ndvi"]
        assert "href" in entry
        assert entry["type"] == "image/png"
        assert "{z}" in entry["href"]

    def test_renders_map_contains_style_link(self):
        contributor = RendersStacContributor()
        ref = _ref(item_assets=_COG_ASSET, default_style_id="ndvi")

        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=True,
        ):
            contributions = list(contributor.contribute_stac(ref))

        entry = contributions[0].extra_fields["renders"]["ndvi"]
        links = entry.get("links", [])
        assert any(lnk.get("rel") == "style" for lnk in links)

    def test_no_emission_when_route_not_registered(self):
        contributor = RendersStacContributor()
        ref = _ref(item_assets=_COG_ASSET, default_style_id="ndvi")

        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=False,
        ):
            contributions = list(contributor.contribute_stac(ref))

        assert contributions == []

    def test_never_mutates_item_assets(self):
        """Enrich-don't-rewrite: the original assets dict must be unchanged."""
        contributor = RendersStacContributor()
        original_assets = dict(_COG_ASSET)
        original_href = original_assets["cog_band1"]["href"]
        ref = _ref(item_assets=original_assets, default_style_id="ndvi")

        with patch(
            "dynastore.extensions.renders.stac_contributor._renders_route_registered",
            return_value=True,
        ):
            list(contributor.contribute_stac(ref))
            list(contributor.contribute(ref))

        # Original href must not have been modified
        assert original_assets["cog_band1"]["href"] == original_href
