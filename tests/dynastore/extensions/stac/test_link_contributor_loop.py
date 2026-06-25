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

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pystac
import pytest

from dynastore.models.protocols.link_contrib import AnchoredLink


_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class _FakeContrib:
    def __init__(self, links):
        self.priority = 100
        self._links = links

    async def contribute_links(self, ref):
        for l in self._links:
            yield l


def _make_context(**overrides):
    # Lightweight AssetContext — the loop only reads catalog_id,
    # collection_id, base_url, asset_id, and request.query_params.
    fake_request = SimpleNamespace(query_params={})
    defaults = {
        "base_url": "http://ex",
        "catalog_id": "cat1",
        "collection_id": "col1",
        "request": fake_request,
        "stac_config": MagicMock(),
        "asset_id": None,
        "default_style_id": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_item():
    item = pystac.Item(
        id="item1",
        geometry=None,
        bbox=None,
        datetime=_NOW,
        properties={},
    )
    item.add_asset("data", pystac.Asset(href="http://ex/data"))
    return item


def _make_collection():
    return pystac.Collection(
        id="col1", description="c", extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent([[None, None]]),
        ),
    )


@pytest.mark.asyncio
async def test_resource_root_anchor_lands_on_item_links():
    from dynastore.extensions.stac.asset_factory import add_dynamic_assets_and_links

    link = AnchoredLink(
        anchor="resource_root", rel="styles",
        href="http://ex/styles", title="t", media_type="application/json",
    )
    item = _make_item()
    with patch(
        "dynastore.extensions.stac.asset_factory.get_protocols",
        return_value=[_FakeContrib([link])],
    ):
        await add_dynamic_assets_and_links(item, _make_context())
    rels = [l.rel for l in item.links]
    assert "styles" in rels


@pytest.mark.asyncio
async def test_data_asset_anchor_lands_on_asset_nested_links():
    from dynastore.extensions.stac.asset_factory import add_dynamic_assets_and_links

    link = AnchoredLink(
        anchor="data_asset", rel="style",
        href="http://ex/s", title="t", media_type="application/json",
    )
    item = _make_item()
    with patch(
        "dynastore.extensions.stac.asset_factory.get_protocols",
        return_value=[_FakeContrib([link])],
    ):
        await add_dynamic_assets_and_links(item, _make_context())
    data_asset = item.assets["data"]
    nested = (data_asset.extra_fields or {}).get("links", [])
    assert any(l["rel"] == "style" for l in nested)
    # Item-level links must NOT carry the style link.
    assert "style" not in [l.rel for l in item.links]


@pytest.mark.asyncio
async def test_data_asset_falls_back_to_resource_root_when_no_data_asset():
    from dynastore.extensions.stac.asset_factory import add_dynamic_assets_and_links

    item = pystac.Item(
        id="item2",
        geometry=None, bbox=None, datetime=_NOW, properties={},
    )  # no 'data' asset
    link = AnchoredLink(
        anchor="data_asset", rel="style",
        href="http://ex/s", title="t", media_type="application/json",
    )
    with patch(
        "dynastore.extensions.stac.asset_factory.get_protocols",
        return_value=[_FakeContrib([link])],
    ):
        await add_dynamic_assets_and_links(item, _make_context())
    assert "style" in [l.rel for l in item.links]


@pytest.mark.asyncio
async def test_collection_root_anchor_on_collection():
    from dynastore.extensions.stac.asset_factory import add_dynamic_assets_and_links

    link = AnchoredLink(
        anchor="collection_root", rel="styles",
        href="http://ex/styles", title="t", media_type="application/json",
    )
    coll = _make_collection()
    with patch(
        "dynastore.extensions.stac.asset_factory.get_protocols",
        return_value=[_FakeContrib([link])],
    ):
        await add_dynamic_assets_and_links(coll, _make_context())
    assert "styles" in [l.rel for l in coll.links]


@pytest.mark.asyncio
async def test_contributor_exception_does_not_crash_loop(caplog):
    from dynastore.extensions.stac.asset_factory import add_dynamic_assets_and_links

    class Broken:
        priority = 100

        async def contribute_links(self, ref):
            raise RuntimeError("boom")
            yield  # pragma: no cover — makes this an async generator

    item = _make_item()
    with patch(
        "dynastore.extensions.stac.asset_factory.get_protocols",
        return_value=[Broken()],
    ):
        await add_dynamic_assets_and_links(item, _make_context())
    # Item survives — no crash, no new links added.
    assert item.id == "item1"
    assert "boom" in caplog.text


@pytest.mark.asyncio
async def test_link_extras_merged_into_emitted_link():
    from dynastore.extensions.stac.asset_factory import add_dynamic_assets_and_links

    link = AnchoredLink(
        anchor="resource_root", rel="custom",
        href="http://ex/x", title="t", media_type="application/json",
        extras={"hreflang": "en"},
    )
    item = _make_item()
    with patch(
        "dynastore.extensions.stac.asset_factory.get_protocols",
        return_value=[_FakeContrib([link])],
    ):
        await add_dynamic_assets_and_links(item, _make_context())
    emitted = next(l for l in item.links if l.rel == "custom")
    link_dict = emitted.to_dict()
    assert link_dict.get("hreflang") == "en"
