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

import dataclasses

import pytest

from dynastore.models.protocols.asset_contrib import ResourceRef
from dynastore.models.protocols.link_contrib import (
    AnchoredLink,
    LinkContributor,
    make_resource_root_contributor,
)


def test_anchored_link_is_frozen_dataclass():
    link = AnchoredLink(
        anchor="resource_root",
        rel="styles",
        href="http://example/styles",
        title="Styles list",
        media_type="application/json",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        link.rel = "other"  # type: ignore[misc]


def test_link_contributor_structural_protocol():
    # A plain class with the right shape satisfies the protocol.
    class Fake:
        priority = 100

        def contribute_links(self, ref: ResourceRef):
            yield AnchoredLink(
                anchor="data_asset",
                rel="style",
                href="http://example/s",
                title="s",
                media_type="application/json",
            )

    assert isinstance(Fake(), LinkContributor)


def test_anchor_documents_supported_values():
    # Literal is a typing construct, not a runtime guard — this test pins the
    # intended value set as documentation. Runtime acceptance of other strings
    # is caught by type-checkers (pyright/mypy), not by dataclass validation.
    for anchor in ("resource_root", "data_asset", "collection_root"):
        AnchoredLink(
            anchor=anchor,
            rel="x",
            href="h",
            title="t",
            media_type="application/json",
        )


@pytest.mark.asyncio
async def test_resource_root_contributor_factory_yields_one_link_per_method():
    contributor = make_resource_root_contributor(
        rel="join",
        path_template="{base}/join/catalogs/{catalog_id}/collections/{collection_id}/join",
        methods=(("GET", "describe"), ("POST", "execute")),
        priority=180,
    )
    assert isinstance(contributor, LinkContributor)
    assert contributor.priority == 180

    ref = ResourceRef(catalog_id="cat1", collection_id="col1", base_url="http://ex/")
    links = [link async for link in contributor.contribute_links(ref)]

    assert [link.title for link in links] == ["describe", "execute"]
    assert [link.extras["method"] for link in links] == ["GET", "POST"]
    for link in links:
        assert link.anchor == "resource_root"
        assert link.rel == "join"
        assert link.href == "http://ex/join/catalogs/cat1/collections/col1/join"


@pytest.mark.asyncio
async def test_resource_root_contributor_factory_skips_item_scoped_refs():
    contributor = make_resource_root_contributor(
        rel="dwh-join",
        path_template="{base}/dwh/catalogs/{catalog_id}/join",
        methods=(("POST", "legacy join"),),
        priority=100,
    )
    ref = ResourceRef(catalog_id="cat1", collection_id="col1", item_id="item1")
    links = [link async for link in contributor.contribute_links(ref)]
    assert links == []


def test_resource_ref_base_strips_trailing_slash_and_tolerates_none():
    assert ResourceRef(catalog_id="c", collection_id="col", base_url="http://ex/").base == "http://ex"
    assert ResourceRef(catalog_id="c", collection_id="col", base_url="").base == ""
