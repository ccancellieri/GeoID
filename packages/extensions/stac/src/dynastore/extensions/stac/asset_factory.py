#    Copyright 2025 FAO
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

# dynastore/extensions/stac/asset_factory.py

import logging
from typing import Optional, Union

import pystac
from fastapi import Request
from pydantic import BaseModel, ConfigDict

from dynastore.modules import get_protocols
from dynastore.modules.stac.stac_config import StacPluginConfig
from dynastore.models.protocols.asset_contrib import (
    AssetContributor,
    AssetLink,
    ResourceRef,
)
from dynastore.models.protocols.link_contrib import AnchoredLink, LinkContributor

logger = logging.getLogger(__name__)


class AssetContext(BaseModel):
    """A structured model for the context required to generate dynamic assets."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    base_url: str
    catalog_id: str
    collection_id: str
    request: Request
    stac_config: StacPluginConfig
    asset_id: Optional[str] = None


StacTarget = Union[pystac.Item, pystac.Collection]


def _to_resource_ref(target: StacTarget, context: AssetContext) -> ResourceRef:
    is_item = isinstance(target, pystac.Item)
    bbox = tuple(target.bbox) if is_item and target.bbox is not None else None
    return ResourceRef(
        catalog_id=context.catalog_id,
        collection_id=context.collection_id,
        item_id=target.id if is_item else None,
        bbox=bbox,  # type: ignore[arg-type]
        geometry=getattr(target, "geometry", None) if is_item else None,
        base_url=context.base_url,
        style=context.request.query_params.get("style"),
        extras={"asset_id": context.asset_id} if context.asset_id else {},
    )


def _attach(target: StacTarget, link: AssetLink) -> None:
    target.add_asset(
        key=link.key,
        asset=pystac.Asset(
            href=link.href,
            title=link.title,
            media_type=link.media_type,
            roles=list(link.roles) or None,
        ),
    )


def _add_source_file_asset(item: StacTarget, context: AssetContext) -> None:
    """STAC-local: source-file asset derived from the item's `asset_id` property.

    This is not a cross-protocol contribution — it's STAC-specific and stays
    here rather than being exposed as an `AssetContributor`.
    """
    asset_id = context.asset_id or item.properties.get("asset_id")  # type: ignore[attr-defined]
    if not asset_id:
        return
    href = (
        f"{context.base_url}/stac/catalogs/{context.catalog_id}"
        f"/collections/{context.collection_id}/assets/{asset_id}/source"
    )
    item.add_asset(
        "source_file",
        pystac.Asset(
            href=href, title="Original Source File", roles=["data", "source"]
        ),
    )


def _add_source_asset_to_collection(
    collection: pystac.Collection, context: AssetContext
) -> None:
    """STAC-local: source-file asset for virtual collections that wrap a single asset."""
    if not context.asset_id:
        return
    href = (
        f"{context.base_url}/stac/catalogs/{context.catalog_id}"
        f"/collections/{context.collection_id}/assets/{context.asset_id}/source"
    )
    collection.add_asset(
        "source_file",
        pystac.Asset(
            href=href, title="Original Ingested File", roles=["source", "data"]
        ),
    )


def add_dynamic_assets(item: StacTarget, context: AssetContext) -> None:
    """Attach cross-protocol `AssetContributor` links plus STAC-local source assets.

    Synchronous — only iterates sync `AssetContributor` producers. Use
    ``add_dynamic_assets_and_links`` (async) to also run the
    ``LinkContributor`` loop.
    """
    ref = _to_resource_ref(item, context)
    is_collection = isinstance(item, pystac.Collection)

    if is_collection:
        _add_source_asset_to_collection(item, context)
    else:
        for contributor in sorted(
            get_protocols(AssetContributor), key=lambda c: getattr(c, "priority", 100)
        ):
            try:
                for link in contributor.contribute(ref):
                    _attach(item, link)
            except Exception as e:
                logger.error(
                    "AssetContributor %s failed: %s",
                    type(contributor).__name__,
                    e,
                )
        _add_source_file_asset(item, context)


async def add_dynamic_assets_and_links(item: StacTarget, context: AssetContext) -> None:
    """Run AssetContributor (sync) + LinkContributor (async) + STAC-local assets.

    The LinkContributor loop dispatches each yielded ``AnchoredLink`` to
    the correct anchor on the STAC document:

    - ``anchor="resource_root"``  → ``item.links[]`` (or ``collection.links[]``)
    - ``anchor="data_asset"``     → ``item.assets["data"].extra_fields["links"][]``
                                   (falls back to resource_root if no data asset)
    - ``anchor="collection_root"``→ ``collection.links[]`` (on collections; on
                                   items this falls back to ``item.links``)

    Callers that don't need async LinkContributor emission can keep calling
    the synchronous ``add_dynamic_assets`` — both functions are safe to
    call independently or in sequence; they don't share mutable state.
    """
    # Synchronous AssetContributor loop + STAC-local source files.
    add_dynamic_assets(item, context)

    ref = _to_resource_ref(item, context)
    contributors = sorted(
        get_protocols(LinkContributor),
        key=lambda c: getattr(c, "priority", 100),
    )
    for contributor in contributors:
        try:
            async for link in contributor.contribute_links(ref):
                _attach_link(item, link)
        except Exception as e:
            logger.error(
                "LinkContributor %s failed: %s",
                type(contributor).__name__,
                e,
            )


def _attach_link(target: StacTarget, link: AnchoredLink) -> None:
    """Dispatch an AnchoredLink onto the correct STAC container.

    pystac has no first-class nested-asset-links API, so
    ``anchor="data_asset"`` uses ``Asset.extra_fields["links"]`` — the
    sanctioned escape hatch per pystac's own extension patterns.
    """
    link_dict = {
        "rel": link.rel,
        "href": link.href,
        "title": link.title,
        "type": link.media_type,
        **dict(link.extras or {}),
    }

    if link.anchor == "resource_root":
        target.links.append(pystac.Link.from_dict(link_dict))
        return

    if link.anchor == "data_asset":
        is_item = isinstance(target, pystac.Item)
        assets = target.assets if is_item else target.assets  # pystac.Collection also has .assets
        data_asset = assets.get("data") if assets else None
        if data_asset is None:
            # No data asset to nest into — fall back to resource_root.
            target.links.append(pystac.Link.from_dict(link_dict))
            return
        extras = dict(data_asset.extra_fields or {})
        existing = list(extras.get("links") or [])
        existing.append(link_dict)
        extras["links"] = existing
        data_asset.extra_fields = extras
        return

    if link.anchor == "collection_root":
        # Items fall back to their own links; collections use their links list.
        target.links.append(pystac.Link.from_dict(link_dict))
        return

    logger.warning("Unknown AnchoredLink anchor %r; dropping link", link.anchor)
