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

"""
StylesCollectionPipeline — CollectionPipelineProtocol stage that merges
the resolved default style into the collection's ``item_assets`` dict.

This implements OGC_STYLES.md's "Collection-Level Inheritance": the
default style is attached once at the collection level, and per-item
emissions inherit via the STAC ``item-assets`` extension's merge
semantic. Per-item style links remain the job of
``StylesLinkContributor``; this stage only touches the collection.

Additive merge: if the collection already declares a
``item_assets.default_style`` the stage leaves it alone. Author-provided
values win over platform-derived defaults.

Discovered via ``get_protocols(CollectionPipelineProtocol)`` — consumers
(STAC, Features, Coverages collection endpoints) don't import this
class directly.
"""

from __future__ import annotations

from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)

from dynastore.modules.styles.encodings import STYLE_FORMAT_TO_MEDIA_TYPE
from dynastore.modules.styles.resolver import StylesResolver


# Loader contract mirrors StylesLinkContributor's:
#   (available: {style_id: [sheet, ...]},
#    coverages_default_id: Optional[str],
#    item_assets_default_id: Optional[str])
CollectionStylesLoader = Callable[
    [str, str],
    Awaitable[Tuple[Dict[str, list], Optional[str], Optional[str]]],
]


class StylesCollectionPipeline:
    """Merge the resolved default style into ``collection["item_assets"]``.

    Attributes:
        pipeline_id: ``"styles_item_assets_merge"``.
        priority: 100 (default).
        base_url: Platform base URL used to build absolute style hrefs.
    """

    pipeline_id: str = "styles_item_assets_merge"
    priority: int = 100

    def __init__(
        self,
        loader: CollectionStylesLoader,
        *,
        base_url: str,
        resolver: Optional[StylesResolver] = None,
    ) -> None:
        self._loader = loader
        self._base_url = base_url.rstrip("/")
        self._resolver = resolver or StylesResolver()

    def can_apply(self, catalog_id: str, collection_id: str) -> bool:
        return True

    async def apply(
        self,
        catalog_id: str,
        collection_id: str,
        collection: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        # item_assets carried by the caller overrides anything we'd add.
        existing_default = (
            (collection.get("item_assets") or {}).get("default_style")
        )

        # Hint ``item_assets_default_id`` to the resolver so a caller-set
        # default_style carries through when no CoveragesConfig default wins.
        existing_default_id = (
            existing_default.get("id") if isinstance(existing_default, dict) else None
        )

        try:
            available, coverages_default, loader_item_assets_default = await self._loader(
                catalog_id, collection_id,
            )
        except Exception:
            return collection  # unchanged on loader failure

        resolution = self._resolver.resolve(
            available=available,
            coverages_config_default_id=coverages_default,
            item_assets_default_id=existing_default_id or loader_item_assets_default,
        )
        if resolution.default_style_id is None:
            return collection  # nothing to merge

        # If the caller already set a default_style entry, preserve it —
        # author-provided wins over platform-derived.
        if existing_default is not None:
            return collection

        sheets = resolution.stylesheets_by_style_id.get(resolution.default_style_id) or []
        primary_media_type = _pick_primary_media_type(sheets)
        if primary_media_type is None:
            # No recognisable encoding — don't invent one.
            return collection

        merged = {**collection}
        merged_item_assets = dict(merged.get("item_assets") or {})
        style_href = (
            f"{self._base_url}/styles/catalogs/{catalog_id}"
            f"/collections/{collection_id}/styles/{resolution.default_style_id}"
        )
        merged_item_assets["default_style"] = {
            "id": resolution.default_style_id,
            "href": style_href,
            "type": primary_media_type,
            "roles": ["style"],
            "title": f"Default style ({primary_media_type})",
        }
        merged["item_assets"] = merged_item_assets
        return merged


# --- Helpers ---------------------------------------------------------------


def _pick_primary_media_type(sheets: List[Any]) -> Optional[str]:
    """Return the media type of the first encoding that maps to a known type."""
    for sheet in sheets:
        mt = _stylesheet_media_type(sheet)
        if mt is not None:
            return mt
    return None


def _stylesheet_media_type(sheet: Any) -> Optional[str]:
    """Same shape-tolerant extractor as StylesLinkContributor uses."""
    if isinstance(sheet, str):
        return sheet
    if isinstance(sheet, dict):
        content = sheet.get("content") or {}
        fmt = content.get("format") if isinstance(content, dict) else None
        fmt = fmt or sheet.get("format")
        if isinstance(fmt, str):
            return STYLE_FORMAT_TO_MEDIA_TYPE.get(fmt)
        return None
    content = getattr(sheet, "content", None)
    fmt = getattr(content, "format", None) if content is not None else None
    if fmt is None:
        return None
    fmt_str = fmt.value if hasattr(fmt, "value") else str(fmt)
    return STYLE_FORMAT_TO_MEDIA_TYPE.get(fmt_str)
