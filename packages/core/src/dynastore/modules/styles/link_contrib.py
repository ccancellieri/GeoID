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
StylesLinkContributor — producer emitting OGC API - Styles links.

For every (catalog_id, collection_id) pair this contributor emits:

1. A ``rel="styles"`` link at ``resource_root``, always present, pointing
   at the collection's styles list endpoint. Consumers (STAC items,
   OGC features responses, coverage metadata) attach it to their
   response-root links. This is the "there are styles registered here;
   go discover them" link.

2. Zero or more ``rel="style"`` links at ``data_asset``, one per
   encoding of the default style. Consumers attach these inside the
   data asset's ``links`` array per OGC_STYLES.md's nested-linkage
   recommendation. STAC items get them on ``assets.data.links``;
   OGC-Features consumers without a distinct data asset fall back to
   ``resource_root``.

Lookup of available styles + the two precedence defaults requires
request-scoped DB access and is not part of the contributor's core
concern. Callers — typically the extension's ``lifespan`` — inject a
``load_styles(ref)`` coroutine that returns the resolved style set.
For tests, the load function is a simple stub.

Precedence logic lives in ``StylesResolver`` (pure), not here.
"""

from __future__ import annotations

from typing import Any, Awaitable, AsyncIterator, Callable, Dict, Optional, Tuple

from dynastore.models.protocols.asset_contrib import ResourceRef
from dynastore.models.protocols.link_contrib import AnchoredLink
from dynastore.modules.styles.encodings import STYLE_FORMAT_TO_MEDIA_TYPE
from dynastore.modules.styles.resolver import StylesResolver


# The loader's return shape:
#   (available styles: {style_id: [stylesheet-records]},
#    coverages_config_default_id: Optional[str],
#    item_assets_default_id: Optional[str])
StylesLoader = Callable[
    [ResourceRef],
    Awaitable[Tuple[Dict[str, list], Optional[str], Optional[str]]],
]


class StylesLinkContributor:
    """LinkContributor implementation for OGC API - Styles.

    Attributes:
        priority: 100 (default contributor priority).
        loader: async callable that loads available styles + precedence
                defaults for the given ResourceRef.
    """

    priority: int = 100

    def __init__(
        self,
        loader: StylesLoader,
        *,
        resolver: Optional[StylesResolver] = None,
    ) -> None:
        self._loader = loader
        self._resolver = resolver or StylesResolver()

    async def contribute_links(
        self, ref: ResourceRef,
    ) -> AsyncIterator[AnchoredLink]:
        # The styles list link is emitted unconditionally. Even when no
        # styles are registered, the endpoint returns an empty list — the
        # link advertises discoverability, not presence.
        styles_list_href = _styles_list_href(ref)
        yield AnchoredLink(
            anchor="resource_root",
            rel="styles",
            href=styles_list_href,
            title="Applicable styles",
            media_type="application/json",
        )

        try:
            available, coverages_default, item_assets_default = await self._loader(ref)
        except Exception:
            # Loader failure is non-fatal for the response. The styles
            # list link already yielded above; skip the per-encoding
            # style links.
            return

        resolution = self._resolver.resolve(
            available=available,
            coverages_config_default_id=coverages_default,
            item_assets_default_id=item_assets_default,
        )
        if resolution.default_style_id is None:
            return

        default_style_href = _style_href(ref, resolution.default_style_id)
        for sheet in resolution.stylesheets_by_style_id[resolution.default_style_id]:
            media_type = _stylesheet_media_type(sheet)
            if media_type is None:
                continue
            yield AnchoredLink(
                anchor="data_asset",
                rel="style",
                href=default_style_href,
                title=f"Default style ({media_type})",
                media_type=media_type,
            )


# --- Helpers ---------------------------------------------------------------


def _styles_list_href(ref: ResourceRef) -> str:
    base = (ref.base_url or "").rstrip("/")
    return (
        f"{base}/styles/catalogs/{ref.catalog_id}"
        f"/collections/{ref.collection_id}/styles"
    )


def _style_href(ref: ResourceRef, style_id: str) -> str:
    base = (ref.base_url or "").rstrip("/")
    return (
        f"{base}/styles/catalogs/{ref.catalog_id}"
        f"/collections/{ref.collection_id}/styles/{style_id}"
    )


def _stylesheet_media_type(sheet: Any) -> Optional[str]:
    """Extract a wire-level media type from a stylesheet record.

    Accepts the existing ``StyleSheet`` model (which has
    ``content.format`` carrying the ``StyleFormatEnum``) and bare
    strings (for test stubs). Returns ``None`` when the format doesn't
    map to a known media type — callers skip unknown encodings rather
    than emit mangled links.
    """
    # bare string: assume the caller already passed a media type
    if isinstance(sheet, str):
        return sheet
    # dict: look for "content"."format" or "format"
    if isinstance(sheet, dict):
        content = sheet.get("content") or {}
        fmt = content.get("format") if isinstance(content, dict) else None
        fmt = fmt or sheet.get("format")
        if isinstance(fmt, str):
            return STYLE_FORMAT_TO_MEDIA_TYPE.get(fmt)
        return None
    # StyleSheet / similar pydantic model: .content.format
    content = getattr(sheet, "content", None)
    fmt = getattr(content, "format", None) if content is not None else None
    if fmt is None:
        return None
    # StyleFormatEnum → str via its .value
    fmt_str = fmt.value if hasattr(fmt, "value") else str(fmt)
    return STYLE_FORMAT_TO_MEDIA_TYPE.get(fmt_str)
