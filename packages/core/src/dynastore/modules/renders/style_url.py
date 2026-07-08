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

"""Helpers for rendering rasters with source-linked SLD styles.

The cache decorator uses the process cache manager: Valkey when the cache
module registered it, otherwise the local async backend. Renderers consume the
fetched SLD body; they still cache rendered tile bytes through TileStorageProtocol.
"""

from __future__ import annotations

import hashlib
import urllib.request
from typing import Any, Iterable, Optional
from urllib.parse import urlparse

from dynastore.modules.concurrency import run_in_thread
from dynastore.tools.cache import cached

_MAX_STYLE_BYTES = 1_048_576
_STYLE_TIMEOUT_SECONDS = 10
_SLD_MEDIA_MARKERS = ("sld", "xml")


def style_url_cache_id(style_url: str) -> str:
    """Return a short, cache-key-safe identifier for an external style URL."""
    return hashlib.sha256(style_url.encode("utf-8")).hexdigest()[:16]


def _fetch_text(style_url: str) -> str:
    parsed = urlparse(style_url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("style_url must use http or https")
    req = urllib.request.Request(
        style_url,
        headers={
            "Accept": "application/vnd.ogc.sld+xml,application/xml,text/xml,*/*",
            "User-Agent": "dynastore-render-style/1.0",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=_STYLE_TIMEOUT_SECONDS) as resp:  # noqa: S310
        content_type = (resp.headers.get("content-type") or "").lower()
        if content_type and not any(marker in content_type for marker in _SLD_MEDIA_MARKERS):
            raise ValueError(f"style_url did not return an XML/SLD media type: {content_type}")
        data = resp.read(_MAX_STYLE_BYTES + 1)
    if len(data) > _MAX_STYLE_BYTES:
        raise ValueError("style_url response exceeds maximum supported SLD size")
    return data.decode("utf-8")


@cached(maxsize=512, ttl=3600, jitter=60, namespace="render_style_url_sld")
async def fetch_sld_body(style_url: str) -> str:
    """Fetch and cache an SLD body by URL."""
    return await run_in_thread(_fetch_text, style_url)


def _iter_links(item: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for link in item.get("links") or []:
        if isinstance(link, dict):
            yield link
    for asset in (item.get("assets") or {}).values():
        if not isinstance(asset, dict):
            continue
        for link in asset.get("links") or []:
            if isinstance(link, dict):
                yield link


def _iter_style_assets(item: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for asset_key, asset in (item.get("assets") or {}).items():
        if isinstance(asset, dict):
            enriched = dict(asset)
            enriched.setdefault("_asset_key", asset_key)
            yield enriched


def _matches_style_id(candidate: dict[str, Any], style_id: Optional[str]) -> bool:
    if not style_id:
        return True
    needle = style_id.lower()
    values = [
        candidate.get("href"),
        candidate.get("title"),
        candidate.get("name"),
        candidate.get("_asset_key"),
    ]
    return any(needle in str(value).lower() for value in values if value)


def style_url_from_item(item: dict[str, Any], style_id: Optional[str] = None) -> Optional[str]:
    """Find an SLD/style URL attached to a STAC/Feature item.

    Prefers explicit ``rel=sld`` links, then SLD-looking assets, then generic
    ``rel=style`` links whose URL/title matches the requested style id.
    """
    fallback_style_href: Optional[str] = None
    for link in _iter_links(item):
        href = link.get("href")
        if not href:
            continue
        rel = str(link.get("rel") or "").lower()
        media_type = str(link.get("type") or "").lower()
        if rel == "sld" or "sld" in media_type or str(href).lower().endswith("/sld"):
            if _matches_style_id(link, style_id):
                return str(href)
        if rel == "style" and _matches_style_id(link, style_id):
            fallback_style_href = str(href)

    for asset in _iter_style_assets(item):
        href = asset.get("href")
        if not href:
            continue
        roles = {str(role).lower() for role in (asset.get("roles") or [])}
        media_type = str(asset.get("type") or "").lower()
        if (
            {"style", "sld"} & roles
            or "sld" in media_type
            or str(href).lower().endswith((".sld", "/sld"))
        ) and _matches_style_id(asset, style_id):
            return str(href)

    return fallback_style_href
