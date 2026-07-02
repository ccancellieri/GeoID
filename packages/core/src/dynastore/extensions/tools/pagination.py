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

"""Shared pagination link builder for OGC-protocol extensions."""

from typing import List, Optional

from fastapi import Request

from dynastore.models.shared_models import Link


def resolve_page_limit(
    limit: Optional[int], *, default_limit: int, max_limit: int
) -> int:
    """Resolve the effective page ``limit`` from a request value + policy.

    ``limit is None`` (parameter omitted) falls back to ``default_limit``.
    Any supplied value is clamped to ``[1, max_limit]`` rather than
    rejected — OGC API - Features Part 1 Core, requirement
    ``/req/core/fc-limit-response-1``: a ``limit`` larger than the maximum
    SHALL NOT result in an error, the maximum is used instead. ``default_limit``
    and ``max_limit`` normally come from the caller's plugin config
    (``FeaturesPluginConfig`` / ``StacPluginConfig`` / ``RecordsPluginConfig``),
    so operators can tune page-size policy without a code change.
    """
    eff = default_limit if limit is None else limit
    return max(1, min(eff, max_limit))


def _paged_link(request: Request, offset: int, rel: str, media_type: str) -> Link:
    """Build a single ``rel`` link at the given ``offset``, preserving every
    other query parameter of the current request. Shared by
    :func:`build_pagination_links` (prev/next) and :func:`build_next_link`
    (byte-budget-corrected next).
    """
    base_url = str(request.url).split("?")[0]
    params = dict(request.query_params)
    params["offset"] = str(offset)
    href = f"{base_url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
    return Link(href=href, rel=rel, type=media_type)


def build_pagination_links(
    request: Request,
    offset: int,
    limit: int,
    total_count: int,
    media_type: str = "application/geo+json",
) -> List[Link]:
    """Build self/prev/next links for any OGC paginated response.

    Args:
        request: The current FastAPI request (used for URL and query params).
        offset: Current page offset.
        limit: Page size.
        total_count: Total number of matching items.
        media_type: Media type for link ``type`` attribute.

    Returns:
        List of Link objects containing at minimum a ``self`` link,
        plus ``prev`` and/or ``next`` when applicable.

    Note: the ``next`` link built here assumes the page actually returned
    ``limit`` items. A caller whose page can be cut short by something other
    than the SQL ``LIMIT`` (e.g. a response byte budget) must replace it with
    :func:`build_next_link`, computed from the number of items actually
    served.
    """
    links: List[Link] = [
        Link(href=str(request.url), rel="self", type=media_type),
    ]

    if offset > 0:
        links.append(_paged_link(request, max(0, offset - limit), "prev", media_type))

    if (offset + limit) < total_count:
        links.append(_paged_link(request, offset + limit, "next", media_type))

    return links


def build_next_link(
    request: Request,
    offset: int,
    returned_count: int,
    total_count: Optional[int],
    media_type: str = "application/geo+json",
) -> Optional[Link]:
    """Build a ``next`` link from the ACTUAL number of items served on this
    page, rather than the requested ``limit``.

    ``offset + returned_count`` resumes exactly where this page stopped
    serving items — correct both for the ordinary case (page cut short only
    by the SQL ``LIMIT``, i.e. the natural end of the result set) and for a
    page cut short by a response byte budget (fewer items served than
    ``limit`` even though more still match). Returns ``None`` when there is
    nothing left to return, or ``total_count`` is unknown.
    """
    if total_count is None:
        return None
    next_offset = offset + returned_count
    if next_offset >= total_count:
        return None
    return _paged_link(request, next_offset, "next", media_type)
