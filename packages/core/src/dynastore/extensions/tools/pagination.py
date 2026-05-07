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

"""Shared pagination link builder for OGC-protocol extensions."""

from typing import List

from fastapi import Request

from dynastore.models.shared_models import Link


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
    """
    base_url = str(request.url).split("?")[0]
    query_params = dict(request.query_params)

    links: List[Link] = [
        Link(href=str(request.url), rel="self", type=media_type),
    ]

    if offset > 0:
        prev_params = query_params.copy()
        prev_params["offset"] = str(max(0, offset - limit))
        links.append(
            Link(
                href=f"{base_url}?{'&'.join(f'{k}={v}' for k, v in prev_params.items())}",
                rel="prev",
                type=media_type,
            )
        )

    if (offset + limit) < total_count:
        next_params = query_params.copy()
        next_params["offset"] = str(offset + limit)
        links.append(
            Link(
                href=f"{base_url}?{'&'.join(f'{k}={v}' for k, v in next_params.items())}",
                rel="next",
                type=media_type,
            )
        )

    return links
