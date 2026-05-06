"""JoinsLinkContributor — advertises OGC API - Joins endpoints in collection responses.

Emits two ``resource_root`` links per collection so OGC Features, STAC, and
other consumers surface the /join surface without callers needing to know
the join extension's prefix.

Links are collection-scoped (skipped for individual item refs).
"""

from __future__ import annotations

from typing import AsyncIterator

from dynastore.models.protocols.asset_contrib import ResourceRef
from dynastore.models.protocols.link_contrib import AnchoredLink


class JoinsLinkContributor:
    """LinkContributor for OGC API - Joins.

    Contributes:
    - ``rel="join"`` (GET)  — describe endpoint (capabilities + supported drivers)
    - ``rel="join"`` (POST) — execute endpoint
    """

    priority: int = 180  # matches JoinsService.priority

    async def contribute_links(self, ref: ResourceRef) -> AsyncIterator[AnchoredLink]:
        if ref.item_id is not None:
            return  # join is collection-scoped

        base = (ref.base_url or "").rstrip("/")
        href = (
            f"{base}/join/catalogs/{ref.catalog_id}"
            f"/collections/{ref.collection_id}/join"
        )

        yield AnchoredLink(
            anchor="resource_root",
            rel="join",
            href=href,
            title="OGC API - Joins: describe",
            media_type="application/json",
            extras={"method": "GET"},
        )
        yield AnchoredLink(
            anchor="resource_root",
            rel="join",
            href=href,
            title="OGC API - Joins: execute",
            media_type="application/json",
            extras={"method": "POST"},
        )
