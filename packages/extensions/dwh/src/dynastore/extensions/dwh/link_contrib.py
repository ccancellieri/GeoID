"""DwhLinkContributor — advertises legacy /dwh join endpoint in collection responses.

Emits a ``resource_root`` link per collection so consumers can discover the
DWH join surface alongside the OGC API - Joins surface. Remove once clients
have migrated to /join and DwhService is retired.
"""

from __future__ import annotations

from typing import AsyncIterator

from dynastore.models.protocols.asset_contrib import ResourceRef
from dynastore.models.protocols.link_contrib import AnchoredLink


class DwhLinkContributor:
    """LinkContributor for the legacy Data Warehouse join surface (/dwh).

    Contributes ``rel="dwh-join"`` (POST) at ``resource_root`` — the
    per-catalog join endpoint that accepts a DWHJoinRequestBase body.
    """

    priority: int = 100  # matches DwhService.priority

    async def contribute_links(self, ref: ResourceRef) -> AsyncIterator[AnchoredLink]:
        if ref.item_id is not None:
            return  # join is collection-scoped

        base = (ref.base_url or "").rstrip("/")
        href = f"{base}/dwh/catalogs/{ref.catalog_id}/join"

        yield AnchoredLink(
            anchor="resource_root",
            rel="dwh-join",
            href=href,
            title="Data Warehouse join (legacy — use /join instead)",
            media_type="application/json",
            extras={"method": "POST"},
        )
