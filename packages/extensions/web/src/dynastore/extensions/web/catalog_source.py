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

"""Default (public, unfiltered) catalog-list provider for UI pickers.

Registered by the Web extension at priority 100 — the fallback that wins
only when no higher-priority, auth-aware provider (e.g. IAM) is mounted.
On an IAM-less deployment there is no tenant-scope enforcement, so listing
every catalog is correct; when IAM is present its provider outranks this
one and is authoritative (see ``CatalogListProvider``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List

from dynastore.models.protocols import CatalogsProtocol
from dynastore.models.protocols.catalog_source import CatalogOption
from dynastore.tools.discovery import get_protocol
from dynastore.tools.language_utils import resolve_localized_field

if TYPE_CHECKING:
    from starlette.requests import Request

# Page through the shared CatalogsProtocol so deployments with hundreds of
# catalogs are not silently truncated (mirrors the former front-end paging).
_PAGE = 1000
_MAX_PAGES = 50  # 50k catalogs ceiling — well past any real deployment


class DefaultCatalogListProvider:
    """Lists every catalog via the shared ``CatalogsProtocol``, localized.

    Structurally satisfies ``CatalogListProvider`` (``priority`` +
    ``list_catalog_options``); registered via ``register_plugin`` so
    ``get_protocols(CatalogListProvider)`` discovers it.
    """

    priority: int = 100

    async def list_catalog_options(
        self, request: "Request", language: str = "en"
    ) -> List[CatalogOption]:
        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return []
        out: List[CatalogOption] = []
        for page in range(_MAX_PAGES):
            rows = await catalogs.list_catalogs(
                limit=_PAGE, offset=page * _PAGE, lang=language
            )
            for c in rows or []:
                title = resolve_localized_field(
                    getattr(c, "title", None), language
                ) or c.id
                out.append(CatalogOption(id=c.id, title=title))
            if not rows or len(rows) < _PAGE:
                break
        return out
