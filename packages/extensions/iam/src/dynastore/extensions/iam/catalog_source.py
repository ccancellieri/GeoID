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

"""IAM-side catalog-list provider (priority 10) for UI pickers.

Wins over the public full-list provider whenever IAM is mounted: returns the
catalogs the authenticated principal may see — every catalog for a sysadmin,
the principal's granted catalogs otherwise, and an empty list for an
unauthenticated caller. The result is authoritative (see
``CatalogListProvider``): the empty list is NOT overridden by a lower-priority
full-list provider, so a restricted principal never sees catalogs outside
their grants.

Identity is read from ``request.state`` (populated by ``IamMiddleware``) and
the catalog rows come from the shared ``CatalogsProtocol`` — no IAM-internal
storage import, keeping this a thin, protocol-only consumer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional, cast

from dynastore.models.protocols import CatalogsProtocol
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.models.protocols.catalog_source import CatalogOption
from dynastore.tools.discovery import get_protocol
from dynastore.tools.language_utils import resolve_localized_field

if TYPE_CHECKING:
    from starlette.requests import Request

_LIMIT = 1000  # single-page ceiling; per-principal grants are small, sysadmin rare


def _roles_from_state(request: "Request") -> List[str]:
    state_roles = getattr(request.state, "principal_role", None)
    if not state_roles:
        return []
    return list(state_roles) if isinstance(state_roles, list) else [state_roles]


def _principal_id_from_state(request: "Request") -> Optional[str]:
    principal = getattr(request.state, "principal", None)
    if principal is None:
        return None
    return (
        str(getattr(principal, "id", "") or "")
        or getattr(principal, "subject_id", None)
    ) or None


class IamCatalogListProvider:
    """Grant-aware catalog list; authoritative whenever IAM is mounted."""

    priority: int = 10

    async def list_catalog_options(
        self, request: "Request", language: str = "en"
    ) -> List[CatalogOption]:
        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            return []

        roles = _roles_from_state(request)
        principal_id = _principal_id_from_state(request)

        if IamRolesConfig().sysadmin_role_name in roles:
            rows = await catalogs.list_catalogs(limit=_LIMIT, offset=0, lang=language)
        elif principal_id:
            # Forward the principal filter so the protocol restricts to the
            # caller's catalogs; fall back to the unfiltered call only if the
            # implementation predates principal_id support.
            try:
                rows = await cast(Any, catalogs).list_catalogs(
                    limit=_LIMIT, offset=0, lang=language, principal_id=principal_id
                )
            except TypeError:
                rows = await catalogs.list_catalogs(
                    limit=_LIMIT, offset=0, lang=language
                )
        else:
            # Unauthenticated caller on an IAM-enforced deployment → nothing.
            return []

        return [
            CatalogOption(
                id=c.id,
                title=resolve_localized_field(getattr(c, "title", None), language)
                or c.id,
            )
            for c in rows or []
        ]
