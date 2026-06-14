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

"""IAM implementation of ``PageVisibilityFilter``.

Reads the caller's role list from ``request.state.principal_role``
(populated by ``IamMiddleware`` for every authenticated request) and
applies the standard visibility rule:

  - sysadmin principal → sees every page (operator override).
  - page with no audience declaration → visible to everyone.
  - page with ``audience_policy_id`` → resolve policy bindings via
    ``PermissionProtocol`` and admit if any role intersects the
    caller's flat role list. Preferred path: operators rebind the
    policy via REST without touching decorator code.
  - page with ``required_roles`` (legacy literal list) → admit if the
    caller's roles intersect.

The web route never names a role. The match logic and the role-name
literals stay inside IAM, where they belong.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from starlette.requests import Request

from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.models.protocols.role_admin import RoleAdminProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


class IamPageVisibilityFilter:
    """Concrete ``PageVisibilityFilter`` implementation owned by IAM.

    Structurally satisfies ``PageVisibilityFilter`` — no inheritance.
    Registered as a plugin in ``IamExtension.lifespan`` so consumers
    (e.g. the web ``/web/config/pages`` route) resolve it via
    ``get_protocol(PageVisibilityFilter)``.

    The sysadmin role name is configurable per construction so
    deployments that rename the platform super-user role wire it
    through. Defaults to ``IamRolesConfig().sysadmin_role_name``.
    """

    def __init__(self, sysadmin_role_name: Optional[str] = None) -> None:
        cfg = IamRolesConfig()
        self._sysadmin_role = sysadmin_role_name or cfg.sysadmin_role_name
        self._anonymous_role = cfg.anonymous_role_name

    async def filter_visible(
        self,
        pages: List[Dict[str, Any]],
        request: Request,
    ) -> List[Dict[str, Any]]:
        user_roles = self._caller_roles(request)
        is_sysadmin = self._sysadmin_role in user_roles
        anonymous = self._anonymous_role

        # Catalog context: IamMiddleware writes request.state.catalog_id when
        # the request targets a /catalogs/{cat}/… path. Use it to include
        # tenant-schema roles in the audience map so a page gated on a
        # catalog-tier role is visible to holders of that role.
        catalog_id: Optional[str] = getattr(request.state, "catalog_id", None)

        # Build a lazy policy_id → {role_names} map. Covers platform-scope
        # roles plus, when a catalog context is present, catalog-tier roles.
        policy_audience: Optional[Dict[str, Set[str]]] = None

        async def _resolve_policy_audience() -> Dict[str, Set[str]]:
            nonlocal policy_audience
            if policy_audience is not None:
                return policy_audience
            policy_audience = await self._build_policy_audience_map(catalog_id=catalog_id)
            return policy_audience

        visible: List[Dict[str, Any]] = []
        for page in pages:
            if is_sysadmin:
                visible.append(page)
                continue

            audience_policy_id = page.get("audience_policy_id")
            if audience_policy_id:
                audience = (await _resolve_policy_audience()).get(audience_policy_id, set())
                if any(r in audience for r in user_roles):
                    visible.append(page)
                continue

            required = page.get("required_roles") or []
            if not required or anonymous in required:
                visible.append(page)
                continue
            if user_roles and any(r in user_roles for r in required):
                visible.append(page)

        return visible

    def _caller_roles(self, request: Request) -> List[str]:
        state_roles = getattr(request.state, "principal_role", None)
        if not state_roles:
            return []
        if isinstance(state_roles, list):
            return [str(r) for r in state_roles]
        return [str(state_roles)]

    async def _build_policy_audience_map(
        self,
        catalog_id: Optional[str] = None,
    ) -> Dict[str, Set[str]]:
        """Return ``{policy_id: {role_name, ...}}`` snapshot.

        Reads via ``RoleAdminProtocol.list_roles`` and inverts the
        role.policies relation. When ``catalog_id`` is provided the map
        is built from the union of platform-scope roles (``iam.roles``)
        and catalog-tier roles (the tenant schema). This ensures pages
        gated on a catalog-tier ``audience_policy_id`` are visible to
        holders of that catalog role.

        Failure modes (no provider registered, list_roles raises) yield
        an empty map so ``audience_policy_id`` pages fall back to
        anonymous-only admission rather than blocking the entire response.
        """
        ra = get_protocol(RoleAdminProtocol)
        if ra is None:
            return {}
        try:
            platform_roles = await ra.list_roles()
            catalog_roles: List[Any] = []
            if catalog_id:
                catalog_roles = await ra.list_roles(catalog_id=catalog_id)
        except Exception as e:
            logger.debug("PageVisibilityFilter: list_roles failed: %s", e)
            return {}

        out: Dict[str, Set[str]] = {}
        for role in (platform_roles or []) + (catalog_roles or []):
            for policy_id in getattr(role, "policies", None) or []:
                out.setdefault(str(policy_id), set()).add(str(role.name))
        return out
