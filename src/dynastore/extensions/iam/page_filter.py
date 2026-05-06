#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License").

"""IAM implementation of ``PageVisibilityFilter``.

Reads the caller's role list from ``request.state.principal_role``
(populated by ``IamMiddleware`` for every authenticated request) and
applies the standard visibility rule:

  - pages with no ``required_roles`` → visible to everyone.
  - pages listing ``anonymous`` in ``required_roles`` → visible to
    everyone (legacy explicit anonymous tag).
  - pages with the sysadmin role in their ``required_roles`` are
    accessible to sysadmin (other roles still need to match).
  - sysadmin principal → sees every page (operator override).
  - any other principal → sees pages where their roles intersect
    ``required_roles``.

The web route never names a role. The match logic and the role-name
literals stay inside IAM, where they belong.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from starlette.requests import Request

from dynastore.models.protocols.authorization import DefaultRole

logger = logging.getLogger(__name__)


class IamPageVisibilityFilter:
    """Concrete ``PageVisibilityFilter`` implementation owned by IAM.

    Structurally satisfies ``PageVisibilityFilter`` — no inheritance.
    Registered as a plugin in ``IamExtension.lifespan`` so consumers
    (e.g. the web ``/web/config/pages`` route) resolve it via
    ``get_protocol(PageVisibilityFilter)``.

    The sysadmin role name is configurable per construction so
    deployments that rename the platform super-user role wire it
    through. Defaults to ``DefaultRole.SYSADMIN.value``.
    """

    def __init__(self, sysadmin_role_name: Optional[str] = None) -> None:
        self._sysadmin_role = sysadmin_role_name or DefaultRole.SYSADMIN.value

    def filter_visible(
        self,
        pages: List[Dict[str, Any]],
        request: Request,
    ) -> List[Dict[str, Any]]:
        user_roles = self._caller_roles(request)
        is_sysadmin = self._sysadmin_role in user_roles
        anonymous = DefaultRole.ANONYMOUS.value
        visible: List[Dict[str, Any]] = []
        for page in pages:
            required = page.get("required_roles") or []
            if not required or anonymous in required:
                visible.append(page)
                continue
            if is_sysadmin:
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
