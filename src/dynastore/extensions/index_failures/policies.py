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

"""Policy registration for the ``index_failures`` extension.

The endpoint is per-catalog read access — tenant members of the
catalog identified by the ``{catalog_id}`` path segment are allowed.
Sysadmin and platform-grant principals pass via the standard
``catalog_membership_required`` ConditionHandler bypass.
"""
from __future__ import annotations

import logging

from dynastore.models.auth import Condition
from dynastore.models.protocols.authorization import DefaultRole
from dynastore.models.protocols.policies import (
    PermissionProtocol,
    Policy,
    Role,
)
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


_INDEX_FAILURES_POLICY_ID = "index_failures_per_catalog_access"


def register_index_failures_policies() -> None:
    """Register the per-catalog read policy for index-failures.

    The path is anchored so the regex doesn't match unintended siblings
    (the framework treats Policy.resources as ``re.match`` patterns).
    Roles ADMIN + USER pick up the policy; the
    ``catalog_membership_required`` condition does the actual
    membership check using ``catalog_id`` extracted by IamMiddleware
    from the URL.
    """
    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning(
            "IndexFailureService: PermissionProtocol unavailable; "
            "index_failures policy not registered.",
        )
        return

    policy = Policy(
        id=_INDEX_FAILURES_POLICY_ID,
        description=(
            "Per-catalog read access to recent indexing failures. "
            "Membership-gated: tenants only see their own catalog's "
            "failure log."
        ),
        actions=["GET", "OPTIONS"],
        resources=[
            r"^/index_failures/catalogs/[^/]+/index-failures(\?.*)?$",
        ],
        effect="ALLOW",
        conditions=[Condition(type="catalog_membership_required", config={})],
    )
    pm.register_policy(policy)
    for role_name in (DefaultRole.ADMIN.value, DefaultRole.USER.value):
        pm.register_role(
            Role(name=role_name, policies=[_INDEX_FAILURES_POLICY_ID]),
        )
    logger.debug(
        "IndexFailureService: per-catalog policy registered (id=%s).",
        _INDEX_FAILURES_POLICY_ID,
    )
