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

"""Pure declarations of catalog_status extension authorization policies.

The IAM module's ``PolicyContributor`` consumer reads these declarations
through the preset registered in ``presets/__init__.py`` and forwards them
to ``PermissionProtocol``.  This file never calls ``register_policy``
directly.

``catalog_status_read``
    ALLOW ``GET`` on catalog/collection status views, gated by the
    ``catalog_membership_required`` condition. The status payload exposes
    operational detail (internal ``physical_schema`` name, provision task
    ``error_message``), so it is restricted to callers with a stake in the
    catalog: any principal holding a grant on the catalog (a member, not
    only catalog-admins), plus sysadmin / platform principals via the
    handler's built-in bypasses. The condition fails closed for anonymous
    requests (``principal_obj is None`` -> deny), so unauthenticated callers
    get nothing even though the policy is bound to the universal base role.
    The 404-on-hidden enforcement in the route handler is the second layer
    (a member of catalog A cannot probe catalog B's status).

Mutation surfaces (reprovision, dead-letter list/requeue) have moved to the
unified Tasks API and are governed by the ``tasks_admin`` /
``tasks_system_admin`` policies registered by the tasks extension.
"""
from typing import List, Optional

from dynastore.models.auth import Condition, Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig


def catalog_status_policies() -> List[Policy]:
    """Pure declaration of the catalog_status extension's policies."""
    return [
        Policy(
            id="catalog_status_read",
            description=(
                "Allows catalog members (any principal with a grant on the "
                "catalog), plus sysadmin/platform principals, to reach the "
                "catalog/collection status read surfaces (GET "
                "/catalog/catalogs/{id} and "
                "GET /catalog/catalogs/{id}/collections/{col}). Gated by "
                "catalog_membership_required, which fails closed for anonymous "
                "requests. Per-catalog visibility (404 for hidden catalogs) is "
                "enforced as a second layer in the route handler."
            ),
            actions=["GET"],
            resources=[
                r"^/catalog/catalogs/[^/]+(/collections/[^/]+)?$",
            ],
            effect="ALLOW",
            conditions=[
                Condition(type="catalog_membership_required", config={}),
            ],
        ),
    ]


def catalog_status_role_bindings(
    sysadmin_role_name: Optional[str] = None,
    admin_role_name: Optional[str] = None,
) -> List[Role]:
    """Pure declaration of the catalog_status extension's role bindings.

    ``catalog_status_read`` is bound to the universal base role
    (``IamRolesConfig().anonymous_role_name`` — ``"unauthenticated"`` by
    default, which every request carries directly or via the role hierarchy).
    The binding only makes the policy *reachable*; the
    ``catalog_membership_required`` condition is what actually grants access,
    so the effective audience is "catalog members + sysadmin/platform" and
    anonymous requests are denied (the handler returns deny when there is no
    principal). Binding to the base role rather than to ``admin`` is
    deliberate: a non-admin member with a catalog grant must still be able to
    read status (the maintainer's "not just admins" requirement), and only the
    base role is guaranteed to be in every member's effective set.

    Mutation surfaces (reprovision, dead-letter) have moved to the tasks
    extension and are governed by ``tasks_admin`` / ``tasks_system_admin``.
    """
    cfg = IamRolesConfig()
    return [
        # Read surface: reachable by every request via the universal base
        # role; catalog_membership_required is the actual access control and
        # fails closed for anonymous callers.
        Role(name=cfg.anonymous_role_name, policies=["catalog_status_read"]),
    ]
