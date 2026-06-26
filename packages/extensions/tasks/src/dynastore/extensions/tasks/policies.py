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

"""Pure declarations of tasks extension authorization policies.

The IAM module's ``PolicyContributor`` consumer reads these declarations
through the preset registered in ``presets/__init__.py`` and forwards them
to ``PermissionProtocol``.  This file never calls ``register_policy``
directly.

Four policies:

``tasks_read``
    ALLOW ``GET`` on catalog/collection task list and get-one surfaces
    (routes 3-6), gated by ``catalog_membership_required`` so any principal
    with a grant on the catalog can monitor their own tasks.  Fails closed
    for anonymous requests.  Visibility-gating (404 for hidden catalogs) is
    the second layer, enforced in the route handler.

``tasks_system_read``
    ALLOW ``GET`` on the system-scope task list (route 1).  No condition —
    scoped to sysadmin role only via the role binding.

``tasks_admin``
    ALLOW spawn and dead-letter operations on the catalog/collection trees
    (routes 8-9, 11-12, 14-15), gated by ``catalog_admin_required``.

``tasks_system_admin``
    ALLOW system-scope mutations: unscoped get (route 2), system spawn
    (route 7), system DLQ list/requeue (routes 10, 13).  No condition —
    bound to sysadmin role only.
"""
from typing import List, Optional

from dynastore.models.auth import Condition, Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig


def tasks_policies() -> List[Policy]:
    """Pure declaration of the tasks extension's policies."""
    return [
        # ----------------------------------------------------------------
        # tasks_read: catalog/collection GET surfaces
        # Routes: 3 (list catalog), 4 (get catalog), 5 (list collection),
        #         6 (get collection)
        # ----------------------------------------------------------------
        Policy(
            id="tasks_read",
            description=(
                "Allows catalog members (any principal with a grant on the "
                "catalog) to reach the catalog/collection task read surfaces "
                "(GET /task/catalogs/{id} and "
                "GET /task/catalogs/{id}/collections/{col}[/tasks/{task_id}]). "
                "Gated by catalog_membership_required, which fails closed for "
                "anonymous requests. Per-catalog visibility (404 for hidden "
                "catalogs) is enforced as a second layer in the route handler."
            ),
            actions=["GET"],
            resources=[
                r"^/task/catalogs/[^/]+(/collections/[^/]+)?(/tasks/[^/]+)?$",
            ],
            effect="ALLOW",
            conditions=[
                Condition(type="catalog_membership_required", config={}),
            ],
        ),
        # ----------------------------------------------------------------
        # tasks_system_read: system-scope list (route 1)
        # ----------------------------------------------------------------
        Policy(
            id="tasks_system_read",
            description=(
                "Allows sysadmin principals to list system-scope tasks "
                "(GET /task and GET /task/tasks/{task_id}). "
                "No condition — effective audience is sysadmin only via the "
                "role binding."
            ),
            actions=["GET"],
            resources=[
                r"^/task(/tasks/[0-9a-fA-F-]+)?$",
            ],
            effect="ALLOW",
            conditions=[],
        ),
        # ----------------------------------------------------------------
        # tasks_admin: catalog/collection spawn + DLQ
        # Routes: 8 (spawn catalog), 9 (spawn collection),
        #         11 (DLQ list catalog), 12 (DLQ list collection),
        #         14 (DLQ requeue catalog), 15 (DLQ requeue collection)
        # ----------------------------------------------------------------
        Policy(
            id="tasks_admin",
            description=(
                "Per-catalog admin access for the tasks mutation surfaces: "
                "generic spawn and dead-letter list/requeue at catalog and "
                "collection scope. Gated by catalog_admin_required so only "
                "catalog-tier admins and above may trigger these operations."
            ),
            actions=["GET", "POST"],
            resources=[
                r"^/task/catalogs/[^/]+(/collections/[^/]+)?(/dead-letter(/.*)?)?$",
            ],
            effect="ALLOW",
            conditions=[
                Condition(
                    type="catalog_admin_required",
                    config={"required_roles": [IamRolesConfig().admin_role_name]},
                )
            ],
        ),
        # ----------------------------------------------------------------
        # tasks_system_admin: system-scope lookups + mutations
        # Routes: 2 (get unscoped), 7 (spawn system), 10 (DLQ system),
        #         13 (requeue system)
        # ----------------------------------------------------------------
        Policy(
            id="tasks_system_admin",
            description=(
                "Sysadmin access for system-scope task lookups and mutations: "
                "unscoped get-one (route 2, GET /task/tasks/{id}), system "
                "spawn (route 7), system DLQ list (route 10) and requeue "
                "(route 13). No condition — bound to sysadmin role only via "
                "role binding."
            ),
            actions=["GET", "POST"],
            resources=[
                r"^/task(/tasks/[0-9a-fA-F-]+|/dead-letter(/.*)?)?$",
            ],
            effect="ALLOW",
            conditions=[],
        ),
    ]


def tasks_role_bindings(
    sysadmin_role_name: Optional[str] = None,
    admin_role_name: Optional[str] = None,
) -> List[Role]:
    """Pure declaration of the tasks extension's role bindings.

    ``tasks_read`` is bound to the universal base role so every principal
    can reach the read surface; ``catalog_membership_required`` is the actual
    access gate (fails closed for anonymous).

    ``tasks_admin`` is bound to sysadmin + admin so both privileged tiers
    can trigger spawns and DLQ operations on catalogs they administer.

    ``tasks_system_read`` and ``tasks_system_admin`` are sysadmin-only.
    """
    cfg = IamRolesConfig()
    sysadmin_role_name = sysadmin_role_name or cfg.sysadmin_role_name
    admin_role_name = admin_role_name or cfg.admin_role_name
    return [
        # Read surface: reachable by every request; catalog_membership_required
        # is the actual access control and fails closed for anonymous callers.
        Role(name=cfg.anonymous_role_name, policies=["tasks_read"]),
        # Catalog/collection mutations: sysadmin and admin.
        Role(name=sysadmin_role_name, policies=["tasks_admin"]),
        Role(name=admin_role_name, policies=["tasks_admin"]),
        # System-scope read and mutations: sysadmin only.
        Role(name=sysadmin_role_name, policies=["tasks_system_read"]),
        Role(name=sysadmin_role_name, policies=["tasks_system_admin"]),
    ]
