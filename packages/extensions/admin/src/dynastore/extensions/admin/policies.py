#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Pure declarations of admin-extension authorization policies.

The IAM module's ``PolicyContributor`` consumer reads these declarations
through ``AdminService.get_policies`` / ``get_role_bindings`` and
forwards them to ``PermissionProtocol``. This file never calls
``register_policy`` directly — see PR #308 for the architectural rule.
"""

from typing import List, Optional
from dynastore.models.auth import Condition, Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig


def admin_policies() -> List[Policy]:
    """Pure declaration of the admin extension's policies.

    Three policies:

    - ``admin_access`` — broad ``/admin/.*`` ALLOW. Default role
      bindings (returned by ``admin_role_bindings``) carry SYSADMIN +
      ADMIN for back-compat with existing deployments; operators may
      add/remove via REST.

    - ``admin_catalog_access`` — per-catalog mutation surface gated by
      ``catalog_admin_required``. Ships with ``required_roles=[]``: no
      role gains catalog-admin authority unless an operator names it
      (either by editing the policy's condition or by binding a role to
      the policy via ``POST /admin/roles``). Behaves like a custom role
      end-to-end.

    - ``admin_catalogs_list`` — narrow ALLOW for ``GET /admin/catalogs``
      (the catalog picker, #723). Bound by default to the
      ``catalog_admin`` sentinel role (auto-added by ``IamMiddleware``
      to any principal who holds the catalog-tier admin role) so
      catalog admins can list catalogs they administer.  The handler
      filters the response per caller; sysadmin / platform-admin reach
      this route via ``admin_access`` and get the unfiltered list.
    """
    return [
        Policy(
            id="admin_access",
            description="Grants full access to admin management endpoints.",
            actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            resources=[
                "/admin",
                "/admin/",
                "/admin/.*",
                "/web/pages/admin_panel",
            ],
            effect="ALLOW",
        ),
        Policy(
            id="admin_catalog_access",
            description=(
                "Per-catalog admin access; admits roles declared in this "
                "policy's catalog_admin_required condition. Sysadmin and "
                "platform-grant principals bypass."
            ),
            actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            resources=[r"^/admin/catalogs/[^/]+(/.*)?$"],
            effect="ALLOW",
            conditions=[
                Condition(
                    type="catalog_admin_required",
                    config={"required_roles": []},
                )
            ],
        ),
        Policy(
            id="admin_catalogs_list",
            description=(
                "Lets catalog-tier admins reach GET /admin/catalogs. The "
                "handler filters the response so each catalog admin only "
                "sees the catalogs they actually administer; sysadmin / "
                "platform-admin reach this route via admin_access and "
                "see the full list."
            ),
            actions=["GET"],
            resources=[r"^/admin/catalogs/?$"],
            effect="ALLOW",
        ),
    ]


def admin_role_bindings(
    sysadmin_role_name: Optional[str] = None,
    admin_role_name: Optional[str] = None,
) -> List[Role]:
    """Pure declaration of the admin extension's role bindings.

    - ``admin_access`` is bound to sysadmin + admin (back-compat).
    - ``admin_catalogs_list`` is bound to the ``catalog_admin`` sentinel
      role so catalog-tier admins reach the catalog picker (#723).
    - ``admin_catalog_access`` is intentionally unbound — operators wire
      catalog-admin authority through the same DB-resident flow they
      use for any custom role.
    """
    cfg = IamRolesConfig()
    sysadmin_role_name = sysadmin_role_name or cfg.sysadmin_role_name
    admin_role_name = admin_role_name or cfg.admin_role_name
    return [
        Role(name=sysadmin_role_name, policies=["admin_access"]),
        Role(name=admin_role_name, policies=["admin_access"]),
        Role(name="catalog_admin", policies=["admin_catalogs_list"]),
    ]
