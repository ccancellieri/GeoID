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
from dynastore.models.protocols.authorization import DefaultRole


def admin_policies() -> List[Policy]:
    """Pure declaration of the admin extension's policies.

    Two policies:

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
    ]


def admin_role_bindings(
    sysadmin_role_name: Optional[str] = None,
    admin_role_name: Optional[str] = None,
) -> List[Role]:
    """Pure declaration of the admin extension's role bindings.

    Only ``admin_access`` gets default bindings (back-compat).
    ``admin_catalog_access`` is intentionally unbound — operators wire
    catalog-admin authority through the same DB-resident flow they use
    for any custom role.
    """
    sysadmin_role_name = sysadmin_role_name or DefaultRole.SYSADMIN.value
    admin_role_name = admin_role_name or DefaultRole.ADMIN.value
    return [
        Role(name=sysadmin_role_name, policies=["admin_access"]),
        Role(name=admin_role_name, policies=["admin_access"]),
    ]
