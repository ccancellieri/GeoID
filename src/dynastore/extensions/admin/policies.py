#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

import logging
from dynastore.models.auth import Condition
from dynastore.models.protocols.authorization import DefaultRole
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

_ADMIN_ROLES = (DefaultRole.SYSADMIN.value, DefaultRole.ADMIN.value)


def register_admin_policies():
    """Register admin-access policy (sysadmin/admin only) via PermissionProtocol."""
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; admin policies not registered.")
        return

    admin_policy = Policy(
        id="admin_access",
        description="Grants full access to admin management endpoints.",
        actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        resources=[
            "/admin",
            "/admin/",
            "/admin/.*",
            "/web/pages/admin_panel",  # expose_web_page route
        ],
        effect="ALLOW",
    )
    pm.register_policy(admin_policy)

    for role_name in _ADMIN_ROLES:
        pm.register_role(Role(name=role_name, policies=["admin_access"]))

    # Catalog-scoped admin: permits the holder of catalog-admin role to
    # mutate role bindings within their catalog (/admin/catalogs/{cat}/...).
    # Sysadmin/platform-admin already covered by admin_access above; the
    # CatalogAdminHandler bypasses both via allow_sysadmin/allow_platform.
    # Effective for catalog-only-admin principals once role grants are
    # surfaced into request.state.principal_role (separate change). Until
    # then this policy is harmless — the conditional check just narrows
    # an already-granted ALLOW, never widens.
    admin_catalog_policy = Policy(
        id="admin_catalog_access",
        description=(
            "Per-catalog admin access; requires catalog-admin role. "
            "Sysadmin and platform-grant principals bypass."
        ),
        actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        resources=[r"^/admin/catalogs/[^/]+(/.*)?$"],
        effect="ALLOW",
        conditions=[Condition(type="catalog_admin_required", config={})],
    )
    pm.register_policy(admin_catalog_policy)
    pm.register_role(
        Role(name=DefaultRole.ADMIN.value, policies=["admin_catalog_access"])
    )

    logger.debug("Admin policies registered via PermissionProtocol.")
