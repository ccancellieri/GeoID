#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

import logging
from dynastore.models.protocols.authorization import DefaultRole
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

_REINDEX_ADMIN_ROLES = (DefaultRole.SYSADMIN.value, DefaultRole.ADMIN.value)


def register_search_policies():
    """Register admin-only policy for the reindex endpoints via PermissionProtocol."""
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; search policies not registered.")
        return

    reindex_policy = Policy(
        id="search_reindex_admin",
        description="Grants access to the bulk reindex trigger endpoints (admin only).",
        actions=["POST"],
        resources=[
            "/search/reindex",
            "/search/reindex/",
            "/search/reindex/.*",
        ],
        effect="ALLOW",
    )
    pm.register_policy(reindex_policy)

    for role_name in _REINDEX_ADMIN_ROLES:
        pm.register_role(Role(name=role_name, policies=["search_reindex_admin"]))

    logger.debug("Search reindex policies registered via PermissionProtocol.")
