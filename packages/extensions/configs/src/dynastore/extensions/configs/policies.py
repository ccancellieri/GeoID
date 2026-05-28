#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

import logging
from dynastore.models.protocols.policies import Policy
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def register_configs_policies():
    """Register configs-access policy (sysadmin only) via PermissionProtocol."""
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; configs policies not registered.")
        return

    configs_policy = Policy(
        id="configs_access",
        description="Grants full access to configuration management endpoints.",
        actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        resources=[
            "/configs",
            "/configs/",
            "/configs/.*",
            "/web/pages/configs_editor",  # expose_web_page route
        ],
        effect="ALLOW",
    )
    pm.register_policy(configs_policy)

    logger.debug("Configs policies registered via PermissionProtocol.")
