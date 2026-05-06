import logging
from dynastore.models.protocols.authorization import DefaultRole
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def register_notebooks_policies():
    """Register notebooks public-access policy via PermissionProtocol."""
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; notebooks policies not registered.")
        return

    notebooks_policy = Policy(
        id="notebooks_public_access",
        description="Allows anonymous read access to platform notebooks and the notebooks web page.",
        actions=["GET", "OPTIONS"],
        resources=[
            "/notebooks/platform",
            "/notebooks/platform/.*",
            "/web/pages/notebooks",
        ],
        effect="ALLOW",
    )
    pm.register_policy(notebooks_policy)
    pm.register_role(Role(name=DefaultRole.ANONYMOUS.value, policies=["notebooks_public_access"]))

    logger.debug("Notebooks policies registered via PermissionProtocol.")
