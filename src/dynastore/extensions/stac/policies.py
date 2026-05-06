#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Pure declarations of the STAC extension's authz policies.

Consumed by IAM via ``STACService.get_policies`` / ``get_role_bindings``
through the ``PolicyContributor`` Protocol — see PR #308.
"""

from typing import List, Optional

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import DefaultRole


def stac_policies() -> List[Policy]:
    return [
        Policy(
            id="stac_public_access",
            description="Allows anonymous GET access to STAC API and browser.",
            actions=["GET", "OPTIONS"],
            resources=[
                "/stac",
                "/stac/",
                "/stac/.*",
                "/web/pages/stac_browser",
            ],
            effect="ALLOW",
        ),
    ]


def stac_role_bindings(anonymous_role_name: Optional[str] = None) -> List[Role]:
    return [
        Role(
            name=anonymous_role_name or DefaultRole.ANONYMOUS.value,
            policies=["stac_public_access"],
        ),
    ]
