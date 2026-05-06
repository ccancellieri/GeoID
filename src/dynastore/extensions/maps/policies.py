#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Pure declarations of the Maps extension's authz policies.

Consumed by IAM via ``MapsService.get_policies`` / ``get_role_bindings``
through the ``PolicyContributor`` Protocol — see PR #308.
"""

from typing import List, Optional

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import DefaultRole


def maps_policies() -> List[Policy]:
    return [
        Policy(
            id="maps_public_access",
            description="Allows anonymous GET access to Maps tile and viewer endpoints.",
            actions=["GET", "OPTIONS"],
            resources=[
                "/maps",
                "/maps/",
                "/maps/.*",
                "/web/pages/map_viewer",
            ],
            effect="ALLOW",
        ),
    ]


def maps_role_bindings(anonymous_role_name: Optional[str] = None) -> List[Role]:
    return [
        Role(
            name=anonymous_role_name or DefaultRole.ANONYMOUS.value,
            policies=["maps_public_access"],
        ),
    ]
