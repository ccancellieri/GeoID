#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Pure declarations of the OGC Records extension's authz policies.

Consumed by IAM via ``RecordsService.get_policies`` /
``get_role_bindings`` through the ``PolicyContributor`` Protocol —
see PR #308.
"""

from typing import List, Optional

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import DefaultRole


def records_policies() -> List[Policy]:
    return [
        Policy(
            id="records_public_access",
            description="Allows anonymous access to OGC API Records endpoints.",
            actions=["GET", "OPTIONS"],
            resources=[
                "/records.*",
                "/records/.*",
            ],
            effect="ALLOW",
        ),
    ]


def records_role_bindings(anonymous_role_name: Optional[str] = None) -> List[Role]:
    return [
        Role(
            name=anonymous_role_name or DefaultRole.ANONYMOUS.value,
            description="Anonymous user with limited access.",
            policies=["records_public_access"],
        ),
    ]
