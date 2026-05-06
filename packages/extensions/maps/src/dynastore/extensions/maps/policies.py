#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Pure declarations of the Maps extension's authz policies.

Consumed by IAM via ``MapsService.get_policies`` / ``get_role_bindings``
through the ``PolicyContributor`` Protocol — see PR #308.

Implementation delegates to the shared OGC helpers in
``extensions/tools/ogc_policies.py`` since the Maps shape (anonymous
GET on ``/maps/...`` plus the ``map_viewer`` page) is the standard
OGC public-access pattern with one extra resource.
"""

from typing import List, Optional

from dynastore.extensions.tools.ogc_policies import (
    ogc_anonymous_role_binding,
    ogc_public_access_policy,
)
from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role


def maps_policies() -> List[Policy]:
    return [
        ogc_public_access_policy(
            "maps",
            extra_resources=["/web/pages/map_viewer"],
        ),
    ]


def maps_role_bindings(anonymous_role_name: Optional[str] = None) -> List[Role]:
    return [ogc_anonymous_role_binding("maps", anonymous_role_name)]
