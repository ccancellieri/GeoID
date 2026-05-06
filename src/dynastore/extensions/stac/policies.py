#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Pure declarations of the STAC extension's authz policies.

Consumed by IAM via ``STACService.get_policies`` / ``get_role_bindings``
through the ``PolicyContributor`` Protocol — see PR #308.

Implementation delegates to the shared OGC helpers in
``extensions/tools/ogc_policies.py`` since the STAC shape (anonymous
GET on ``/stac/...`` plus the ``stac_browser`` page) is the standard
OGC public-access pattern with one extra resource.
"""

from typing import List, Optional

from dynastore.extensions.tools.ogc_policies import (
    ogc_anonymous_role_binding,
    ogc_public_access_policy,
)
from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role


def stac_policies() -> List[Policy]:
    return [
        ogc_public_access_policy(
            "stac",
            extra_resources=["/web/pages/stac_browser"],
        ),
    ]


def stac_role_bindings(anonymous_role_name: Optional[str] = None) -> List[Role]:
    return [ogc_anonymous_role_binding("stac", anonymous_role_name)]
