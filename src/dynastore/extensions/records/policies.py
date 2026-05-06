#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Pure declarations of the OGC Records extension's authz policies.

Consumed by IAM via ``RecordsService.get_policies`` /
``get_role_bindings`` through the ``PolicyContributor`` Protocol —
see PR #308.

Implementation delegates to the shared OGC helpers in
``extensions/tools/ogc_policies.py`` since the Records shape is
identical to the standard OGC public-access pattern.
"""

from typing import List, Optional

from dynastore.extensions.tools.ogc_policies import (
    ogc_anonymous_role_binding,
    ogc_public_access_policy,
)
from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role


def records_policies() -> List[Policy]:
    return [ogc_public_access_policy("records")]


def records_role_bindings(anonymous_role_name: Optional[str] = None) -> List[Role]:
    return [ogc_anonymous_role_binding("records", anonymous_role_name)]
