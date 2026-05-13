#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Pure declarations of the events extension's authorization policies.

Read by ``EventsExtension.get_policies`` / ``get_role_bindings`` and
forwarded to ``PermissionProtocol`` by IAM's ``PolicyContributor``
consumer. ``IamMiddleware`` evaluates them at request time. This file
never calls ``register_policy`` / ``check_permission`` directly — see
``feedback_iam_isolation`` and the project CLAUDE.md "Authorization
(load-bearing)" section.

Two policies cover the surface:

- ``events_platform_access`` — ``/events/system`` and the platform-tier
  ``/events/subscriptions`` family. Sysadmin only.

- ``events_catalog_access`` — every per-catalog event + subscription
  path under ``/events/catalogs/{cat}/...``. Gated by the existing
  ``catalog_membership_required`` condition handler (which has its own
  sysadmin bypass; we pass an empty config and let the handler resolve
  ``sysadmin_role`` from ``IamRoleConfig`` internally).
"""

from typing import List, Optional

from dynastore.models.auth import Condition, Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig


def events_policies() -> List[Policy]:
    return [
        Policy(
            id="events_platform_access",
            description=(
                "Sysadmin-only access to platform-tier event surfaces "
                "(/events/system, /events/subscriptions). Webhook-subscription "
                "configs reveal integration endpoints — restrict accordingly."
            ),
            actions=["GET", "POST", "DELETE", "OPTIONS"],
            resources=[
                r"^/events/system$",
                r"^/events/subscriptions(/.*)?$",
            ],
            effect="ALLOW",
        ),
        Policy(
            id="events_catalog_access",
            description=(
                "Catalog-member access to a catalog's events and webhook "
                "subscriptions. Sysadmin bypass via condition handler."
            ),
            actions=["GET", "POST", "DELETE", "OPTIONS"],
            resources=[r"^/events/catalogs/[^/]+(/.*)?$"],
            effect="ALLOW",
            conditions=[Condition(type="catalog_membership_required", config={})],
        ),
    ]


def events_role_bindings(
    sysadmin_role_name: Optional[str] = None,
    admin_role_name: Optional[str] = None,
) -> List[Role]:
    """Default role bindings: sysadmin gets both policies; admin gets per-catalog only."""
    cfg = IamRolesConfig()
    return [
        Role(
            name=sysadmin_role_name or cfg.sysadmin_role_name,
            policies=["events_platform_access", "events_catalog_access"],
        ),
        Role(
            name=admin_role_name or cfg.admin_role_name,
            policies=["events_catalog_access"],
        ),
    ]
