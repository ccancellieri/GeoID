#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License").

"""``PolicyContributor`` — plugins declare their authz needs to IAM.

Architectural rule (per project feedback): IAM/auth concepts stay
isolated to the IAM module + middleware. Plugins must NOT call
``PermissionProtocol.register_policy`` or ``register_role`` directly,
must NOT import ``modules/iam.*``, and must NOT make authz decisions
inside route handlers. Authorization is enforced exclusively by
``IamMiddleware``; plugins only **declare** what they need.

Each plugin that ships a route surface gated by a Policy implements
``PolicyContributor`` (structural — ``@runtime_checkable``, no base
class required). At plugin-discovery time the IAM extension iterates
``get_protocols(PolicyContributor)`` and forwards the declared
policies/role-bindings to ``PermissionProtocol``. The plugin stays
agnostic of how (or whether) the platform enforces them.

The two methods return iterables of *model classes* (``Policy`` /
``Role``) defined under ``models/`` — those are pure data, not IAM
implementation. Plugins can import them safely without violating the
isolation rule.
"""

from __future__ import annotations

from typing import Iterable, Protocol, runtime_checkable

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role


@runtime_checkable
class PolicyContributor(Protocol):
    """Plugin contract for declarative authz wiring.

    Implement on any extension/module that wants its routes guarded by
    PermissionProtocol policies. The IAM module discovers contributors
    at lifespan and registers their declarations centrally.

    Both methods may return empty iterables — IAM treats that as "no
    policies to register" and skips silently. Re-running the IAM
    lifespan re-registers idempotently (PermissionProtocol upserts).
    """

    def get_policies(self) -> Iterable[Policy]:
        """Return the policies this plugin needs the platform to enforce.

        Each ``Policy`` is a pure declaration: id, resource regex,
        actions, effect, optional Conditions. The plugin owns the
        contents — IAM owns the enforcement.
        """
        ...

    def get_role_bindings(self) -> Iterable[Role]:
        """Return role-to-policy bindings this plugin contributes.

        Empty list is a valid answer: a plugin can declare policies
        without binding them to any role, leaving role binding entirely
        to operator REST calls. Sysadmin/anonymous bindings often do
        belong here for back-compat with seeded ``DefaultRole`` rows.
        """
        ...
