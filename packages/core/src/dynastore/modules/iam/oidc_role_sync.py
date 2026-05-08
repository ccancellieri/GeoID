#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""Pure (no-I/O) reconciler for OIDC -> internal role grants.

Keep this module dependency-free aside from the config dataclass: it makes
the diff trivially unit-testable without DB or HTTP fixtures.
"""

from dataclasses import dataclass
from typing import Iterable, List, Literal, Mapping, Sequence

from .oidc_role_sync_config import OidcRoleSyncConfig


Action = Literal["grant", "revoke"]


@dataclass(frozen=True)
class RoleAction:
    role_name: str
    action: Action


def diff(
    *,
    oidc_roles: Iterable[str],
    current_internal_roles: Iterable[str],
    role_mapping: Mapping[str, str],
) -> List[RoleAction]:
    """Compute the grant/revoke list for one principal.

    The reconciler operates only on the *mapped* internal role names
    (``role_mapping.values()``). Internal roles outside this set are
    invisible here — the caller must filter ``current_internal_roles``
    to those values before calling, but as a safety net we also
    intersect inside.
    """
    mapped_internal = set(role_mapping.values())
    desired = {
        role_mapping[r] for r in oidc_roles if r in role_mapping
    }
    current = {r for r in current_internal_roles if r in mapped_internal}

    actions: List[RoleAction] = []
    for role in sorted(desired - current):
        actions.append(RoleAction(role_name=role, action="grant"))
    for role in sorted(current - desired):
        actions.append(RoleAction(role_name=role, action="revoke"))
    return actions


def is_issuer_allowed(
    issuer: str | None, whitelist: Sequence[str] | None
) -> bool:
    """Defense-in-depth: return False if a whitelist is set and ``issuer``
    is not in it. ``None`` whitelist means no gating.
    """
    if not whitelist:
        return True
    return issuer is not None and issuer in whitelist


def initial_role_overlay(
    *,
    oidc_roles: Iterable[str],
    base_roles: Sequence[str],
    config: OidcRoleSyncConfig,
    issuer: str | None,
) -> List[str]:
    """Compute the role list for first-time auto-registration.

    If sync is disabled, returns ``base_roles`` unchanged. Otherwise
    overlays the mapped roles drawn from the OIDC token (gated by the
    issuer whitelist when configured). The mapped roles replace any
    existing entries from the same set in ``base_roles`` so the principal
    lands with exactly what the token says for those roles.
    """
    if not config.enabled or not is_issuer_allowed(issuer, config.issuer_whitelist):
        return list(base_roles)

    mapped_internal = set(config.role_mapping.values())
    mapped_from_token = {
        config.role_mapping[r] for r in oidc_roles if r in config.role_mapping
    }
    kept = [r for r in base_roles if r not in mapped_internal]
    return kept + sorted(mapped_from_token)
