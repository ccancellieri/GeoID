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

"""Runtime config for the Keycloak/OIDC -> internal role reconciler."""

from typing import ClassVar, Dict, List, Optional, Tuple

from pydantic import Field

from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.modules.db_config.platform_config_service import Mutable, PluginConfig


class OidcRoleSyncConfig(ExposableConfigMixin, PluginConfig):
    """Reconcile selected OIDC roles into platform-scope internal grants.

    Keycloak is authoritative for the mapped roles only: any internal grant
    whose role_name is one of ``role_mapping.values()`` will be added or
    removed at auth time so it matches the OIDC token. Roles outside the
    mapping (catalog-scope roles, viewer, etc.) are never touched.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "iam", "oidc_role_sync")

    enabled: Mutable[bool] = Field(
        default=False,
        description="If False the reconciler is a no-op (default).",
    )
    # Default values track IamRolesConfig.{sysadmin,editor}_role_name; a
    # unit test (issue #659) pins them so any future rename of the
    # platform role defaults fails CI here instead of silently breaking
    # the OIDC reconciler. Operators that rename roles at runtime should
    # PATCH this config alongside the IamRolesConfig change.
    role_mapping: Mutable[Dict[str, str]] = Field(
        default_factory=lambda: {
            "geoid.sysadmin": "sysadmin",
            "geoid.editor": "editor",
        },
        description=(
            "Map OIDC role names (as they appear in the normalized "
            "identity['roles'] field) to internal role names."
        ),
    )
    ttl_seconds: Mutable[int] = Field(
        default=60,
        description=(
            "Per-principal cache TTL: skip reconciliation if the same "
            "principal was synced more recently than this."
        ),
    )
    issuer_whitelist: Mutable[Optional[List[str]]] = Field(
        default=None,
        description=(
            "If set, only tokens whose 'iss' claim is in this list are "
            "allowed to grant the mapped roles. None disables the gate."
        ),
    )
