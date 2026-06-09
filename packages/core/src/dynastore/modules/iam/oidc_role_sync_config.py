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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Runtime config for the Keycloak/OIDC -> internal role reconciler."""

from typing import ClassVar, Dict, List, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class OidcRoleSyncConfig(PluginConfig):
    """Reconcile selected OIDC roles into platform-scope internal grants.

    Keycloak is authoritative for the mapped roles only: any internal grant
    whose role_name is one of ``role_mapping.values()`` will be added or
    removed at auth time so it matches the OIDC token. Roles outside the
    mapping (catalog-scope roles, viewer, etc.) are never touched.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "iam", "oidc_role_sync")

    reconcile_enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "If False the OIDC role-sync reconciler is a no-op (default). "
            "Distinct from the (now-removed) ExposableConfigMixin.enabled "
            "exposure toggle; IAM routes are always-on."
        ),
    )
    # Default mapping carries only roles that ``default_roles_baseline``
    # actually provisions (sysadmin). ``geoid.editor`` was removed when the
    # editor/user catalog roles were dropped from the baseline; operators
    # can re-add it via PATCH after creating a matching role.
    role_mapping: Mutable[Dict[str, str]] = Field(
        default_factory=lambda: {
            "geoid.sysadmin": "sysadmin",
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
