#    Copyright 2025 FAO
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

"""
Runtime-configurable security settings.

Registered as a PluginConfig so it can be managed via the
PlatformConfigsProtocol (REST API at /configs/security).
Storage is fully pluggable — any PlatformConfigsProtocol implementation works.
"""

from typing import ClassVar, List, Optional, Tuple
from pydantic import Field
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.modules.db_config.platform_config_service import PluginConfig

class SecurityPluginConfig(ExposableConfigMixin, PluginConfig):
    """Platform-level security configuration — changeable at runtime."""
    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "security")


    # -- CORS --
    cors_allowed_origins: List[str] = Field(
        default_factory=lambda: ["*"],
        description=(
            "List of allowed CORS origins. "
            "Use ['*'] for unrestricted (dev only). "
            "When explicit origins are set, credentials are automatically allowed."
        ),
    )
    cors_allow_methods: List[str] = Field(
        default_factory=lambda: ["*"],
        description="HTTP methods permitted in CORS preflight.",
    )
    cors_allow_headers: List[str] = Field(
        default_factory=lambda: ["*"],
        description="HTTP headers permitted in CORS preflight.",
    )
    cors_max_age: int = Field(
        default=600,
        description="Seconds browsers may cache CORS preflight responses.",
    )

    # -- Rate limiting --
    login_max_attempts: int = Field(
        default=5,
        description="Max failed login attempts before lockout.",
    )
    login_lockout_seconds: int = Field(
        default=300,
        description="Lockout duration in seconds after max failed attempts.",
    )

    # -- JWT --
    jwt_access_ttl_seconds: int = Field(
        default=3600,
        description="Access token TTL in seconds.",
    )
    jwt_refresh_ttl_seconds: int = Field(
        default=604800,
        description="Refresh token TTL in seconds (default 7 days).",
    )
    jwt_rotate_refresh: bool = Field(
        default=True,
        description="Issue a new refresh token on each refresh (rotation).",
    )

    # -- Audit --
    audit_auth_events: bool = Field(
        default=True,
        description="Log authentication events to the audit table.",
    )
    audit_authz_decisions: bool = Field(
        default=False,
        description="Log every authorization decision (verbose).",
    )
