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

"""``auth_bootstrap`` preset — cold-boot seed of :class:`IdpConfig` from ENV.

Idempotent absent-row seed: when NO ``IdpConfig`` row exists in the
platform-configs table, this preset writes one built from ``IDP_*``
environment variables.  When any row already exists the preset returns
immediately so operator or seed-file edits are never overwritten — even a
deliberately ``type='saml2'`` or partially-configured row. The seed heals
only the absent-row case (the dev / Cloud Run failure documented in #2210).

Field resolution order: ``params.<field>`` (explicit) → ``os.getenv("IDP_<FIELD>")``.
IDP_* only.

When no issuer is resolved, the preset logs a WARNING and returns without
writing (fail-closed per #2024 — no anonymous IdP registration).

The encryption-failure retry: persisting ``client_secret`` requires a Fernet
key.  When the first ``set_config`` raises, the preset retries WITHOUT
``client_secret`` so the OIDC provider still registers and bearer-token
validation continues to work.  Confidential flows stay degraded until the
operator provisions a key and re-seeds ``client_secret`` via the Configs API.
"""
from __future__ import annotations

import logging
import os
from typing import Any, ClassVar, Dict, Literal, Optional, Tuple, Type, cast

from pydantic import BaseModel, Field

from dynastore.modules.storage.presets.preset import AppliedDescriptor
from dynastore.modules.storage.presets.protocol import PresetTier

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Params model
# ---------------------------------------------------------------------------


class AuthBootstrapParams(BaseModel):
    """Optional ENV-override parameters for the ``auth_bootstrap`` preset.

    Each field defaults to ``None``, which causes the preset to fall back to
    the corresponding ``IDP_*`` environment variable.  Providing a non-None
    value here takes precedence over the environment.
    """

    type: Optional[str] = Field(
        default=None,
        description="IdP backend type (default 'oidc'). Overrides IDP_TYPE if set.",
    )
    issuer_url: Optional[str] = Field(
        default=None,
        description="OIDC issuer URL. Overrides IDP_ISSUER_URL if set.",
    )
    client_id: Optional[str] = Field(
        default=None,
        description="OIDC client_id. Overrides IDP_CLIENT_ID if set.",
    )
    client_secret: Optional[str] = Field(
        default=None,
        description="Raw OIDC client secret. Overrides IDP_CLIENT_SECRET if set.",
    )
    audience: Optional[str] = Field(
        default=None,
        description="Expected token audience. Overrides IDP_AUDIENCE if set.",
    )
    public_url: Optional[str] = Field(
        default=None,
        description="Externally reachable issuer URL. Overrides IDP_PUBLIC_URL if set.",
    )
    roles_claim_path: Optional[str] = Field(
        default=None,
        description="Dotted path to the roles claim. Overrides IDP_ROLES_CLAIM_PATH if set.",
    )


# ---------------------------------------------------------------------------
# Preset
# ---------------------------------------------------------------------------


class AuthBootstrapPreset:
    """Idempotent cold-boot seed of :class:`IdpConfig` from ``IDP_*`` ENV.

    ``apply`` writes an :class:`IdpConfig` row when none exists.  It is safe
    to call on every cold-boot: the existing-row guard returns immediately
    without touching a row that the operator or a seed file already wrote.
    """

    name: ClassVar[str] = "auth_bootstrap"
    description: ClassVar[str] = (
        "Cold-boot seed of IdpConfig from IDP_* ENV variables. "
        "Runs only when no IdpConfig row already exists in the platform-configs table."
    )
    keywords: ClassVar[Tuple[str, ...]] = ("iam", "platform", "foundational", "idp", "bootstrap")
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = AuthBootstrapParams

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: Any,
    ) -> Any:
        from dynastore.modules.storage.presets.preset import PresetPlan, PresetPlanEntry

        p: AuthBootstrapParams = params  # type: ignore[assignment]
        issuer = p.issuer_url or os.getenv("IDP_ISSUER_URL")
        entries = []
        if issuer:
            entries.append(
                PresetPlanEntry(
                    kind="set_config",
                    target="IdpConfig",
                    detail={"issuer_url": issuer, "note": "absent-row only"},
                )
            )
        else:
            entries.append(
                PresetPlanEntry(
                    kind="noop",
                    target="IdpConfig",
                    detail={"reason": "IDP_ISSUER_URL not set — seed skipped"},
                )
            )
        return PresetPlan(
            preset_name=self.name,
            scope_key=scope,
            entries=tuple(entries),
        )

    async def apply(
        self,
        params: BaseModel,
        scope: str,
        ctx: Any,
    ) -> AppliedDescriptor:
        """Seed :class:`IdpConfig` from params / ``IDP_*`` ENV when no row exists."""
        from dynastore.modules.iam.idp_config import IdpConfig
        from dynastore.tools.secrets import Secret

        p: AuthBootstrapParams = params  # type: ignore[assignment]

        # Resolve PlatformConfigsProtocol DIRECTLY — not ctx.config. ctx.config
        # is the ConfigsProtocol facade whose list_configs() returns a paginated
        # {"total", "results"} shape; the existing-row guard below needs the
        # class-keyed dict that PlatformConfigsProtocol.list_configs() returns
        # (persisted.get(IdpConfig)). Using ctx.config silently breaks the guard
        # and re-seeds over an operator-configured row on every cold boot (#2210
        # semantics: seed only when the row is ABSENT, never overwrite).
        from dynastore.models.protocols.platform_configs import (
            PlatformConfigsProtocol,
        )
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(PlatformConfigsProtocol)
        if configs is None:
            logger.debug(
                "auth_bootstrap: PlatformConfigsProtocol not available — skipping."
            )
            return AppliedDescriptor(payload={"skipped": "no_configs_protocol"})

        # Resolve the engine from ctx for the ensure-and-retry path.
        engine = getattr(ctx, "db", None)
        if engine is None:
            engine = getattr(ctx, "engine", None)

        # Check whether a row already exists — never overwrite.
        try:
            persisted: Dict[Any, Any] = await configs.list_configs()
        except Exception:
            try:
                from dynastore.modules.db_config.platform_config_service import PlatformConfigService
                await PlatformConfigService.initialize_storage(engine)  # type: ignore[arg-type]
                persisted = await configs.list_configs()
            except Exception:
                logger.warning(
                    "auth_bootstrap: platform configs storage unavailable after "
                    "ensure-and-retry — skipping IdpConfig seed.",
                    exc_info=True,
                )
                return AppliedDescriptor(payload={"skipped": "storage_unavailable"})

        if persisted.get(IdpConfig) is not None:
            # Existing row wins — operator or seed-file data is preserved.
            logger.debug(
                "auth_bootstrap: IdpConfig row already exists — skipping seed."
            )
            return AppliedDescriptor(payload={"skipped": "row_exists"})

        # Resolve each field: explicit param → IDP_* ENV → default.
        issuer = p.issuer_url or os.getenv("IDP_ISSUER_URL")
        if not issuer:
            logger.warning(
                "auth_bootstrap: no configured idp_config row and IDP_ISSUER_URL "
                "is not set in the environment. No identity provider will be "
                "registered — token-authenticated requests will receive 401. "
                "To fix: seed 'idp_config' via the Configs API "
                "(platform_configs class_key 'idp_config') or set IDP_ISSUER_URL "
                "in the environment before starting."
            )
            return AppliedDescriptor(payload={"skipped": "no_issuer"})

        raw_secret = p.client_secret or os.getenv("IDP_CLIENT_SECRET")
        client_secret = Secret(raw_secret) if raw_secret else None
        client_id = p.client_id or os.getenv("IDP_CLIENT_ID") or "dynastore-api"

        raw_type = p.type or "oidc"
        seed = IdpConfig(
            type=cast(Literal["oidc", "saml2"], raw_type),
            issuer_url=issuer,
            client_id=client_id,
            client_secret=client_secret,
            audience=p.audience or os.getenv("IDP_AUDIENCE"),
            public_url=p.public_url or os.getenv("IDP_PUBLIC_URL"),
            roles_claim_path=(
                p.roles_claim_path
                or os.getenv("IDP_ROLES_CLAIM_PATH")
                or "realm_access.roles"
            ),
        )

        try:
            await configs.set_config(IdpConfig, seed)
            logger.info(
                "auth_bootstrap: seeded IdpConfig from environment "
                "(issuer_url=%s, client_id=%s). "
                "This is a one-shot cold-boot bridge — edit via the Configs API "
                "(platform_configs class_key 'idp_config') to change without restart.",
                seed.issuer_url,
                seed.client_id,
            )
            return AppliedDescriptor(
                payload={"seeded": True, "issuer_url": seed.issuer_url, "client_id": seed.client_id}
            )
        except Exception:
            # The dominant failure here is secret encryption being unavailable.
            # Retry once WITHOUT client_secret so the OIDC provider still registers.
            if seed.client_secret is None:
                logger.warning(
                    "auth_bootstrap: IdpConfig seed failed.",
                    exc_info=True,
                )
                return AppliedDescriptor(payload={"skipped": "seed_failed"})

            logger.warning(
                "auth_bootstrap: IdpConfig seed could not persist client_secret "
                "(secret encryption unavailable — no DYNASTORE_SECRET_KEY / "
                "JWT_SECRET / SESSION_SECRET_KEY). Retrying WITHOUT client_secret "
                "so the OIDC provider still registers and bearer-token validation "
                "works. Set a key source and re-seed client_secret via the Configs "
                "API to restore confidential flows.",
                exc_info=True,
            )

        public_seed = seed.model_copy(update={"client_secret": None})
        try:
            await configs.set_config(IdpConfig, public_seed)
            logger.info(
                "auth_bootstrap: seeded IdpConfig from environment WITHOUT "
                "client_secret (issuer_url=%s, client_id=%s).",
                public_seed.issuer_url,
                public_seed.client_id,
            )
            return AppliedDescriptor(
                payload={
                    "seeded": True,
                    "issuer_url": public_seed.issuer_url,
                    "client_id": public_seed.client_id,
                    "without_secret": True,
                }
            )
        except Exception:
            logger.warning(
                "auth_bootstrap: IdpConfig seed failed even without client_secret.",
                exc_info=True,
            )
            return AppliedDescriptor(payload={"skipped": "seed_failed_no_secret"})

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: Any,
    ) -> None:
        """Revoke is a no-op for the auth bootstrap preset.

        The IdpConfig row is operator data once seeded; removing it
        programmatically could lock operators out of their own cluster.
        Operators should edit or delete via the Configs API directly.
        """
        logger.info(
            "auth_bootstrap revoke: IdpConfig is not removed automatically. "
            "Edit or delete via the Configs API if needed."
        )
