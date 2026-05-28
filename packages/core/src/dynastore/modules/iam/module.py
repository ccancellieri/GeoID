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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

# File: dynastore/modules/iam/module.py

import asyncio
import logging
from uuid import UUID
from contextlib import AsyncExitStack, asynccontextmanager

# Distribution-presence SCOPE gate (#1003).
#
# `tools/discovery.py:165 discover_and_load_plugins` skips entry-points whose
# top-level imports raise ImportError, so raising ImportError here when the
# `dynastore-ext-iam` distribution is absent ensures IamModule is *not*
# registered on services whose SCOPE excludes the `iam` extras.
#
# History: this gate previously used a package-import side-effect (first
# `from tenacity import ...`, then `import jwt`).  Both packages were silently
# pulled in transitively by other extras (tenacity via pyiceberg in
# drivers_grp; PyJWT via gcloud-aio-auth in module_gcp).  A package-import
# gate is fragile by construction — any new transitive dep can break it.
#
# Checking the *distribution name* `dynastore-ext-iam` instead is robust:
# distribution identities are unique and a transitive dep cannot
# accidentally install a distribution with that name.  The `iam` aggregate
# extras (`iam = ["dynastore[module_iam,extension_iam]"]`) always pull both
# `module_iam` and `extension_iam` together — checking for the extension
# distribution is therefore a valid signal for whether the module should
# be active too.
import importlib.metadata as _importlib_metadata

try:
    _importlib_metadata.distribution("dynastore-ext-iam")
except _importlib_metadata.PackageNotFoundError as _exc:
    raise ImportError(
        "Skipping IamModule: dynastore-ext-iam distribution not installed "
        "(SCOPE excludes the iam extras)"
    ) from _exc

from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.models.auth import (
    AuthenticationProtocol,
    AuthorizationProtocol,
    Principal,
)
from .iam_service import IamService
from .iam_storage import AbstractIamStorage
from .policies import PolicyService
from .authorization.iam_authorizer import IamAuthorizer
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config.query_executor import DbResource
from typing import Optional, Any, AsyncGenerator, List, Tuple, cast
from dynastore.tools.discovery import register_plugin, unregister_plugin

logger = logging.getLogger(__name__)

# Module-level DQLQuery objects used by _bootstrap_public_access_baseline so
# that tests can patch them without fighting inline construction.
from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler  # noqa: E402

_BOOTSTRAP_SELECT_SENTINEL = DQLQuery(
    """
    SELECT 1 FROM iam.applied_presets
    WHERE preset_name = 'public_access_baseline'
      AND scope_key   = 'platform'
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

_BOOTSTRAP_INSERT_SENTINEL = DQLQuery(
    """
    INSERT INTO iam.applied_presets
        (preset_name, scope_key, state, applied_at, applied_by,
         params_snapshot, revoke_descriptor, updated_at)
    VALUES
        ('public_access_baseline', 'platform', 'applied', NOW(), NULL,
         :params_snapshot, :revoke_descriptor, NOW())
    ON CONFLICT (preset_name, scope_key) DO NOTHING
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)


from dynastore.models.protocols.policies import PermissionProtocol
class IamModule(ModuleProtocol, AuthenticationProtocol, AuthorizationProtocol, PermissionProtocol):
    priority: int = 100
    _iam_manager: Optional[IamService] = None
    _policy_service: Optional[PolicyService] = None
    _authorizer: Optional[IamAuthorizer] = None
    storage: Optional[AbstractIamStorage] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._role_lock = asyncio.Lock()
        # Register as early as possible to be discoverable during extension configuration
        register_plugin(self)

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        from .postgres_iam_storage import PostgresIamStorage
        from .postgres_policy_storage import PostgresPolicyStorage
        from dynastore.modules.db_config.query_executor import managed_transaction
        from dynastore.models.protocols import DatabaseProtocol

        self.storage = PostgresIamStorage(app_state)
        policy_storage = PostgresPolicyStorage(app_state)
        # Single IamRolesConfig instance shared between policy seeding and
        # runtime authentication. Lifespan-time value comes from app_state
        # or the PluginConfig defaults; the IamService hot path re-resolves
        # via ``_get_roles_config`` so a runtime PATCH takes effect on the
        # next request without restart.
        from dynastore.models.protocols.authorization import IamRolesConfig
        role_config = getattr(app_state, "iam_roles_config", None) or IamRolesConfig()
        self._policy_service = PolicyService(
            app_state,
            storage=policy_storage,
            iam_storage=self.storage,
            role_config=role_config,
        )
        self._iam_manager = IamService(
            self.storage,
            self._policy_service,
            app_state=app_state,
            role_config=role_config,
        )
        # Register early to avoid race conditions in middleware discovery
        register_plugin(self._iam_manager)

        async with AsyncExitStack() as stack:
            # Manage manager lifespan
            await stack.enter_async_context(self._iam_manager.lifespan(app_state))

            import os

            # Global initialization
            try:
                from .applied_presets_service import AppliedPresetsService

                db = get_protocol(DatabaseProtocol)
                engine = db.engine if db else None
                async with managed_transaction(engine) as conn:
                    await self.storage._initialize_schema(conn, schema="iam")
                    await policy_storage._initialize_schema(conn, schema="iam")
                    # Preset audit table — idempotent CREATE IF NOT EXISTS.
                    applied_svc = AppliedPresetsService(engine)
                    await applied_svc.ensure_table(conn=conn)

                    # Backfill: insert a synthetic iam_baseline audit row so
                    # that DELETE /admin/presets/iam_baseline works on upgraded
                    # deployments that had the contributor-loop seeding run.
                    # Idempotent — no-op when the row already exists.
                    try:
                        from dynastore.modules.iam.migrations.backfill_iam_baseline_audit import run_backfill
                        await run_backfill(
                            engine=engine,
                            policy_storage=policy_storage,
                            iam_storage=self.storage,
                        )
                    except Exception as _bf_exc:
                        logger.warning(
                            "IamModule: iam_baseline backfill failed (non-fatal): %s",
                            _bf_exc,
                        )

                    # Backfill: insert a synthetic default_roles_baseline audit row
                    # so that DELETE /admin/presets/default_roles_baseline works on
                    # upgraded deployments. Idempotent — no-op when row already exists.
                    try:
                        from dynastore.modules.iam.migrations.backfill_default_roles_baseline_audit import run_backfill as _run_roles_bf
                        await _run_roles_bf(engine=engine)
                    except Exception as _roles_bf_exc:
                        logger.warning(
                            "IamModule: default_roles_baseline backfill failed (non-fatal): %s",
                            _roles_bf_exc,
                        )

                    # One-shot migration: backfill the allowed_preset_names
                    # safe-subset on the catalog_preset_delegation policy
                    # (#1426). Pre-fix iam_baseline shipped without the
                    # allowlist, leaving catalog admins able to POST/DELETE
                    # any registered preset at their scope.
                    try:
                        from dynastore.modules.iam.migrations.tighten_catalog_preset_allowlist import run_migration as _run_tighten
                        await _run_tighten(engine=engine)
                    except Exception as _tighten_exc:
                        logger.warning(
                            "IamModule: catalog_preset_delegation allowlist "
                            "migration failed (non-fatal): %s",
                            _tighten_exc,
                        )

                    # One-shot migration: add attribute_predicates JSONB
                    # column to every grants table (#1441).  Existing rows
                    # default to '[]' — full backwards compatibility with
                    # the pure-RBAC path.
                    try:
                        from dynastore.modules.iam.migrations.add_grants_attribute_predicates import run_migration as _run_attr_preds
                        await _run_attr_preds(engine=engine)
                    except Exception as _attr_exc:
                        logger.warning(
                            "IamModule: add_grants_attribute_predicates "
                            "migration failed (non-fatal): %s",
                            _attr_exc,
                        )

                    # Self-heal guard: if the platform-tier sysadmin role is
                    # absent (e.g. a DB reset happened before this restart),
                    # seed it directly so the service is never left in a
                    # locked-out state. Per geoid#643 the platform tier
                    # collapses to {sysadmin} — catalog-tier roles are seeded
                    # by the per-catalog lifecycle hook, not here.
                    sysadmin_name = role_config.sysadmin_role_name
                    sysadmin = await self.storage.get_role(
                        sysadmin_name, schema="iam", conn=conn
                    )
                    if sysadmin is None:
                        logger.critical(
                            "IamModule: %r role missing — "
                            "DB may have been reset. Seeding sysadmin survival row.",
                            sysadmin_name,
                        )
                        from dynastore.models.auth_models import Role as _Role
                        _seed = _Role(name=sysadmin_name, policies=["sysadmin_full_access"])
                        await self.storage.create_role(_seed, schema="iam", conn=conn)

            except Exception as e:
                logger.error(f"Failed to initialize IamModule: {e}", exc_info=True)

            # Seed OidcRoleSyncConfig — idempotent one-shot; skipped when a
            # row already exists so operator PATCHes are preserved.
            try:
                db = get_protocol(DatabaseProtocol)
                engine = db.engine if db else None
                await _seed_oidc_role_sync_config(engine)
            except Exception as e:
                logger.warning("IamModule: OidcRoleSyncConfig seed failed (non-fatal): %s", e)

            # Warn operators when JWT-claim attribute enrichment is active
            # (claim_map non-empty) but no issuer_allowlist is configured.
            # In multi-provider deployments any verified issuer's claim values
            # would be merged into Principal.attributes, potentially widening
            # or restricting ABAC access silently.
            await _warn_jwt_attr_no_issuer_allowlist()

            # Cold-boot fallback for headless infrastructure (#1412 follow-up).
            # Issue #1412 moved all extension policies into operator-applied presets but
            # headless infra probes (Cloud Run /health startup probe) run BEFORE any
            # operator can log in. Apply `public_access_baseline` ONCE per DB to land the
            # minimal anon-allow Policy. Sentinel: iam.applied_presets row.
            # On restart, the row is present and this block no-ops. Sysadmin DELETE of
            # the preset removes both the Policy and the audit row, re-enabling re-apply.
            try:
                db = get_protocol(DatabaseProtocol)
                engine = db.engine if db else None
                await self._bootstrap_public_access_baseline(engine)
            except Exception as exc:
                logger.error(
                    "public_access_baseline bootstrap failed; /health may 403 until "
                    "operator applies it manually via POST /admin/presets/public_access_baseline/apply: %s",
                    exc,
                )

            # Register plugins
            register_plugin(self._iam_manager)

            # Register the AuthorizerProtocol implementor. When IAM is loaded,
            # `require_permission` routes through IamAuthorizer; otherwise
            # DefaultAuthorizer enforces fail-closed role-only checks.
            self._authorizer = IamAuthorizer()
            register_plugin(self._authorizer)

            # Usage-counter drivers for rate-limit / quota conditions.
            # Always register PG (the durable single-source-of-truth).
            # Layer on Valkey when a CountingCacheBackend is up so
            # cross-pod increments stay atomic without a parallel client.
            await self._register_usage_counter_drivers(stack)

            # Compiled-rule cache TTL / rule-version refresher (#1343).
            # Pulls the live ``IamScaleConfig`` TTL + maxsize snapshot and
            # the platform binding-version counter on a slow timer so the
            # sync hot path consumes them without an async hop. Registered
            # on the exit stack so it stops cleanly on module unload.
            try:
                from dynastore.modules.iam.compiled_rule_cache import (
                    start_background_refresh,
                )
                _stop_refresh = await start_background_refresh()

                @asynccontextmanager
                async def _refresh_ctx():
                    try:
                        yield
                    finally:
                        await _stop_refresh()

                await stack.enter_async_context(_refresh_ctx())
            except Exception:
                logger.warning(
                    "IamModule: compiled-rule cache refresher failed to start; "
                    "TTL/version snapshots fall back to in-source defaults",
                    exc_info=True,
                )

            # IdP factory — IDP_TYPE selects the backend (default: oidc)
            # KEYCLOAK_* vars are read as fallbacks for backward compatibility.
            # See modules/iam/identity_providers/README.md for adding new types.
            idp_type = os.environ.get("IDP_TYPE", "oidc").lower()
            idp_issuer = (
                os.environ.get("IDP_ISSUER_URL")
                or os.environ.get("KEYCLOAK_ISSUER_URL")
            )
            if idp_type == "oidc" and idp_issuer:
                from .identity_providers.oidc_identity import OidcIdentityProvider
                register_plugin(
                    OidcIdentityProvider(
                        issuer_url=idp_issuer,
                        client_id=(
                            os.environ.get("IDP_CLIENT_ID")
                            or os.environ.get("KEYCLOAK_CLIENT_ID", "dynastore-api")
                        ),
                        client_secret=(
                            os.environ.get("IDP_CLIENT_SECRET")
                            or os.environ.get("KEYCLOAK_CLIENT_SECRET")
                        ),
                        audience=(
                            os.environ.get("IDP_AUDIENCE")
                            or os.environ.get("KEYCLOAK_AUDIENCE")
                        ),
                        public_url=(
                            os.environ.get("IDP_PUBLIC_URL")
                            or os.environ.get("KEYCLOAK_PUBLIC_URL")
                        ),
                        roles_claim_path=os.environ.get("IDP_ROLES_CLAIM_PATH"),
                    )
                )
                logger.info("Registered OIDC identity provider: %s", idp_issuer)
            elif idp_type == "saml2":
                logger.warning(
                    "IDP_TYPE=saml2 is not yet implemented; no IdP registered. "
                    "See modules/iam/identity_providers/README.md."
                )
            elif idp_issuer:
                logger.warning(
                    "Unknown IDP_TYPE=%r with IDP_ISSUER_URL set; no IdP registered.",
                    idp_type,
                )

            yield

            # Unregister plugins on exit
            unregister_plugin(self._iam_manager)
            if self._authorizer is not None:
                unregister_plugin(self._authorizer)
                self._authorizer = None
            # IamService stops via AsyncExitStack
        
        # Finally unregister self
        unregister_plugin(self)

    async def _bootstrap_public_access_baseline(self, engine: Any) -> None:
        """Apply ``public_access_baseline`` once per DB on cold-boot.

        Uses iam.applied_presets as a sentinel so subsequent restarts no-op.
        Sysadmin DELETE of the preset removes the sentinel row and re-arms
        this block on the next restart.
        """
        import json

        from dynastore.modules.db_config.locking_tools import acquire_startup_lock
        from dynastore.modules.storage.presets.preset import NoParams, PresetContext
        from dynastore.modules.storage.presets.registry import find_preset
        from dynastore.modules.iam.iam_service import IamService

        async with acquire_startup_lock(engine, "iam_seed:public_access_baseline") as conn:
            if conn is None:
                # Another worker holds the lock and is performing the bootstrap.
                return

            row = await _BOOTSTRAP_SELECT_SENTINEL.execute(conn)
            if row is not None:
                logger.debug(
                    "public_access_baseline bootstrap: sentinel present — skipping"
                )
                return

            preset = find_preset("public_access_baseline")
            if preset is None:
                logger.warning(
                    "public_access_baseline bootstrap: preset not registered — skipping"
                )
                return

            ctx = PresetContext(
                db=engine,
                iam=get_protocol(IamService),
                policy=self,
                config=None,
                tasks=None,
                cron=None,
                libs=None,
                principal=None,
                scope="platform",
            )

            descriptor = await preset.apply(NoParams(), "platform", ctx)

            await _BOOTSTRAP_INSERT_SENTINEL.execute(
                conn,
                params_snapshot=json.dumps({}),
                revoke_descriptor=json.dumps(descriptor.payload),
            )
            logger.info(
                "public_access_baseline preset applied on cold-boot — "
                "anon /health probe enabled"
            )

    async def _register_usage_counter_drivers(self, stack: AsyncExitStack) -> None:
        """Wire a :class:`UsageCounterProtocol` driver for rate-limit / quota.

        Exactly one driver is registered (``get_protocol`` returns the
        lowest-priority match). When a :class:`CountingCacheBackend` is
        up we use the layered Valkey-hot + PG-durable driver; otherwise
        we fall back to the standalone Postgres driver.

        The layered driver owns a background flush task; binding its
        lifespan to ``stack`` ensures it stops cleanly on module unload.
        """
        from contextlib import asynccontextmanager

        from dynastore.models.protocols.cache import CountingCacheBackend
        from dynastore.modules.iam.usage_counter_pg import PostgresUsageCounter

        pg_counter = PostgresUsageCounter()

        try:
            from dynastore.tools.cache import get_cache_manager

            active = get_cache_manager().get_async_backend()
        except Exception:
            active = None

        if not isinstance(active, CountingCacheBackend):
            # Valkey-mandatory deployments (#1344) refuse to fall back to the
            # per-pod PG counter — cross-pod rate-limit / quota correctness
            # requires the shared atomic backend. Fail fast and loud so a
            # mis-provisioned prod never silently serves un-coordinated
            # counters. Defaults to off, so dev / test / single-node keep the
            # PG fallback unchanged.
            from dynastore.modules.iam.scale_config import (
                valkey_required_at_startup,
            )

            if await valkey_required_at_startup():
                raise RuntimeError(
                    "Valkey is required (IAM_VALKEY_REQUIRED / "
                    "IamScaleConfig.valkey_required) but no "
                    "CountingCacheBackend (Valkey) is active; refusing to "
                    "start on the per-pod PG counter fallback. Provision "
                    "Valkey or clear the requirement."
                )
            register_plugin(pg_counter)
            stack.callback(unregister_plugin, pg_counter)
            logger.info(
                "UsageCounter: no CountingCacheBackend active; using "
                "PG-only driver for rate-limit / quota enforcement."
            )
            return

        from dynastore.modules.iam.usage_counter_layered import LayeredUsageCounter

        layered = LayeredUsageCounter(postgres=pg_counter)

        @asynccontextmanager
        async def _lifespan():
            await layered.start()
            try:
                yield
            finally:
                await layered.stop()

        await stack.enter_async_context(_lifespan())
        register_plugin(layered)
        stack.callback(unregister_plugin, layered)
        logger.info("UsageCounter: layered Valkey+PG driver registered.")

    async def resolve_principal(self, credentials: Any) -> Optional[Principal]:
        """
        Implementation of AuthenticationProtocol.
        Validates credentials directly, avoiding HTTP layer dependencies.
        """
        if not self._iam_manager:
            return None

        # Support only string credentials (Bearer tokens via JWT/Keycloak)
        if isinstance(credentials, str):
            try:
                roles, principal = await self._iam_manager.authenticate_and_get_role(
                    type("Request", (), {"headers": {"Authorization": f"Bearer {credentials}"}, "state": type("State", (), {"catalog_id": None})()})()
                )
                return principal
            except Exception:
                return None
        return None

    async def check_permission(
        self, principal: Principal, action: str, resource: str
    ) -> bool:
        """
        Implementation of AuthorizationProtocol.
        Delegates to the PolicyService.
        """
        if not self._policy_service:
            return False

        return await self._policy_service.check_permission(principal, action, resource)

    # ------------------------------------------------------------------ #
    # PermissionProtocol delegating implementations.                       #
    #                                                                     #
    # IamModule is registered as the PermissionProtocol implementor (via  #
    # ``register_plugin(self)`` in lifespan), so every protocol caller —  #
    # IamMiddleware.evaluate_access included — resolves here. Without the #
    # explicit delegations below, the Protocol stub bodies (``...``)      #
    # would be inherited and silently return ``None``, which              #
    # IamMiddleware then treats as ALLOW (``result if result is not None  #
    # else (True, "")``) — turning every policy gate into a no-op for any #
    # request not protected by another middleware layer.                  #
    # ``register_policy`` and ``register_role`` are intentionally NOT     #
    # delegated: they buffer for cross-pod-safe upsert at lifespan flush. #
    # ------------------------------------------------------------------ #

    async def evaluate_access(
        self,
        principals: List[str],
        path: str,
        method: str,
        request_context: Any = None,
        catalog_id: Optional[str] = None,
        custom_policies: Optional[List[Any]] = None,
        principal_id: Optional[UUID] = None,
        collection_id: Optional[str] = None,
    ) -> Tuple[bool, str]:
        if self._policy_service is None:
            return False, "PolicyService not initialized"
        return await self._policy_service.evaluate_access(
            principals=principals,
            path=path,
            method=method,
            request_context=request_context,
            catalog_id=catalog_id,
            custom_policies=custom_policies,
            principal_id=principal_id,
            collection_id=collection_id,
        )

    async def compile_read_filter(
        self,
        principals: List[str],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        *,
        principal: Optional[Principal] = None,
        principal_id: Optional[UUID] = None,
    ) -> Any:
        if self._policy_service is None:
            # Fail closed: no engine ⟹ no documents are visible via search.
            from dynastore.models.protocols.access_filter import AccessFilter
            return AccessFilter.deny_everything()
        return await self._policy_service.compile_read_filter(
            principals,
            catalog_id=catalog_id,
            collection_id=collection_id,
            principal=principal,
            principal_id=principal_id,
        )

    async def evaluate_policy_statements(
        self, policy: Any, method: str, path: str, request_context: Any = None,
    ) -> bool:
        if self._policy_service is None:
            return False
        return await self._policy_service.evaluate_policy_statements(
            policy=policy, method=method, path=path, request_context=request_context,
        )

    async def get_policy(
        self, policy_id: str, catalog_id: Optional[str] = None,
    ) -> Optional[Any]:
        if self._policy_service is None:
            return None
        return await self._policy_service.get_policy(policy_id, catalog_id=catalog_id)

    async def create_policy(
        self, policy: Any, catalog_id: Optional[str] = None,
    ) -> Any:
        if self._policy_service is None:
            return None
        return await self._policy_service.create_policy(policy, catalog_id=catalog_id)

    async def update_policy(
        self, policy: Any, catalog_id: Optional[str] = None,
    ) -> Optional[Any]:
        if self._policy_service is None:
            return None
        return await self._policy_service.update_policy(policy, catalog_id=catalog_id)

    async def list_policies(
        self, limit: int = 100, offset: int = 0, catalog_id: Optional[str] = None,
    ) -> List[Any]:
        if self._policy_service is None:
            return []
        return await self._policy_service.list_policies(
            limit=limit, offset=offset, catalog_id=catalog_id,
        )

    async def delete_policy(
        self, policy_id: str, catalog_id: Optional[str] = None,
    ) -> bool:
        if self._policy_service is None:
            return False
        return await self._policy_service.delete_policy(policy_id, catalog_id=catalog_id)

    async def search_policies(
        self,
        resource_pattern: str,
        action_pattern: str,
        limit: int = 10,
        offset: int = 0,
        catalog_id: Optional[str] = None,
    ) -> List[Any]:
        if self._policy_service is None:
            return []
        return await self._policy_service.search_policies(
            resource_pattern=resource_pattern,
            action_pattern=action_pattern,
            limit=limit,
            offset=offset,
            catalog_id=catalog_id,
        )


async def _seed_oidc_role_sync_config(engine: Any) -> None:
    """Idempotent one-shot seed of OidcRoleSyncConfig(reconcile_enabled=True).

    Skipped when a row already exists so operator PATCHes are preserved.
    Handles the cold-boot race where configs storage may not yet exist.
    """
    from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
    from .oidc_role_sync_config import OidcRoleSyncConfig

    configs = get_protocol(PlatformConfigsProtocol)
    if configs is None:
        logger.debug("OidcRoleSyncConfig seed skipped: PlatformConfigsProtocol not registered.")
        return

    try:
        persisted = await configs.list_configs()
    except Exception:
        try:
            from dynastore.modules.db_config.platform_config_service import PlatformConfigService
            await PlatformConfigService.initialize_storage(engine)
            persisted = await configs.list_configs()
        except Exception:
            logger.warning(
                "OidcRoleSyncConfig seed skipped: platform configs storage "
                "unavailable after ensure-and-retry.",
                exc_info=True,
            )
            return

    existing = cast(Any, persisted.get(OidcRoleSyncConfig))
    if existing is None:
        seed = OidcRoleSyncConfig(reconcile_enabled=True)
        try:
            await configs.set_config(OidcRoleSyncConfig, seed)
            logger.info("Seeded OidcRoleSyncConfig(reconcile_enabled=True).")
        except Exception:
            logger.warning("OidcRoleSyncConfig seed failed.", exc_info=True)
        effective = seed
    else:
        effective = existing

    # Warn operators when reconcile_enabled=True but no issuer_whitelist is
    # set — the reconciler will accept tokens from any issuer for mapped roles,
    # which is a security gap in multi-tenant deployments.
    if getattr(effective, "reconcile_enabled", False) and not getattr(effective, "issuer_whitelist", None):
        logger.warning(
            "OidcRoleSyncConfig: reconcile_enabled=True but issuer_whitelist "
            "is not set. OIDC role sync will accept tokens from any issuer. "
            "Set issuer_whitelist to restrict mapped-role grants to trusted issuers."
        )


async def _warn_jwt_attr_no_issuer_allowlist() -> None:
    """Warn at startup when JWT-claim attribute enrichment is active without an
    issuer_allowlist.  Mirrors the OidcRoleSyncConfig warning above.
    """
    from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
    from .jwt_attr_config import JwtAttributeClaimsConfig

    configs = get_protocol(PlatformConfigsProtocol)
    if configs is None:
        return
    try:
        persisted = await configs.list_configs()
    except Exception:
        return
    raw = persisted.get(JwtAttributeClaimsConfig)
    if raw is None:
        return
    cfg: Any = raw if isinstance(raw, JwtAttributeClaimsConfig) else JwtAttributeClaimsConfig.model_validate(raw)
    if cfg.claim_map and not cfg.issuer_allowlist:
        logger.warning(
            "JwtAttributeClaimsConfig: claim_map is set but issuer_allowlist "
            "is not. JWT-claim attribute enrichment will accept tokens from any "
            "verified issuer. Set issuer_allowlist to restrict attribute injection "
            "to trusted issuers and prevent cross-provider ABAC interference."
        )


async def _seed_catalog_roles(conn: Any, schema: str, iam_storage: Any) -> None:
    """Idempotent seed of per-catalog roles and hierarchy from IamRolesConfig.

    Called by the catalog lifecycle initializer when a new tenant schema is
    created. Seeds the catalog-tier role rows (admin, editor, user,
    unauthenticated) and the role hierarchy edges so auth evaluations work
    on first tenant access.
    """
    from dynastore.models.protocols.authorization import IamRolesConfig, _canonical_role_seeds
    from dynastore.models.auth_models import Role

    role_config = IamRolesConfig()
    _, canonical_catalog = _canonical_role_seeds()
    # Merge canonical catalog-tier seeds with any operator-defined extras.
    # role_config.catalog_roles is the operator extra field (default []);
    # canonical_catalog contains the platform defaults (admin, editor, user, unauthenticated).
    all_catalog_seeds = list(canonical_catalog) + list(role_config.catalog_roles)
    for seed in all_catalog_seeds:
        role_def = Role(name=seed.name, description=seed.description, policies=list(seed.policies))
        existing = await iam_storage.get_role(role_def.name, schema=schema, conn=conn)
        if existing is None:
            await iam_storage.create_role(role_def, schema=schema, conn=conn)

    for seed in all_catalog_seeds:
        if seed.parent:
            try:
                await iam_storage.add_role_hierarchy(
                    parent_role=seed.parent, child_role=seed.name, schema=schema, conn=conn,
                )
            except Exception as exc:
                logger.warning(
                    "_seed_catalog_roles: failed to seed hierarchy %r → %r in schema %r: %s",
                    seed.parent, seed.name, schema, exc,
                )


@lifecycle_registry.sync_catalog_initializer()
async def initialize_iam_tenant(conn: DbResource, schema: str, catalog_id: str):
    """Initializes IAM and Policy tables within a tenant schema."""
    logger.info(
        f"Initializing IAM tables for tenant: {catalog_id} in schema {schema}"
    )

    from .postgres_iam_storage import PostgresIamStorage
    from .postgres_policy_storage import PostgresPolicyStorage

    storage = PostgresIamStorage()
    policy_storage = PostgresPolicyStorage()

    await policy_storage._initialize_schema(conn, schema=schema)
    await storage._initialize_schema(conn, schema=schema)

    # Seed per-catalog roles and role hierarchy idempotently.
    # Platform-tier policies (sysadmin_full_access, public_access,
    # self_service_access) are seeded during iam_baseline preset apply
    # and are not auto-seeded here. Per-catalog role seeding mirrors
    # the IamRolesConfig catalog_roles so auth evaluations work on first
    # tenant access without requiring an explicit preset apply.
    await _seed_catalog_roles(conn, schema=schema, iam_storage=storage)
