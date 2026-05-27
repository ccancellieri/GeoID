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

from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)
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


from dynastore.models.protocols.policies import PermissionProtocol
class IamModule(ModuleProtocol, AuthenticationProtocol, AuthorizationProtocol, PermissionProtocol):
    priority: int = 100
    _iam_manager: Optional[IamService] = None
    _policy_service: Optional[PolicyService] = None
    _authorizer: Optional[IamAuthorizer] = None
    storage: Optional[AbstractIamStorage] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # In-memory buffers deduped across extensions. Roles with the same
        # name coming from multiple extensions (e.g. 'anonymous' from features,
        # stac, coverages, records, notebooks, web) are merged here and
        # persisted exactly once during flush_pending_registrations().
        self._pending_policies: dict[str, Any] = {}
        self._pending_roles: dict[str, Any] = {}
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

                    # One-shot migration: normalize the public_access policy
                    # resource list — remove the stale /web/.* catch-all (fixed
                    # in eaa8cbf89). With _ALWAYS_REFRESH gone from PR-5 the
                    # old broken row would not be auto-corrected at boot without
                    # this explicit migration step.
                    try:
                        from dynastore.modules.iam.migrations.normalize_public_access import run_migration as _run_normalize
                        await _run_normalize(engine=engine)
                    except Exception as _norm_exc:
                        logger.warning(
                            "IamModule: public_access normalize migration failed (non-fatal): %s",
                            _norm_exc,
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

    async def _persist_policy(self, policy: Any):
        """Persist a policy to the database via storage upsert."""
        policy_service = self._policy_service
        if policy_service is None:
            return
        try:
            from dynastore.modules.db_config.query_executor import managed_transaction
            from dynastore.models.protocols import DatabaseProtocol
            db = get_protocol(DatabaseProtocol)
            engine = db.engine if db else None

            async with managed_transaction(engine) as conn:
                await policy_service.storage.update_policy(policy, schema="iam", conn=conn)
                logger.debug(f"Persisted policy: {policy.id}")
                policy_service.invalidate_cache()
        except Exception as e:
            logger.warning(f"Failed to persist policy {policy.id}: {e}")

    async def _persist_role(self, role: Any):
        """Persist a role to the database via storage upsert.

        Lock serializes cross-worker-safe read-modify-write merges so a
        role registered in a different process does not clobber ours.
        """
        async with self._role_lock:
            storage = self.storage
            policy_service = self._policy_service
            if storage is None or policy_service is None:
                return
            try:
                from dynastore.modules.db_config.query_executor import managed_transaction
                from dynastore.models.protocols import DatabaseProtocol
                db = get_protocol(DatabaseProtocol)
                engine = db.engine if db else None

                async with managed_transaction(engine) as conn:
                    existing = await storage.get_role(role.name, schema="iam", conn=conn)
                    if existing:
                        merged_policies = list(set(existing.policies + role.policies))
                        role = role.model_copy(update={"policies": merged_policies})
                        await storage.update_role(role, schema="iam", conn=conn)
                    else:
                        await storage.create_role(role, schema="iam", conn=conn)
                    logger.debug(f"Persisted role: {role.name}")
                    policy_service.invalidate_cache()
            except Exception as e:
                logger.warning(f"Failed to persist role {role.name}: {e}")

    def register_policy(self, policy: Any) -> Any:
        """Buffer a policy for persistence at flush time.

        Extensions call this during their lifespan. Duplicate IDs replace
        the earlier entry (policy IDs are globally unique per extension, so
        this only guards against re-registration within one process).
        """
        if not self._policy_service:
            logger.warning(f"PolicyService not ready; cannot register policy {policy.id}")
            return policy
        self._pending_policies[policy.id] = policy
        return policy

    def register_role(self, role: Any) -> Any:
        """Buffer a role for persistence at flush time, merging policies by name.

        Multiple extensions register roles with the same name (e.g. every
        OGC extension adds its public-access policy to 'anonymous'). We
        union policy lists in-memory so the DB sees exactly one upsert
        per unique role name in flush_pending_registrations().
        """
        if not self._policy_service:
            logger.warning(f"PolicyService not ready; cannot register role {getattr(role, 'name', role)}")
            return role
        existing = self._pending_roles.get(role.name)
        if existing is None:
            self._pending_roles[role.name] = role
        else:
            merged_policies = list(set(existing.policies + role.policies))
            self._pending_roles[role.name] = existing.model_copy(update={"policies": merged_policies})
        return role

    async def flush_pending_registrations(self):
        """Persist buffered policies and roles in a single atomic transaction.

        The previous implementation fanned each policy/role out to a
        separate ``managed_transaction(engine)`` via
        ``asyncio.gather(*tasks, return_exceptions=True)``. That had two
        compounding bugs (issue #203):

        1. **Deadlock window**: ~30 simultaneous upserts on the same
           partitioned table racing against parallel sibling-service
           seeds (catalog + web + worker + maps + tools + auth all run
           IAM lifespan at startup, all hitting ``iam.policies_global``
           concurrently). Postgres picks deadlock victims and rolls
           them back. Whether a particular row commits depends on
           timing — repro is intermittent: sometimes ``iam.policies``
           ends up at 20 rows, sometimes 0.
        2. **Silent failures**: ``return_exceptions=True`` collected
           the rolled-back exceptions into the gather result and
           dropped them on the floor. The per-task ``except Exception``
           in ``_persist_policy`` would have logged a WARNING, but the
           "Persisted policy" log line had already fired in the body
           BEFORE the implicit-commit-at-aexit raised — so the operator
           sees "Persisted policy: X" lines for rows that never landed.

        Fix: one transaction, sequential upserts, exceptions propagate.
        ~30 policies + 4 roles is cheap (~30ms); serial vs concurrent
        is not a perf concern.

        Sibling-service race retry (issue #209.2): sibling services
        (catalog + web + worker + maps + tools + auth + geoid) still
        open separate transactions that all upsert the same partition
        rows. PG ``ON CONFLICT DO UPDATE`` serialises correctly but can
        still surface SQLSTATE ``40001`` (serialization_failure) or
        ``40P01`` (deadlock_detected) when the concurrency is tight at
        cold start. Bounded retry — 3 attempts, exponential backoff —
        handles the cluster cold-start window without masking real bugs:
        any other exception propagates immediately.
        """
        policies = list(self._pending_policies.values())
        roles = list(self._pending_roles.values())
        self._pending_policies.clear()
        self._pending_roles.clear()
        if not policies and not roles:
            return

        policy_service = self._policy_service
        if policy_service is None:
            return
        storage = self.storage
        from dynastore.modules.db_config.query_executor import managed_transaction
        from dynastore.models.protocols import DatabaseProtocol
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        if engine is None:
            logger.warning("IamModule.flush_pending_registrations: no DB engine available")
            return

        attempted_p = [p.id for p in policies]
        attempted_r = [r.name for r in roles]

        # Seed-policy lookup keyed by role name. ``provision_default_policies``
        # only writes ``platform_roles`` into the platform ``iam`` schema
        # (catalog-tier RoleSeeds are seeded per-catalog by the lifecycle
        # hook, not here). But ``flush_pending_registrations`` writes ALL
        # buffered roles to ``schema="iam"`` regardless of tier — so when
        # contributors register, say, ``Role(name="unauthenticated",
        # policies=["web_public_access", ...])``, the read-modify-write below
        # starts from ``existing=None`` (no seed write ever landed for the
        # catalog-tier role in platform schema) and persists the row with
        # ONLY contributor policies. The seed-declared ``public_access`` is
        # silently dropped → ``/health`` 403 (geoid#902). Union the
        # ``RoleSeed.policies`` for the role name into the merge so the seed
        # binding survives. Applies to both platform_roles and catalog_roles
        # since the platform ``iam.roles`` table holds the role rows the
        # request-scope eval looks up when no catalog context is present
        # (e.g. /health).
        seed_policies_by_role: dict[str, list[str]] = {}
        if policy_service is not None:
            role_config = policy_service._role_config
            for seed in list(role_config.platform_roles) + list(role_config.catalog_roles):
                seed_policies_by_role[seed.name] = list(seed.policies)

        async def _flush_once() -> None:
            async with managed_transaction(engine) as conn:
                # Cross-process / cross-worker serialization (closes #263).
                # Postgres advisory transaction-scoped lock — only one flush
                # runs the read-modify-write block at a time, system-wide.
                # `pg_advisory_xact_lock` is auto-released at COMMIT/ROLLBACK,
                # so leaks are impossible.  Without this, the SQLSTATE-40001
                # retry caught the *visible* race but a second class —
                # read-modify-write on `iam.roles` returning a stale snapshot
                # from an in-flight concurrent tx and overwriting the winner
                # — silently left the roles table empty even though the
                # flush log reported success.  Reproduced 2026-05-05 during
                # the keycloak-fix browser verification.
                from sqlalchemy import text
                await conn.execute(  # type: ignore[misc]
                    text("SELECT pg_advisory_xact_lock(hashtext('iam_seed:iam'))")
                )

                # Policies: one storage call per policy, all in this tx.
                # `update_policy` opens a nested SAVEPOINT via
                # `managed_transaction(conn)`. Each per-policy SAVEPOINT
                # commits or rolls back independently — but a row that
                # rolls back STILL surfaces as an exception here, instead
                # of being silently swallowed by `asyncio.gather`.
                for p in policies:
                    await policy_service.storage.update_policy(p, schema="iam", conn=conn)

                # Roles: same pattern, with the existing read-modify-write
                # merge so concurrent extensions registering the same role
                # name (every OGC extension adds to 'anonymous', etc.) end
                # up with the union of policy ids — PLUS the RoleSeed
                # defaults (see ``seed_policies_by_role`` above for the
                # geoid#902 motivation).
                if storage is not None:
                    async with self._role_lock:
                        for r in roles:
                            existing = await storage.get_role(r.name, schema="iam", conn=conn)
                            seed_policies = seed_policies_by_role.get(r.name, [])
                            base_policies = list(existing.policies) if existing else []
                            merged = list(set(base_policies + seed_policies + r.policies))
                            if existing:
                                merged_role = r.model_copy(update={"policies": merged})
                                await storage.update_role(merged_role, schema="iam", conn=conn)
                            else:
                                # First write — start from the seed-augmented
                                # merge so the row lands with seed + contributor
                                # policies even when no prior row exists.
                                # Without this, the catalog-tier
                                # ``unauthenticated`` seed would be lost on
                                # cold boot because no platform-tier seed
                                # write touches it.
                                seeded_role = r.model_copy(update={"policies": merged})
                                await storage.create_role(seeded_role, schema="iam", conn=conn)

        try:
            # ``reraise=True`` propagates the underlying exception on
            # exhaustion (no ``RetryError`` wrapper). Other exceptions
            # propagate immediately because ``retry_if_exception``
            # returns False for them — no retry attempted.
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=0.05, max=0.5),
                retry=retry_if_exception(_is_transient_serialization_failure),
                reraise=True,
            ):
                with attempt:
                    await _flush_once()

            policy_service.invalidate_cache()
            logger.info(
                "IamModule: flushed %d policies + %d roles in one transaction",
                len(policies), len(roles),
            )
        except Exception as e:
            logger.error(
                "IamModule.flush_pending_registrations: transaction failed; "
                "policies=%s roles=%s — error: %s",
                attempted_p, attempted_r, e,
            )
            raise


def _is_transient_serialization_failure(exc: BaseException) -> bool:
    """True for PG SQLSTATE 40001 (serialization_failure) / 40P01
    (deadlock_detected). Used to bound the IAM-seeding retry window —
    these are the only two SQLSTATEs that are safe to retry blindly
    because Postgres has already rolled back the offending transaction.
    Any other DB error indicates a real problem and propagates.
    """
    orig = getattr(exc, "orig", None)
    pgcode = getattr(orig, "pgcode", None) or getattr(exc, "pgcode", None)
    return pgcode in ("40001", "40P01")


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


async def _seed_catalog_roles(conn: Any, schema: str, iam_storage: Any) -> None:
    """Idempotent seed of per-catalog roles and hierarchy from IamRolesConfig.

    Called by the catalog lifecycle initializer when a new tenant schema is
    created. Seeds the catalog-tier role rows (admin, editor, user,
    unauthenticated) and the role hierarchy edges so auth evaluations work
    on first tenant access.
    """
    from dynastore.models.protocols.authorization import IamRolesConfig
    from dynastore.models.auth_models import Role

    role_config = IamRolesConfig()
    for seed in role_config.catalog_roles:
        role_def = Role(name=seed.name, description=seed.description, policies=list(seed.policies))
        existing = await iam_storage.get_role(role_def.name, schema=schema, conn=conn)
        if existing is None:
            await iam_storage.create_role(role_def, schema=schema, conn=conn)

    for seed in role_config.catalog_roles:
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
