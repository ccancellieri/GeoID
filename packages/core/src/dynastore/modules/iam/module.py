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
from contextlib import AsyncExitStack, asynccontextmanager

# Top-level tenacity import — load-bearing for SCOPE gating.
# `tools/discovery.py:159 discover_and_load_plugins` skips entry-points whose
# top-level imports raise ImportError, so importing tenacity at module load
# (rather than lazily inside `flush_pending_registrations`) ensures IamModule
# is *not* registered on services whose SCOPE excludes `module_iam` (which
# declares the tenacity dep).  Without this, IamModule loaded on every
# service and crashed at lifespan time when tenacity wasn't installed —
# see GeoID#252 / PR #249 (the symptom-fix that promoted tenacity to `core`).
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
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.db_config.query_executor import DbResource
from typing import Optional, Any, AsyncGenerator, List, Tuple
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
                db = get_protocol(DatabaseProtocol)
                engine = db.engine if db else None
                async with managed_transaction(engine) as conn:
                    await self.storage._initialize_schema(conn, schema="iam")
                    await policy_storage._initialize_schema(conn, schema="iam")

                    # Provision default global policies
                    await self._policy_service.provision_default_policies(conn=conn)

                    # Self-heal guard: if the platform-tier sysadmin role
                    # is still absent after normal provision (e.g. iam_storage
                    # was None on first pass, or a DB reset happened before a
                    # previous restart), force a full re-seed so the service
                    # is never left in a locked-out state. Per geoid#643 the
                    # platform tier collapses to {sysadmin} — catalog-tier
                    # roles (admin/editor/user/unauthenticated) are seeded
                    # by the per-catalog lifecycle hook, not here.
                    sysadmin_name = role_config.sysadmin_role_name
                    sysadmin = await self.storage.get_role(
                        sysadmin_name, schema="iam", conn=conn
                    )
                    if sysadmin is None:
                        logger.critical(
                            "IamModule: %r role missing after provision — "
                            "DB may have been reset. Forcing full re-seed.",
                            sysadmin_name,
                        )
                        await self._policy_service.provision_default_policies(
                            conn=conn, force=True
                        )

                # One-shot purge of inert `viewer` rows left behind by
                # tenants seeded before #643 slice 1. Runs after schema
                # init + default-policy provision so the table exists.
                # Guarded inside the migration — operator-customized
                # viewer rows survive.
                from dynastore.modules.iam.viewer_role_purge_migration import (
                    purge_viewer_role_rows,
                )
                await purge_viewer_role_rows(engine)

            except Exception as e:
                logger.error(f"Failed to initialize IamModule: {e}", exc_info=True)

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

    async def provision_default_policies(
        self,
        catalog_id: Optional[str] = None,
        conn: Optional[Any] = None,
        schema: Optional[str] = None,
        force: bool = False,
    ) -> None:
        if self._policy_service is None:
            return
        await self._policy_service.provision_default_policies(
            catalog_id=catalog_id, conn=conn, schema=schema, force=force,
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
                # up with the union of policy ids.
                if storage is not None:
                    async with self._role_lock:
                        for r in roles:
                            existing = await storage.get_role(r.name, schema="iam", conn=conn)
                            if existing:
                                merged = list(set(existing.policies + r.policies))
                                merged_role = r.model_copy(update={"policies": merged})
                                await storage.update_role(merged_role, schema="iam", conn=conn)
                            else:
                                await storage.create_role(r, schema="iam", conn=conn)

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

    # Provision default policies for the new tenant
    from .policies import PolicyService

    policy_service = PolicyService(
        None, storage=policy_storage, iam_storage=storage
    )  # app_state not strictly needed for provisioning with explicit conn
    await policy_service.provision_default_policies(
        catalog_id, conn=conn, schema=schema
    )
