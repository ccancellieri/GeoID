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

import re
import logging
import uuid
import time
from typing import List, Optional, Tuple, Any, Dict, cast
from uuid import UUID
from dynastore.tools.cache import cached
import json

_SAFE_SCHEMA_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


def _validate_schema_name(schema: str) -> str:
    """Validate a schema name to prevent SQL injection via identifier interpolation."""
    if not _SAFE_SCHEMA_RE.match(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema

from dynastore.models.protocols.authorization import IamRoleConfig

from .models import PolicyBundle, Policy, Condition, Role, Principal
from .policy_storage import AbstractPolicyStorage
from .iam_storage import AbstractIamStorage
from .postgres_policy_storage import PostgresPolicyStorage
from dynastore.modules.db_config.query_executor import managed_transaction, DbResource
from dynastore.modules import get_protocol
from dynastore.models.protocols import DatabaseProtocol
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)



class PolicyService:
    _instance = None
    storage: AbstractPolicyStorage
    iam_storage: Optional[AbstractIamStorage]
    _state: object
    _engine: Any

    def __init__(
        self,
        app_state: object,
        storage: Optional[AbstractPolicyStorage] = None,
        iam_storage: Optional[AbstractIamStorage] = None,
        role_config: Optional[IamRoleConfig] = None,
    ):
        self._state = app_state
        db = get_protocol(DatabaseProtocol)
        self._engine = db.engine if db else None
        self.storage = storage or PostgresPolicyStorage(app_state=app_state)
        self.iam_storage = iam_storage
        # Role names used by the seed + runtime checks. Defaults read from
        # IAM_ROLE_* env vars (with seeded ``DefaultRole`` values as the
        # ultimate fallback), so deployments configure custom names without
        # subclassing.
        self._role_config = role_config or IamRoleConfig()

    async def _resolve_schema(
        self, catalog_id: Optional[str], conn: Optional[Any] = None
    ) -> str:
        """Resolve physical schema from catalog_id, with fallback to 'iam' for global."""
        from dynastore.models.protocols import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)

        if not catalog_id or catalog_id == "_system_":
            return "iam"

        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not available.")

        db_resource = conn or cast(Any, self.storage).engine
        # Use allow_missing=True during tenant initialization when catalog may not exist yet
        res = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=db_resource), allow_missing=True
        )
        schema = res if res else "iam"
        return _validate_schema_name(schema)

    async def initialize(
        self, catalog_id: Optional[str] = None, conn: Optional[Any] = None
    ):
        """Initializes storage for the specified catalog/schema."""
        schema = await self._resolve_schema(catalog_id, conn=conn)
        async with managed_transaction(conn or self._engine) as db:
            await cast(Any, self.storage).initialize(conn=db, schema=schema)

    async def check_permission(
        self, principal: Principal, action: str, resource: str
    ) -> bool:
        """
        Evaluates if the given principal can perform 'action' on 'resource'.
        Delegates to the evaluation engine (evaluate_access).

        Note: this method MUST NOT pattern-match on role names (D4 — no
        hardcoded role identifiers). The historical sysadmin
        short-circuit lives in ``IamMiddleware``'s privilege-elevation
        guard (see ``extensions/iam/guards.py``); permission decisions
        here flow exclusively through role → policy → permission.
        """
        # 1. Collect all roles and identity IDs to check
        identities = []
        if principal.subject_id:
            identities.append(principal.subject_id)
        if principal.roles:
            identities.extend(principal.roles)

        # 2. Delegate to the main evaluation logic
        # Note: we might need to extract catalog_id from resource if it's a catalog-scoped check
        catalog_id_match = re.search(r"catalog[:/]([^:/]+)", resource)
        catalog_id = catalog_id_match.group(1) if catalog_id_match else None

        allowed, reason = await self.evaluate_access(
            principals=identities,
            path=resource,
            method=action,
            catalog_id=catalog_id,
            custom_policies=principal.custom_policies or None,
        )
        return allowed

    def get_storage(self) -> AbstractPolicyStorage:
        if not self.storage:
            raise RuntimeError("PolicyService not initialized.")
        return self.storage

    def _derive_partition_key(self, path: str) -> str:
        """
        Derives a partition key from a path.
        """
        # 1. Custom mapping for documentation
        if any(path.startswith(p) for p in ["/docs", "/openapi.json", "/redoc"]):
            return "docs"

        # 2. Heuristic: first segment of the path
        pattern = path.strip(" /").split("/")[0]
        if not pattern or "*" in pattern or "|" in pattern:
            return "default"
        return pattern[:32]

    def invalidate_cache(self):
        """Invalidates the evaluation cache."""
        try:
            # Clear the @cached cache for get_effective_policies
            cast(Any, self.get_effective_policies).cache_clear()
        except AttributeError:
            # In case the decorator is missing or hasn't finished wrapping
            pass
        except Exception as e:
            logger.error(f"Failed to clear policy cache: {e}")

    # --- CRUD ---

    async def create_policy(
        self, policy: Policy, catalog_id: Optional[str] = None
    ) -> Policy:
        schema = await self._resolve_schema(catalog_id)
        if not policy.partition_key:
            policy.partition_key = "global"

        async with managed_transaction(self._engine) as conn:
            # 1. Ensure partition exists before inserting
            from .postgres_policy_storage import PostgresPolicyStorage

            if isinstance(self.storage, PostgresPolicyStorage):
                await self.storage.ensure_policy_partition(
                    conn, policy.partition_key, schema=schema
                )

            # 2. Check for duplicate IDs in the target partition. Filtering
            # by partition is required: the same id can exist across
            # partitions (e.g., default policies seeded into both
            # ``global`` and a catalog partition); a global lookup would
            # spuriously reject a legitimate per-catalog create.
            existing = await self.storage.get_policy(
                policy.id, schema=schema, conn=conn, partition_key=policy.partition_key,
            )
            if existing:
                raise ValueError(f"Policy with ID '{policy.id}' already exists.")

            res = await self.storage.create_policy(policy, schema=schema, conn=conn)
            self.invalidate_cache()
            return res

    async def get_policy(
        self, policy_id: str, catalog_id: Optional[str] = None
    ) -> Optional[Policy]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.get_policy(
            policy_id, schema=schema, partition_key=catalog_id or "global",
        )

    async def update_policy(
        self, policy: Policy, catalog_id: Optional[str] = None
    ) -> Optional[Policy]:
        schema = await self._resolve_schema(catalog_id)
        res = await self.storage.update_policy(policy, schema=schema)
        self.invalidate_cache()
        return res

    async def list_policies(
        self, limit: int = 100, offset: int = 0, catalog_id: Optional[str] = None
    ) -> List[Policy]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.list_policies(
            limit=limit, offset=offset, schema=schema
        )

    async def delete_policy(
        self, policy_id: str, catalog_id: Optional[str] = None
    ) -> bool:
        schema = await self._resolve_schema(catalog_id)
        res = await self.storage.delete_policy(
            policy_id, schema=schema, partition_key=catalog_id or "global",
        )
        self.invalidate_cache()
        return res

    async def search_policies(
        self,
        resource_pattern: str,
        action_pattern: str,
        limit: int = 10,
        offset: int = 0,
        catalog_id: Optional[str] = None,
    ) -> List[Policy]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.search_policies(
            resource_pattern, action_pattern, limit, offset, schema=schema
        )

    # --- Provisioning & Defaults ---

    def _get_default_policies(self, partition_key: str = "global") -> List[Policy]:
        """Returns the core default policies for the platform."""
        return [
            Policy(
                id="sysadmin_full_access",
                description="Unrestricted access for system administrators.",
                actions=["*"],
                resources=[".*"],
                effect="ALLOW",
                partition_key=partition_key,
            ),
            Policy(
                id="public_access",
                description="Allows anonymous access to public endpoints. "
                            "Resources are enumerated explicitly (no /web/.* "
                            "catch-all) so dashboard data endpoints stay gated "
                            "by web_dashboard_*_access policies declared in "
                            "extensions/web/web.py via the PolicyContributor "
                            "Protocol (Web.get_policies / get_role_bindings).",
                actions=["GET", "POST", "OPTIONS", "HEAD"],
                resources=[
                    "/$",
                    "/health",
                    "/docs.*",
                    "/openapi.json",
                    "/redoc",
                    # OGC API & STAC API metadata — anonymous-visible by
                    # spec convention. Without /conformance the home-page
                    # live OGC matrix can never probe real platform state
                    # and always falls back to the static snapshot. Only
                    # the metadata surface (landing root + /conformance)
                    # is opened here; data endpoints (/collections,
                    # /items, item search) keep their existing gating.
                    "/conformance/?$",
                    "/stac/?$",
                    "/stac/conformance/?$",
                    "/features/?$",
                    "/features/conformance/?$",
                    "/processes/?$",
                    "/processes/conformance/?$",
                    "/records/?$",
                    "/records/conformance/?$",
                    "/coverages/?$",
                    "/coverages/conformance/?$",
                    "/dggs/?$",
                    "/dggs/conformance/?$",
                    "/consys/?$",
                    "/consys/conformance/?$",
                    "/movingfeatures/?$",
                    "/movingfeatures/conformance/?$",
                    # /web/* — explicit safe sub-paths only; mirrors the
                    # web_public_access policy in extensions/web/web.py
                    "/web/?$",
                    "/web/pages/.*",
                    "/web/extension-static/.*",
                    "/web/geoid/.*",  # GeoID-branded static assets — CSS, JS, snapshot, logo
                    "/web/static/.*",
                    "/web/website/.*",
                    "/web/docs-content/.*",
                    "/web/docs-manifest",
                    "/web/config/.*",
                    "/web/health",
                    "/web/dashboard/?$",
                    "/web/lite/.*",  # JupyterLite WASM kernel — anonymous-OK
                    "/iam/auth/login",
                    "/iam/auth/validate",
                    "/iam/auth/jwks.json",
                    "/auth/.*",
                ],
                effect="ALLOW",
                partition_key=partition_key,
            ),
            Policy(
                id="self_service_access",
                description="Allows authenticated users to access their own /me endpoints.",
                actions=["GET"],
                resources=["/iam/me/.*", "/auth/me"],
                effect="ALLOW",
                partition_key=partition_key,
            ),
        ]

    def _get_default_roles(self) -> List[Role]:
        """Return the core default roles, named via ``self._role_config``.

        Operators wiring custom role names (env or constructor arg) seed
        their chosen names through this method without any code change.
        """
        cfg = self._role_config
        return [
            Role(
                name=cfg.sysadmin,
                description="System Administrator with full access.",
                policies=["sysadmin_full_access"],
            ),
            Role(
                name=cfg.admin,
                description="Administrator with full access.",
                policies=["sysadmin_full_access"],
            ),
            Role(
                name=cfg.anonymous,
                description="Anonymous user with limited access.",
                policies=["public_access"],
            ),
            Role(
                name=cfg.user,
                description="Default role for any authenticated user.",
                policies=["self_service_access"],
            ),
        ]

    async def provision_default_policies(
        self,
        catalog_id: Optional[str] = None,
        conn: Optional[Any] = None,
        schema: Optional[str] = None,
        force: bool = False,
    ):
        """Provisions core default policies and roles.

        Args:
            catalog_id: Optional catalog for tenant-scoped provisioning.
            conn: Optional existing DB connection.
            schema: Optional explicit schema name.
            force: If True, upsert all defaults (reset). If False, only create missing ones.
        """
        if not schema:
            schema = await self._resolve_schema(catalog_id, conn=conn)
        pk = catalog_id or "global"

        async with managed_transaction(conn or self._engine) as db:
            # Ensure partition exists if catalog-scoped
            if catalog_id:
                await self.storage.ensure_policy_partition(
                    db, partition_key=catalog_id, schema=schema
                )

            # Provision default policies. ``public_access`` is force-updated
            # on every provision so deployments inheriting an older overly-broad
            # version (pre-2026-04-29 ``/web/.*`` catch-all that accidentally
            # allowed anonymous access to gated dashboard endpoints) self-heal
            # to the narrowed enumeration without operator intervention. Other
            # defaults stay idempotent so operator edits via the policy API
            # are preserved.
            _ALWAYS_REFRESH = {"public_access"}
            for policy_def in self._get_default_policies(partition_key=pk):
                if force or policy_def.id in _ALWAYS_REFRESH:
                    await self.storage.update_policy(policy_def, schema=schema, conn=db)
                else:
                    existing = await self.storage.get_policy(
                        policy_def.id, schema=schema, conn=db, partition_key=pk,
                    )
                    if not existing:
                        await self.storage.update_policy(policy_def, schema=schema, conn=db)

            # Provision default roles
            if self.iam_storage:
                for role_def in self._get_default_roles():
                    existing = await self.iam_storage.get_role(
                        role_def.name, schema=schema, conn=db
                    )
                    if force or not existing:
                        if existing:
                            await self.iam_storage.update_role(
                                role_def, schema=schema, conn=db
                            )
                        else:
                            await self.iam_storage.create_role(
                                role_def, schema=schema, conn=db
                            )

                # Seed default role hierarchy:
                #   sysadmin > admin > user > anonymous
                # Direction: parent inherits child's policies (see
                # `get_role_hierarchy` in postgres_iam_storage.py — it returns
                # `role_names + children`). With this seed an authenticated
                # ``user`` automatically inherits every ``*_public_access``
                # policy bound to ``anonymous``, which is the natural
                # "everything anonymous can do, authenticated users can do
                # too" semantics. Without it, registering with Keycloak
                # silently *narrows* the user's access vs. browsing
                # unauthenticated.
                #
                # Idempotent: ON CONFLICT DO NOTHING in `add_role_hierarchy`,
                # so reprovision on every cold start is safe and self-heals
                # databases that pre-date this seed.
                cfg = self._role_config
                _DEFAULT_HIERARCHY = [
                    (cfg.sysadmin, cfg.admin),
                    (cfg.admin,    cfg.user),
                    (cfg.user,     cfg.anonymous),
                ]
                for parent, child in _DEFAULT_HIERARCHY:
                    try:
                        await self.iam_storage.add_role_hierarchy(
                            parent_role=parent, child_role=child,
                            schema=schema, conn=db,
                        )
                    except Exception as e:
                        # add_role_hierarchy is upsert-by-PK in the default
                        # storage impl (`INSERT ... ON CONFLICT DO NOTHING`);
                        # the swallow exists for deployments that override
                        # the storage with a stricter, non-idempotent
                        # implementation. Log at WARNING so the swallow is
                        # observable — silent failures here put the platform
                        # into a "no inheritance" state where authenticated
                        # users see narrower access than anonymous browsers
                        # (the seed wires sysadmin → admin → user → anonymous,
                        # so users *inherit* every public policy bound to
                        # anonymous; without the chain, registering for an
                        # account silently *removes* access).
                        logger.warning(
                            "provision_default_policies: failed to seed role "
                            "hierarchy edge %r → %r in schema %r: %s",
                            parent, child, schema, e,
                        )

    # --- Evaluation ---

    @cached(maxsize=512, namespace="policies")
    async def get_effective_policies(
        self, partition_key: str, schema: str
    ) -> List[Policy]:
        """Caches policy sets per partition/schema."""
        return await self.storage.list_policies(
            partition_key=partition_key, limit=1000, schema=schema
        )

    async def evaluate_policy_statements(
        self, policy: PolicyBundle, method: str, path: str, request_context: Any = None
    ) -> bool:
        """
        Evaluates the statements in an PolicyBundle (embedded in Key or Principal).
        Iterates through statements: if any DENY matches, return False.
        If any ALLOW matches, return True.
        If none match, return False (Implicit Deny).
        """
        if not policy.statements:
            return True

        has_allow_match = False
        for s in policy.statements:
            # Check method and path
            method_match = not s.actions or ".*" in s.actions or method in s.actions or s.matches_action(method)
            path_match = s.matches_resource(path)

            if method_match and path_match:
                # Check conditions
                conditions_met = True
                if s.conditions:
                    for cond in s.conditions:
                        if not await self._evaluate_condition(cond, request_context):
                            conditions_met = False
                            break

                if conditions_met:
                    if s.effect == "DENY":
                        return False  # Explicit Deny wins
                    if s.effect == "ALLOW":
                        has_allow_match = True

        return has_allow_match

    async def evaluate_access(
        self,
        principals: List[str],
        path: str,
        method: str,
        request_context: Any = None,
        catalog_id: Optional[str] = None,
        custom_policies: Optional[List[Policy]] = None,
    ) -> Tuple[bool, str]:
        """
        The central Zero-Trust evaluation engine.
        Returns (is_allowed, reason).
        """
        schema = await self._resolve_schema(catalog_id)
        logger.debug(
            f"EVAL: Evaluating access for {principals} on {method} {path} (schema: {schema})"
        )

        # 1. Fetch all roles matching any of the principals to resolve policy IDs
        # Always check the global "iam" schema first for roles
        all_policy_ids = set()
        schemas_to_check = ["iam"]  # Always check global schema
        if schema != "iam":
            schemas_to_check.append(
                schema
            )  # Also check catalog-specific schema if different

        for check_schema in schemas_to_check:
            for principal in principals:
                if not principal:
                    continue
                role_obj = None
                # Lookup role permissions in storage
                if self.iam_storage:
                    role_obj = await self.iam_storage.get_role(
                        principal, schema=check_schema
                    )
                    if role_obj:
                        logger.warning(
                            f"DBG-ROLE: Found role '{principal}' schema={check_schema} policies={role_obj.policies}"
                        )
                        all_policy_ids.update(role_obj.policies)
                    else:
                        logger.debug(
                            f"EVAL: Role '{principal}' not found in schema '{check_schema}'."
                        )

        # 2. Fetch all unique policies from both global and catalog-specific schemas
        effective_policies = []
        for pid in all_policy_ids:
            # Try to get policy from catalog schema first, then fall back to global
            pol = await self.get_policy(pid, catalog_id=catalog_id)
            if not pol and catalog_id:
                # Fall back to global schema
                pol = await self.get_policy(pid, catalog_id=None)
            if pol:
                effective_policies.append(pol)
                if pid in ("web_public_access", "public_access"):
                    logger.warning(f"DBG-POL: {pid} resources={pol.resources}")

        # 3. Include custom policies directly attached to the principal
        if custom_policies:
            effective_policies.extend(custom_policies)

        logger.debug(
            f"EVAL: Total effective policies to check: {len(effective_policies)}"
        )

        # 4. Evaluate
        for p in effective_policies:
            # Check action (method) and resource (path)
            # Actions are also regex patterns after transformation
            method_match = (
                not p.actions
                or ".*" in p.actions
                or method in p.actions
                or p.matches_action(method)
            )
            path_match = p.matches_resource(path)

            logger.debug(
                f"EVAL: Checking policy '{p.id}': method_match={method_match}, path_match={path_match}"
            )

            if method_match and path_match:
                # Check conditions
                conditions_met = True
                if p.conditions:
                    for cond in p.conditions:
                        if not await self._evaluate_condition(cond, request_context):
                            conditions_met = False
                            break

                if conditions_met:
                    if p.effect == "DENY":
                        logger.info(f"EVAL: DENIED by {p.id}")
                        return False, f"Explicit DENY by policy {p.id}"
                    if p.effect == "ALLOW":
                        logger.info(f"EVAL: ALLOWED by {p.id}")
                        return True, f"Allowed by policy {p.id}"

        logger.warning(
            f"EVAL: DENIED (No matching ALLOW policy found) for {principals} on {method} {path}"
        )
        return False, "Deny by Default (No matching ALLOW policy found)"

    async def _evaluate_condition(self, condition: Condition, context: Any) -> bool:
        """
        Resolves the appropriate handler and evaluates the condition.
        """
        from .conditions import evaluate_condition

        return await evaluate_condition(condition, context)
