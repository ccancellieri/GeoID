#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
Authorization protocol — narrow, framework-free permission façade.

Used by both extensions (via FastAPI `Depends` wrappers in
`dynastore.extensions.iam.guards`) and tasks (via
`dynastore.modules.iam.authorization.checks.require_permission`).

When `IamModule` is loaded, an `IamAuthorizer` is registered via
`register_plugin` and returned by `get_protocols(AuthorizerProtocol)`.
When it is not loaded, the `DefaultAuthorizer` sentinel takes over and
fails closed on privileged checks.
"""

from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Protocol, Tuple, runtime_checkable

from pydantic import BaseModel, Field, model_validator

from dynastore.models.protocols.authorization_context import SecurityContext
from dynastore.models.mutability import Immutable, Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig


class Permission(str, Enum):
    SYSADMIN = "sysadmin"
    ADMIN = "admin"
    AUTHENTICATED = "authenticated"


class RoleSeed(BaseModel):
    """A single role to seed at startup.

    Operators add a role by appending a ``RoleSeed`` to either
    ``IamRolesConfig.platform_roles`` (seeded once in the global ``iam``
    schema) or ``IamRolesConfig.catalog_roles`` (seeded per-catalog in
    each tenant schema) via ``PATCH /api/catalog/v2/configs?plugin_id=…``
    — no code change.

    ``parent`` names another role whose policies this role should inherit
    via the role-hierarchy table. Parent inherits child's policies (see
    ``get_role_hierarchy`` in ``postgres_iam_storage.py``); the chain
    flows admin → editor → user → unauthenticated so an authenticated
    ``user`` automatically sees every public policy bound to
    ``unauthenticated``.
    """

    name: Mutable[str] = Field(min_length=1)
    description: str = ""
    policies: Mutable[List[str]] = Field(default_factory=list)
    level: int = 50
    parent: Optional[str] = None


# Consumed by the ``default_roles_baseline`` preset; planned removal in PR-5 when boot-time auto-seed is dropped.
_DEFAULT_PLATFORM_ROLES: List[RoleSeed] = [
    RoleSeed(
        name="sysadmin",
        description="System Administrator with full platform access.",
        policies=["sysadmin_full_access"],
        level=100,
        parent=None,
    ),
]


# Consumed by the ``default_roles_baseline`` preset; planned removal in PR-5 when boot-time auto-seed is dropped.
_DEFAULT_CATALOG_ROLES: List[RoleSeed] = [
    RoleSeed(
        name="admin",
        description="Tenant administrator — manages roles, grants, and members.",
        policies=[],
        level=100,
        parent=None,
    ),
    RoleSeed(
        name="editor",
        description="Catalog editor — creates and updates content.",
        policies=["self_service_access"],
        level=50,
        parent="admin",
    ),
    RoleSeed(
        name="user",
        description="Default role for any authenticated user.",
        policies=["self_service_access"],
        level=10,
        parent="editor",
    ),
    RoleSeed(
        name="unauthenticated",
        description="Read-only floor for anonymous (unauthenticated) requests.",
        policies=["public_access"],
        level=0,
        parent="user",
    ),
]


class IamRolesConfig(PluginConfig):
    """Runtime-editable role landscape, split by tier.

    ``platform_roles`` are seeded once in the global ``iam`` schema and
    are NOT grantable per-catalog (e.g. ``sysadmin``). ``catalog_roles``
    are seeded per-catalog and are the set surfaced by
    ``GET /admin/roles?catalog_id=X`` for the catalog grant UI.

    Operators rename or add roles via the standard PluginConfig
    endpoints; a runtime PATCH propagates without redeploy. Only
    ``sysadmin_role_name`` is load-bearing for external conventions —
    the Keycloak claim mapping in
    :mod:`dynastore.modules.iam.oidc_role_sync_config` references the
    sysadmin role name to grant platform-tier admin to a Keycloak-side
    ``geoid.sysadmin`` realm role.

    ``default_user_role_name`` is the role assigned to a newly-registered
    principal when no realm roles are carried in their token.
    ``anonymous_role_name`` is the role attached to requests that arrive
    with no token at all — kept as an explicit slot so the policy engine
    can treat every request uniformly (every request has at least one
    role, no special-case branch). Per geoid#643, the default catalog-tier
    role for unauthenticated requests is ``"unauthenticated"`` (renamed
    from the historical ``"anonymous"``).
    """

    # Type annotation mirrors PluginConfig._address (Tuple[Optional[str], ...]);
    # this config's tuple has no None slots, but pyright's invariance check on
    # mutable ClassVars rejects narrowing the element type.
    _address: ClassVar[Tuple[Optional[str], ...]] = ("platform", "modules", "iam", "roles")

    sysadmin_role_name: Mutable[str] = Field(
        default="sysadmin",
        description=(
            "Role name granting unrestricted platform access. "
            "Load-bearing: the OIDC role-sync default maps "
            "``geoid.sysadmin`` → this name. Must resolve to a "
            "``platform_roles`` entry."
        ),
    )
    admin_role_name: Mutable[str] = Field(
        default="admin",
        description=(
            "Catalog-tier admin role name (used by extension policy bindings). "
            "Must resolve to a ``catalog_roles`` entry."
        ),
    )
    editor_role_name: Mutable[str] = Field(
        default="editor",
        description=(
            "Catalog-tier content-editor role name (used by extension policy "
            "bindings). Must resolve to a ``catalog_roles`` entry."
        ),
    )
    default_user_role_name: Mutable[str] = Field(
        default="user",
        description=(
            "Role assigned to any signed-in principal with no realm roles. "
            "Must resolve to a ``catalog_roles`` entry."
        ),
    )
    anonymous_role_name: Mutable[str] = Field(
        default="unauthenticated",
        description=(
            "Role attached to requests that arrive without a token. "
            "Renamed from 'anonymous' per geoid#643. Must resolve to a "
            "``catalog_roles`` entry."
        ),
    )
    admin_tier_role_names: Mutable[List[str]] = Field(
        default_factory=lambda: ["sysadmin", "admin"],
        description=(
            "Role names that satisfy a ``Permission.ADMIN`` check. Spans "
            "tiers by design: ``sysadmin`` is platform-tier and ``admin`` "
            "is catalog-tier. Operators add a 'super-editor' role to this "
            "list to grant it admin-tier authority without renaming."
        ),
    )
    platform_roles: Mutable[List[RoleSeed]] = Field(
        default_factory=lambda: list(_DEFAULT_PLATFORM_ROLES),
        description=(
            "Roles seeded once in the platform-global ``iam`` schema. NOT "
            "surfaced as grantable in ``GET /admin/roles?catalog_id=X``. "
            "The seed is idempotent — existing rows are not removed if "
            "dropped from this list."
        ),
    )
    catalog_roles: Mutable[List[RoleSeed]] = Field(
        default_factory=lambda: list(_DEFAULT_CATALOG_ROLES),
        description=(
            "Roles seeded per-catalog in each tenant schema. Surfaced by "
            "``GET /admin/roles?catalog_id=X`` for the grant UI. The seed "
            "is idempotent — existing rows are not removed if dropped "
            "from this list."
        ),
    )

    @property
    def admin_role_set(self) -> frozenset[str]:
        """Frozen set of role names treated as 'platform admin' for
        ``Permission.ADMIN`` checks."""
        return frozenset(self.admin_tier_role_names)

    @property
    def platform_admin_tier_role_set(self) -> frozenset[str]:
        """Subset of ``admin_role_set`` whose names are platform-tier.

        Used by catalog-scope grant/revoke guards: a catalog admin must
        be allowed to assign catalog-tier admin authority to colleagues
        within their own catalog, so the privilege-escalation guard at
        those routes must only block names that grant **platform-tier**
        admin. By default this resolves to ``{"sysadmin"}``.
        """
        return self.admin_role_set & self.platform_role_names

    @property
    def role_names(self) -> List[str]:
        """Names of every seeded role across both tiers — used to filter
        incoming realm_roles and to enumerate the known landscape."""
        return [r.name for r in self.platform_roles] + [
            r.name for r in self.catalog_roles
        ]

    @property
    def platform_role_names(self) -> frozenset[str]:
        """Names of platform-tier roles (not grantable per-catalog).

        Used by ``GET /admin/roles?catalog_id=X`` to filter out
        platform-only roles from the catalog grant UI.
        """
        return frozenset(r.name for r in self.platform_roles)

    @model_validator(mode="after")
    def _validate_hierarchy_and_slots(self) -> "IamRolesConfig":
        platform_names = {r.name for r in self.platform_roles}
        catalog_names = {r.name for r in self.catalog_roles}
        if platform_names & catalog_names:
            raise ValueError(
                f"IamRolesConfig: role name(s) {sorted(platform_names & catalog_names)} "
                f"appear in BOTH platform_roles and catalog_roles. Names must be "
                f"globally unique across tiers."
            )

        # Cycle detection on parent links, per tier.
        def _check_cycles(seeds: List[RoleSeed], tier: str) -> None:
            names = {s.name for s in seeds}
            parent_of = {s.name: s.parent for s in seeds if s.parent}
            for start in names:
                seen = set()
                cur: Optional[str] = start
                while cur is not None:
                    if cur in seen:
                        raise ValueError(
                            f"IamRolesConfig.{tier}_roles contains a cycle "
                            f"through {cur!r}"
                        )
                    seen.add(cur)
                    nxt = parent_of.get(cur)
                    if nxt is not None and nxt not in names:
                        raise ValueError(
                            f"IamRolesConfig.{tier}_roles: role {cur!r} has "
                            f"parent={nxt!r} which is not in {tier}_roles. "
                            f"Cross-tier parents are not allowed."
                        )
                    cur = nxt

        _check_cycles(self.platform_roles, "platform")
        _check_cycles(self.catalog_roles, "catalog")

        # Slot integrity — slots must point at a real role IN THE EXPECTED TIER.
        for slot, value in (("sysadmin_role_name", self.sysadmin_role_name),):
            if value not in platform_names:
                raise ValueError(
                    f"IamRolesConfig.{slot}={value!r} is not present in "
                    f"`platform_roles` ({sorted(platform_names)})"
                )
        for slot, value in (
            ("admin_role_name", self.admin_role_name),
            ("editor_role_name", self.editor_role_name),
            ("default_user_role_name", self.default_user_role_name),
            ("anonymous_role_name", self.anonymous_role_name),
        ):
            if value not in catalog_names:
                raise ValueError(
                    f"IamRolesConfig.{slot}={value!r} is not present in "
                    f"`catalog_roles` ({sorted(catalog_names)})"
                )

        all_names = platform_names | catalog_names
        missing_admin = [n for n in self.admin_tier_role_names if n not in all_names]
        if missing_admin:
            raise ValueError(
                f"IamRolesConfig.admin_tier_role_names contains unknown "
                f"role(s): {missing_admin}"
            )
        return self


class IamScaleConfig(PluginConfig):
    """Runtime-editable IAM scale knobs (counter / quota tier).

    Introduced for the per-binding quota + Valkey-mandatory counter work
    (#1344); extended by #1343 with the zero-DB hot-path knobs (the phantom
    -token resolution tier flag + its cache TTL, and the revocation denylist
    flag + TTL). Only fields that are actually consumed are declared here so
    the Configuration Hub never surfaces a dead knob — the remaining design
    knobs (compiled-rule cache TTL, power-user / HS256 baked-claim split)
    land with the code that consumes them.

    Every field is read per-request via ``get_iam_scale_config`` so a
    runtime PATCH applies on the next request without a redeploy, except
    ``usage_counter_hash_partitions`` which is structural: it shapes the
    ``iam.usage_counters`` table at provisioning time and is read at
    schema-init from the ``IAM_USAGE_COUNTER_HASH_PARTITIONS`` env var
    (the persisted config row is not yet readable that early in boot).
    The field is kept here for visibility and so a future migration can
    repartition an existing flat table; converting an already-flat table
    is out of scope (needs the cleanup migration).
    """

    _address: ClassVar[Tuple[Optional[str], ...]] = ("platform", "modules", "iam", "scale")

    valkey_required: Mutable[bool] = Field(
        default=False,
        description=(
            "When true, the IAM module refuses to start unless a "
            "CountingCacheBackend (Valkey) is active — rate-limit / quota "
            "counters must run on the shared atomic backend, not the "
            "per-pod PG fallback. Production sets this true; dev / test / "
            "single-node deployments leave it false so the standalone PG "
            "counter driver continues to serve. The startup guard reads the "
            "``IAM_VALKEY_REQUIRED`` env var first (the persisted config is "
            "not reliably readable that early on a cold boot); this field is "
            "the fallback and the Configuration-Hub-visible mirror."
        ),
    )
    usage_counter_hash_partitions: Immutable[int] = Field(
        default=1,
        ge=1,
        description=(
            "Number of HASH partitions for the durable ``iam.usage_counters`` "
            "flush sink, keyed by ``principal_key``. 1 (default) keeps the "
            "current flat table. Values > 1 only take effect on a FRESH "
            "table (read at schema-init from the "
            "``IAM_USAGE_COUNTER_HASH_PARTITIONS`` env var); repartitioning "
            "an existing flat table requires a migration."
        ),
    )
    default_rate_limit: Mutable[Optional[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Fallback per-binding rate-limit applied to a grant that "
            "carries no ``quota.rate_limit`` of its own. Shape mirrors the "
            "``rate_limit`` condition config, e.g. "
            "``{\"limit\": 100, \"window_seconds\": 60, \"scope\": "
            "\"principal\"}``. None (default) = no implicit rate-limit."
        ),
    )
    default_quota: Mutable[Optional[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Fallback per-binding lifetime quota applied to a grant that "
            "carries no ``quota.max_count`` of its own. Shape mirrors the "
            "``max_count`` condition config, e.g. ``{\"limit\": 100000, "
            "\"scope\": \"principal\"}``. None (default) = no implicit quota."
        ),
    )
    phantom_token_resolution_enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Master switch for the #1343 phantom-token hot path. When true "
            "AND a distributed CountingCacheBackend (Valkey) is active, "
            "identity→(roles, principal) resolution is served from a shared, "
            "version-keyed Valkey tier instead of re-querying Postgres on "
            "every request: after warm, a validated request costs one token "
            "verify + two Valkey reads (binding version + resolution) and "
            "zero DB round-trips. Keyed by ``(provider, subject_id, schema, "
            "platform_version, catalog_version)`` so a binding mutation that "
            "bumps the version invalidates every pod's cache without pub/sub. "
            "Default false keeps the per-request DB resolution path untouched "
            "(A/B against the live path); the cache is an optimization layer — "
            "on any cache miss or backend error it falls through to the "
            "authoritative DB resolver."
        ),
    )
    binding_resolution_ttl_seconds: Mutable[int] = Field(
        default=300,
        ge=1,
        description=(
            "TTL (seconds) for a phantom-token resolution entry in Valkey. "
            "Bounds staleness for the rare case where a binding mutation fails "
            "to bump the version counter; the version key is the primary "
            "invalidation signal, this TTL is the backstop. Mirrors the 5-min "
            "access-token TTL (D-B)."
        ),
    )
    denylist_enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Enable the Valkey revocation denylist for immediate token / "
            "principal kill ahead of natural token expiry. When true, the "
            "auth path rejects a validated token whose ``jti`` or "
            "``sub`` is present in the denylist. Independent of the phantom "
            "-token tier (a deployment may want revocation without the "
            "resolution cache). Default false. A denylist read error fails "
            "open (logged): the token still expires on its own TTL and a "
            "binding-version bump still invalidates cached bindings, so a "
            "transient Valkey blip must not lock every caller out."
        ),
    )
    denylist_ttl_seconds: Mutable[int] = Field(
        default=300,
        ge=1,
        description=(
            "TTL (seconds) for a denylist entry. Set >= the access-token TTL "
            "so a revoked token cannot outlive its denylist entry; a longer "
            "value is harmless (the token is already expired) but wastes "
            "Valkey memory."
        ),
    )
    compiled_rule_cache_ttl_seconds: Mutable[int] = Field(
        default=60,
        ge=1,
        description=(
            "TTL (seconds) for the per-process compiled-policy cache "
            "(``PolicyService.get_effective_policies``). The cache key already "
            "includes the per-schema binding-version counter, so a writer's "
            "``bump_binding_version`` causes immediate cross-pod invalidation "
            "on the next read — this TTL is the backstop that bounds staleness "
            "if a bump failed to land or a pod missed the version bump (e.g. "
            "the distributed backend was briefly unreachable). 60s mirrors the "
            "L1 tier cap on the resolution cache; lower for stricter freshness "
            "at the cost of more DB list_policies round-trips on cold reads."
        ),
    )
    compiled_rule_cache_maxsize: Mutable[int] = Field(
        default=1024,
        ge=1,
        description=(
            "Per-process maximum number of compiled-policy cache entries. The "
            "cache key is ``(partition_key, schema, rule_version)`` so a "
            "high-multitenant deployment with many catalogs may want a larger "
            "value (memory-cheap — each entry is a ``List[Policy]`` bounded by "
            "the per-partition policy count). Default 1024 matches the rest "
            "of the cache decorator defaults."
        ),
    )


@runtime_checkable
class AuthorizerProtocol(Protocol):
    """Narrow façade used by guards and task-side `require_permission`.

    `check(ctx, perm)` must raise an appropriate error (HTTPException in
    FastAPI surfaces, PermissionError in task surfaces) when the
    permission is denied; returns None on success.
    """

    async def check(self, ctx: SecurityContext, permission: Permission) -> None: ...
