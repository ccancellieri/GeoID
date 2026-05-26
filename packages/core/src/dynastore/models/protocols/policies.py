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

from datetime import datetime
from typing import Dict, Protocol, List, Literal, Optional, Tuple, Any, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, Field

# Re-export permission-related models so extensions only need ONE import:
#   from dynastore.models.protocols.policies import PermissionProtocol, Policy, Role, Principal
from dynastore.models.auth import Condition, Policy, Principal  # Policy, Principal, Condition canonical home
from dynastore.models.auth_models import Role               # Role canonical home
# Neutral read-scope projection — see access_filter.py. Re-exported here so a
# consumer needs ONE import alongside PermissionProtocol and never has to reach
# into the IAM module to translate an access decision.
from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    FieldPredicate,
)

__all__ = [
    "PermissionProtocol",
    "Policy",
    "Role",
    "Principal",
    "AccessFilter",
    "AccessClause",
    "FieldPredicate",
    # Wire-schema DTOs for the role/principal/policy management surface.
    # Single canonical home so non-admin consumers (SDK, IAM CLI, other
    # extensions) do not have to import through the admin extension.
    "RoleCreate",
    "RoleUpdate",
    "RoleResponse",
    "AssignRoleRequest",
    "CreateBindingRequest",
    "PrincipalResponse",
    "PolicyCreate",
    "PolicyUpdate",
    "PolicyResponse",
    # Phantom-token denylist admin DTOs (#1343).
    "DenylistEntryRequest",
    "DenylistEntryResponse",
    # Effective-permissions explainer (#1346 backend half).
    "EffectivePermissionRequest",
    "EffectivePermissionResponse",
    "GrantTraceEntry",
    "ConditionTraceEntry",
]


# --- Role wire DTOs ---

class RoleCreate(BaseModel):
    name: str
    description: Optional[str] = None
    policies: List[str] = Field(default_factory=list)


class RoleUpdate(BaseModel):
    description: Optional[str] = None
    policies: Optional[List[str]] = None


class RoleResponse(BaseModel):
    name: str
    description: Optional[str] = None
    policies: List[str] = Field(default_factory=list)


# --- Principal / assignment wire DTOs ---

class AssignRoleRequest(BaseModel):
    role: str


class CreateBindingRequest(BaseModel):
    """A generic IAM binding scoped to a resource (collection / item).

    The resource scope (``resource_kind`` / ``resource_ref``) is taken from
    the route path, not the body, so a request can only ever bind within the
    URL it targets. The body declares *what* is bound (a ``role`` or a
    ``policy``) to *whom* (``principal_id``), with the binding's effect,
    validity window, and optional per-binding ``quota`` (rate-limit /
    lifetime-quota spec consumed by the IAM counter conditions).

    ``principal_id`` is the internal ``principal.id`` UUID, NOT the OIDC
    ``subject_id`` carried on the identity link — the two are different
    fields on :class:`PrincipalResponse` and conflating them returned a
    confusing "Principal not found" 404 (geoid#1399). Resolve the principal
    via ``GET /admin/principals?subject_id=...`` first, then bind by id.
    """

    principal_id: UUID = Field(
        description=(
            "Internal principal UUID that receives the binding (NOT the OIDC "
            "``subject_id``)."
        ),
    )
    object_kind: Literal["role", "policy"] = Field(
        description="Whether the binding grants a role or a direct policy."
    )
    object_ref: str = Field(description="Role name or policy id being bound.")
    effect: Literal["allow", "deny"] = Field(
        default="allow",
        description="allow grants; deny is enforced with deny-precedence.",
    )
    valid_from: Optional[datetime] = Field(
        default=None, description="Binding becomes active at this time (default: now)."
    )
    valid_until: Optional[datetime] = Field(
        default=None, description="Binding expires at this time (default: never)."
    )
    quota: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Per-binding rate-limit / lifetime-quota spec, e.g. "
            "{'rate_limit': {'limit': 100, 'window_seconds': 60}} or "
            "{'max_count': {'limit': 100000}}."
        ),
    )


class PrincipalResponse(BaseModel):
    id: str
    provider: Optional[str] = None
    subject_id: Optional[str] = None
    display_name: Optional[str] = None
    roles: List[str] = Field(default_factory=list)
    is_active: bool = True


# --- Phantom-token denylist admin DTOs (#1343) ---
#
# The denylist is the immediate-revocation companion to the phantom-token
# resolution cache: until the natural access-token TTL elapses, a validated
# token can be killed platform-wide by adding its ``jti`` (or, for a
# principal-wide kill, its ``sub``) to the Valkey denylist. The hot path
# rejects with 401 on every pod within one Valkey ``EXISTS`` round-trip.
# This admin surface lets an operator add / inspect / clear those entries
# without poking the cache directly.


class DenylistEntryRequest(BaseModel):
    """An operator-authored token revocation.

    ``subject`` MUST be one of:

    * ``"jti:<token-id>"`` — revoke one specific token by its ``jti`` claim.
    * ``"principal:<subject-id>"`` — revoke every currently-issued token for
      a principal (subject id, as it appears in the JWT ``sub`` claim).

    ``ttl_seconds`` is clamped server-side to the
    :attr:`IamScaleConfig.denylist_ttl_seconds` ceiling — an operator cannot
    keep a kill alive longer than the configured maximum. ``None`` defaults
    to the ceiling, which is normally aligned with the access-token TTL so
    the entry expires alongside the token it kills.

    ``reason`` is a free-form audit string surfaced by ``GET`` listings;
    it has no enforcement effect.
    """

    subject: str = Field(
        description=(
            "Revocation subject: ``jti:<token-id>`` for a single token "
            "or ``principal:<subject-id>`` for every token of a principal."
        ),
        min_length=5,
    )
    ttl_seconds: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Requested TTL (seconds). Clamped to "
            "``IamScaleConfig.denylist_ttl_seconds``; default = ceiling."
        ),
    )
    reason: Optional[str] = Field(
        default=None,
        max_length=512,
        description="Free-form audit reason surfaced by GET listings.",
    )


class DenylistEntryResponse(BaseModel):
    """An active denylist entry as returned by ``GET /admin/iam/denylist``.

    ``expires_at`` is the absolute Unix epoch second the entry will be
    auto-purged; ``None`` when the backend cannot report per-key TTL.
    """

    subject: str
    reason: Optional[str] = None
    expires_at: Optional[float] = None


# --- Effective-permissions explainer DTOs (#1346 backend half) ---
#
# Diagnostic surface for "why can / can't principal P do action A on
# resource R?". The trace is a BYPRODUCT of the same decision walk
# ``evaluate_access`` runs on the hot path — no parallel evaluator. Every
# field below mirrors a discrete step the walk already takes, so an
# operator reading the response is seeing exactly what the engine saw.
#
# Out of scope here: the frontend "Why?" affordance — ships separately
# (see #1346 follow-up tracker).


class ConditionTraceEntry(BaseModel):
    """One condition evaluated during the decision walk.

    Mirrors a single ``Condition`` (``type`` + ``config``) and reports
    whether the condition handler returned True for THIS principal /
    request context. ``detail`` is a free-form one-liner the handler
    populates for human-readable counters or quota messages
    (e.g. ``"rate 73/100 within window"``); not load-bearing on the
    verdict.
    """

    type: str = Field(description="Condition type (rate_limit, max_count, match, ...).")
    config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Condition spec as authored on the policy.",
    )
    passed: bool = Field(description="Whether this condition admitted the request.")
    detail: Optional[str] = Field(
        default=None,
        description="Optional handler-emitted note (counters, quota state, ...).",
    )


class GrantTraceEntry(BaseModel):
    """One grant the engine considered during the decision walk.

    ``matched=False`` entries are recorded too, so the operator can see
    *why* a binding the principal expected to grant access did not fire.
    ``why_not`` is a one-line reason (method mismatch / resource mismatch
    / condition failed / outside validity window / unsatisfiable
    principal gate / uncompilable). For grant-row-derived entries the
    grant identity (``grant_id``, ``subject_kind``, ``subject_ref``,
    ``object_kind``, ``object_ref``, ``resource_kind``, ``resource_ref``,
    ``valid_from``, ``valid_until``) is set; for policies reached via the
    flat-name ``principals`` path or via ``custom_policies`` the grant
    identity fields default to the policy id and the "policy" object
    kind, with no validity window.
    """

    grant_id: str = Field(
        description=(
            "Grant identifier — the grants-table row id when the policy "
            "was reached via a binding, else the policy id."
        ),
    )
    subject_kind: str = Field(
        description=(
            "Subject side of the binding: ``principal`` (direct grant) "
            "or ``role`` (role-name path). For policies attached via "
            "flat-name resolution this echoes ``role``."
        ),
    )
    subject_ref: str = Field(
        description="Principal id or role name the binding is attached to.",
    )
    object_kind: Literal["role", "policy"] = Field(
        description="What was bound — a role (whose policies fan out) or a policy.",
    )
    object_ref: str = Field(description="Role name or policy id.")
    effect: Literal["allow", "deny"] = Field(
        description="The grant's effect, normalised to lowercase.",
    )
    resource_kind: Optional[str] = Field(
        default=None,
        description="``collection`` / ``item`` / ... when scoped, else None (whole-catalog).",
    )
    resource_ref: Optional[str] = Field(default=None)
    matched: bool = Field(
        description=(
            "Whether the binding's policy admitted the request "
            "(method + resource + conditions all passed)."
        ),
    )
    why_not: Optional[str] = Field(
        default=None,
        description="One-line reason when ``matched=False``.",
    )
    conditions_evaluated: List[ConditionTraceEntry] = Field(
        default_factory=list,
        description="Per-condition trace, in the order the walk evaluated them.",
    )
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None
    in_validity_window: bool = Field(
        default=True,
        description=(
            "False when ``valid_from``/``valid_until`` excluded the grant; "
            "the resolver already drops out-of-window grants before they "
            "reach the walk, so a False entry is a synthetic carry-through "
            "for operator visibility."
        ),
    )


class EffectivePermissionRequest(BaseModel):
    """Inputs to ``GET /admin/iam/effective`` — the verdict to explain.

    ``catalog_id``/``collection_id`` mirror the scope arguments
    ``evaluate_access`` already takes. ``resource_kind``/``resource_ref``
    are forwarded as an explicit narrower scope for resource-scoped
    bindings (collection-scope grants resolve through
    :meth:`resolve_effective_grants` keyed on ``collection_id``; further
    kinds are recorded on the request envelope for symmetry with the
    binding DTO and surface unchanged on the response).
    """

    principal_id: str = Field(description="Subject the verdict is for.")
    catalog_id: Optional[str] = Field(
        default=None,
        description="Catalog scope; ``None`` means the platform plane.",
    )
    collection_id: Optional[str] = Field(default=None)
    action: str = Field(
        description=(
            "Verb being asked about — matches ``evaluate_access``'s "
            "``method`` argument vocabulary (HTTP verb or platform "
            "``Action`` enum value)."
        ),
    )
    resource_kind: Optional[str] = Field(
        default=None,
        description="``collection`` / ``item`` / ``asset`` / ...",
    )
    resource_ref: Optional[str] = Field(default=None)


class EffectivePermissionResponse(BaseModel):
    """Decision + full trace for an effective-permissions probe.

    ``decision`` and ``decision_reason`` are the verdict ``evaluate_access``
    would emit on the hot path for the same request — the trace flag never
    moves them (drift property test asserts this). ``grants_considered``
    lists every policy the engine looked at, matched or not, in the order
    it walked them; ``deny_precedence_applied`` is True when at least one
    DENY matched, regardless of whether a stronger ALLOW exists (the
    ranking step lives below it and may still pick the ALLOW under #915
    priority semantics — see :attr:`decision_reason` for the chosen
    winner). ``compiled_rule_version`` surfaces a cache-tag when the
    evaluator carries one; absent today but reserved so a future cache
    layer can expose staleness without a DTO bump.
    """

    request: EffectivePermissionRequest
    decision: Literal["allow", "deny"]
    decision_reason: str = Field(
        description=(
            "One-line summary of the chosen winner — mirrors the engine's "
            "log line so audit and explainer agree by construction."
        ),
    )
    deny_precedence_applied: bool = Field(
        description="True iff any matching DENY was found during the walk.",
    )
    grants_considered: List[GrantTraceEntry] = Field(
        default_factory=list,
        description=(
            "Full list of policies/bindings the engine walked, in walk "
            "order. Includes non-matching entries with ``why_not`` so "
            "the operator can see what the principal was *expected* to "
            "have."
        ),
    )
    compiled_rule_version: Optional[str] = Field(
        default=None,
        description="Reserved for a future compiled-rule cache version tag.",
    )


# --- Policy wire DTOs ---

class PolicyCreate(BaseModel):
    id: str
    description: Optional[str] = None
    actions: List[str]
    resources: List[str]
    effect: Literal["ALLOW", "DENY"] = "ALLOW"
    priority: int = Field(default=0, ge=-1000, le=1000)
    conditions: List[Condition] = Field(default_factory=list)


class PolicyUpdate(BaseModel):
    description: Optional[str] = None
    actions: Optional[List[str]] = None
    resources: Optional[List[str]] = None
    effect: Optional[Literal["ALLOW", "DENY"]] = None
    priority: Optional[int] = Field(default=None, ge=-1000, le=1000)
    conditions: Optional[List[Condition]] = None


class PolicyResponse(BaseModel):
    id: str
    description: Optional[str] = None
    actions: List[str]
    resources: List[str]
    effect: Literal["ALLOW", "DENY"]
    priority: int = 0
    conditions: List[Condition] = Field(default_factory=list)


@runtime_checkable
class PermissionProtocol(Protocol):
    """Unified protocol for Policy, Role, and permission management/evaluation.

    Extensions register their policies and roles via:
        get_protocol(PermissionProtocol).register_policy(Policy(...))
        get_protocol(PermissionProtocol).register_role(Role(...))
    """

    async def create_policy(
        self, policy: Policy, catalog_id: Optional[str] = None
    ) -> Any: ...

    async def get_policy(
        self, policy_id: str, catalog_id: Optional[str] = None
    ) -> Optional[Policy]: ...

    async def update_policy(
        self, policy: Policy, catalog_id: Optional[str] = None
    ) -> Optional[Policy]: ...

    async def list_policies(
        self, limit: int = 100, offset: int = 0, catalog_id: Optional[str] = None
    ) -> List[Policy]: ...

    async def delete_policy(
        self, policy_id: str, catalog_id: Optional[str] = None
    ) -> bool: ...

    async def search_policies(
        self,
        resource_pattern: str,
        action_pattern: str,
        limit: int = 10,
        offset: int = 0,
        catalog_id: Optional[str] = None,
    ) -> List[Policy]: ...

    async def evaluate_policy_statements(
        self, policy: Policy, method: str, path: str, request_context: Any = None
    ) -> bool: ...

    async def evaluate_access(
        self,
        principals: List[str],
        path: str,
        method: str,
        request_context: Any = None,
        catalog_id: Optional[str] = None,
        custom_policies: Optional[List["Policy"]] = None,
        principal_id: Optional[Any] = None,
        collection_id: Optional[str] = None,
        trace_collector: Optional[Any] = None,
    ) -> Tuple[bool, str]: ...

    async def provision_default_policies(
        self,
        catalog_id: Optional[str] = None,
        conn: Optional[Any] = None,
        schema: Optional[str] = None,
        force: bool = False,
    ) -> None: ...

    async def compile_read_filter(
        self,
        principals: List[str],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        *,
        principal: Optional[Principal] = None,
        principal_id: Optional[Any] = None,
    ) -> "AccessFilter":
        """Compile the read scope for ``principals`` into a neutral filter.

        This is the document-level-security (row-level ABAC) companion to
        :meth:`evaluate_access`. Where ``evaluate_access`` answers "may this
        principal read *this one* resource", ``compile_read_filter`` answers
        "*which* documents in this catalog/collection may this principal read",
        as an :class:`AccessFilter` a storage driver can translate to a native
        predicate (ES ``bool``, SQL ``WHERE``, ...) WITHOUT importing the IAM
        module.

        The result MUST be an equal-or-stricter projection of
        ``evaluate_access`` (deny-precedence preserved; ALLOW → OR branches,
        DENY → negated branches). Conditions that cannot be expressed as an
        index predicate are dropped from the ALLOW side (fail-closed: the
        document is hidden from search but still reachable by a direct GET that
        runs the full engine) and :attr:`AccessFilter.uncompilable` is set. When
        no ALLOW can be compiled the result is
        :meth:`AccessFilter.deny_everything`.

        ``principals`` carries the same role/subject strings ``evaluate_access``
        takes; ``principal`` (optional) supplies ABAC attributes and
        per-principal ``custom_policies`` for compiling attribute conditions.
        """
        ...

    # --- Extension injection points ---

    def register_policy(self, policy: Policy) -> Policy: ...

    def register_role(self, role: Role) -> Role: ...
