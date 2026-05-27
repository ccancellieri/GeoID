#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

from datetime import datetime
from typing import Any, Dict, Optional, List
from uuid import UUID

from pydantic import BaseModel, Field


# --- Principal request DTOs ---
# Response shape is `PrincipalResponse` in dynastore.models.protocols.policies.

class PrincipalCreate(BaseModel):
    username: str
    password: Optional[str] = None
    email: Optional[str] = None
    roles: List[str] = Field(default_factory=list)
    provider: str = "local"
    subject_id: Optional[str] = None


class PrincipalUpdate(BaseModel):
    email: Optional[str] = None
    is_active: Optional[bool] = None
    roles: Optional[List[str]] = None


# --- Role / Policy wire DTOs live in dynastore.models.protocols.policies. ---


class UsageRow(BaseModel):
    """One row of usage-counter state returned by ``GET /admin/policies/{id}/usage``."""

    principal_key: str
    count: int
    window_start: str
    expires_at: Optional[str] = None
    last_seen_at: Optional[str] = None


class UsagePage(BaseModel):
    """Paged listing of usage-counter rows for a given policy."""

    policy_id: str
    rows: List[UsageRow]
    next_offset: Optional[int] = None


class UsageResetResponse(BaseModel):
    policy_id: str
    principal_key: str
    reset_count: int


# --- Catalog provisioning view models ---

class ProvisioningTaskView(BaseModel):
    task_id: UUID
    status: str
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CatalogProvisioningView(BaseModel):
    catalog_id: str
    physical_schema: Optional[str] = None
    provisioning_status: str
    task: Optional[ProvisioningTaskView] = None


# --- Per-binding live counter view (#1342 / #1346) ---
#
# Returned by ``GET /admin/iam/usage/grants``. Each entry maps one
# ``grants`` row (the binding) to its live counter state read from the
# Valkey/PG layered counter — keyed by ``quota_namespace(grant.id)``
# = ``f"grant:{grant.id}"`` (see :func:`dynastore.modules.iam.scale_config`).
# Counter rows are scoped to the principal that actually triggers the
# quota (``scope=principal`` — the binding's ``subject_ref``); ``limit``
# / ``window_seconds`` are echoed from the grant's ``quota`` JSONB so the
# UI doesn't have to re-parse the spec. ``remaining`` is derived
# (``max(0, limit - count)``); ``window_start`` is computed from the
# current bucket alignment so the operator can see when the live counter
# resets (omitted for lifetime quotas).


class GrantRateLimitCounter(BaseModel):
    """Live state for a rate-limit condition on one binding."""

    count: int = Field(description="Current hits in the active window.")
    limit: int = Field(description="Max hits allowed per window (from grant.quota).")
    window_seconds: int = Field(
        description="Window width in seconds — counter rolls every N seconds.",
    )
    remaining: int = Field(
        description="``max(0, limit - count)`` — convenience for the UI.",
    )
    window_start: Optional[str] = Field(
        default=None,
        description=(
            "ISO 8601 timestamp of the current bucket's start, derived from "
            "``bucket_for(window_seconds)``. ``None`` only when the counter "
            "shape doesn't carry a window."
        ),
    )


class GrantMaxCountCounter(BaseModel):
    """Live state for a lifetime-quota (``max_count``) condition on one binding."""

    count: int = Field(description="Lifetime accumulated hits (never resets unless reset).")
    limit: int = Field(description="Max lifetime hits allowed (from grant.quota).")
    remaining: int = Field(description="``max(0, limit - count)``.")


class GrantUsageCounters(BaseModel):
    """The pair of counter views attached to a binding (either may be None)."""

    rate_limit: Optional[GrantRateLimitCounter] = None
    max_count: Optional[GrantMaxCountCounter] = None


class GrantUsageEntry(BaseModel):
    """One binding row and its live counter state."""

    grant_id: str
    subject_kind: str = Field(description="``principal`` (direct grant) or ``role``.")
    subject_ref: str = Field(description="Principal id or role name on the binding's subject side.")
    object_kind: str = Field(description="``role`` or ``policy`` — what's bound.")
    object_ref: str = Field(description="Role name or policy id.")
    effect: str = Field(description="``allow`` or ``deny``.")
    resource_kind: Optional[str] = Field(
        default=None,
        description="``collection`` / ``item`` / ... when scoped to a resource.",
    )
    resource_ref: Optional[str] = None
    quota_spec: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Echo of the grant's ``quota`` JSONB. ``None`` when the binding "
            "carries no per-binding quota — counters will then reflect any "
            "platform-default rate_limit / max_count from "
            ":class:`IamScaleConfig`."
        ),
    )
    counters: GrantUsageCounters = Field(
        default_factory=GrantUsageCounters,
        description="Live counter state for this binding.",
    )
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None


class GrantUsageView(BaseModel):
    """Wire response for ``GET /admin/iam/usage/grants``."""

    principal_id: str
    catalog_id: Optional[str] = None
    entries: List[GrantUsageEntry] = Field(default_factory=list)
    valkey_available: bool = Field(
        description=(
            "False when the Valkey hot tier was unreachable; figures may be "
            "stale (served from the PG durability tier) but are still "
            "valid for inspection. True when the live Valkey counters "
            "were the source of every entry."
        ),
    )
    fetched_at: str = Field(description="ISO 8601 timestamp the response was assembled.")


# --- Applied-presets bulk list (#1425) ------------------------------------


class AppliedRowResponse(BaseModel):
    """One row from ``iam.applied_presets`` for a given scope."""

    preset_name: str
    scope_key: str
    state: str
    applied_at: Optional[str] = Field(
        default=None,
        description="ISO 8601 timestamp when the preset reached 'applied' state.",
    )
    applied_by: Optional[str] = Field(
        default=None, description="UUID of the principal that triggered the apply."
    )
    params_snapshot: Optional[Dict[str, Any]] = Field(
        default=None, description="Parameter snapshot captured at apply time."
    )
    last_error: Optional[str] = Field(
        default=None, description="Last error message when state is 'failed' or 'revoke_failed'."
    )
    updated_at: Optional[str] = Field(
        default=None, description="ISO 8601 timestamp of the last state-machine transition."
    )


class AppliedPresetsPage(BaseModel):
    """Paginated response for ``GET /admin/presets/applied``."""

    rows: List[AppliedRowResponse]
    next_cursor: Optional[str] = Field(
        default=None,
        description=(
            "Opaque keyset cursor. Pass as ``cursor`` on the next request "
            "to retrieve the following page. ``null`` when this is the last page."
        ),
    )
