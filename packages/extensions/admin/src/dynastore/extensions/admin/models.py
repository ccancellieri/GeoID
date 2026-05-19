#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

from datetime import datetime
from typing import Literal, Optional, List
from uuid import UUID

from pydantic import BaseModel, Field

from dynastore.models.auth import Condition


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


# --- Role wire DTOs live in dynastore.models.protocols.policies. ---

# --- Policy models ---

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
    partition_key: Optional[str] = None
    conditions: List[Condition] = Field(default_factory=list)


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


# --- Principal / assignment models ---
# AssignRoleRequest and PrincipalResponse wire DTOs live in
# dynastore.models.protocols.policies. CatalogRoleAssignment is admin-only
# (no second consumer) and stays here.

class CatalogRoleAssignment(BaseModel):
    catalog_id: str
    role: str


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
