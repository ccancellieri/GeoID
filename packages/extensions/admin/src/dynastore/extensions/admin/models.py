#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

from datetime import datetime
from typing import Optional, List
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


# --- Role / Policy / CatalogRoleAssignment wire DTOs live in
#     dynastore.models.protocols.policies. ---


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
