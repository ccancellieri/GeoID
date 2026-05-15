#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

from typing import Literal, Optional, List
from pydantic import BaseModel, Field


# --- User models ---

class UserCreate(BaseModel):
    username: str
    password: Optional[str] = None
    email: Optional[str] = None
    roles: List[str] = Field(default_factory=list)
    provider: str = "local"
    subject_id: Optional[str] = None


class UserUpdate(BaseModel):
    email: Optional[str] = None
    is_active: Optional[bool] = None
    roles: Optional[List[str]] = None


class UserResponse(BaseModel):
    id: str
    username: str
    email: Optional[str] = None
    is_active: bool = True
    roles: List[str] = Field(default_factory=list)


# --- Role models ---

class RoleCreate(BaseModel):
    name: str
    description: Optional[str] = None
    policies: List[str] = Field(default_factory=list)
    parent_roles: List[str] = Field(default_factory=list)


class RoleUpdate(BaseModel):
    description: Optional[str] = None
    policies: Optional[List[str]] = None
    parent_roles: Optional[List[str]] = None


class RoleResponse(BaseModel):
    name: str
    description: Optional[str] = None
    policies: List[str] = Field(default_factory=list)
    parent_roles: List[str] = Field(default_factory=list)


# --- Policy models ---

class PolicyCreate(BaseModel):
    id: str
    description: Optional[str] = None
    actions: List[str]
    resources: List[str]
    effect: Literal["ALLOW", "DENY"] = "ALLOW"


class PolicyUpdate(BaseModel):
    description: Optional[str] = None
    actions: Optional[List[str]] = None
    resources: Optional[List[str]] = None
    effect: Optional[Literal["ALLOW", "DENY"]] = None


class PolicyResponse(BaseModel):
    id: str
    description: Optional[str] = None
    actions: List[str]
    resources: List[str]
    effect: Literal["ALLOW", "DENY"]
    partition_key: Optional[str] = None


# --- Principal / assignment models ---

class AssignRoleRequest(BaseModel):
    role: str


class CatalogRoleAssignment(BaseModel):
    catalog_id: str
    role: str


class PrincipalResponse(BaseModel):
    id: str
    provider: Optional[str] = None
    subject_id: Optional[str] = None
    display_name: Optional[str] = None
    roles: List[str] = Field(default_factory=list)
    is_active: bool = True
