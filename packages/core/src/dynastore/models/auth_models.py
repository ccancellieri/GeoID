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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
Authentication and Authorization Models for DynaStore.

This module contains platform-level models for authentication, authorization,
and identity management. These models are used across multiple modules and
extensions.

Models:
- Role: Role definitions with hierarchy support
- IdentityLink: Links external identities to principals
- IdentityAuthorization: Authorization metadata for identities (IAG v2.1)
- RefreshToken: Refresh token model
- TokenResponse, TokenExchangeRequest: Token exchange models
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, Dict, Any, List, Literal, Union, Annotated
from uuid import UUID, uuid4
from enum import Enum
from datetime import datetime, timezone
import re

from dynastore.models.auth import Principal, Policy, Condition

# --- Constants ---
SYSTEM_USER_ID = "system:platform"

# --- 1. Policy & Rule Definitions ---

class PolicyBundle(BaseModel):
    """
    A collection of policy statements.
    Used as an embedded policy set in Principals.
    """
    version: str = "2024-01-01"
    statements: List[Policy] = Field(default_factory=list)


# --- 2. Dynamic Role Definitions ---

class Role(BaseModel):
    """
    A reusable collection of Policies.
    Stored in DB: {schema}.roles
    """
    id: Optional[str] = Field(default=None, description="Unique slug, e.g., 'catalog_editor'")
    name: str
    description: Optional[str] = None
    level: int = 0
    
    # Inheritance: Allows creating "Super Roles" without duplicating policy logic
    parent_roles: List[str] = Field(default_factory=list, description="List of Role IDs this role inherits from")
    
    # Composition: Direct links to policies defined in the system
    policies: List[str] = Field(default_factory=list, description="List of Policy IDs directly assigned")
    
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode='after')
    def sync_id(self):
        if not self.id:
            self.id = self.name
        return self

# --- 3. Identity Authorization (Simplified IAG) ---

class IdentityAuthorization(BaseModel):
    """
    Authorization metadata for an identity (Simplified IAG v2.1).
    Replaces the principals + identity_links indirection.
    Stored in: {schema}.identity_authorization
    
    This model stores authorization state and metadata directly on identities,
    eliminating the need for a separate principals table.
    """
    provider: str = Field(..., description="Identity provider, e.g., 'local', 'keycloak'")
    subject_id: str = Field(..., description="Immutable external ID (sub, email)")
    
    # Authorization metadata
    display_name: Optional[str] = None
    is_active: bool = True
    valid_from: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    valid_until: Optional[datetime] = None
    
    # ABAC attributes
    attributes: Dict[str, Any] = Field(default_factory=dict, description="Custom attributes for ABAC")
    
    # Audit
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    def is_valid(self) -> bool:
        """Check if authorization is currently valid."""
        now = datetime.now(timezone.utc)
        if not self.is_active:
            return False
        if self.valid_from and now < self.valid_from:
            return False
        if self.valid_until and now > self.valid_until:
            return False
        return True



class IdentityLink(BaseModel):
    """
    The Global Anchor.
    Stored in: {schema}.identity_links
    Separates 'Authentication' (IdP) from 'Authorization' (Principal).
    """
    provider: str = Field(..., description="e.g., 'keycloak', 'google', 'api-key'")
    subject_id: str = Field(..., description="Immutable external ID (sub, email, or key hash)")
    principal_id: UUID # Links to the local Principal
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# --- 4. API DTOs ---

class TokenExchangeRequest(BaseModel):
    grant_type: str = "urn:ietf:params:oauth:grant-type:token-exchange"
    subject_token: str
    subject_token_type: str = "urn:ietf:params:oauth:token-type:jwt"
    requested_catalog: Optional[str] = None

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    expires_at: datetime
    expires_in: int = 3600
    principal_id: UUID
    scope: Optional[str] = None

# --- 5. Refresh Tokens ---

class RefreshToken(BaseModel):
    id: str
    key_hash: str
    principal_id: UUID
    family_id: Optional[str] = Field(
        None, description="Token family for rotation tracking. All tokens in a chain share the same family_id."
    )
    is_active: bool = True
    expires_at: datetime
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
