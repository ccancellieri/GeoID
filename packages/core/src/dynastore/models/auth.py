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

from typing import Optional, List, Dict, Any, Union, Protocol, runtime_checkable, Literal
from enum import Enum
import re
from uuid import UUID, uuid4
from datetime import datetime, timezone
from pydantic import BaseModel, Field, model_validator, field_validator, PrivateAttr

# --- 1. Action Definitions ---

class Action(str, Enum):
    """Enumeration of standard platform actions."""
    READ = "READ"
    LIST = "LIST"
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    EXECUTE = "EXECUTE"
    SEARCH = "SEARCH"
    PURGE = "PURGE"

# --- 2. Policy & Rule Definitions ---

class Condition(BaseModel):
    """
    ABAC Condition.
    Highly extensible via type/config pattern.
    """
    type: str = Field(..., description="Target handler type (e.g., 'rate_limit', 'query_match', 'and').")
    config: Dict[str, Any] = Field(default_factory=dict, description="Custom parameters for the handler.")

class Policy(BaseModel):
    """
    A granular permission unit.
    Strict typing here ensures database integrity.
    """
    id: str = Field(..., description="Unique slug, e.g., 'collection:read_public'")
    version: str = Field(default="1.0", description="Schema version for future-proofing policy logic.")
    description: Optional[str] = None
    effect: Literal["ALLOW", "DENY"] = "ALLOW"
    
    # "actions" and "resources" are standard IAM patterns
    actions: List[str] = Field(..., description="List of allowed actions, e.g., ['READ', 'LIST', 'STAC:GET']")
    resources: List[str] = Field(default=["*"], description="Regex list for resource targeting, e.g., ['catalogs/A/collections/*']")
    
    # Fine-grained logic
    conditions: List[Condition] = Field(default_factory=list)
    
    partition_key: Optional[str] = Field(default="global", description="Partitioning key for multi-tenant storage.")

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Pre-compiled regex caches (not serialized)
    _compiled_actions: List[re.Pattern] = PrivateAttr(default_factory=list)
    _compiled_resources: List[re.Pattern] = PrivateAttr(default_factory=list)

    def model_post_init(self, __context: Any) -> None:
        self._compiled_actions = [re.compile(p) for p in self.actions]
        self._compiled_resources = [re.compile(p) for p in self.resources]

    def matches_action(self, action: str) -> bool:
        """Test if an action string matches any of this policy's action patterns."""
        return any(p.match(action) for p in self._compiled_actions)

    def matches_resource(self, resource: str) -> bool:
        """Test if a resource path matches any of this policy's resource patterns."""
        return any(p.match(resource) for p in self._compiled_resources)

    @field_validator("effect", mode="before")
    @classmethod
    def validate_effect(cls, v: Any) -> Any:
        """Ensures effect is uppercase 'ALLOW' or 'DENY'."""
        if isinstance(v, str):
            v_upper = v.upper()
            if v_upper in ["ALLOW", "DENY"]:
                return v_upper
        return v

    @field_validator("resources", "actions", mode="before")
    @classmethod
    def transform_wildcards(cls, v: Any) -> Any:
        """Converts simple wildcard '*' to regex '.*' before validation."""
        if isinstance(v, list):
            return [".*" if item == "*" else item for item in v]
        return v

    @field_validator("resources", "actions")
    @classmethod
    def validate_regex(cls, v: List[str]) -> List[str]:
        """Pre-validate regexes and reject patterns vulnerable to ReDoS."""
        _REDOS_PATTERN = re.compile(r"(\(.+\+\)\+|\(.+\*\)\*|\(.+\+\)\*|\(.+\*\)\+)")
        for pattern in v:
            try:
                compiled = re.compile(pattern)
            except re.error:
                raise ValueError(f"Invalid regex in policy pattern: {pattern}")
            if _REDOS_PATTERN.search(pattern):
                raise ValueError(
                    f"Potentially unsafe regex pattern (nested quantifiers): {pattern}"
                )
            if len(pattern) > 512:
                raise ValueError(
                    f"Regex pattern too long ({len(pattern)} chars, max 512): {pattern[:50]}..."
                )
        return v

# --- Protocols ---

@runtime_checkable
class AuthenticationProtocol(Protocol):
    """
    Protocol for modules capable of resolving a Principal from credentials.
    """
    async def resolve_principal(self, credentials: Any) -> Optional['Principal']:
        ...

@runtime_checkable
class AuthorizationProtocol(Protocol):
    """
    Protocol for modules capable of checking permissions.
    """
    async def check_permission(self, principal: 'Principal', action: str, resource: str) -> bool:
        ...

    def register_policy(self, policy: Policy) -> Policy:
        ...

class Principal(BaseModel):
    """
    Runtime Authorization Object (Platform Level).
    Represents the effective permissions of an identity within a specific context.
    MOVED from dynastore.modules.iam.models to here for decoupling.
    """
    id: Optional[Union[UUID, str]] = None
    provider: Optional[str] = "local"
    subject_id: Optional[str] = None
    display_name: Optional[str] = None
    
    # RBAC: Roles merged from global + catalog
    roles: List[str] = Field(default_factory=list)
    
    # ABAC: Custom policies merged from global + catalog
    custom_policies: List[Policy] = Field(default_factory=list)
    
    # Attributes from identity_authorization
    attributes: Dict[str, Any] = Field(default_factory=dict)
    
    # Lifecycle state
    is_active: bool = True
    valid_until: Optional[datetime] = None

    @model_validator(mode='after')
    def sync_id(self):
        if not self.id:
            self.id = uuid4()
        if not self.subject_id and self.display_name:
            self.subject_id = self.display_name
            
        return self

    @property
    def identity(self) -> Dict[str, str]:
        """Return identity dict."""
        return {"provider": self.provider, "sub": self.subject_id}  # type: ignore[return-value]
