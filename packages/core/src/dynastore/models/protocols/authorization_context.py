#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""
`SecurityContext` — framework-free carrier for authorization state.

Used by both FastAPI guards (extensions/iam/guards.py) and background
tasks (modules/iam/authorization/checks.py). Carries the principal id,
effective role set, whether policy evaluation already succeeded, and
arbitrary contextual attributes.
"""

from dataclasses import dataclass, field
from typing import Any, FrozenSet, Mapping, Optional


@dataclass(frozen=True)
class SecurityContext:
    principal_id: Optional[str] = None
    roles: FrozenSet[str] = field(default_factory=frozenset)
    policy_allowed: bool = False
    attributes: Mapping[str, Any] = field(default_factory=dict)
