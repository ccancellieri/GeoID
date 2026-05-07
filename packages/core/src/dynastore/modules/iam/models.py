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

# File: dynastore/modules/iam/models.py

"""
Backward compatibility re-exports for IAM models.

All models have been moved to dynastore.models.auth_models for better
separation of concerns. This file provides backward-compatible imports.
"""

# Re-export models from dynastore.models.auth_models for backward compatibility
from dynastore.models.auth_models import (
    SYSTEM_USER_ID,
    PolicyBundle,
    Role,
    IdentityAuthorization,
    IdentityLink,
    TokenExchangeRequest,
    TokenResponse,
    RefreshToken,
)

# Re-export from dynastore.models.auth (these were already moved)
from dynastore.models.auth import Principal, Policy, Condition

__all__ = [
    # Constants
    "SYSTEM_USER_ID",
    # Auth models
    "Principal",
    "Policy",
    "Condition",
    # Role and Identity models
    "Role",
    "IdentityAuthorization",
    "IdentityLink",
    "PolicyBundle",
    # Token models
    "RefreshToken",
    "TokenExchangeRequest",
    "TokenResponse",
]