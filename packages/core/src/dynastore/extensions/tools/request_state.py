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
Typed accessors for Starlette request.state fields set by auth middleware.

Replaces scattered ``getattr(request.state, "field", default)`` and
``hasattr(request.state, "field")`` patterns with a single typed helper.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from starlette.requests import Request

if TYPE_CHECKING:
    pass


def get_principal(request: Request) -> Optional[Any]:
    """Return the authenticated Principal, or None."""
    return getattr(request.state, "principal", None)


def get_principal_role(request: Request) -> list[str]:
    """Return the principal's role list, or empty list."""
    return getattr(request.state, "principal_role", [])


def get_catalog_id(request: Request) -> Optional[str]:
    """Return the resolved catalog_id from the request path, or None."""
    return getattr(request.state, "catalog_id", None)


def get_principal_id(request: Request) -> Optional[str]:
    """Return the effective principal ID, or None."""
    return getattr(request.state, "principal_id", None)


def is_policy_allowed(request: Request) -> bool:
    """Return True if the request passed policy evaluation."""
    return getattr(request.state, "policy_allowed", False)


def get_identity(request: Request) -> Optional[Any]:
    """Return the raw identity object from auth middleware, or None."""
    return getattr(request.state, "identity", None)
