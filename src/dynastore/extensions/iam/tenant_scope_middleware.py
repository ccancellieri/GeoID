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

"""Global tenant-scope authorization middleware.

Runs after ``IamMiddleware`` (which populates ``request.state.principal``
and ``principal_role``). For URLs matching a rule in
``tenant_scope_registry.TENANT_SCOPED_ROUTES``:

  - resolves the principal's catalog memberships via ``IamQueryProtocol``
    (cached, per-pod L1) and pins them to ``request.state.authorized_catalogs``;
  - extracts the requested catalog id from the rule's source (default:
    ``?catalog_id=`` query param);
  - short-circuits with 401/403 + structured ``dashboard_authz.denied``
    log when the caller may not see that catalog.

Routes themselves carry zero authz wiring. The gate lives entirely in
middleware, satisfying the project's "no inline authz / no Depends-injected
authz in route definitions" rule.
"""

from __future__ import annotations

import logging
from re import error as RegexError
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Dict, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from dynastore.extensions.iam.membership_cache import get_membership_cached as _get_membership
from dynastore.models.protocols.authorization import IamRoleConfig
from dynastore.models.protocols.iam_query import IamQueryProtocol
from dynastore.tools.discovery import get_protocol

from .tenant_scope_registry import (
    TenantScopeRule,
    match_tenant_scope_rule,
)

logger = logging.getLogger(__name__)

PLATFORM_CATALOG_ID = "_system_"


def _caller_id(principal: Optional[Any]) -> str:
    if principal is None:
        return "anonymous"
    return (
        f"{getattr(principal, 'provider', '?') or '?'}"
        f":{getattr(principal, 'subject_id', '?') or '?'}"
    )


def _log_denial(
    reason: str, *, caller_id: str, catalog_id: str,
) -> None:
    """Structured WARNING for denied dashboard requests.

    Token material is never logged — only the resolved provider/subject_id
    and the denial reason. Cloud Logging string-indexes ``dashboard_authz.denied``
    cleanly without any new dependency.
    """
    logger.warning(
        "dashboard_authz.denied caller=%s catalog=%s reason=%s",
        caller_id, catalog_id, reason,
    )


def _extract_from_source(
    request: Request,
    rule: TenantScopeRule,
    source: str,
    default: Optional[str] = None,
) -> Optional[str]:
    """Resolve a value from one of the supported sources.

    ``regex_group:NAME`` reads from a named capture group on the rule's
    pre-compiled pattern by re-running ``rule.pattern.match`` against the
    request path. ``request.path_params`` is intentionally NOT used —
    Starlette ``BaseHTTPMiddleware`` runs BEFORE FastAPI route matching, so
    path_params is empty at this layer.
    """
    src_kind, _, src_key = source.partition(":")
    if src_kind == "query":
        return request.query_params.get(src_key, default)
    if src_kind == "header":
        return request.headers.get(src_key, default)
    if src_kind == "regex_group":
        match = rule.pattern.match(request.url.path)
        if match is None:
            return default
        try:
            value = match.group(src_key)
        except (IndexError, RegexError):
            return default
        return value if value else default
    if src_kind == "path":
        # ``BaseHTTPMiddleware`` runs before FastAPI route matching;
        # ``request.path_params`` is empty here. Documented as a no-op so
        # callers see the default rather than an AttributeError.
        return default
    if src_kind == "default":
        # Pin the resolved scope to the literal value after the colon —
        # used by platform-tier rules whose URL never carries a catalog id.
        return src_key or default
    return default


def _extract_catalog_id(request: Request, rule: TenantScopeRule) -> str:
    return (
        _extract_from_source(request, rule, rule.catalog_source, rule.default)
        or rule.default
    )


def _extract_collection_id(
    request: Request, rule: TenantScopeRule,
) -> Optional[str]:
    """Read collection_id when the rule declares a ``collection_source``.

    Returns ``None`` when the rule has no collection scope (catalog-only rule)
    or when the URL doesn't carry a collection segment. Used only to pin
    ``request.state.tenant_scope.collection_id`` for downstream Protocol
    consumers — gating decisions are catalog-only.
    """
    source = getattr(rule, "collection_source", None)
    if not source:
        return None
    return _extract_from_source(request, rule, source, default=None)


class TenantScopeMiddleware(BaseHTTPMiddleware):
    """Gate per-catalog dashboard routes against the principal's grants."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        rule = match_tenant_scope_rule(request.url.path)
        if rule is None:
            return await call_next(request)

        principal = getattr(request.state, "principal", None)
        roles_raw = getattr(request.state, "principal_role", None) or []
        if isinstance(roles_raw, str):
            roles = [roles_raw]
        elif isinstance(roles_raw, (list, tuple, set)):
            roles = [str(r) for r in roles_raw]
        else:
            roles = []

        catalog_id = _extract_catalog_id(request, rule)
        collection_id = _extract_collection_id(request, rule)
        # Pin scope early so downstream Protocol consumers can read it
        # regardless of which gate branch the request takes.
        request.state.tenant_scope = SimpleNamespace(
            catalog_id=catalog_id, collection_id=collection_id,
        )
        caller_id = _caller_id(principal)

        if principal is None:
            if not rule.allow_anonymous:
                _log_denial(
                    "anonymous", caller_id=caller_id, catalog_id=catalog_id,
                )
                return JSONResponse(
                    {"detail": "Authentication required"}, status_code=401,
                )
            return await call_next(request)

        # Sysadmin label-based bypass — fast path that avoids the IAM round
        # trip when the role hierarchy already expanded to sysadmin. Role
        # name read from active IamRoleConfig so deployments that rename
        # the platform super-user role wire it through without code edits.
        if IamRoleConfig().sysadmin in roles:
            return await call_next(request)

        iam_query = get_protocol(IamQueryProtocol)
        if iam_query is None:
            _log_denial(
                "iam_unavailable", caller_id=caller_id, catalog_id=catalog_id,
            )
            return JSONResponse(
                {"detail": "IAM service unavailable"}, status_code=503,
            )

        provider = getattr(principal, "provider", None)
        subject_id = getattr(principal, "subject_id", None)
        if not provider or not subject_id:
            _log_denial(
                "identity_not_resolvable",
                caller_id=caller_id, catalog_id=catalog_id,
            )
            return JSONResponse(
                {"detail": "Identity not resolvable"}, status_code=403,
            )

        membership = await _get_membership(iam_query, provider, subject_id)
        # Pin scope so downstream Protocols / handlers can read it without
        # round-tripping IAM again.
        request.state.authorized_catalogs = membership

        # Membership-based platform bypass (custom platform roles whose label
        # didn't expand to sysadmin still carry platform=True via grants).
        if membership.get("platform"):
            return await call_next(request)

        if catalog_id == PLATFORM_CATALOG_ID:
            _log_denial(
                "non_sysadmin_for_system",
                caller_id=caller_id, catalog_id=catalog_id,
            )
            return JSONResponse(
                {"detail": "Platform-scope dashboard requires sysadmin"},
                status_code=403,
            )

        if catalog_id not in membership.get("catalogs", []):
            _log_denial(
                "not_in_memberships",
                caller_id=caller_id, catalog_id=catalog_id,
            )
            return JSONResponse(
                {"detail": "Not authorized for this catalog"}, status_code=403,
            )

        return await call_next(request)
