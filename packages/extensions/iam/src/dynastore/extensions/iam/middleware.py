#    Copyright 2025 FAO
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
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

# File: dynastore/extensions/iam/middleware.py

import time
import jwt
import logging
from typing import Any, Awaitable, Callable, List, Optional

from dynastore.tools.discovery import get_protocol

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from dynastore.modules.iam.conditions import condition_manager, EvaluationContext
from dynastore.models.protocols.stats import StatsProtocol
from dynastore.models.protocols.policies import PermissionProtocol
from dynastore.models.protocols.authentication import AuthenticatorProtocol
from dynastore.models.protocols.authorization import IamRoleConfig
from dynastore.modules.iam.models import PolicyBundle

logger = logging.getLogger(__name__)


class IamMiddleware(BaseHTTPMiddleware):
    _iam_manager: Optional[AuthenticatorProtocol] = None
    _policy_service: Optional[PermissionProtocol] = None

    # Sentinel role name configuration.
    #
    # ``catalog_admin_role`` is the catalog-tier role *checked* for
    # presence in the caller's catalog grants — it's a foreign key into
    # ``iam.roles`` like any other role name.
    #
    # ``catalog_admin_sentinel`` is the role name *added* to the
    # principal's flat role list when the check passes. It exists so
    # platform-tier policies can bind to a name distinct from "admin",
    # which keeps catalog-only admins from accidentally inheriting
    # platform-tier authority through identically-named bindings.
    #
    # Both are class attributes so subclasses (or test harnesses) can
    # rename them without touching the dispatch logic. ``catalog_admin_role``
    # defaults to the active IamRoleConfig's admin name so env-renamed
    # deployments derive the catalog sentinel from their own role names.
    @staticmethod
    def _default_catalog_admin_role() -> str:
        return IamRoleConfig().admin

    catalog_admin_role: str = ""
    catalog_admin_sentinel: str = "catalog_admin"

    def __init__(self, app, **kwargs):
        super().__init__(app)
        self._iam_manager: Optional[AuthenticatorProtocol] = None
        self._policy_service: Optional[PermissionProtocol] = None

    @property
    def _effective_catalog_admin_role(self) -> str:
        return self.catalog_admin_role or self._default_catalog_admin_role()

    async def _augment_with_catalog_sentinels(
        self,
        principal_role: Optional[List[str]],
        principal_obj: Any,
    ) -> Optional[List[str]]:
        """Append catalog-tier sentinel role(s) to ``principal_role``.

        Idempotent: if the sentinel is already present, returns
        ``principal_role`` unchanged. Returns the input untouched when
        ``principal_obj`` is None (anonymous) or when no
        ``IamQueryProtocol`` is registered (slim deployment).
        """
        if principal_obj is None:
            return principal_role
        provider = getattr(principal_obj, "provider", None)
        subject_id = getattr(principal_obj, "subject_id", None)
        if not provider or not subject_id:
            return principal_role
        try:
            from dynastore.models.protocols.iam_query import IamQueryProtocol
            from dynastore.extensions.iam.membership_cache import (
                get_membership_cached,
            )
            iam_query = get_protocol(IamQueryProtocol)
            if iam_query is None:
                return principal_role
            membership = await get_membership_cached(iam_query, provider, subject_id)
            catalog_roles = membership.get("catalog_roles") or {}
            holds_catalog_admin = any(
                self._effective_catalog_admin_role in (roles or [])
                for roles in catalog_roles.values()
            )
            if not holds_catalog_admin:
                return principal_role
            current = list(principal_role) if principal_role else []
            if self.catalog_admin_sentinel in current:
                return current
            current.append(self.catalog_admin_sentinel)
            return current
        except Exception as e:
            logger.debug("catalog-sentinel augmentation skipped: %s", e)
            return principal_role

    def _emit_audit(
        self, event_type: str, principal_id: str, ip: str, schema: str,
        detail: Optional[dict] = None,
    ) -> None:
        """Fire-and-forget audit event (non-blocking)."""
        import asyncio
        storage = getattr(self._iam_manager, "storage", None)
        if storage and hasattr(storage, "log_audit_event"):
            try:
                asyncio.get_running_loop().create_task(
                    storage.log_audit_event(
                        event_type=event_type,
                        principal_id=principal_id,
                        ip_address=ip,
                        detail=detail,
                        schema=schema,
                    )
                )
            except RuntimeError:
                pass

    def lazy_init_manager(self) -> bool:
        """Returns True if IAM is available, False if this scope runs without IAM."""
        if self._iam_manager is None:
            authenticator = get_protocol(AuthenticatorProtocol)
            if not authenticator:
                logger.debug(
                    "AuthenticatorProtocol not registered — IamMiddleware running in pass-through mode."
                )
                return False

            policy_service = get_protocol(PermissionProtocol)
            if not policy_service:
                logger.warning(
                    "PermissionProtocol not registered — IamMiddleware running in pass-through mode."
                )
                return False

            self._iam_manager = authenticator
            self._policy_service = policy_service

        return True

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        start_time = time.time()
        method = request.method
        raw_path = request.url.path
        # Strip API_ROOT_PATH so policy patterns are relative to the service root
        root_path = request.scope.get("root_path", "") or ""
        if root_path and root_path != "/" and raw_path.startswith(root_path):
            path = raw_path[len(root_path):] or "/"
        else:
            path = raw_path

        query_params = dict(request.query_params)
        query_params_tuple = tuple(sorted(query_params.items()))

        client_ip = request.client.host if request.client else "unknown"

        # Initialize identity variables
        principal_role = None
        principal_obj = None

        context_extras = {
            "client_ip": client_ip,
            "user_agent": request.headers.get("user-agent", ""),
            "referer": request.headers.get("referer", ""),
            "principal_obj": principal_obj,
        }

        if self._iam_manager is None:
            if not self.lazy_init_manager():
                return await call_next(request)

        catalog_id = getattr(request.state, "catalog_id", None)
        if not catalog_id:
            # Simple path-based extraction heuristic for common patterns
            if "/catalogs/" in path:
                parts = path.split("/")
                try:
                    idx = parts.index("catalogs")
                    if idx + 1 < len(parts):
                        catalog_id = parts[idx + 1]
                        request.state.catalog_id = catalog_id
                except ValueError:
                    pass

        # Resolve physical schema
        schema = await self._iam_manager.resolve_schema(catalog_id)  # type: ignore[union-attr,attr-defined]

        # 1. Authenticate and get Principal
        (
            principal_role,
            principal_obj,
        ) = await self._iam_manager.authenticate_and_get_role(request)  # type: ignore[union-attr]

        # 1b. Derive catalog-tier sentinel role(s).
        #
        # ``authenticate_and_get_role`` returns *platform-tier* roles only.
        # A principal whose only authority is "admin in catalog X" therefore
        # carries an empty role list, so policy bindings (which match by
        # role name) never apply to them. Augment by checking catalog
        # memberships and adding sentinel role names that policies can bind
        # to without conflating with platform-tier role names.
        #
        # Sentinel rule (default): if the caller holds the configured
        # ``catalog_admin_role`` ("admin" by default) in any catalog,
        # augment with the configured ``catalog_admin_sentinel`` ("catalog_admin"
        # by default). This is the only role name added — viewer/user
        # catalog-tier grants do NOT bleed into the flat list because that
        # could over-grant on platform-tier policies that happen to bind
        # those names (e.g. ``web_admin_access`` bound to "user").
        principal_role = await self._augment_with_catalog_sentinels(
            principal_role, principal_obj,
        )

        request.state.principal_role = principal_role
        request.state.principal = principal_obj

        # 2. Extract Identity Metadata from JWT
        token_str = self._iam_manager.extract_token_from_request(request)  # type: ignore[union-attr]

        token_identifier = None
        source = "unauthenticated"

        if token_str:
            source = "JWT"
            try:
                unverified_payload = jwt.decode(
                    token_str, options={"verify_signature": False}
                )
                token_identifier = unverified_payload.get("jti")
            except jwt.InvalidTokenError:
                pass

        cfg = IamRoleConfig()
        if principal_role and cfg.sysadmin in principal_role:
            source = cfg.sysadmin

        effective_principal_id = (
            (principal_obj.display_name or principal_obj.subject_id)
            if principal_obj
            else (principal_role[0] if principal_role else cfg.anonymous)
        )
        request.state.principal_id = effective_principal_id

        # 3. Build Evaluation Context
        ctx = EvaluationContext(
            request=request,
            storage=self._iam_manager.storage,  # type: ignore[union-attr,attr-defined]
            manager=self._iam_manager,
            token_identifier=token_identifier,
            principal_id=effective_principal_id,
            path=path,
            method=method,
            query_params=query_params,
            schema=schema,
            catalog_id=catalog_id,
            extras=context_extras,
        )

        # 4. Aggregating Policies (The Hierarchy)
        all_conditions = []

        # A. Token/Principal Policy (The "User")
        if principal_obj and principal_obj.custom_policies:
            p_policy = PolicyBundle(statements=principal_obj.custom_policies)
            if not self._policy_service.evaluate_policy_statements(p_policy, method, path):  # type: ignore[union-attr,arg-type]
                return JSONResponse(
                    {"detail": "Access denied by Principal policy."}, status_code=403
                )
            # Add Conditions
            for p in principal_obj.custom_policies:
                if p.conditions:
                    all_conditions.extend(p.conditions)

        # C. Global System Policies
        # Include both the Identifier and the Roles for policy matching
        principals_to_check = [effective_principal_id] + (
            principal_role
            if isinstance(principal_role, list)
            else ([principal_role] if principal_role else [])
        )

        result = await self._policy_service.evaluate_access(  # type: ignore[union-attr]
            principals=principals_to_check,
            path=path,
            method=method,
            request_context=ctx,
            catalog_id=catalog_id,
        )
        allowed_by_global, reason = result if result is not None else (True, "")
        if not allowed_by_global:
            logger.debug(f"Access denied by Global Security policy: {reason}")
            self._emit_audit(
                "authz_denied", effective_principal_id, client_ip, schema,
                {"path": path, "method": method, "reason": reason},
            )
            return JSONResponse(
                {"detail": f"Access denied by Global Security policy: {reason}"},
                status_code=403,
            )

        request.state.policy_allowed = True

        # 5. Evaluate All Accumulated Conditions (from Key and Principal)
        if all_conditions:
            from dynastore.modules.iam.conditions import evaluate_conditions

            if not await evaluate_conditions(all_conditions, ctx):
                logger.warning(
                    f"Rate Limit/Quota Exceeded for '{effective_principal_id}' via '{source}'"
                )
                return JSONResponse(
                    {"detail": "Rate limit or Quota exceeded."}, status_code=429
                )

        response = await call_next(request)

        # Stats Logging (Best Effort)
        try:
            processing_time_ms = (time.time() - start_time) * 1000
            details = {
                "auth_user": effective_principal_id,
                "auth_source": source,
            }
            # 6. Log completion metrics
            stats_service = get_protocol(StatsProtocol)
            if stats_service:
                stats_service.log_request_completion(
                    request=request,
                    status_code=response.status_code,
                    processing_time_ms=processing_time_ms,
                    details=details,
                    catalog_id=catalog_id,
                )
        except Exception as e:
            logger.error(f"Stats error: {e}")

        return response
