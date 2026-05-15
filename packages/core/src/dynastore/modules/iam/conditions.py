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
# 
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

# File: dynastore/modules/iam/conditions.py

import abc
import re
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from pydantic import BaseModel, Field
from starlette.requests import Request

from .models import Condition
from .exceptions import IamError, QuotaExceededError, RateLimitExceededError
from dynastore.models.protocols.usage_counter import UsageCounterProtocol
from dynastore.modules.iam.iam_storage import AbstractIamStorage
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

@dataclass
class EvaluationContext:
    request: Optional[Request]
    storage: AbstractIamStorage
    manager: Optional[Any] = None # IamService
    token_identifier: Optional[str] = None # The ID of the specific Token/Session
    principal_id: Optional[str] = None # The User ID
    path: str = ""
    method: str = ""
    query_params: Optional[Dict[str, str]] = None
    requested_ttl: int = 0 
    schema: str = "catalog" # Default to global/catalog schema
    catalog_id: Optional[str] = None
    extras: Dict[str, Any] = field(default_factory=dict)

class ConditionHandler(abc.ABC):
    @property
    @abc.abstractmethod
    def type(self) -> str: pass

    @abc.abstractmethod
    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        pass

    async def inspect(self, config: Dict[str, Any], ctx: EvaluationContext) -> Optional[Dict[str, Any]]:
        """
        Returns the current status of the condition (e.g., current usage vs limit).
        Returns None if the condition is stateless or not trackable.
        """
        return None

# --- Condition Handlers ---

def _policy_id_for(config: Dict[str, Any], ctx: EvaluationContext) -> Optional[str]:
    """Resolve the owning policy id for a condition's config dict.

    The middleware stores a mapping ``id(config) -> policy_id`` in
    ``ctx.extras["_policy_id_by_config_id"]`` so handlers can namespace
    their counter rows without the middleware mutating
    ``condition.config`` in place (which would leak across requests if
    the Principal is cached). The legacy on-config ``_policy_id`` key
    is honored as a fallback for tests that build conditions by hand.
    """
    mapping = (ctx.extras or {}).get("_policy_id_by_config_id")
    if mapping:
        pid = mapping.get(id(config))
        if pid:
            return pid
    return config.get("_policy_id")


def _principal_key_for(scope: str, ctx: EvaluationContext) -> Optional[str]:
    """Resolve the opaque ``principal_key`` for a condition scope.

    Returns ``None`` when the scope can't be evaluated for the current
    request (anonymous traffic on a ``principal`` scope, missing role,
    etc.) — the handler treats that as fail-open per ``rate_limit``
    semantics (no principal → no rate-limit row to update). Anonymous
    quotas live behind ``scope=client_ip``.
    """
    if scope == "principal":
        return ctx.principal_id
    if scope == "role":
        principal = (ctx.extras or {}).get("principal_obj")
        roles = getattr(principal, "roles", None) if principal else None
        # Use the first role — multi-role principals fall back to a deterministic pick.
        return roles[0] if roles else None
    if scope == "client_ip":
        request = ctx.request
        client = getattr(request, "client", None) if request else None
        host = getattr(client, "host", None) if client else None
        return f"ip:{host}" if host else None
    if scope == "catalog":
        return ctx.catalog_id
    # Unknown scope — log once and fail-open via None.
    logger.warning("rate_limit/max_count: unknown scope %r", scope)
    return None


def _path_method_matches(config: Dict[str, Any], ctx: EvaluationContext) -> bool:
    """Apply ``path_pattern`` / ``methods`` gate; conditions skip non-matching requests."""
    pattern = config.get("path_pattern")
    if pattern and not re.search(pattern, ctx.path or ""):
        return False
    methods = config.get("methods")
    if methods and ctx.method.upper() not in {m.upper() for m in methods}:
        return False
    return True


class RateLimitHandler(ConditionHandler):
    """Per-window rate limit backed by :class:`UsageCounterProtocol`.

    Config keys
    -----------
    * ``limit`` (int, required)         — max hits per window.
    * ``window_seconds`` (int, required) — window width.
    * ``scope`` (str)                   — ``principal`` (default), ``role``,
      ``client_ip``, ``catalog``.
    * ``path_pattern`` (str, optional)   — regex; condition only enforces
      when ``ctx.path`` matches. Other requests pass through.
    * ``methods`` (list[str], optional)  — same allow-list semantics on
      ``ctx.method``.
    * ``mode`` ('graceful'|'strict')    — when no counter Protocol is
      registered, ``graceful`` (default) allows the request,
      ``strict`` fails closed.
    """

    @property
    def type(self) -> str: return "rate_limit"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        if not _path_method_matches(config, ctx):
            return True

        policy_id = _policy_id_for(config, ctx)
        if not policy_id:
            # Caller failed to inject the policy id; without it we can't
            # namespace the counter — allow but log once.
            logger.debug("rate_limit: missing policy_id mapping, skipping")
            return True

        scope = config.get("scope", "principal")
        principal_key = _principal_key_for(scope, ctx)
        if principal_key is None:
            return True  # see _principal_key_for docstring

        counter = get_protocol(UsageCounterProtocol)
        if counter is None:
            mode = config.get("mode", "graceful")
            if mode == "strict":
                raise RateLimitExceededError(
                    "rate_limit condition cannot be enforced (no counter backend)"
                )
            logger.debug("rate_limit: no counter backend; allowing request")
            return True

        limit = int(config.get("limit", 0))
        window = int(config.get("window_seconds", 60))
        if limit <= 0 or window <= 0:
            return True  # misconfigured policy — no enforcement

        _, allowed = await counter.incr_if_below(
            policy_id, principal_key, limit, window_seconds=window
        )
        if not allowed:
            raise RateLimitExceededError(
                f"Rate limit of {limit} requests per {window}s exceeded "
                f"for {scope}={principal_key}."
            )
        return True

    async def inspect(self, config: Dict[str, Any], ctx: EvaluationContext) -> Optional[Dict[str, Any]]:
        policy_id = _policy_id_for(config, ctx)
        scope = config.get("scope", "principal")
        principal_key = _principal_key_for(scope, ctx) if policy_id else None
        limit = int(config.get("limit", 0))
        window = int(config.get("window_seconds", 60))

        used = 0
        counter = get_protocol(UsageCounterProtocol)
        if counter is not None and policy_id and principal_key is not None:
            try:
                used = await counter.get(
                    policy_id, principal_key, window_seconds=window
                )
            except Exception:
                logger.debug("rate_limit inspect failed", exc_info=True)

        # Bucket end = next window boundary after now.
        now_ts = int(datetime.now(timezone.utc).timestamp())
        reset_at = ((now_ts // window) + 1) * window if window > 0 else None
        return {
            "type": "rate_limit",
            "scope": scope,
            "limit": limit,
            "used": used,
            "remaining": max(limit - used, 0),
            "window_seconds": window,
            "reset_at": reset_at,
        }


class MaxCountHandler(ConditionHandler):
    """Lifetime quota backed by :class:`UsageCounterProtocol`.

    Config keys
    -----------
    * ``limit`` (int, required) — max lifetime hits.
    * ``scope`` (str)           — as in :class:`RateLimitHandler`.
    * ``path_pattern`` / ``methods`` (optional) — same gate semantics.
    * ``mode`` ('graceful'|'strict') — same fallback semantics.
    """

    @property
    def type(self) -> str: return "max_count"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        if not _path_method_matches(config, ctx):
            return True

        policy_id = _policy_id_for(config, ctx)
        if not policy_id:
            logger.debug("max_count: missing policy_id mapping, skipping")
            return True

        scope = config.get("scope", "principal")
        principal_key = _principal_key_for(scope, ctx)
        if principal_key is None:
            return True

        counter = get_protocol(UsageCounterProtocol)
        if counter is None:
            mode = config.get("mode", "graceful")
            if mode == "strict":
                raise QuotaExceededError(
                    "max_count condition cannot be enforced (no counter backend)"
                )
            return True

        # Accept either "limit" (new) or "max_count" (legacy config shape).
        limit = int(config.get("limit", config.get("max_count", 0)))
        if limit <= 0:
            return True

        _, allowed = await counter.incr_if_below(
            policy_id, principal_key, limit, window_seconds=None
        )
        if not allowed:
            raise QuotaExceededError(
                f"Lifetime quota of {limit} exceeded for {scope}={principal_key}."
            )
        return True

    async def inspect(self, config: Dict[str, Any], ctx: EvaluationContext) -> Optional[Dict[str, Any]]:
        policy_id = _policy_id_for(config, ctx)
        scope = config.get("scope", "principal")
        principal_key = _principal_key_for(scope, ctx) if policy_id else None
        limit = int(config.get("limit", config.get("max_count", 0)))

        used = 0
        counter = get_protocol(UsageCounterProtocol)
        if counter is not None and policy_id and principal_key is not None:
            try:
                used = await counter.get(
                    policy_id, principal_key, window_seconds=None
                )
            except Exception:
                logger.debug("max_count inspect failed", exc_info=True)

        return {
            "type": "max_count",
            "scope": scope,
            "limit": limit,
            "used": used,
            "remaining": max(limit - used, 0),
        }

class QueryParamHandler(ConditionHandler):
    @property
    def type(self) -> str: return "query_match"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        if not ctx.query_params: return True
        param_key = config.get("param")
        pattern = config.get("pattern")
        if not param_key or not pattern: return True
        
        val = ctx.query_params.get(param_key)
        if val is None: return False
        if not re.fullmatch(pattern, val): return False
        return True

class TimeWindowHandler(ConditionHandler):
    @property
    def type(self) -> str: return "time_window"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        now = datetime.now(timezone.utc)
        
        # Absolute ISO windows
        start_str = config.get("start")
        end_str = config.get("end")
        if start_str:
            try:
                start_dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                if now < start_dt:
                    raise IamError(f"Key is outside of its valid time window (valid from {start_dt.isoformat()}).")
            except ValueError: pass
        if end_str:
            try:
                end_dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                if now > end_dt:
                    raise IamError(f"Key is outside of its valid time window (expired at {end_dt.isoformat()}).")
            except ValueError: pass

        # Hour-based recurring windows
        start_h = config.get("start_hour", 0)
        end_h = config.get("end_hour", 24)
        weekdays_only = config.get("weekdays_only", False)
        
        if weekdays_only and now.weekday() > 4:
            raise IamError("Key is outside of its valid time window (only valid on weekdays).")
        if not (start_h <= now.hour < end_h):
            raise IamError(f"Key is outside of its valid time window (only valid between {start_h}:00 and {end_h}:00 UTC).")
        return True

class TimeExpirationHandler(ConditionHandler):
    @property
    def type(self) -> str: return "expiration"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        exp_str = config.get("expires_at")
        if not exp_str: return True
        try:
            exp_date = datetime.fromisoformat(exp_str.replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            if now > exp_date:
                raise IamError(f"Key expired at {exp_date.isoformat()}.")
        except ValueError:
            logger.error(f"Invalid date format in policy: {exp_str}")
            raise IamError("Invalid expiration date format in policy.")
        return True
    
    async def inspect(self, config: Dict[str, Any], ctx: EvaluationContext) -> Optional[Dict[str, Any]]:
        exp_str = config.get("expires_at")
        return {"type": "expiration", "expires_at": exp_str}

class MaxTokenTTLHandler(ConditionHandler):
    @property
    def type(self) -> str: return "max_token_ttl"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        if ctx.requested_ttl <= 0: return True
        max_ttl = config.get("max_ttl_seconds", 3600)
        if ctx.requested_ttl > max_ttl: return False
        return True

# --- Logical and Support Handlers ---

class AttributeMatchHandler(ConditionHandler):
    """
    Matches an attribute from the context against a value.
    Syntax for attribute: 'query.param_name', 'header.header-name', 'path', 'method', 'principal.id', 'principal.attributes.key'.
    """
    @property
    def type(self) -> str: return "match"

    def _get_value(self, path: str, ctx: EvaluationContext) -> Any:
        try:
            if path.startswith("query."):
                return ctx.query_params.get(path[6:]) if ctx.query_params else None
            elif path.startswith("header."):
                return ctx.request.headers.get(path[7:]) if ctx.request and ctx.request.headers else None
            elif path == "path":
                return ctx.path
            elif path == "method":
                return ctx.method
            elif path == "principal.id":
                return ctx.principal_id
            elif path.startswith("principal.attributes."):
                # We need to access principal object from extras or somewhere
                # For now assume it's in extras['principal_obj']
                p_obj = ctx.extras.get("principal_obj")
                if p_obj and hasattr(p_obj, "attributes"):
                     return p_obj.attributes.get(path[21:])
                return None
            elif path.startswith("extras."):
                return ctx.extras.get(path[7:])
        except Exception:
            return None
        return None

    def _compare(self, actual: Any, operator: str, expected: Any) -> bool:
        if operator == "eq": return str(actual) == str(expected)
        if operator == "neq": return str(actual) != str(expected)
        if operator == "contains": return str(expected) in str(actual)
        if operator == "regex": return bool(re.match(str(expected), str(actual)))
        if operator == "gt": 
            try: return float(actual) > float(expected)
            except Exception: return False
        if operator == "lt":
            try: return float(actual) < float(expected)
            except Exception: return False
        if operator == "in":
            if isinstance(expected, list): return actual in expected
            return actual in str(expected).split(",")
        return False

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        attr_path = config.get("attribute")
        if not attr_path: return True
        
        actual_value = self._get_value(attr_path, ctx)
        if actual_value is None: return False
        
        operator = config.get("operator", "eq")
        expected_value = config.get("value")
        
        return self._compare(actual_value, operator, expected_value)

class LogicalAndHandler(ConditionHandler):
    @property
    def type(self) -> str: return "and"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        conditions_cfg = config.get("conditions", [])
        for c_cfg in conditions_cfg:
            c = Condition(**c_cfg)
            if not await evaluate_condition(c, ctx):
                return False
        return True

class LogicalOrHandler(ConditionHandler):
    @property
    def type(self) -> str: return "or"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        conditions_cfg = config.get("conditions", [])
        if not conditions_cfg: return True
        for c_cfg in conditions_cfg:
            c = Condition(**c_cfg)
            if await evaluate_condition(c, ctx):
                return True
        return False

class LogicalNotHandler(ConditionHandler):
    @property
    def type(self) -> str: return "not"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        c_cfg = config.get("condition")
        if not c_cfg: return True
        c = Condition(**c_cfg)
        return not await evaluate_condition(c, ctx)

class CatalogAdminConditionConfig(BaseModel):
    """Strongly-typed config for ``catalog_admin_required``.

    Every field is operator data — role *names* are foreign keys into
    ``iam.roles`` rows. ``required_roles`` is empty by default so no
    role gains catalog-admin authority unless the policy or operator
    names it explicitly. ``sysadmin_role`` defaults to the seeded
    ``DefaultRole.SYSADMIN`` name; deployments that rename the
    platform super-user role override it.
    """
    required_roles: List[str] = Field(
        default_factory=list,
        description=(
            "Role names admitted at catalog scope. Each is a foreign "
            "key into the iam.roles table. Empty (with bypasses off) "
            "denies everyone."
        ),
    )
    sysadmin_role: str = Field(
        default_factory=lambda: IamRolesConfig().sysadmin_role_name,
        description=(
            "Name of the platform super-user role used by the "
            "sysadmin-bypass. Defaults to the active "
            "``IamRolesConfig.sysadmin_role_name``."
        ),
    )
    allow_platform: bool = Field(
        default=True,
        description="When True, principals with a platform-scope grant bypass.",
    )
    allow_sysadmin: bool = Field(
        default=True,
        description="When True, principals holding ``sysadmin_role`` bypass.",
    )


class CatalogAdminHandler(ConditionHandler):
    """Allow when the principal holds *any of the named roles* in ``ctx.catalog_id``.

    Generic over role names — every role identity is configuration. This
    handler does NOT bake in ``"admin"`` (or any other label); the policy
    that uses it declares which catalog-scope role(s) authorise access
    via ``required_roles`` (or the singular ``required_role`` alias).
    Different policies can gate different operations on different role
    names without any code change here.

    Sibling of :class:`CatalogMembershipHandler` — the membership variant
    grants any-role-in-catalog access (suitable for read-only data
    routes); this variant gates a *named* role (suitable for the
    ``/admin/catalogs/{cat}/...`` mutation surface and any analogous
    catalog-scoped admin/curator/owner endpoints).

    Bypasses are also configurable: operators turn off sysadmin or
    platform-grant bypass by setting ``allow_sysadmin: false`` or
    ``allow_platform: false`` on the policy's condition config. The
    sysadmin role name itself is a config key (``sysadmin_role``,
    default ``DefaultRole.SYSADMIN.value``) so deployments that rename
    the platform-tier super-user role can still wire it through.

    Config keys:
      - ``required_roles`` (list[str]) — catalog-scope role names that
                            authorise access. Any one match is enough.
      - ``required_role`` (str) — singular alias; convenience when the
                            policy gates on exactly one role.
      - ``allow_platform`` (bool, default True) — ``membership.platform``
                            True passes regardless of catalog role.
      - ``allow_sysadmin`` (bool, default True) — sysadmin-role bypass.
      - ``sysadmin_role`` (str, default ``DefaultRole.SYSADMIN.value``)
                            — name of the platform-tier super-user role
                            for the bypass check.

    A policy that supplies neither ``required_roles`` nor ``required_role``
    will deny on the catalog-role match step (sysadmin/platform bypasses
    still apply). This is intentional: the role choice is the policy's
    declaration, not the handler's default.
    """

    @property
    def type(self) -> str:
        return "catalog_admin_required"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        from dynastore.models.protocols.iam_query import IamQueryProtocol
        from dynastore.tools.discovery import get_protocol
        from dynastore.extensions.iam.membership_cache import get_membership_cached

        catalog_id = ctx.catalog_id
        if not catalog_id:
            return False
        principal = (ctx.extras or {}).get("principal_obj")
        if principal is None:
            return False

        # Parse via Pydantic — accepts the singular ``required_role`` alias
        # by promoting it into ``required_roles`` before validation.
        raw = dict(config or {})
        if "required_roles" not in raw and "required_role" in raw:
            single = raw.pop("required_role")
            raw["required_roles"] = [single] if single else []
        cfg = CatalogAdminConditionConfig.model_validate(raw)

        roles = getattr(principal, "roles", None) or []
        if cfg.allow_sysadmin and cfg.sysadmin_role in roles:
            return True
        provider = getattr(principal, "provider", None)
        subject_id = getattr(principal, "subject_id", None)
        if not provider or not subject_id:
            return False
        iam_query = get_protocol(IamQueryProtocol)
        if iam_query is None:
            return False
        membership = await get_membership_cached(iam_query, provider, subject_id)
        if cfg.allow_platform and membership.get("platform"):
            return True
        if not cfg.required_roles:
            return False
        catalog_roles_for_cat = (membership.get("catalog_roles") or {}).get(catalog_id) or []
        return any(r in catalog_roles_for_cat for r in cfg.required_roles)


class CatalogMembershipHandler(ConditionHandler):
    """Allow only when the principal has grants for ``ctx.catalog_id``.

    Reads ``catalog_id`` from ``EvaluationContext`` (``IamMiddleware`` already
    extracts it from ``/catalogs/X/...`` paths) and the principal object from
    ``ctx.extras["principal_obj"]`` (also set by ``IamMiddleware``). Resolves
    memberships via ``IamQueryProtocol`` through the per-pod cache.

    Sysadmin and platform-grant principals pass transparently — those bypasses
    are part of the IAM model, not a URL-specific check, and remain configurable
    via the ``allow_sysadmin`` / ``allow_platform`` config keys. The sysadmin
    role *name* is itself a config key so deployments that rename the
    platform super-user role can wire it through without code changes.

    Config keys (all optional):
      - ``allow_platform`` (bool, default True) — ``membership.platform``
                            True passes regardless of catalog_id.
      - ``allow_sysadmin`` (bool, default True) — sysadmin-role bypass.
      - ``sysadmin_role`` (str, default ``DefaultRole.SYSADMIN.value``)
                            — name of the platform super-user role for
                            the bypass check.
    """

    @property
    def type(self) -> str:
        return "catalog_membership_required"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        from dynastore.models.protocols.iam_query import IamQueryProtocol
        from dynastore.tools.discovery import get_protocol
        from dynastore.extensions.iam.membership_cache import get_membership_cached

        catalog_id = ctx.catalog_id
        if not catalog_id:
            # path declared per-catalog but extractor failed — fail closed
            return False
        principal = (ctx.extras or {}).get("principal_obj")
        if principal is None:
            # anonymous on a per-catalog policy — fail closed
            return False
        roles = getattr(principal, "roles", None) or []
        sysadmin_role = str(config.get("sysadmin_role", IamRolesConfig().sysadmin_role_name))
        if config.get("allow_sysadmin", True) and sysadmin_role in roles:
            return True
        provider = getattr(principal, "provider", None)
        subject_id = getattr(principal, "subject_id", None)
        if not provider or not subject_id:
            return False
        iam_query = get_protocol(IamQueryProtocol)
        if iam_query is None:
            # IAM query Protocol not registered — fail closed
            return False
        membership = await get_membership_cached(iam_query, provider, subject_id)
        if config.get("allow_platform", True) and membership.get("platform"):
            return True
        return catalog_id in (membership.get("catalogs") or [])


# --- Registry ---

class ConditionRegistry:
    def __init__(self):
        self._handlers: Dict[str, ConditionHandler] = {}
        self.register(RateLimitHandler())
        self.register(MaxCountHandler())
        self.register(QueryParamHandler())
        self.register(TimeWindowHandler())
        self.register(TimeExpirationHandler())
        self._handlers["time_expiration"] = self._handlers["expiration"] # Alias
        self.register(MaxTokenTTLHandler())
        self.register(AttributeMatchHandler())
        self.register(LogicalAndHandler())
        self.register(LogicalOrHandler())
        self.register(LogicalNotHandler())
        self.register(CatalogMembershipHandler())
        self.register(CatalogAdminHandler())
        # Filter inspection framework (geospatial, temporal, etc.)
        from dynastore.modules.iam.filter_inspectors import filter_handler
        self.register(filter_handler)

    def register(self, handler: ConditionHandler):
        self._handlers[handler.type] = handler

    async def evaluate_all(self, conditions: List[Condition], ctx: EvaluationContext) -> bool:
        if not conditions: return True
        for condition in conditions:
            handler = self._handlers.get(condition.type)
            if not handler:
                logger.warning(f"Unknown condition type: {condition.type}. Skipping.")
                continue
            allowed = await handler.evaluate(condition.config, ctx)
            if not allowed: return False
        return True
    
    async def inspect_all(self, conditions: List[Condition], ctx: EvaluationContext) -> List[Dict[str, Any]]:
        results = []
        if not conditions: return results
        
        for condition in conditions:
            handler = self._handlers.get(condition.type)
            if handler:
                status = await handler.inspect(condition.config, ctx)
                if status:
                    results.append(status)
        return results

condition_registry = ConditionRegistry()

# --- Legacy Aliases ---
ConditionManager = ConditionRegistry
condition_manager = condition_registry

async def evaluate_condition(condition: Condition, ctx: EvaluationContext) -> bool:
    """Helper to evaluate a single condition."""
    return await condition_registry.evaluate_all([condition], ctx)

async def evaluate_conditions(conditions: List[Condition], ctx: EvaluationContext) -> bool:
    """Helper to evaluate a list of conditions."""
    return await condition_registry.evaluate_all(conditions, ctx)

def register_condition_handler(handler: ConditionHandler):
    """
    Public SPI to register custom condition handlers.
    Example:
        from dynastore.modules.iam.conditions import register_condition_handler, ConditionHandler
        class MyHandler(ConditionHandler): ...
        register_condition_handler(MyHandler())
    """
    condition_registry.register(handler)