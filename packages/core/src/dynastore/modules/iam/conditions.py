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
from dynastore.tools.ttl_gate import TTLGate

logger = logging.getLogger(__name__)

# Per-key denial-log throttle. At ≥1000 req/s sustained denials a naive
# WARNING line per request would flood Cloud Logging; the gate emits at
# most one line per (condition_type, policy_id, principal_key) per minute.
# Sized for ~4k distinct (policy, principal) pairs in flight per pod.
_DENIAL_LOG_GATE: TTLGate = TTLGate(maxsize=4096, ttl_seconds=60.0)

# Per-pattern invalid-regex warning throttle.  Emits at most once per unique
# bad pattern per 5 minutes so a mis-typed policy condition doesn't flood logs.
# Uses a plain set + timestamp dict so the warning path stays synchronous and
# captures correctly in both async and sync test contexts.
_INVALID_REGEX_WARNED: dict = {}  # pattern -> last_warned_epoch_seconds
_INVALID_REGEX_WARN_TTL: float = 300.0


def _safe_regex_matches(
    pattern: str, value: str, *, kind: str, fullmatch: bool = False
) -> Optional[bool]:
    """Compile and apply a user-supplied regex; return None on invalid pattern.

    Returns True/False on success.  Returns None when the pattern is
    syntactically invalid, and logs a throttled WARNING naming the condition
    kind and the offending pattern so operators can locate the mis-config.

    Callers treat None as non-match and follow their existing no-match path
    (fail-open for rate_limit/max_count path gates; no-match for query_match
    and attribute-match comparisons).  None is a distinct sentinel: callers
    must check ``result is None`` rather than ``not result``.

    ``fullmatch=True`` uses ``re.fullmatch`` semantics (anchors both ends),
    matching the behaviour of the original ``QueryParamHandler`` call site.
    """
    try:
        fn = re.fullmatch if fullmatch else re.search
        return bool(fn(pattern, value))
    except re.error as exc:
        import time
        now = time.monotonic()
        last = _INVALID_REGEX_WARNED.get(pattern, 0.0)
        if now - last >= _INVALID_REGEX_WARN_TTL:
            _INVALID_REGEX_WARNED[pattern] = now
            logger.warning(
                "invalid_regex condition_kind=%s pattern=%r error=%s — "
                "treating as non-match (fail-open); fix the policy condition",
                kind,
                pattern,
                exc,
            )
        return None


async def _log_usage_counter_denied(
    *,
    condition_type: str,
    policy_id: str,
    principal_key: str,
    scope: str,
    count: int,
    limit: int,
    window_seconds: Optional[int],
) -> None:
    """Emit a structured WARNING on usage-counter denial, throttled per
    ``(condition_type, policy_id, principal_key)`` so a sustained 429
    storm does not flood logs. Format mirrors the ``liveness_reconcile_pass
    service=… RACE_LOST=N`` house style so a single log-based metric can
    alert on the ``usage_counter_denied`` token."""
    key = (condition_type, policy_id, principal_key)
    async with _DENIAL_LOG_GATE.acquire(key) as h:
        if not h.should_run:
            return
        window_part = (
            f"window_seconds={window_seconds}"
            if window_seconds is not None
            else "window_seconds=lifetime"
        )
        logger.warning(
            "usage_counter_denied condition_type=%s policy_id=%s "
            "principal_key=%s scope=%s count=%d limit=%d %s",
            condition_type,
            policy_id,
            principal_key,
            scope,
            count,
            limit,
            window_part,
        )
        h.mark()

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

def _client_ip_from_request(request: Any) -> Optional[str]:
    """Resolve the originating client IP for a ``scope=client_ip`` policy.

    ``request.client.host`` is the proxy/LB address in any deployment
    that doesn't run the app directly behind the public Internet — for
    GeoID that's Cloud Run + the in-front Load Balancer (see
    project_distributed_stateless_runtime). The LB sets
    ``X-Forwarded-For`` to ``<original-client>, <intermediate>, ...`` so
    the leftmost token is the real caller.

    Trust model: XFF is honoured unconditionally. This assumes a trusted
    proxy in front of the app — true for every documented GeoID
    deployment topology. Deployments that don't have a trusted proxy
    must not use ``scope=client_ip`` rate-limit policies (a direct
    client can otherwise spoof the header).
    """
    if request is None:
        return None
    headers = getattr(request, "headers", None)
    if headers is not None:
        try:
            xff = headers.get("x-forwarded-for")
        except Exception:
            xff = None
        if xff:
            # Leftmost token, stripped. Header is comma-separated.
            first = xff.split(",", 1)[0].strip()
            if first:
                return first
    client = getattr(request, "client", None)
    host = getattr(client, "host", None) if client else None
    return host or None


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
        host = _client_ip_from_request(ctx.request)
        return f"ip:{host}" if host else None
    if scope == "catalog":
        return ctx.catalog_id
    # Unknown scope — log once and fail-open via None.
    logger.warning("rate_limit/max_count: unknown scope %r", scope)
    return None


def _path_method_matches(config: Dict[str, Any], ctx: EvaluationContext) -> bool:
    """Apply ``path_pattern`` / ``methods`` gate; conditions skip non-matching requests.

    An invalid ``path_pattern`` regex is treated as non-matching: the condition
    is skipped and the request is allowed (fail-open — availability beats
    enforcing a mis-typed limit).  A throttled WARNING is emitted via
    :func:`_safe_regex_matches` so operators can find the mis-config.
    """
    pattern = config.get("path_pattern")
    if pattern:
        matched = _safe_regex_matches(pattern, ctx.path or "", kind="path_pattern")
        # None → invalid pattern → treat as non-match → skip condition (return False)
        if not matched:
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

        count, allowed = await counter.incr_if_below(
            policy_id, principal_key, limit, window_seconds=window
        )
        if not allowed:
            await _log_usage_counter_denied(
                condition_type="rate_limit",
                policy_id=policy_id,
                principal_key=principal_key,
                scope=scope,
                count=count,
                limit=limit,
                window_seconds=window,
            )
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

        count, allowed = await counter.incr_if_below(
            policy_id, principal_key, limit, window_seconds=None
        )
        if not allowed:
            await _log_usage_counter_denied(
                condition_type="max_count",
                policy_id=policy_id,
                principal_key=principal_key,
                scope=scope,
                count=count,
                limit=limit,
                window_seconds=None,
            )
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

class LookupOnlySearchHandler(ConditionHandler):
    """Pass only when the request looks like a needle-lookup search.

    Used to gate the geoid lookup surface
    ``/search/catalogs/{cat}/items-search`` (and, where operators open it,
    the STAC search at ``/stac/catalogs/{cat}/search``) to
    anonymous (or otherwise narrowly-scoped) callers as a retrieve-by-id
    surface — the caller can find an item they already know the GeoID
    or external_id of, but cannot enumerate the catalog.

    The request **must** carry at least one lookup field (``geoid`` or
    ``external_id``) and **must not** carry any broadening field
    (``bbox`` / ``intersects`` / ``datetime`` / ``filter`` / ``q``).
    Pagination + scoping fields (``collections``, ``ids``, ``limit``,
    ``page``, ``next``, ``fields``, ``sortby``, ``filter_lang``) are
    permitted alongside the lookup.

    For ``GET`` the fields are read from ``ctx.query_params``; for
    ``POST`` (the canonical /search shape from #819) the JSON body is
    read via ``await ctx.request.json()`` — Starlette caches the body so
    the downstream route handler still sees it.

    Config keys (all optional — operator can tighten or relax the
    defaults without touching code):

    * ``lookup_fields``    (list[str]) — fields that count as a
        lookup. Default ``["geoid", "external_id"]``. At least one
        must be truthy.
    * ``broadening_fields`` (list[str]) — fields that, if truthy,
        deny the request. Default
        ``["bbox", "intersects", "datetime", "filter", "q"]``.
    """

    DEFAULT_LOOKUP_FIELDS = ("geoid", "external_id")
    DEFAULT_BROADENING_FIELDS = ("bbox", "intersects", "datetime", "filter", "q")

    @property
    def type(self) -> str:
        return "lookup_only_search"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        lookup_fields = tuple(
            config.get("lookup_fields") or self.DEFAULT_LOOKUP_FIELDS
        )
        broadening_fields = tuple(
            config.get("broadening_fields") or self.DEFAULT_BROADENING_FIELDS
        )

        method = (ctx.method or "").upper()
        if method == "GET":
            fields = ctx.query_params or {}
        elif method == "POST":
            fields = await self._read_body(ctx)
            if fields is None:
                # Body unreadable or not JSON — fail closed; a needle
                # lookup MUST carry a JSON body with the lookup field.
                return False
        else:
            return False

        has_lookup = any(_field_present(fields, k) for k in lookup_fields)
        if not has_lookup:
            return False
        for bf in broadening_fields:
            if _field_present(fields, bf):
                return False
        return True

    @staticmethod
    async def _read_body(ctx: EvaluationContext) -> Optional[Dict[str, Any]]:
        request = ctx.request
        if request is None:
            return None
        try:
            body = await request.json()
        except Exception:
            return None
        return body if isinstance(body, dict) else None


def _field_present(fields: Any, key: str) -> bool:
    """Return True when ``fields[key]`` is present and meaningful.

    Treat empty string / empty list / None as absent; treat ``0`` and
    ``False`` as absent too (broadening fields don't have semantic
    zero values).
    """
    if fields is None:
        return False
    try:
        v = fields.get(key)
    except Exception:
        return False
    if v is None or v == "" or v == [] or v == {}:
        return False
    return True


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
        # _safe_regex_matches(fullmatch=True) replicates the original re.fullmatch
        # semantics and returns None on invalid pattern (non-match direction).
        matched = _safe_regex_matches(pattern, val, kind="query_match", fullmatch=True)
        # None → invalid pattern → treat as non-match → False.
        if not matched:
            return False
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
            except ValueError as e:
                logger.warning(
                    "Ignoring malformed time-window 'start' value %r in validity condition: %s",
                    start_str, e,
                )
                start_dt = None
            if start_dt is not None and now < start_dt:
                raise IamError(f"Key is outside of its valid time window (valid from {start_dt.isoformat()}).")
        if end_str:
            try:
                end_dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
            except ValueError as e:
                logger.warning(
                    "Ignoring malformed time-window 'end' value %r in validity condition: %s",
                    end_str, e,
                )
                end_dt = None
            if end_dt is not None and now > end_dt:
                raise IamError(f"Key is outside of its valid time window (expired at {end_dt.isoformat()}).")

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
        except ValueError as e:
            logger.error(f"Invalid date format in policy: {exp_str}")
            raise IamError("Invalid expiration date format in policy.") from e
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
        if operator == "regex":
            matched = _safe_regex_matches(
                str(expected), str(actual), kind="match/regex"
            )
            # None → invalid pattern → non-match direction → False.
            return bool(matched)
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
    allowed_preset_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional safe-subset guard for catalog-scoped preset "
            "delegation. When set, requests against "
            "``/admin/catalogs/{cat}/presets/{name}`` are denied unless "
            "``{name}`` appears in the list. None disables the guard "
            "(handler keeps legacy role-only behaviour). Sysadmin and "
            "platform-grant bypasses are NOT subject to the allowlist."
        ),
    )


# Path shape for catalog-scoped preset endpoints; captures the preset name.
# Matches POST/DELETE /admin/catalogs/{cat}/presets/{name} exactly.
_CATALOG_PRESET_PATH_RE = re.compile(
    r"^/admin/catalogs/[^/]+/presets/([^/]+)$"
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
      - ``allowed_preset_names`` (list[str], optional) — safe-subset
                            allowlist for ``/admin/catalogs/{cat}/presets/{name}``
                            requests. When set, role-matched principals can
                            POST/DELETE only the named presets at their
                            catalog scope. Sysadmin and platform-grant
                            bypasses skip this guard. None (default) keeps
                            the legacy role-only behaviour. Non-preset
                            paths are unaffected regardless of value.

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
        role_match = any(r in catalog_roles_for_cat for r in cfg.required_roles)
        if not role_match:
            return False
        # Safe-subset allowlist for catalog-scoped preset routes only.
        # None disables the guard. Non-preset paths pass through unchanged
        # so the same policy may legally gate broader catalog-admin routes
        # without unintended side effects.
        if cfg.allowed_preset_names is not None:
            match = _CATALOG_PRESET_PATH_RE.match(ctx.path or "")
            if match is not None:
                preset_name = match.group(1)
                if preset_name not in cfg.allowed_preset_names:
                    return False
        return True


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
        self.register(LookupOnlySearchHandler())
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
        # Audience handlers (per-catalog / per-collection anonymous opt-ins)
        from dynastore.modules.iam.audience_handlers import (
            CatalogLookupAudienceHandler,
            CollectionWriteAudienceHandler,
        )
        self.register(CatalogLookupAudienceHandler())
        self.register(CollectionWriteAudienceHandler())
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

# --- Legacy Aliases ---
ConditionManager = ConditionRegistry

# Module-level singleton is instantiated lazily via PEP 562 ``__getattr__``
# to break a circular import with ``dynastore.modules.iam.audience_handlers``:
#   audience_handlers imports ``ConditionHandler`` / ``EvaluationContext`` from
#   this module at import time. If this module also instantiates
#   ``ConditionRegistry()`` at import time (which calls
#   ``_register_default_handlers`` → re-imports ``audience_handlers``), Python
#   re-enters a partially initialised ``audience_handlers`` module and
#   ``CatalogLookupAudienceHandler`` / ``CollectionWriteAudienceHandler`` are
#   not yet bound at the module level.
#
# Deferring the singleton until first attribute access (e.g. from middleware
# at request time, or from ``evaluate_condition`` below) leaves ``conditions``
# importable in isolation while still exposing ``condition_registry`` /
# ``condition_manager`` as module-level attributes for legacy callers. Default
# handler registration runs exactly once on first access, preserving the prior
# registration order (built-ins → audience handlers → filter inspectors).
_condition_registry_singleton: Optional["ConditionRegistry"] = None


def _get_condition_registry() -> "ConditionRegistry":
    global _condition_registry_singleton
    if _condition_registry_singleton is None:
        _condition_registry_singleton = ConditionRegistry()
    return _condition_registry_singleton


def __getattr__(name: str) -> Any:
    if name in ("condition_registry", "condition_manager"):
        return _get_condition_registry()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


async def evaluate_condition(condition: Condition, ctx: EvaluationContext) -> bool:
    """Helper to evaluate a single condition."""
    return await _get_condition_registry().evaluate_all([condition], ctx)

async def evaluate_conditions(conditions: List[Condition], ctx: EvaluationContext) -> bool:
    """Helper to evaluate a list of conditions."""
    return await _get_condition_registry().evaluate_all(conditions, ctx)

def register_condition_handler(handler: ConditionHandler):
    """
    Public SPI to register custom condition handlers.
    Example:
        from dynastore.modules.iam.conditions import register_condition_handler, ConditionHandler
        class MyHandler(ConditionHandler): ...
        register_condition_handler(MyHandler())
    """
    _get_condition_registry().register(handler)