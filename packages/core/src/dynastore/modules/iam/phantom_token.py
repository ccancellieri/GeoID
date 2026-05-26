"""Phantom-token hot path (#1343): shared, version-keyed resolution cache.

DynaStore does not mint the user access token on the OIDC path — it validates
Keycloak's. So the zero-DB hot path cannot rely on self-contained JWT claims;
instead it resolves a principal's identity+roles ONCE against Postgres and
caches the result keyed by
``(provider, subject_id, schema, platform_version, catalog_version)``. A
binding mutation bumps the per-schema version counter, which changes the key
and so invalidates every pod's cache without any pub/sub channel (the cache
layer has none).

The resolution cache leans on the existing two-tier cache machinery so a hot
principal costs no network at all:

* **L1** — an in-process :class:`TieredAsyncBackend` tier serves principals
  already seen on this pod (no Valkey round-trip).
* **L2** — the shared Valkey backend serves warm / cross-pod principals.
* **DB** — the authoritative resolver runs only on a cold miss or after a
  version bump, then back-fills L1+L2.

The per-schema binding-version read is memoized in-process for a couple of
seconds (``_VERSION_L1_TTL``) so the hot path is zero-network; a local
``bump_binding_version`` evicts that memo so the bumping pod sees its own
change immediately and every other pod converges within the memo window. A
short L2 TTL backstops a missed bump.

The cache is strictly an optimization: every read/write failure falls through
to the authoritative DB resolver passed by the caller. Only the resolver's own
failure is authoritative — the caller (`authenticate_and_get_role`) owns the
fail-closed contract for that. The denylist is an immediate-revocation
convenience layered on top of natural token expiry; it is deliberately NOT
tiered (an L1 copy would delay a revocation per-pod) and a denylist read error
fails OPEN (logged) so a transient Valkey blip cannot lock every caller out.
"""

from __future__ import annotations

import json
import logging
from time import monotonic
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional, Tuple

if TYPE_CHECKING:
    from dynastore.models.protocols.authorization import IamScaleConfig
    from dynastore.models.protocols.cache import CacheBackend, CountingCacheBackend

logger = logging.getLogger(__name__)

_VERSION_PREFIX = "iam:bv:"
_RESOLUTION_PREFIX = "iam:phantom:"
_DENYLIST_PREFIX = "iam:denylist:"

# In-process L1 cap for the tiered resolution backend (seconds). The runtime
# ``binding_resolution_ttl_seconds`` config drives the L2 TTL; this caps how
# long a pod may serve a resolution from its own memory before re-checking L2.
_RESOLUTION_L1_TTL_CAP = 30.0

# In-process memo window for the binding-version counter read (seconds). Bounds
# how long a pod may serve a stale version after another pod bumps it; a local
# bump evicts the memo entry immediately.
_VERSION_L1_TTL = 2.0

# Memoized tiered (L1+L2) resolution backend and the identity of the L2 backend
# it was built over (so we rebuild if the backend is re-registered / swapped).
_tiered: "Optional[CacheBackend]" = None
_tiered_l2_id: Optional[int] = None

# In-process binding-version memo: schema -> (value, monotonic_timestamp).
_version_memo: Dict[str, Tuple[int, float]] = {}


def _distributed_backend() -> "Optional[CountingCacheBackend]":
    """Return the active backend iff it is a distributed counting backend.

    Distributed = ``priority < 1000`` (local/in-memory backends use 1000) AND
    a :class:`CountingCacheBackend` (Valkey/Redis). Otherwise ``None``. The
    imports are deferred to call time to avoid an import cycle with the cache
    layer and to avoid triggering cache initialization at import.

    This raw L2 handle backs the version counter (``incr``/``get_count``) and
    the denylist (``set``/``exists``), which need atomic / direct-to-shared
    semantics. Resolution get/set go through :func:`_resolution_backend`.
    """
    try:
        from dynastore.models.protocols.cache import CountingCacheBackend
        from dynastore.tools.cache import get_cache_manager

        backend = get_cache_manager().get_async_backend()
        if backend is None:
            return None
        if getattr(backend, "priority", 1000) >= 1000:
            return None
        if not isinstance(backend, CountingCacheBackend):
            return None
        return backend
    except Exception:  # pragma: no cover - defensive: never let lookup raise
        return None


def _resolution_backend() -> "Optional[CacheBackend]":
    """Return a memoized L1+L2 tiered backend for resolution get/set.

    Wraps a persistent in-process :class:`LocalAsyncCacheBackend` (L1) over the
    shared L2 backend so a principal already resolved on this pod is served
    without a network round-trip. Returns ``None`` when no distributed L2 is
    present (the caller then falls through to the DB resolver). The tier is
    memoized so the L1 store survives across calls; it is rebuilt if the L2
    backend identity changes (re-registration, or a test swap).
    """
    global _tiered, _tiered_l2_id
    try:
        l2 = _distributed_backend()
        if l2 is None:
            return None
        if _tiered is not None and _tiered_l2_id == id(l2):
            return _tiered
        from dynastore.tools.cache import LocalAsyncCacheBackend, TieredAsyncBackend

        _tiered = TieredAsyncBackend(
            [LocalAsyncCacheBackend(max_size=4096), l2],
            l1_ttl_cap=_RESOLUTION_L1_TTL_CAP,
        )
        _tiered_l2_id = id(l2)
        return _tiered
    except Exception:  # pragma: no cover - defensive: never let lookup raise
        return None


def _reset_caches() -> None:
    """Clear the in-process tier + version memo. For tests only."""
    global _tiered, _tiered_l2_id
    _tiered = None
    _tiered_l2_id = None
    _version_memo.clear()


def phantom_token_active(cfg: "IamScaleConfig") -> bool:
    """True iff the phantom-token cache is enabled and a backend is present."""
    return (
        bool(getattr(cfg, "phantom_token_resolution_enabled", False))
        and _distributed_backend() is not None
    )


async def get_binding_version(schema: str) -> int:
    """Return the current per-schema binding-version counter (0 if unset).

    Memoized in-process for ``_VERSION_L1_TTL`` seconds so the hot path does
    not round-trip to L2 on every request; :func:`bump_binding_version` evicts
    the memo for the schema it bumps so the bumping pod sees its own change at
    once.
    """
    backend = _distributed_backend()
    if backend is None:
        return 0
    cached = _version_memo.get(schema)
    if cached is not None and (monotonic() - cached[1]) < _VERSION_L1_TTL:
        return cached[0]
    try:
        v = await backend.get_count(_VERSION_PREFIX + schema)
        value = int(v) if v is not None else 0
    except Exception:
        logger.warning("phantom_token: get_binding_version failed for %s", schema, exc_info=True)
        return 0
    _version_memo[schema] = (value, monotonic())
    return value


async def bump_binding_version(schema: str) -> None:
    """Bump the per-schema binding-version counter, invalidating cached keys.

    A missed bump is backstopped by the resolution TTL, so a failure here is
    logged but never raised. The local version memo is evicted regardless so
    the bumping pod re-reads its own change immediately.
    """
    backend = _distributed_backend()
    if backend is None:
        return
    try:
        await backend.incr(_VERSION_PREFIX + schema)
    except Exception:
        logger.warning("phantom_token: bump_binding_version failed for %s", schema, exc_info=True)
    finally:
        _version_memo.pop(schema, None)


def _resolution_key(
    provider: str,
    subject_id: str,
    schema: str,
    platform_v: int,
    catalog_v: int,
) -> str:
    return f"{_RESOLUTION_PREFIX}{provider}:{subject_id}:{schema}:{platform_v}:{catalog_v}"


async def resolve_bindings_cached(
    *,
    provider: str,
    subject_id: str,
    schema: str,
    resolver: Callable[[], Awaitable[Optional[Dict[str, Any]]]],
    cfg: "IamScaleConfig",
) -> Optional[Dict[str, Any]]:
    """Resolve a principal's bindings, caching the result in the L1+L2 tier.

    On any cache read/write error the call falls through to ``resolver``. A
    resolver exception propagates (the caller owns fail-closed). A cached
    ``None`` is a legitimate negative hit and is returned as such.
    """
    backend = _resolution_backend()
    if backend is None:
        return await resolver()

    platform_v = await get_binding_version("iam")
    catalog_v = platform_v if (not schema or schema == "iam") else await get_binding_version(schema)
    key = _resolution_key(provider, subject_id, schema, platform_v, catalog_v)

    raw: Optional[bytes] = None
    try:
        raw = await backend.get(key)
    except Exception:
        logger.warning("phantom_token: cache read failed for %s", key, exc_info=True)
        raw = None

    if raw is not None:
        try:
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
            decoded = json.loads(text)
            if isinstance(decoded, dict) and "v" in decoded:
                # Cache HIT — ``v`` may legitimately be ``None`` (cached negative).
                return decoded["v"]
        except Exception:
            logger.warning("phantom_token: cache decode failed for %s", key, exc_info=True)
            # Fall through to the resolver.

    result = await resolver()

    try:
        payload = json.dumps({"v": result}).encode("utf-8")
        await backend.set(
            key,
            payload,
            ttl=float(getattr(cfg, "binding_resolution_ttl_seconds", 300) or 300),
        )
    except Exception:
        # Includes a non-serializable resolver result: still return it, uncached.
        logger.warning("phantom_token: cache write failed for %s", key, exc_info=True)

    return result


async def deny(token_id: str, *, ttl_seconds: int) -> None:
    """Add a token id to the revocation denylist for ``ttl_seconds``.

    Writes straight to L2 (not the tiered backend): a revocation must be
    visible to every pod at once, so it is never cached in an L1 tier.
    """
    backend = _distributed_backend()
    if backend is None:
        return
    try:
        await backend.set(_DENYLIST_PREFIX + token_id, b"1", ttl=float(ttl_seconds))
    except Exception:
        logger.warning("phantom_token: deny failed for %s", token_id, exc_info=True)


async def is_denied(token_id: str) -> bool:
    """Return True iff the token id is on the revocation denylist.

    Reads straight from L2 (no L1 tier) so a revocation takes effect platform
    -wide immediately. On a backend error this FAILS OPEN (returns ``False``,
    logs) so a transient Valkey blip cannot lock every caller out.
    """
    backend = _distributed_backend()
    if backend is None:
        return False
    try:
        return await backend.exists(_DENYLIST_PREFIX + token_id)
    except Exception:
        # FAIL OPEN: a denylist read error must not deny legitimate callers.
        logger.warning("phantom_token: is_denied failed for %s", token_id, exc_info=True)
        return False
