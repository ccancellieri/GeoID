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

"""TTL- and version-bounded compiled-policy cache (#1343).

The per-worker LRU that historically wrapped ``PolicyService.get_effective_policies``
had two gaps that limited its usefulness at scale:

* **No TTL.** An entry served until the process died or until policy CRUD on
  *this* pod called ``invalidate_cache``. A mutation on a sibling pod left
  every other pod's view stale until the next restart.
* **No cross-pod invalidation hook.** The cache layer in this project has no
  pub/sub channel (see :mod:`dynastore.modules.iam.phantom_token` — the same
  decision is documented there). The existing idiom for "every pod converges
  on a write" is the per-schema **binding-version counter** in Valkey: a
  writer increments it, every reader includes its current value in the cache
  key, so an entry written under version N becomes unreachable under version
  N+1 and the next read falls through to the authoritative resolver.

This module reuses that idiom verbatim. The compiled-policy cache key is
``(partition_key, schema, rule_version)``; a TTL backstops the rare case
where the counter is unreachable (Valkey down) or the bump itself failed.
The hot path is sync-callable: the TTL value is snapshotted from the async
``IamScaleConfig`` accessor on a slow background refresh so a read never
blocks on a config fetch (config has its own fail-closed defaults).

The rule_version is also exposed as :func:`iam_rule_version` so the
effective-permissions explainer can stamp it on the response — an operator
can see at a glance whether the verdict came off a fresh or stale view.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Hashable,
    Optional,
    Tuple,
    TypeVar,
)

from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)

logger = logging.getLogger(__name__)


_T = TypeVar("_T")


# --------------------------------------------------------------------------- #
# Rule-version readout — shared with phantom-token's per-schema counter so
# every IAM mutation that already bumps the counter automatically invalidates
# our cache too. Each compiled-rule entry is tagged with the version it was
# computed under; that version is the third component of the cache key, so a
# bump shifts the keyspace and the old entries become unreachable (and age
# out under TTL).
#
# The async accessor below is the canonical one — it round-trips to L2 at
# most once per ``_VERSION_L1_TTL`` (the same memo window phantom_token uses,
# kept in sync via the shared helper). The sync snapshot is for the hot path
# where introducing an async hop would change the call shape; it returns the
# last value the async path observed, defaulting to 0 if cold.
# --------------------------------------------------------------------------- #

_rule_version_snapshot: int = 0


async def iam_rule_version_async(schema: str = "iam") -> int:
    """Return the current per-schema rule version counter (0 if unset).

    Delegates to :func:`phantom_token.get_binding_version` so policy and
    phantom-token share one signal: every write that already bumps the
    counter (role / grant / policy / hierarchy CRUD via the IAM storage
    layer) implicitly invalidates this cache on every pod with no extra
    wiring. A backend error in the underlying read returns 0 and falls
    through to the TTL backstop.
    """
    global _rule_version_snapshot
    from dynastore.modules.iam.phantom_token import get_binding_version
    value = await get_binding_version(schema)
    if schema == "iam":
        _rule_version_snapshot = value
    return value


def iam_rule_version() -> int:
    """Sync snapshot of the platform rule version, suitable for the hot path.

    Returns 0 before the first async refresh has landed. The
    effective-permissions explainer (the explicit consumer) reads this via
    :meth:`EffectivePermissionResponse.compiled_rule_version`, and a 0 there
    is a legitimate "no IAM mutation has happened yet" signal — never an
    error condition.
    """
    return _rule_version_snapshot


# --------------------------------------------------------------------------- #
# TTL-bounded async cache, keyed on whatever the caller supplies. The reason
# for a hand-rolled cache (vs. the generic ``@cached`` decorator) is that the
# value we cache (``List[Policy]``) must travel with its provenance — the
# ``rule_version`` it was computed under — so the explainer can surface
# staleness. The decorator returns the raw value with no envelope.
# --------------------------------------------------------------------------- #

class _Entry(Generic[_T]):
    __slots__ = ("value", "fetched_at", "rule_version")

    def __init__(self, value: _T, fetched_at: float, rule_version: int) -> None:
        self.value = value
        self.fetched_at = fetched_at
        self.rule_version = rule_version


class CompiledRuleCache(Generic[_T]):
    """Bounded TTL+LRU cache used by :meth:`PolicyService.get_effective_policies`.

    Concurrency model: a ``threading.Lock`` guards the underlying ``dict``
    (insertion + eviction). The compile work itself runs outside the lock
    — under contention several workers may compile in parallel for the same
    key on a cold miss; the result is idempotent and the bound on duplicate
    work is the per-key compile latency, which is in-RAM-cheap per the
    #1337 design ("bounded_rules" walk). A single-flight wrapper is therefore
    deliberately omitted; if profiling later shows a hot-key thundering herd
    on cache expiry, an ``asyncio.Lock`` per key can be added.
    """

    def __init__(self, *, maxsize: int = 1024) -> None:
        self._maxsize = max(1, int(maxsize))
        self._store: Dict[Hashable, _Entry[_T]] = {}
        self._lock = threading.Lock()
        # Stats are opportunistic — reads under the lock would defeat the
        # point of a hot-path cache; race-tolerant integer increments are
        # fine for a coarse observability signal.
        self.hits = 0
        self.misses = 0

    def configure(self, *, maxsize: int) -> None:
        """Resize at runtime. Existing entries are kept; new maxsize takes
        effect on the next insertion that would overflow."""
        with self._lock:
            self._maxsize = max(1, int(maxsize))

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def snapshot(self) -> Dict[Hashable, Tuple[int, float]]:
        """For tests: ``{key: (rule_version, fetched_at)}`` without exposing
        the cached value (a ``List[Policy]`` is intentionally opaque here)."""
        with self._lock:
            return {k: (e.rule_version, e.fetched_at) for k, e in self._store.items()}

    async def get_or_compute(
        self,
        key: Hashable,
        compute: Callable[[], Awaitable[_T]],
        *,
        ttl_seconds: float,
        rule_version: int,
        now: Optional[Callable[[], float]] = None,
    ) -> Tuple[_T, int]:
        """Return ``(value, rule_version_used)``.

        A cache HIT requires (a) the entry exists, (b) it is not older than
        ``ttl_seconds``, AND (c) its ``rule_version`` matches the current
        ``rule_version``. Any of those failing → re-compute. The returned
        version is the version stamped on the entry that won (whether
        served from cache or freshly computed), so the explainer always
        sees the version that the cached policy set was actually compiled
        under (not just "the current platform version").
        """
        clock = now or time.monotonic

        with self._lock:
            entry = self._store.get(key)
            if entry is not None:
                if (
                    entry.rule_version == rule_version
                    and (clock() - entry.fetched_at) < ttl_seconds
                ):
                    self.hits += 1
                    return entry.value, entry.rule_version

        # Miss — compile outside the lock (the call is async; holding a
        # threading.Lock across an await would block the loop).
        self.misses += 1
        value = await compute()

        with self._lock:
            self._store[key] = _Entry(
                value=value, fetched_at=clock(), rule_version=rule_version
            )
            if len(self._store) > self._maxsize:
                # FIFO eviction — cheap, no usage-order bookkeeping. The
                # working set for IAM is tiny relative to maxsize (one
                # entry per (partition_key, schema, rule_version) and
                # most deployments have <100 catalogs).
                try:
                    overflow_key = next(iter(self._store))
                    if overflow_key != key:  # never evict the entry we just inserted
                        self._store.pop(overflow_key, None)
                except StopIteration:  # pragma: no cover — defensive
                    pass

        return value, rule_version


# --------------------------------------------------------------------------- #
# Module-global cache instance + TTL snapshot. The TTL is read async-ly from
# IamScaleConfig but consumed synchronously by the cache check (the read
# path is async, but the TTL value itself is a scalar that need not be
# refreshed on every call). A small background refresh task in the IamModule
# lifespan keeps it current; failure to refresh just means the previous
# value continues to be honoured (no security or correctness consequence —
# this is purely a performance knob).
# --------------------------------------------------------------------------- #


_DEFAULT_TTL_SECONDS: float = 60.0
_TTL_SECONDS: float = _DEFAULT_TTL_SECONDS
_TTL_SECONDS_LOCK = threading.Lock()

# Refresh interval for the TTL/maxsize snapshot (seconds). The values
# themselves are knobs operators rarely change; reading them every few
# seconds is cheap and bounds the lag between a config PATCH and the cache
# honouring it.
_CONFIG_REFRESH_INTERVAL = 30.0


_COMPILED_RULE_CACHE: CompiledRuleCache[Any] = CompiledRuleCache(maxsize=1024)


def get_compiled_rule_cache() -> CompiledRuleCache[Any]:
    """Module singleton — one cache per process, shared by every
    :class:`PolicyService` instance."""
    return _COMPILED_RULE_CACHE


def get_ttl_seconds() -> float:
    """Snapshot of the configured TTL. The hot path consumes this and never
    blocks on a config fetch; a slow background task refreshes it."""
    with _TTL_SECONDS_LOCK:
        return _TTL_SECONDS


def _set_ttl_seconds(value: float) -> None:
    global _TTL_SECONDS
    with _TTL_SECONDS_LOCK:
        _TTL_SECONDS = max(0.1, float(value))


async def refresh_config_snapshot() -> None:
    """Pull TTL + maxsize from the current :class:`IamScaleConfig`.

    Safe to call on any error: a failed read leaves the previous snapshot
    in place. Designed to be invoked (a) once at module startup so the
    very first read does not fall back to the in-source default, and
    (b) periodically by the background refresher.
    """
    try:
        from dynastore.modules.iam.scale_config import get_iam_scale_config

        cfg = await get_iam_scale_config()
        ttl = float(getattr(cfg, "compiled_rule_cache_ttl_seconds", _DEFAULT_TTL_SECONDS))
        maxsize = int(getattr(cfg, "compiled_rule_cache_maxsize", 1024))
    except Exception:
        logger.debug(
            "compiled_rule_cache: config refresh failed; keeping previous "
            "snapshot",
            exc_info=True,
        )
        return
    _set_ttl_seconds(ttl)
    _COMPILED_RULE_CACHE.configure(maxsize=maxsize)


# --------------------------------------------------------------------------- #
# Background refresher — owned by the IAM module lifespan via BackgroundSupervisor.
# IamRuleCacheRefreshService is registered once in IamModule.lifespan; the
# supervisor drives its tick() on every pod (RUN_EVERYWHERE / ALL) so the sync
# hot path always has a fresh TTL snapshot and rule-version without an async hop.
# --------------------------------------------------------------------------- #


class IamRuleCacheRefreshService(PeriodicService):
    """Periodic service that refreshes the compiled-rule cache TTL snapshot
    and the IAM rule-version counter on a fixed cadence.

    Leadership policy: ``RUN_EVERYWHERE`` — every pod must keep its own
    in-process snapshots current. There is no shared mutable state to
    serialise; each pod writes only into its own module-globals.

    Pod policy: ``ALL`` — job pods must also refresh so the sync hot path
    (``get_ttl_seconds()``, ``iam_rule_version()``) is valid there too.
    """

    name = "iam_rule_cache_refresher"
    leadership = Leadership.RUN_EVERYWHERE
    pod_policy = PodPolicy.ALL
    cadence_seconds: float = _CONFIG_REFRESH_INTERVAL

    async def tick(self, ctx: ServiceContext) -> None:
        """One refresh pass: pull TTL/maxsize snapshot then update the
        rule-version counter. A failed pass leaves the previous values in
        place — no security or correctness consequence, just a stale knob."""
        await refresh_config_snapshot()
        try:
            await iam_rule_version_async("iam")
        except Exception:
            logger.debug(
                "compiled_rule_cache: rule_version refresh failed", exc_info=True
            )


def _reset_for_tests() -> None:
    """Reset module-global state. Test-only."""
    global _TTL_SECONDS, _rule_version_snapshot
    _COMPILED_RULE_CACHE.clear()
    _COMPILED_RULE_CACHE.configure(maxsize=1024)
    with _TTL_SECONDS_LOCK:
        _TTL_SECONDS = _DEFAULT_TTL_SECONDS
    _rule_version_snapshot = 0
