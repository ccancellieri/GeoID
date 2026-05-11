"""Capability liveness oracle backed by the shared cache.

Answers the question "does any live worker pod advertise capability X?"
by checking a sentinel key in the shared :class:`CacheBackend`. The
publisher side (:mod:`capability_publisher`) refreshes the same key on
each heartbeat tick; when the last pod with the capability dies the TTL
expires and :meth:`is_capability_live` flips to ``False``.

Used by the dispatcher's reactive reaper (see ``dispatcher.py``): when
``TaskProtocol.can_claim`` rejects a row, the dispatcher consults this
oracle. If no pod is alive with the required capability, the row is
moved to ``DEAD_LETTER`` with a clear ``error_message`` instead of being
left PENDING forever.

Fail-safe by design: any error (no backend registered, network blip,
cache outage) is swallowed and the oracle returns ``True`` — preferring
"leave PENDING + WARN" over a false DLQ.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


CAPABILITY_KEY_PREFIX = "dynastore:caps:"


def capability_key(capability_id: str) -> str:
    """Return the cache key used by both publisher and oracle."""
    return f"{CAPABILITY_KEY_PREFIX}{capability_id}"


async def is_capability_live(capability_id: str) -> bool:
    """Return ``True`` iff at least one pod has refreshed the sentinel
    key for ``capability_id`` within its TTL window.

    Returns ``True`` (fail-open) on any cache error or when no async
    backend is registered — a missing oracle must never cause a false
    DLQ.
    """
    try:
        from dynastore.tools.cache import get_cache_manager

        backend = get_cache_manager().get_async_backend()
        return await backend.exists(capability_key(capability_id))
    except Exception as exc:  # noqa: BLE001 — fail-open
        logger.debug(
            "capability_oracle: is_live(%r) failed (%s); returning True",
            capability_id, exc,
        )
        return True
