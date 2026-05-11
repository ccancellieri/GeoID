"""Capability publisher — refreshes liveness sentinel keys in the shared cache.

Each pod periodically writes a sentinel key for every capability it can
service (today: every :class:`~dynastore.models.protocols.indexer.Indexer`
registered in this process). The :mod:`capability_oracle` reads those
keys to answer "is any pod alive that can claim this row?" When the last
pod with a capability dies, no one refreshes → TTL expires → oracle
returns ``False`` → the dispatcher's reactive reaper DLQs unclaimable
rows instead of leaving them PENDING forever (see #502, follow-up to
#491).

The primitive is generic: ``capability_id`` is any string. New
``can_claim``-style task types reuse the same publisher by extending the
list of capability ids returned by :func:`_collect_local_capabilities`.

Config: :class:`TasksPluginConfig.capability_publisher_ttl_seconds` and
``.capability_publisher_refresh_seconds``. Defaults pair to 60s TTL +
30s refresh — worst case 60s after last pod dies before the oracle
reflects truth.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Iterable, List

from dynastore.modules.tasks.capability_oracle import capability_key

logger = logging.getLogger(__name__)


def _collect_local_capabilities() -> List[str]:
    """Return the set of capability ids served by this process.

    Today this is just the snake-cased Indexer class names — the only
    consumer of ``can_claim`` is :class:`IndexPropagationTask`. Generalize
    here when a second consumer lands (#522).
    """
    try:
        from dynastore.tasks.index_propagation.task import (
            registered_indexer_ids,
        )

        return list(registered_indexer_ids())
    except Exception as exc:  # noqa: BLE001 — never break lifespan
        logger.debug("capability_publisher: enumerate failed (%s)", exc)
        return []


async def _refresh_once(capabilities: Iterable[str], ttl_seconds: float) -> int:
    """Write the sentinel key for every capability. Returns count written.

    Each write is independent — a single failure does not abort the
    batch. Errors are logged at DEBUG; the next tick will retry. No
    exceptions escape.
    """
    try:
        from dynastore.tools.cache import get_cache_manager

        backend = get_cache_manager().get_async_backend()
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "capability_publisher: no async cache backend available (%s)", exc,
        )
        return 0

    written = 0
    for cap in capabilities:
        try:
            await backend.set(capability_key(cap), b"1", ttl=ttl_seconds)
            written += 1
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "capability_publisher: set(%r) failed (%s)", cap, exc,
            )
    return written


async def run_capability_publisher(
    shutdown_event: asyncio.Event,
    *,
    ttl_seconds: float = 60.0,
    refresh_seconds: float = 30.0,
) -> None:
    """Periodic loop that refreshes capability sentinel keys.

    Mirrors the shape of :func:`_warn_stuck_pending_tasks` in
    :mod:`tasks_module` — sleeps with ``asyncio.wait_for(shutdown_event)``
    so shutdowns are immediate, swallows all exceptions inside the loop
    so a transient backend hiccup never crashes the publisher.

    Writes one sentinel per local capability per tick. With the default
    60s TTL + 30s refresh, every key is rewritten twice before expiry —
    one missed tick is absorbed.
    """
    if refresh_seconds <= 0:
        logger.warning(
            "capability_publisher: refresh_seconds=%.1f <= 0; disabling.",
            refresh_seconds,
        )
        return

    logger.info(
        "capability_publisher: starting (ttl=%.1fs, refresh=%.1fs)",
        ttl_seconds, refresh_seconds,
    )

    # First refresh runs immediately so dispatchers see liveness from
    # tick zero, not refresh_seconds later.
    while True:
        try:
            caps = _collect_local_capabilities()
            if caps:
                n = await _refresh_once(caps, ttl_seconds=ttl_seconds)
                logger.debug(
                    "capability_publisher: refreshed %d/%d sentinels",
                    n, len(caps),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "capability_publisher: refresh raised — swallowing (%s)", exc,
            )

        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=refresh_seconds,
            )
            return  # shutdown signalled during sleep
        except asyncio.TimeoutError:
            pass  # normal wakeup
