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

import logging
from typing import Iterable, List, Optional, Union

from dynastore.modules.tasks.capability_oracle import capability_key
from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)

logger = logging.getLogger(__name__)


def _collect_local_capabilities() -> List[str]:
    """Return the set of capability ids served by this process.

    Currently empty — no capability-gated ``TaskProtocol`` (a task
    declaring ``can_claim``/``required_capability``) is registered.
    Populate here when one lands (#522).
    """
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


class CapabilityPublisherService(PeriodicService):
    """Refreshes capability sentinel keys in the shared cache on a fixed cadence.

    Runs on every pod (RUN_EVERYWHERE) so each pod refreshes its own
    capability sentinels independently — no single writer is needed.
    Skips ephemeral Cloud Run Job pods (SKIP_EPHEMERAL): they run one task
    and exit, so there is nothing to advertise and no cache to maintain.
    Resolves #2279 for this loop.

    Each tick collects local capabilities and writes a sentinel key per
    capability. PeriodicService supplies the loop, shutdown handling, and
    the initial-tick-before-first-sleep guarantee — so dispatchers see
    liveness from tick zero, not refresh_seconds later.
    """

    name = "capability_publisher"
    leadership = Leadership.RUN_EVERYWHERE
    pod_policy = PodPolicy.SKIP_EPHEMERAL
    lock_key: Optional[Union[int, str]] = None

    def __init__(self, *, ttl_seconds: float = 60.0, refresh_seconds: float = 30.0) -> None:
        self._ttl_seconds = ttl_seconds
        self.cadence_seconds = refresh_seconds

    async def tick(self, ctx: ServiceContext) -> None:
        try:
            caps = _collect_local_capabilities()
            if caps:
                n = await _refresh_once(caps, ttl_seconds=self._ttl_seconds)
                logger.debug(
                    "capability_publisher: refreshed %d/%d sentinels",
                    n, len(caps),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "capability_publisher: refresh raised — swallowing (%s)", exc,
            )
