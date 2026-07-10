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
from typing import Any, Optional

logger = logging.getLogger(__name__)


CAPABILITY_KEY_PREFIX = "dynastore:caps:"


# Task types whose capability id is sourced from a single ``inputs.<key>``
# JSONB field. Used by the reactive reaper's bulk-DLQ path (#529) to sweep
# every sibling row for a confirmed-dead capability in one UPDATE instead
# of waiting for each row to be re-claimed and rejected individually.
#
# Keys must be safe SQL identifiers — interpolated into JSONB extraction
# (``inputs->>'<key>'``). Add a new entry here when a new capability-gated
# TaskProtocol lands; the mapping intentionally lives alongside the oracle
# so all capability-aware code paths share one source of truth.
TASK_TYPE_CAPABILITY_INPUTS_KEY: dict[str, str] = {}


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


def resolve_required_capability(
    task_instance: Any, payload: Any,
) -> Optional[str]:
    """Return the capability id required to claim ``payload``, or ``None``.

    Pure helper consumed by every callsite that needs to translate a task
    row into a capability id (the reactive reaper in ``dispatcher.py``,
    the stuck-pending warner in ``tasks_module.py``, and the inbound
    admin route for ``requeue_dead_letter_tasks_by_type`` — #523).

    Defensive: returns ``None`` when ``task_instance`` is missing,
    ``required_capability`` is not declared on the class, the method
    raises, or the result is not a non-empty string. Never raises.
    """
    if task_instance is None:
        return None
    required_cap_fn = getattr(
        type(task_instance), "required_capability", None,
    )
    if not callable(required_cap_fn):
        return None
    try:
        cap = required_cap_fn(payload)
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "capability_oracle: required_capability raised on %s: %s",
            type(task_instance).__name__, exc,
        )
        return None
    return cap if isinstance(cap, str) and cap else None
