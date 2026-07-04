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

"""Evict asyncpg connections whose protocol state machine is corrupted.

A cancelled request or a timeout can abandon an asyncpg connection
mid-operation: the connection's internal protocol state machine is left
stuck, but the wire itself is still open, so SQLAlchemy's ``QueuePool``
happily returns it to circulation. Every later checkout of that connection
then fails immediately at its first statement (often the ``SET LOCAL
lock_timeout`` every executor issues), because asyncpg refuses to start a
new operation on a connection that never finished its previous one::

    InternalClientError: cannot switch to state 11; another operation (2)
    is in progress

Once poisoned this way, the connection keeps failing and keeps being
returned to the pool — the same broken slot recurs across unrelated
callers (lease CAS ticks, drain ticks, request handlers) until the pod is
recycled. ``pool_pre_ping`` does not catch this: the connection answers a
ping fine, it just cannot start a second concurrent operation.

:func:`register_connection_poison_guard` closes that hole by hooking
SQLAlchemy's ``handle_error`` dialect event. When the wrapped exception is
one of these specific protocol-state-corruption shapes, it flips
``context.is_disconnect = True`` so SQLAlchemy invalidates the connection
(and bumps the pool's stale generation) instead of recycling it.
"""

import logging
from typing import Optional

from sqlalchemy import event
from sqlalchemy.engine import ExceptionContext
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

# Import defensively: asyncpg is only installed when SCOPE includes the
# async DB driver (see the module-level comment in db/db_service.py). The
# real classes let isinstance() match both the raw asyncpg exception and,
# via __cause__ unwrapping below, whatever the SQLAlchemy asyncpg dialect
# wraps it as.
try:
    from asyncpg.exceptions import InternalClientError as _AsyncpgInternalClientError
except ImportError:  # pragma: no cover - asyncpg not installed for this SCOPE
    _AsyncpgInternalClientError = type("InternalClientError", (Exception,), {})

try:
    from asyncpg.exceptions import (
        ProtocolViolationError as _AsyncpgProtocolViolationError,
    )
except ImportError:  # pragma: no cover - asyncpg not installed for this SCOPE
    _AsyncpgProtocolViolationError = type("ProtocolViolationError", (Exception,), {})

# The SQLAlchemy asyncpg dialect re-raises the caught asyncpg exception as a
# same-named class of its own (AsyncAdapt_asyncpg_dbapi.InternalClientError),
# chained via ``raise translated_error from error``. That wrapper is not an
# isinstance() of the real asyncpg class, so class-name matching is the only
# way to recognize it directly — matched conservatively below, together with
# a message check, to survive that kind of dialect-internal renaming/refactor.
_POISON_CLASS_NAMES = frozenset({"InternalClientError", "ProtocolViolationError"})

_POISON_MESSAGE_FRAGMENTS = (
    "another operation is in progress",
    "cannot switch to state",
)

_MAX_CAUSE_UNWRAP_DEPTH = 5


def _is_connection_poison_exception(exc: BaseException) -> bool:
    """True only for asyncpg protocol-state-corruption shapes (#2900).

    Deliberately narrow: a bare ``InterfaceError``/``OperationalError`` (e.g.
    a plain dropped connection) is NOT matched here — those already have
    their own recovery path (``pool_pre_ping``, the retry-on-disconnect logic
    in ``query_executor.py``). Matching them here too would mean evicting
    healthy connections on ordinary transient errors.
    """
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    depth = 0
    while current is not None and depth < _MAX_CAUSE_UNWRAP_DEPTH:
        if id(current) in seen:
            break
        seen.add(id(current))

        if isinstance(
            current, (_AsyncpgInternalClientError, _AsyncpgProtocolViolationError)
        ):
            return True

        if type(current).__name__ in _POISON_CLASS_NAMES:
            msg = str(current)
            if any(fragment in msg for fragment in _POISON_MESSAGE_FRAGMENTS):
                return True

        current = current.__cause__
        depth += 1

    return False


def register_connection_poison_guard(engine: AsyncEngine, *, service: str) -> None:
    """Evict asyncpg connections poisoned by protocol-state corruption (#2900).

    Registers a ``handle_error`` listener on ``engine.sync_engine`` (the
    dialect-level event bus SQLAlchemy async engines still dispatch through
    — see the identical attach pattern for the "connect" event in
    ``_arm_client_socket_keepalive`` above). Only the narrow poison shapes
    matched by :func:`_is_connection_poison_exception` flip
    ``context.is_disconnect``; every other error is left untouched so
    existing disconnect detection (dead wire, server restart, etc.) is
    unaffected.
    """

    @event.listens_for(engine.sync_engine, "handle_error")
    def _evict_poisoned_connection(context: ExceptionContext) -> None:
        if context.is_disconnect:
            return
        original = context.original_exception
        if original is None or not _is_connection_poison_exception(original):
            return
        context.is_disconnect = True
        logger.warning(
            "db_connection_poisoned_evicted service=%s err=%s: %s",
            service,
            type(original).__name__,
            str(original)[:120],
        )
