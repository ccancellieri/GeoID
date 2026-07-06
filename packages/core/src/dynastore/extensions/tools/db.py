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

from typing import Annotated, AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection
from fastapi import Depends, Request

from dynastore.modules.db_config.query_executor import (
    _read_live_fg_acquire_timeout,
    managed_transaction,
)
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import DatabaseProtocol


def get_async_engine(request: Request) -> AsyncEngine:
    """
    Gets the asynchronous engine.
    Prioritizes request.app.state.engine (standard FastApi/Starlette pattern).
    Fallbacks to DatabaseProtocol discovery.
    """
    if hasattr(request.app.state, "engine") and request.app.state.engine:
        return request.app.state.engine

    # Fallback to Protocol
    db_service = get_protocol(DatabaseProtocol)
    if db_service and db_service.engine:
        return db_service.engine  # type: ignore[return-value]

    raise RuntimeError(
        "Database service not available. The async engine is not initialized."
    )


async def get_write_connection(
    request: Request,
) -> AsyncGenerator[AsyncConnection, None]:
    """
    FastAPI dependency that provides a transaction-managed asynchronous
    read-write `Connection` to API endpoints (#2753 write lane). This is the
    correct pattern for endpoint dependencies that mutate data.
    """
    engine = get_async_engine(request)

    # managed_transaction adds pool hygiene on acquire (pre-ping recovery,
    # transient-connect retry, poisoned-slot eviction, shielded rollback
    # drain) and raises PoolSaturationError on acquire timeout, which the
    # shared exception handlers map to HTTP 503.
    async with managed_transaction(engine) as conn:
        # managed_transaction is dual-mode (sync/async engines, connections,
        # sessions); with an AsyncEngine input it always yields AsyncConnection.
        assert isinstance(conn, AsyncConnection)
        yield conn


# Backward-compatible alias: every existing handler and test importing
# ``get_async_connection`` keeps working, byte-for-byte unchanged behaviour
# (#2753 step 2, phase 0 — landing the lane primitives with zero handler
# edits). New/migrating write handlers should prefer ``get_write_connection``
# or the ``WriteConn`` annotation below.
get_async_connection = get_write_connection


async def get_async_connection_bounded(
    request: Request,
) -> AsyncGenerator[AsyncConnection, None]:
    """FastAPI dependency variant of :func:`get_write_connection` for read
    surfaces that should fail fast under pool saturation (#2933/#2948).

    Identical to :func:`get_write_connection` except the checkout is bounded
    by ``ConnectionHealthConfig.foreground_pool_acquire_timeout_s`` instead of
    only the engine's own ``pool_timeout`` — a saturated pool raises
    ``PoolSaturationError`` (mapped to a 503 + Retry-After by the existing
    ``PoolSaturationExceptionHandler``) well before the request rides the
    full pool_timeout. Reserved for item-GET/search reads; write routes keep
    the plain :func:`get_write_connection`.

    Predates the explicit ``ReadConn``/``WriteConn`` lanes (#2753) and is
    left as-is — still a read-write transaction, just fail-fast on acquire —
    so its existing callers see no behaviour change. New read surfaces should
    prefer :func:`get_read_connection`, which adds ``READ ONLY`` semantics on
    top of the same fail-fast acquire.
    """
    engine = get_async_engine(request)

    async with managed_transaction(
        engine, acquire_timeout=await _read_live_fg_acquire_timeout()
    ) as conn:
        assert isinstance(conn, AsyncConnection)
        yield conn


async def get_read_connection(
    request: Request,
) -> AsyncGenerator[AsyncConnection, None]:
    """FastAPI dependency for the read lane (#2753 step 2).

    Fail-fast bounded pool acquire (same as :func:`get_async_connection_bounded`)
    plus ``READ ONLY`` transaction semantics: PostgreSQL rejects any write
    attempted through this connection instead of silently allowing it, and a
    single consistent snapshot is kept for the whole request (unlike
    AUTOCOMMIT, which would let a count-then-page request observe phantom
    drift between statements). Reserved for handlers that never write and
    never trigger lazy DDL/provisioning; such handlers stay on
    :func:`get_write_connection`.

    This lane shares the single serving engine/pool with the write lane —
    no new connections are opened by this change — so it carries the exact
    same lock-safety GUCs (``lock_timeout``, ``idle_in_transaction_session_timeout``,
    ``statement_timeout``) and TCP keepalive/user-timeout settings the engine
    was built with (``modules/db/db_service.py``). A dedicated read-side
    budget reserve and/or replica routing are later, separately-soaked phases
    of #2753 — not part of this change.
    """
    engine = get_async_engine(request)

    async with managed_transaction(
        engine,
        acquire_timeout=await _read_live_fg_acquire_timeout(),
        read_only=True,
    ) as conn:
        assert isinstance(conn, AsyncConnection)
        yield conn


WriteConn = Annotated[AsyncConnection, Depends(get_write_connection)]
ReadConn = Annotated[AsyncConnection, Depends(get_read_connection)]
