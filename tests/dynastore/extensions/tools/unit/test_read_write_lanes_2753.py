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

"""Read/write connection lane primitives (#2753 step 2, phase 0).

``get_write_connection``/``get_read_connection`` (and the ``WriteConn``/
``ReadConn`` FastAPI annotations built on top of them) are thin typed
wrappers over the existing ``managed_transaction`` hygiene path -- no new
engine/pool is introduced. This module pins:

* ``get_async_connection`` stays a backward-compatible alias of
  ``get_write_connection`` (zero behaviour change for the ~80 handlers still
  depending on the old name).
* ``get_write_connection`` opens a plain (non-read-only) transaction with no
  bounded acquire, same as the pre-#2753 ``get_async_connection``.
* ``get_read_connection`` threads a live ``acquire_timeout`` AND
  ``read_only=True`` into ``managed_transaction`` -- fail-fast pool
  saturation handling plus PostgreSQL-enforced ``READ ONLY`` semantics.
* Both lanes resolve their engine from the same ``request.app.state.engine``
  -- this phase does not add a second pool, so both lanes automatically
  carry whatever lock-safety (`lock_timeout`, `idle_in_transaction_session_timeout`,
  `statement_timeout`) and TCP keepalive settings that single engine was
  built with (`modules/db/db_service.py`).
* A saturated pool on the read lane still surfaces as 503 + Retry-After via
  the existing ``PoolSaturationExceptionHandler``.
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from dynastore.extensions.tools.db import (
    ReadConn,
    WriteConn,
    get_async_connection,
    get_read_connection,
    get_write_connection,
)
from dynastore.extensions.tools.exception_handlers import setup_exception_handlers
from dynastore.modules.db_config.exceptions import PoolSaturationError


class _FakeTxnCm:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc):
        return False


class _FakeConn(AsyncConnection):
    """``AsyncConnection`` subclass (instantiated via ``__new__``) so the
    lane dependencies' ``isinstance`` check succeeds without a real DBAPI
    connection -- mirrors the pattern in
    ``test_bounded_pool_acquire_records_features_2948.py``."""

    def begin(self):
        return _FakeTxnCm()

    async def close(self) -> None:
        pass


class _FakeEngine(AsyncEngine):
    """Skips the real ``AsyncEngine.__init__`` since acquisition is mocked."""


class _FakeManagedTransactionCm:
    def __init__(self, engine, *, acquire_timeout=None, read_only=False):
        self.engine = engine
        self.acquire_timeout = acquire_timeout
        self.read_only = read_only

    async def __aenter__(self):
        return _FakeConn.__new__(_FakeConn)

    async def __aexit__(self, *exc):
        return False


def _fake_request(engine):
    class _Request:
        class app:
            class state:
                pass

    _Request.app.state.engine = engine
    return _Request()


def test_get_async_connection_is_the_write_lane_alias():
    """Backward compat: the pre-#2753 dependency name is now the write lane,
    same function object -- existing ``Depends(get_async_connection)`` /
    ``app.dependency_overrides[get_async_connection]`` call sites in the ~80
    handlers keep working untouched."""
    assert get_async_connection is get_write_connection


@pytest.mark.asyncio
async def test_get_write_connection_opens_plain_unbounded_transaction(monkeypatch):
    """The write lane must NOT set ``read_only`` or an ``acquire_timeout`` --
    identical behaviour to the original ``get_async_connection``."""
    from dynastore.extensions.tools import db as db_module

    engine = _FakeEngine.__new__(_FakeEngine)
    captured = {}

    def _fake_managed_transaction(eng, *, acquire_timeout=None, read_only=False):
        captured["engine"] = eng
        captured["acquire_timeout"] = acquire_timeout
        captured["read_only"] = read_only
        return _FakeManagedTransactionCm(
            eng, acquire_timeout=acquire_timeout, read_only=read_only
        )

    monkeypatch.setattr(db_module, "managed_transaction", _fake_managed_transaction)

    gen = db_module.get_write_connection(_fake_request(engine))  # type: ignore[arg-type]
    conn = await gen.__anext__()

    assert isinstance(conn, _FakeConn)
    assert captured["engine"] is engine
    assert captured["acquire_timeout"] is None
    assert captured["read_only"] is False


@pytest.mark.asyncio
async def test_get_read_connection_threads_bounded_timeout_and_read_only(monkeypatch):
    """The read lane must pass a live ``acquire_timeout`` AND
    ``read_only=True`` into ``managed_transaction`` (#2753)."""
    from dynastore.extensions.tools import db as db_module

    engine = _FakeEngine.__new__(_FakeEngine)
    captured = {}

    def _fake_managed_transaction(eng, *, acquire_timeout=None, read_only=False):
        captured["engine"] = eng
        captured["acquire_timeout"] = acquire_timeout
        captured["read_only"] = read_only
        return _FakeManagedTransactionCm(
            eng, acquire_timeout=acquire_timeout, read_only=read_only
        )

    async def _fake_read_live_timeout():
        return 3.5

    monkeypatch.setattr(db_module, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(
        db_module, "_read_live_fg_acquire_timeout", _fake_read_live_timeout
    )

    gen = db_module.get_read_connection(_fake_request(engine))  # type: ignore[arg-type]
    conn = await gen.__anext__()

    assert isinstance(conn, _FakeConn)
    assert captured["engine"] is engine
    assert captured["acquire_timeout"] == 3.5
    assert captured["read_only"] is True


@pytest.mark.asyncio
async def test_read_and_write_lanes_share_the_same_single_engine(monkeypatch):
    """Phase 0 introduces no second engine/pool -- both lanes must resolve
    the identical ``request.app.state.engine`` object, so whatever
    lock-safety GUCs and TCP-keepalive settings that engine was built with
    (``modules/db/db_service.py``) apply uniformly to both lanes."""
    from dynastore.extensions.tools import db as db_module

    shared_engine = _FakeEngine.__new__(_FakeEngine)
    captured_engines = []

    def _fake_managed_transaction(eng, *, acquire_timeout=None, read_only=False):
        captured_engines.append(eng)
        return _FakeManagedTransactionCm(
            eng, acquire_timeout=acquire_timeout, read_only=read_only
        )

    async def _fake_read_live_timeout():
        return 3.5

    monkeypatch.setattr(db_module, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(
        db_module, "_read_live_fg_acquire_timeout", _fake_read_live_timeout
    )

    write_gen = db_module.get_write_connection(_fake_request(shared_engine))  # type: ignore[arg-type]
    await write_gen.__anext__()
    read_gen = db_module.get_read_connection(_fake_request(shared_engine))  # type: ignore[arg-type]
    await read_gen.__anext__()

    assert len(captured_engines) == 2
    assert captured_engines[0] is shared_engine
    assert captured_engines[1] is shared_engine


def test_write_conn_and_read_conn_annotations_wire_to_the_right_dependency():
    """``WriteConn``/``ReadConn`` are the typed FastAPI-boundary lanes
    handlers declare intent with (#2753)."""
    assert WriteConn.__metadata__[0].dependency is get_write_connection
    assert ReadConn.__metadata__[0].dependency is get_read_connection


def test_read_conn_returns_503_with_retry_after_on_pool_saturation():
    """End-to-end: a saturated pool on the read lane fails fast with 503 +
    Retry-After, same contract as the pre-existing bounded read lane."""
    app = FastAPI()
    setup_exception_handlers(app)

    async def _saturated_pool():
        raise PoolSaturationError(
            "Database connection pool saturated after waiting 2.0s "
            "for a free connection (fail-fast bound).",
            retry_after=7,
        )
        yield  # pragma: no cover - unreachable, keeps this an async generator

    @app.get("/read-lane-probe")
    async def _read_route(conn: ReadConn):  # noqa: ARG001
        return {"ok": True}

    app.dependency_overrides[get_read_connection] = _saturated_pool

    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/read-lane-probe")

    assert r.status_code == 503, r.text
    assert r.headers.get("Retry-After") == "7"


def test_write_conn_route_uses_write_connection_dependency():
    """A handler declaring ``WriteConn`` resolves through
    ``get_write_connection``, not the read lane."""
    app = FastAPI()

    captured = {}

    async def _fake_write_conn():
        captured["called"] = True
        yield _FakeConn.__new__(_FakeConn)

    @app.get("/write-lane-probe")
    async def _write_route(conn: WriteConn):  # noqa: ARG001
        return {"ok": True}

    app.dependency_overrides[get_write_connection] = _fake_write_conn

    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/write-lane-probe")

    assert r.status_code == 200, r.text
    assert captured.get("called") is True
