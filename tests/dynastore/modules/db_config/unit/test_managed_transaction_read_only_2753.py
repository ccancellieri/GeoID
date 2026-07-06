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

"""``managed_transaction(..., read_only=True)`` -- the read-lane primitive
introduced for #2753 step 2.

On the engine branch, ``read_only=True`` must put the checked-out connection
into ``postgresql_readonly`` execution mode (PostgreSQL then opens the
transaction with ``SET TRANSACTION READ ONLY``) BEFORE ``begin()`` is called,
so any write attempted through it is rejected by the database rather than
silently applied. ``read_only`` defaults to ``False`` and every existing
caller that omits the kwarg must see byte-for-byte unchanged behaviour (no
``execution_options`` call at all).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import List

import pytest


class _FakeTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self, *, log: List[str]):
        self._log = log
        self.begin_count = 0
        self.closed = False
        self.sync_connection = SimpleNamespace()
        self.execution_options_calls: List[dict] = []

    async def rollback(self):
        self._log.append("rollback")

    async def invalidate(self):
        self._log.append("invalidate")

    async def close(self):
        self.closed = True
        self._log.append("close")

    def begin(self):
        self.begin_count += 1
        self._log.append("begin")
        return _FakeTx()

    def in_transaction(self) -> bool:
        return False

    async def execution_options(self, **opts):
        self.execution_options_calls.append(opts)
        self._log.append(f"execution_options:{opts}")
        return self


class _FakeEngine:
    def __init__(self, conns: List[_FakeConn]):
        self._conns = list(conns)
        self.connect_calls = 0

    async def connect(self):
        self.connect_calls += 1
        return self._conns.pop(0)


def _patch_async_engine(monkeypatch):
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)
    monkeypatch.setattr(qe, "AsyncEngine", _FakeEngine, raising=True)
    monkeypatch.setattr(qe, "is_async_resource", lambda r: True, raising=True)
    return qe


@pytest.mark.asyncio
async def test_read_only_true_sets_postgresql_readonly_before_begin(monkeypatch):
    qe = _patch_async_engine(monkeypatch)

    log: List[str] = []
    conn = _FakeConn(log=log)
    engine = _FakeEngine([conn])

    async with qe.managed_transaction(engine, read_only=True) as yielded:  # type: ignore[arg-type]
        assert yielded is conn

    assert conn.execution_options_calls == [{"postgresql_readonly": True}]
    # execution_options must be applied before begin() opens the transaction.
    assert log.index("execution_options:{'postgresql_readonly': True}") < log.index(
        "begin"
    )
    assert log == [
        "rollback",
        "execution_options:{'postgresql_readonly': True}",
        "begin",
        "close",
    ]


@pytest.mark.asyncio
async def test_read_only_default_false_never_calls_execution_options(monkeypatch):
    """Every existing caller omits ``read_only`` -- behaviour must be
    byte-for-byte unchanged: no ``execution_options`` call at all."""
    qe = _patch_async_engine(monkeypatch)

    log: List[str] = []
    conn = _FakeConn(log=log)
    engine = _FakeEngine([conn])

    async with qe.managed_transaction(engine) as yielded:  # type: ignore[arg-type]
        assert yielded is conn

    assert conn.execution_options_calls == []
    assert log == ["rollback", "begin", "close"]
