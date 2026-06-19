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

"""Regression guard for #1234 — catalog.catalogs read transaction must be
released before the router metadata fan-out runs.

Incident (#1233/#1234): a backend sat ``idle in transaction`` holding an
``AccessShareLock`` on ``catalog.catalogs`` (from ``SELECT * FROM
catalog.catalogs WHERE id = $1``) with ``wait_event = ClientRead`` — the
catalog read transaction was held open across the router metadata fan-out
(``_resolve_catalog_router_metadata`` -> ``get_catalog_metadata``), which
reaches network-capable domain drivers (e.g. Elasticsearch).  While that
fan-out awaited non-DB I/O, the Postgres backend idled in transaction and
its ``AccessShareLock`` convoyed a DDL ``AccessExclusive`` waiter, freezing
the platform.

The fix: read the catalog row inside the transaction, release it, and only
then run the router fan-out — on its own connection (``db_resource`` is no
longer the read connection).  These tests pin both halves of that invariant
for every ``catalog.catalogs`` reader:

* the router fan-out happens strictly AFTER the read transaction has exited;
* the fan-out is not handed the read connection (``db_resource`` is not the
  in-transaction connection), so it never extends the lock hold across I/O.
"""

from __future__ import annotations

from typing import Any, List, Optional

import pytest

import dynastore.modules.catalog.catalog_service as cs


_CONN_SENTINEL = object()  # stands in for the in-transaction read connection


class _FakeTxn:
    """Async CM mimicking ``managed_transaction``; logs enter/exit order."""

    def __init__(self, log: List[Any]) -> None:
        self._log = log

    async def __aenter__(self):
        self._log.append("txn_enter")
        return _CONN_SENTINEL

    async def __aexit__(self, *exc) -> bool:
        self._log.append("txn_exit")
        return False


def _make_service():
    svc = cs.CatalogService.__new__(cs.CatalogService)
    svc.engine = object()  # sentinel; managed_transaction is patched
    return svc


def _install_common(monkeypatch, log: List[Any], router_calls: List[Optional[Any]]):
    """Patch the transaction CM, the row unpack, and the router resolve.

    Returns nothing; mutates ``log`` (event order) and ``router_calls``
    (the ``db_resource`` each fan-out was handed).
    """
    monkeypatch.setattr(cs, "managed_transaction", lambda _res: _FakeTxn(log))

    # router fan-out: record order + the db_resource it received.
    async def _fake_router(self, catalog_id, *, db_resource=None, hints=frozenset()):  # noqa: ANN001
        log.append("router_fanout")
        router_calls.append(db_resource)
        return None

    monkeypatch.setattr(cs.CatalogService, "_resolve_catalog_router_metadata", _fake_router)

    # unpack is pure; return a sentinel truthy object so the caller proceeds.
    monkeypatch.setattr(
        cs.CatalogService,
        "_unpack_catalog_row",
        lambda self, row, router_metadata=None: {"id": "cat", "_row": row},
    )


def _assert_lock_released_before_fanout(
    log: List[Any], router_calls: List[Optional[Any]]
) -> None:
    assert "txn_exit" in log, f"transaction never exited: {log}"
    assert "router_fanout" in log, f"router fan-out never ran: {log}"
    # The read transaction must be CLOSED before the fan-out runs.
    assert log.index("txn_exit") < log.index("router_fanout"), (
        f"router fan-out ran while the catalog.catalogs read transaction was "
        f"still open (idle-in-transaction lock hold): {log}"
    )
    # The fan-out must not be handed the in-transaction read connection.
    assert _CONN_SENTINEL not in router_calls, (
        "router fan-out was handed the in-transaction read connection — it "
        "would extend the catalog.catalogs lock across network I/O"
    )


@pytest.mark.asyncio
async def test_get_catalog_model_db_releases_txn_before_router_fanout(monkeypatch):
    log: List[Any] = []
    router_calls: List[Optional[Any]] = []
    _install_common(monkeypatch, log, router_calls)

    async def _fake_select(conn, **kw):
        log.append("select")
        assert conn is _CONN_SENTINEL  # the SELECT runs in the read txn
        return {"id": "cat"}

    monkeypatch.setattr(cs._get_catalog_query, "execute", _fake_select)

    svc = _make_service()
    out = await svc._get_catalog_model_db("cat")
    assert out is not None
    _assert_lock_released_before_fanout(log, router_calls)


@pytest.mark.asyncio
async def test_get_catalog_model_releases_txn_before_router_fanout(monkeypatch):
    log: List[Any] = []
    router_calls: List[Optional[Any]] = []
    _install_common(monkeypatch, log, router_calls)
    # get_catalog_model runs the pipeline after the read; make it a passthrough.
    monkeypatch.setattr(
        cs.CatalogService,
        "_run_catalog_pipeline",
        lambda self, catalog_id, catalog: _passthrough(catalog),
    )

    async def _fake_select(conn, **kw):
        log.append("select")
        assert conn is _CONN_SENTINEL
        return {"id": "cat"}

    monkeypatch.setattr(cs._get_catalog_query, "execute", _fake_select)

    class _Ctx:
        db_resource = object()  # caller-supplied resource (db_resource path)

    svc = _make_service()
    out = await svc.get_catalog_model("cat", ctx=_Ctx())
    assert out is not None
    _assert_lock_released_before_fanout(log, router_calls)


@pytest.mark.asyncio
async def test_list_catalogs_releases_txn_before_router_fanout(monkeypatch):
    log: List[Any] = []
    router_calls: List[Optional[Any]] = []
    _install_common(monkeypatch, log, router_calls)
    monkeypatch.setattr(cs, "get_catalog_engine", lambda _res=None: object())
    monkeypatch.setattr(
        cs.CatalogService, "_list_catalog_store_driver_types", lambda self: []
    )

    async def _fake_list(conn, **kw):
        log.append("select")
        assert conn is _CONN_SENTINEL
        return [{"id": "cat-1"}, {"id": "cat-2"}]

    monkeypatch.setattr(cs._list_catalogs_query, "execute", _fake_list)

    svc = _make_service()
    out = await svc.list_catalogs(limit=10, offset=0)
    assert len(out) == 2
    # Every per-row fan-out must run after the read txn closed, on its own conn.
    _assert_lock_released_before_fanout(log, router_calls)
    assert _CONN_SENTINEL not in router_calls


async def _passthrough(catalog):
    return catalog
