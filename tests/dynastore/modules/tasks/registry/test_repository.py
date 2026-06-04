"""UPSERT SQL construction + driver-agnostic IO for the task-capability registry."""
from __future__ import annotations

import asyncio
import contextlib

from dynastore.modules.tasks.registry import repository as repo
from dynastore.modules.tasks.registry.model import CapabilityRow


def test_upsert_sql_is_on_conflict_pk():
    sql = repo._UPSERT_SQL.lower()
    assert "insert into configs.task_capability_registry" in sql
    assert "on conflict (service, task_key) do update" in sql
    # last_seen + updated_at refreshed on structural change
    assert "updated_at = now()" in sql


class _SyncOnlyEngine:
    """Mimics a job's sync-driver engine: ``begin()`` returns a *sync* context
    manager (``contextlib._GeneratorContextManager``). ``async with`` on it
    raises ``TypeError: ... does not support the asynchronous context manager
    protocol`` — the exact production failure. If the repository ever reverts to
    ``async with engine.begin()`` instead of the driver-agnostic
    ``managed_transaction``, the heartbeat/upsert tests below will hit this and
    fail.
    """

    def begin(self):
        @contextlib.contextmanager
        def _cm():
            yield object()

        return _cm()

    # connect() would be the read-path equivalent; same sync-only trap.
    connect = begin


def _patch_agnostic_io(monkeypatch):
    """Patch managed_transaction + DQLQuery.execute to record the agnostic path
    without touching a real DB, and return the recorder."""
    calls = {"mt": 0, "exec": [], "engines": [], "conns": []}
    sentinel_conn = object()

    @contextlib.asynccontextmanager
    async def _fake_mt(engine):
        calls["mt"] += 1
        calls["engines"].append(engine)
        yield sentinel_conn  # a fake connection threaded into DQLQuery.execute

    async def _fake_execute(self, conn, **kw):
        calls["conns"].append(conn)
        calls["exec"].append((self.template, kw))
        return None

    monkeypatch.setattr(repo, "managed_transaction", _fake_mt)
    monkeypatch.setattr(repo.DQLQuery, "execute", _fake_execute)
    return calls


def test_heartbeat_is_driver_agnostic(monkeypatch):
    """Regression for the review incident: the heartbeat must route through
    ``managed_transaction`` + ``DQLQuery`` so it works under a job's SYNC driver,
    not ``async with engine.begin()`` (async-only)."""
    calls = _patch_agnostic_io(monkeypatch)

    # A sync-only engine: if heartbeat touched engine.begin() this would raise.
    asyncio.run(repo.heartbeat(_SyncOnlyEngine(), "svc-A"))

    assert calls["mt"] == 1, "heartbeat must acquire via managed_transaction"
    assert len(calls["exec"]) == 1
    template, kw = calls["exec"][0]
    assert "task_capability_registry" in template
    assert kw == {"service": "svc-A"}


def test_upsert_rows_is_driver_agnostic(monkeypatch):
    """upsert_rows runs on deploy from any pod (incl. sync-driver jobs); it must
    also go through the agnostic path and bind every row's columns."""
    calls = _patch_agnostic_io(monkeypatch)

    rows = [
        CapabilityRow(
            service="svc-A",
            task_key="ingestion",
            kind="task",
            service_version="1.2.3",
            service_commit="abc123",
            version="abc123",
        )
    ]
    n = asyncio.run(repo.upsert_rows(_SyncOnlyEngine(), rows))

    assert n == 1
    assert calls["mt"] == 1
    assert len(calls["exec"]) == 1
    template, kw = calls["exec"][0]
    assert "insert into" in template.lower()
    # bind keys must match the SQL placeholders
    assert kw["service"] == "svc-A"
    assert kw["task_key"] == "ingestion"
    # payload_schema is JSON-serialized (None stays None) for the jsonb CAST bind
    assert kw["payload_schema"] is None


def test_upsert_rows_empty_is_noop(monkeypatch):
    calls = _patch_agnostic_io(monkeypatch)
    n = asyncio.run(repo.upsert_rows(_SyncOnlyEngine(), []))
    assert n == 0
    assert calls["mt"] == 0, "no transaction opened for an empty row set"
