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

"""Unit tests for the post-INSERT catalog lifecycle phase (#1131).

``sync_catalog_initializer`` hooks run *before* the ``catalog.catalogs`` row is
inserted (for physical-schema/table setup). Work that must reference the row —
a config write that persists ``bucket_name`` onto ``GcpCatalogBucketConfig``
or an ``UPDATE`` that must match the row — broke when registered there: GCP's
``provision_enabled=False`` path produced a 0-row status UPDATE (catalog stuck
in ``provisioning``) and a foreign-key violation when the config write
referenced a catalog row that did not yet exist.

The fix adds a ``sync_catalog_post_create`` phase whose hooks run in the same
creation transaction *after* the INSERT. These tests pin both the registry
mechanics and the source-level ordering guarantees so a regression that moves
the call back before the INSERT (or re-registers GCP on the pre-INSERT phase)
fails loudly.
"""
from __future__ import annotations

import asyncio
import inspect
from typing import List

import pytest


class _FakeSavepoint:
    def __init__(self, events: List[str]):
        self._events = events

    async def __aenter__(self) -> "_FakeSavepoint":
        self._events.append("savepoint:enter")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        if exc is None:
            self._events.append("savepoint:exit-commit")
        else:
            self._events.append(f"savepoint:exit-rollback({type(exc).__name__})")
        return False


class _FakeAsyncConnection:
    def __init__(self) -> None:
        self.events: List[str] = []

    def begin_nested(self) -> _FakeSavepoint:
        return _FakeSavepoint(self.events)


@pytest.fixture
def fake_conn(monkeypatch):
    import sqlalchemy.ext.asyncio as sa_asyncio

    monkeypatch.setattr(sa_asyncio, "AsyncConnection", _FakeAsyncConnection)
    return _FakeAsyncConnection()


def _build_manager():
    from dynastore.modules.catalog.lifecycle_manager import LifecycleRegistry

    return LifecycleRegistry()


def test_post_create_hook_is_registered_and_executed(fake_conn):
    mgr = _build_manager()

    seen = {}

    @mgr.sync_catalog_post_create()
    async def my_hook(conn, schema, catalog_id):
        conn.events.append(f"post-create:{schema}/{catalog_id}")
        seen["args"] = (schema, catalog_id)

    asyncio.run(mgr.post_create_catalog(fake_conn, "phys_s", "cat1"))

    assert seen["args"] == ("phys_s", "cat1")
    assert fake_conn.events == [
        "savepoint:enter",
        "post-create:phys_s/cat1",
        "savepoint:exit-commit",
    ]


def test_post_create_phase_is_independent_of_init_phase(fake_conn):
    """A hook on the init phase must NOT fire during post_create, and
    vice-versa — the two phases are separate hook lists."""
    mgr = _build_manager()

    @mgr.sync_catalog_initializer()
    async def init_hook(conn, schema, catalog_id):
        conn.events.append("init")

    @mgr.sync_catalog_post_create()
    async def post_hook(conn, schema, catalog_id):
        conn.events.append("post")

    asyncio.run(mgr.post_create_catalog(fake_conn, "s", "c"))
    assert "post" in fake_conn.events
    assert "init" not in fake_conn.events


def test_failing_post_create_hook_is_isolated(fake_conn):
    """A failing post-create hook rolls back its own SAVEPOINT and is
    non-fatal (does not raise out of post_create_catalog)."""
    mgr = _build_manager()

    @mgr.sync_catalog_post_create()
    async def boom(conn, schema, catalog_id):
        conn.events.append("post")
        raise RuntimeError("kaboom")

    # Must not raise — isolated per the lifecycle contract.
    asyncio.run(mgr.post_create_catalog(fake_conn, "s", "c"))
    assert fake_conn.events == [
        "savepoint:enter",
        "post",
        "savepoint:exit-rollback(RuntimeError)",
    ]


def test_post_create_noop_when_no_hooks(fake_conn):
    mgr = _build_manager()
    asyncio.run(mgr.post_create_catalog(fake_conn, "s", "c"))
    assert fake_conn.events == []


def test_create_catalog_runs_post_create_after_insert():
    """Source-level pin: ``_run_core_init`` must invoke ``init_catalog``
    before ``post_create_catalog``, and the synchronous create path must
    delegate to ``_run_core_init`` after inserting the catalog row.  This is
    the #1131 ordering guarantee, now captured in the extracted private
    method; the INSERT→delegate ordering lives in ``_create_catalog_sync``
    since #2329 split create into sync/async paths."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    # The init steps moved into _run_core_init; check ordering there.
    src_init = inspect.getsource(CatalogService._run_core_init)
    idx_init = src_init.find("init_catalog(")
    idx_post = src_init.find("post_create_catalog(")

    assert idx_init != -1, "_run_core_init should call init_catalog"
    assert idx_post != -1, "_run_core_init must call post_create_catalog"
    assert idx_init < idx_post, (
        "ordering regression: post_create_catalog must run AFTER init_catalog "
        f"in _run_core_init. Got init={idx_init}, post={idx_post}."
    )

    # The sync create path must call _run_core_init after inserting the row.
    src_create = inspect.getsource(CatalogService._create_catalog_sync)
    idx_insert = src_create.find("_insert_catalog_row_with_pk_retry(")
    idx_delegate = src_create.find("_run_core_init(")

    assert idx_insert != -1, "_create_catalog_sync should still INSERT the catalog row"
    assert idx_delegate != -1, "_create_catalog_sync must delegate to _run_core_init"
    assert idx_insert < idx_delegate, (
        "ordering regression: _run_core_init must be called AFTER the catalog "
        f"row INSERT. Got insert={idx_insert}, delegate={idx_delegate}."
    )


def test_gcp_registers_hook_on_post_create_phase():
    """Source-level pin: the GCP module must register its catalog hook on
    the post-create phase, not the pre-INSERT init phase (#1131)."""
    from dynastore.modules.gcp import gcp_module

    src = inspect.getsource(gcp_module)
    assert "sync_catalog_post_create()(self._on_post_create_catalog)" in src, (
        "GCP must register _on_post_create_catalog via sync_catalog_post_create"
    )
    assert "sync_catalog_initializer()(self._on_sync_init_catalog)" not in src, (
        "GCP must no longer register its catalog hook on the pre-INSERT "
        "init phase (that is the #1131 bug)"
    )
