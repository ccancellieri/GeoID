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


def test_create_catalog_async_inserts_row_before_checklist():
    """Source-level pin: ``_create_catalog_async`` must INSERT the catalog row
    before building and persisting the provisioning checklist.  This is the
    always-async ordering guarantee (#2329): the checklist barrier must be in
    place before the executor task can start, and the row must exist first so
    ``build_checklist`` can query active provisioners against the committed id."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    src_create = inspect.getsource(CatalogService._create_catalog_async)
    idx_insert = src_create.find("_insert_catalog_row_with_pk_retry(")
    idx_checklist = src_create.find("build_checklist(")
    idx_task = src_create.find("create_task(")

    assert idx_insert != -1, "_create_catalog_async should INSERT the catalog row"
    assert idx_checklist != -1, "_create_catalog_async must build the checklist"
    assert idx_task != -1, "_create_catalog_async must enqueue a catalog_provision task"
    assert idx_insert < idx_checklist, (
        "ordering regression: checklist build must run AFTER the catalog row INSERT. "
        f"Got insert={idx_insert}, checklist={idx_checklist}."
    )
    assert idx_checklist < idx_task, (
        "ordering regression: task enqueue must run AFTER the checklist is seeded. "
        f"Got checklist={idx_checklist}, task={idx_task}."
    )

    # _run_core_init must call init_catalog (schema + lifecycle DDL).
    src_init = inspect.getsource(CatalogService._run_core_init)
    assert "init_catalog(" in src_init, "_run_core_init should call init_catalog"


def test_gcp_does_not_register_post_create_hook():
    """Source-level pin: the GCP module must NOT register _on_post_create_catalog.

    Both provisioning paths are now covered by provisioner checklist steps
    (gcp_config for provision_enabled=False, gcp_bucket/gcp_eventing for True).
    A lingering sync_catalog_post_create registration would be a dead-code
    double-provision trap — if post_create_catalog were ever called it would
    enqueue a second gcp_provision_catalog task and/or race the checklist.
    """
    from dynastore.modules.gcp import gcp_module

    src = inspect.getsource(gcp_module)
    assert "sync_catalog_post_create()(self._on_post_create_catalog)" not in src, (
        "GCP must NOT register _on_post_create_catalog via sync_catalog_post_create "
        "— that hook is dead; both paths are now provisioner checklist steps"
    )
