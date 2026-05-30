"""Catalog DLQ list + one-shot requeue reuse maintenance.py primitives, scoped to
the catalog's task schema_name. Auth is enforced by the admin_catalog_access policy
(tested elsewhere); this pins behavior + scoping."""
from __future__ import annotations

import pytest

from dynastore.extensions.admin import admin_service


@pytest.mark.asyncio
async def test_list_catalog_dead_letter_is_scoped(monkeypatch):
    captured = {}

    async def _list(_engine, schema_name=None):
        captured["schema_name"] = schema_name
        return [{"task_id": "t1", "task_type": "cascade_cleanup"}]

    async def _schema(_cid, _engine):
        return f"cat_{_cid}"

    monkeypatch.setattr(admin_service, "_dlq_list", _list)
    monkeypatch.setattr(admin_service, "_platform_engine", lambda: object())
    monkeypatch.setattr(admin_service, "_catalog_task_schema", _schema)

    out = await admin_service.list_catalog_dead_letter("acme")
    assert captured["schema_name"] == "cat_acme"
    assert out[0]["task_id"] == "t1"


@pytest.mark.asyncio
async def test_requeue_catalog_dead_letter_is_tenant_scoped(monkeypatch):
    # The requeue MUST pass the catalog's resolved schema_name through to the
    # maintenance primitive, so a catalog admin cannot recall another catalog's
    # task by guessing its id (cross-tenant IDOR guard).
    captured = {}

    async def _requeue(_engine, task_id, **kw):
        captured["task_id"] = task_id
        captured["schema_name"] = kw.get("schema_name")
        return True

    async def _schema(_cid, _engine):
        return f"cat_{_cid}"

    monkeypatch.setattr(admin_service, "_dlq_requeue", _requeue)
    monkeypatch.setattr(admin_service, "_platform_engine", lambda: object())
    monkeypatch.setattr(admin_service, "_catalog_task_schema", _schema)

    out = await admin_service.requeue_catalog_dead_letter("acme", "t1")
    assert out == {"task_id": "t1", "requeued": True}
    assert captured["task_id"] == "t1"
    assert captured["schema_name"] == "cat_acme"  # tenant-scoped — no cross-catalog requeue
