"""Tests for the admin task-dispatch surface.

Covers:
  - dispatch_catalog_task routes to correct task_type / inputs
  - dispatch_collection_task routes to correct task_type / inputs
  - 400 on unknown action (both routes)
  - 400 when backfill_envelope_attrs hits the catalog-only route
  - 202 response shape matches AdminTaskResponse
  - policy declarations: two new policies exist with correct resource patterns
  - role-binding declarations include the expected assignments
"""
from __future__ import annotations

import re
import pytest

from dynastore.extensions.admin import task_dispatch as td
from dynastore.extensions.admin.policies import admin_policies, admin_role_bindings


# ---------------------------------------------------------------------------
# Task dispatch unit tests (no DB / app — engine injected via monkeypatch)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_catalog_reindex_correct_task_type_and_inputs(monkeypatch):
    """catalog-only reindex: task_type=elasticsearch_indexer, inputs={catalog_id}."""
    captured = {}

    class _FakeTask:
        task_id = "abc-123"

    async def _fake_create(engine, task_data, catalog_id):
        captured["task_type"] = task_data.task_type
        captured["inputs"] = task_data.inputs
        captured["caller_id"] = task_data.caller_id
        captured["catalog_id"] = catalog_id
        return _FakeTask()

    monkeypatch.setattr(td, "_get_engine", lambda: object())
    import dynastore.modules.tasks.tasks_module as _tm
    monkeypatch.setattr(_tm, "create_task_for_catalog", _fake_create)

    result = await td.dispatch_catalog_task("acme", "reindex", {})

    assert captured["task_type"] == "elasticsearch_indexer"
    assert captured["inputs"] == {"catalog_id": "acme"}
    assert captured["caller_id"] == "system:admin"
    assert captured["catalog_id"] == "acme"
    assert result["task_id"] == "abc-123"
    assert result["action"] == "reindex"
    assert result["target"] == {"catalog_id": "acme", "collection_id": None}
    assert result["status"] == "queued"


@pytest.mark.asyncio
async def test_dispatch_catalog_reindex_with_driver(monkeypatch):
    """driver param is threaded through to inputs when present."""
    captured = {}

    class _FakeTask:
        task_id = "t-driver"

    async def _fake_create(engine, task_data, catalog_id):
        captured["inputs"] = task_data.inputs
        return _FakeTask()

    monkeypatch.setattr(td, "_get_engine", lambda: object())
    import dynastore.modules.tasks.tasks_module as _tm
    monkeypatch.setattr(_tm, "create_task_for_catalog", _fake_create)

    await td.dispatch_catalog_task("acme", "reindex", {"driver": "my_driver"})
    assert captured["inputs"] == {"catalog_id": "acme", "driver": "my_driver"}


@pytest.mark.asyncio
async def test_dispatch_collection_reindex_correct_task_type_and_inputs(monkeypatch):
    """collection reindex: task_type=elasticsearch_indexer, inputs include collection_id."""
    captured = {}

    class _FakeTask:
        task_id = "col-456"

    async def _fake_create(engine, task_data, catalog_id):
        captured["task_type"] = task_data.task_type
        captured["inputs"] = task_data.inputs
        return _FakeTask()

    monkeypatch.setattr(td, "_get_engine", lambda: object())
    import dynastore.modules.tasks.tasks_module as _tm
    monkeypatch.setattr(_tm, "create_task_for_catalog", _fake_create)

    result = await td.dispatch_collection_task("acme", "s2", "reindex", {})

    assert captured["task_type"] == "elasticsearch_indexer"
    assert captured["inputs"] == {"catalog_id": "acme", "collection_id": "s2"}
    assert result["target"] == {"catalog_id": "acme", "collection_id": "s2"}


@pytest.mark.asyncio
async def test_dispatch_collection_backfill_correct_task_type_and_inputs(monkeypatch):
    """backfill_envelope_attrs: task_type=envelope_attrs_backfill_collection, default batch_size=500."""
    captured = {}

    class _FakeTask:
        task_id = "bf-789"

    async def _fake_create(engine, task_data, catalog_id):
        captured["task_type"] = task_data.task_type
        captured["inputs"] = task_data.inputs
        return _FakeTask()

    monkeypatch.setattr(td, "_get_engine", lambda: object())
    import dynastore.modules.tasks.tasks_module as _tm
    monkeypatch.setattr(_tm, "create_task_for_catalog", _fake_create)

    result = await td.dispatch_collection_task("acme", "s2", "backfill_envelope_attrs", {})

    assert captured["task_type"] == "envelope_attrs_backfill_collection"
    assert captured["inputs"] == {
        "catalog_id": "acme",
        "collection_id": "s2",
        "dry_run": False,
        "batch_size": 500,
    }
    assert result["action"] == "backfill_envelope_attrs"
    assert result["status"] == "queued"


@pytest.mark.asyncio
async def test_dispatch_collection_backfill_custom_params(monkeypatch):
    """dry_run and batch_size params are forwarded when explicitly set."""
    captured = {}

    class _FakeTask:
        task_id = "bf-custom"

    async def _fake_create(engine, task_data, catalog_id):
        captured["inputs"] = task_data.inputs
        return _FakeTask()

    monkeypatch.setattr(td, "_get_engine", lambda: object())
    import dynastore.modules.tasks.tasks_module as _tm
    monkeypatch.setattr(_tm, "create_task_for_catalog", _fake_create)

    await td.dispatch_collection_task(
        "acme", "s2", "backfill_envelope_attrs",
        {"dry_run": True, "batch_size": 200},
    )
    assert captured["inputs"]["dry_run"] is True
    assert captured["inputs"]["batch_size"] == 200


@pytest.mark.asyncio
async def test_unknown_action_catalog_route_raises_400(monkeypatch):
    """Unknown action on catalog route returns 400 with supported-actions list."""
    from fastapi import HTTPException

    monkeypatch.setattr(td, "_get_engine", lambda: object())

    with pytest.raises(HTTPException) as exc:
        await td.dispatch_catalog_task("acme", "not_a_real_action", {})

    assert exc.value.status_code == 400
    assert "not_a_real_action" in exc.value.detail
    # The error message lists the supported actions so the caller knows what to use.
    assert "reindex" in exc.value.detail


@pytest.mark.asyncio
async def test_unknown_action_collection_route_raises_400(monkeypatch):
    """Unknown action on collection route returns 400."""
    from fastapi import HTTPException

    monkeypatch.setattr(td, "_get_engine", lambda: object())

    with pytest.raises(HTTPException) as exc:
        await td.dispatch_collection_task("acme", "s2", "does_not_exist", {})

    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_backfill_on_catalog_only_route_raises_400(monkeypatch):
    """backfill_envelope_attrs is collection-only; hitting the catalog route returns 400."""
    from fastapi import HTTPException

    monkeypatch.setattr(td, "_get_engine", lambda: object())

    with pytest.raises(HTTPException) as exc:
        await td.dispatch_catalog_task("acme", "backfill_envelope_attrs", {})

    assert exc.value.status_code == 400
    # Error message should make the collection route obvious.
    assert "collection" in exc.value.detail.lower()


@pytest.mark.asyncio
async def test_dispatch_503_when_engine_unavailable(monkeypatch):
    """Returns 503 when DatabaseProtocol is not available."""
    from fastapi import HTTPException

    monkeypatch.setattr(td, "_get_engine", lambda: None)

    with pytest.raises(HTTPException) as exc:
        await td.dispatch_catalog_task("acme", "reindex", {})

    assert exc.value.status_code == 503


# ---------------------------------------------------------------------------
# 202 response shape test — via AdminTaskResponse model directly
# ---------------------------------------------------------------------------

def test_admin_task_response_shape():
    """AdminTaskResponse serializes to the expected wire shape."""
    from dynastore.extensions.admin.models import AdminTaskResponse, AdminTaskTarget

    resp = AdminTaskResponse(
        task_id="t-001",
        action="reindex",
        target=AdminTaskTarget(catalog_id="acme", collection_id=None),
        status="queued",
    )
    payload = resp.model_dump()
    assert payload["task_id"] == "t-001"
    assert payload["action"] == "reindex"
    assert payload["target"]["catalog_id"] == "acme"
    assert payload["target"]["collection_id"] is None
    assert payload["status"] == "queued"


def test_admin_task_response_shape_with_collection():
    """AdminTaskResponse with collection_id set serializes correctly."""
    from dynastore.extensions.admin.models import AdminTaskResponse, AdminTaskTarget

    resp = AdminTaskResponse(
        task_id="t-002",
        action="backfill_envelope_attrs",
        target=AdminTaskTarget(catalog_id="acme", collection_id="s2"),
        status="queued",
    )
    payload = resp.model_dump()
    assert payload["target"]["collection_id"] == "s2"


# ---------------------------------------------------------------------------
# Policy declaration tests
# ---------------------------------------------------------------------------

def test_admin_task_dispatch_policy_exists():
    """admin_task_dispatch policy is declared with expected resource pattern."""
    policies = {p.id: p for p in admin_policies()}

    assert "admin_task_dispatch" in policies, (
        "admin_task_dispatch policy must be declared in admin_policies()"
    )
    p = policies["admin_task_dispatch"]
    assert "POST" in p.actions
    assert p.effect == "ALLOW"
    # Must match /admin/catalogs/{cat}/tasks (not the collection sub-path)
    pattern = p.resources[0]
    assert re.match(pattern, "/admin/catalogs/acme/tasks"), (
        f"Pattern {pattern!r} should match /admin/catalogs/acme/tasks"
    )
    # Must NOT match the collection-scoped path (different policy)
    assert not re.match(pattern, "/admin/catalogs/acme/collections/s2/tasks"), (
        f"Pattern {pattern!r} must not match the collection-scoped task path"
    )


def test_admin_task_dispatch_collection_policy_exists():
    """admin_task_dispatch_collection policy is declared with expected resource pattern."""
    policies = {p.id: p for p in admin_policies()}

    assert "admin_task_dispatch_collection" in policies, (
        "admin_task_dispatch_collection policy must be declared in admin_policies()"
    )
    p = policies["admin_task_dispatch_collection"]
    assert "POST" in p.actions
    assert p.effect == "ALLOW"
    pattern = p.resources[0]
    assert re.match(pattern, "/admin/catalogs/acme/collections/s2/tasks"), (
        f"Pattern {pattern!r} should match /admin/catalogs/acme/collections/s2/tasks"
    )


def test_admin_task_dispatch_role_bindings():
    """Role bindings wire the two new policies to the correct roles."""
    from dynastore.models.protocols.authorization import IamRolesConfig
    cfg = IamRolesConfig()
    sysadmin = cfg.sysadmin_role_name
    admin = cfg.admin_role_name

    bindings = admin_role_bindings()
    # Build a dict: role_name -> set of policy ids bound to that role
    role_policies: dict = {}
    for rb in bindings:
        role_policies.setdefault(rb.name, set()).update(rb.policies or [])

    assert "admin_task_dispatch" in role_policies.get(sysadmin, set()), (
        f"sysadmin ({sysadmin}) must be bound to admin_task_dispatch"
    )
    assert "admin_task_dispatch" in role_policies.get(admin, set()), (
        f"admin ({admin}) must be bound to admin_task_dispatch"
    )
    assert "admin_task_dispatch_collection" in role_policies.get(sysadmin, set()), (
        f"sysadmin ({sysadmin}) must be bound to admin_task_dispatch_collection"
    )
    # admin role is also bound to the collection-dispatch policy: collection
    # scope is a subset of the catalog scope, so it carries the same
    # admin+sysadmin binding rather than a tighter one.
    assert "admin_task_dispatch_collection" in role_policies.get(admin, set()), (
        f"admin ({admin}) must be bound to admin_task_dispatch_collection "
        "(collection scope is a subset of the catalog scope it can reindex)"
    )
