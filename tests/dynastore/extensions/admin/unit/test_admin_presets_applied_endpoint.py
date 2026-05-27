#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Unit tests for GET /admin/presets/applied (#1425)."""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.admin.admin_service import AdminService


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(AdminService.router)
    return app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dt(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def _make_row(
    preset_name: str,
    scope_key: str,
    state: str = "applied",
    applied_at: Optional[datetime] = None,
) -> dict:
    return {
        "preset_name": preset_name,
        "scope_key": scope_key,
        "state": state,
        "applied_at": applied_at or _dt("2026-05-01T10:00:00+00:00"),
        "applied_by": None,
        "params_snapshot": "{}",
        "revoke_descriptor": None,
        "apply_task_id": None,
        "revoke_task_id": None,
        "last_error": None,
        "updated_at": applied_at or _dt("2026-05-01T10:00:00+00:00"),
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class _FakeIamService:
    """Minimal stand-in for IamService presence check."""


def _fake_get_protocol_with_iam(proto):
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.protocols import DatabaseProtocol

    if proto is IamService:
        return _FakeIamService()
    if proto is DatabaseProtocol:
        db = MagicMock()
        db.engine = MagicMock()
        return db
    return None


def _fake_get_protocol_no_iam(proto):
    from dynastore.modules.iam.iam_service import IamService

    if proto is IamService:
        return None
    return None


# ---------------------------------------------------------------------------
# 200 — valid scope_key variants
# ---------------------------------------------------------------------------

def test_list_applied_platform_scope_returns_200(monkeypatch):
    rows = [_make_row("defaults_postgres", "platform")]

    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol_with_iam,
    )

    with patch(
        "dynastore.modules.iam.applied_presets_service.AppliedPresetsService.list_for_scope",
        new_callable=AsyncMock,
        return_value=(rows, None),
    ):
        client = TestClient(_app())
        resp = client.get("/admin/presets/applied", params={"scope_key": "platform"})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "rows" in body
    assert body["next_cursor"] is None
    assert body["rows"][0]["preset_name"] == "defaults_postgres"


def test_list_applied_catalog_scope_returns_200(monkeypatch):
    rows = [_make_row("public_catalog", "catalog:my-cat")]

    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol_with_iam,
    )

    with patch(
        "dynastore.modules.iam.applied_presets_service.AppliedPresetsService.list_for_scope",
        new_callable=AsyncMock,
        return_value=(rows, None),
    ):
        client = TestClient(_app())
        resp = client.get("/admin/presets/applied", params={"scope_key": "catalog:my-cat"})

    assert resp.status_code == 200, resp.text
    assert resp.json()["rows"][0]["scope_key"] == "catalog:my-cat"


def test_list_applied_collection_scope_returns_200(monkeypatch):
    scope = "catalog:my-cat/collection:col-1"
    rows = [_make_row("private_collection", scope)]

    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol_with_iam,
    )

    with patch(
        "dynastore.modules.iam.applied_presets_service.AppliedPresetsService.list_for_scope",
        new_callable=AsyncMock,
        return_value=(rows, None),
    ):
        client = TestClient(_app())
        resp = client.get("/admin/presets/applied", params={"scope_key": scope})

    assert resp.status_code == 200, resp.text


# ---------------------------------------------------------------------------
# 400 — malformed scope_key
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_scope", [
    "",
    "catalog:",
    "catalog:cat/extra/too/many",
    "unknown:foo",
    "PLATFORM",
])
def test_list_applied_bad_scope_key_returns_400(bad_scope, monkeypatch):
    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol_with_iam,
    )
    client = TestClient(_app())
    resp = client.get("/admin/presets/applied", params={"scope_key": bad_scope})
    assert resp.status_code == 400, f"Expected 400 for {bad_scope!r}, got {resp.status_code}"


def test_list_applied_missing_scope_key_returns_422():
    """scope_key is a required query parameter."""
    client = TestClient(_app())
    resp = client.get("/admin/presets/applied")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# 400 — malformed cursor
# ---------------------------------------------------------------------------

def test_list_applied_bad_cursor_returns_400(monkeypatch):
    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol_with_iam,
    )
    client = TestClient(_app())
    resp = client.get(
        "/admin/presets/applied",
        params={"scope_key": "platform", "cursor": "not_valid_base64!!!"},
    )
    assert resp.status_code == 400, resp.text


# ---------------------------------------------------------------------------
# 503 — IAM not loaded
# ---------------------------------------------------------------------------

def test_list_applied_503_when_iam_not_loaded(monkeypatch):
    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol_no_iam,
    )
    client = TestClient(_app())
    resp = client.get("/admin/presets/applied", params={"scope_key": "platform"})
    assert resp.status_code == 503, resp.text
    assert "IAM" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Cursor round-trip
# ---------------------------------------------------------------------------

def test_list_applied_returns_and_accepts_cursor(monkeypatch):
    from dynastore.modules.iam.applied_presets_service import _encode_cursor

    rows = [_make_row("preset_a", "platform")]
    next_cursor = _encode_cursor("2026-05-01T10:00:00+00:00", "preset_a")

    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol_with_iam,
    )

    with patch(
        "dynastore.modules.iam.applied_presets_service.AppliedPresetsService.list_for_scope",
        new_callable=AsyncMock,
        return_value=(rows, next_cursor),
    ) as mock_svc:
        client = TestClient(_app())

        # First page returns next_cursor.
        resp1 = client.get("/admin/presets/applied", params={"scope_key": "platform"})
        assert resp1.status_code == 200
        assert resp1.json()["next_cursor"] == next_cursor

        # Use the cursor on the next request — must succeed.
        resp2 = client.get(
            "/admin/presets/applied",
            params={"scope_key": "platform", "cursor": next_cursor},
        )
        assert resp2.status_code == 200
        # list_for_scope was called twice.
        assert mock_svc.await_count == 2
        # Second call received the cursor.
        second_call_kwargs = mock_svc.await_args_list[1][1]
        assert second_call_kwargs["cursor"] == next_cursor


# ---------------------------------------------------------------------------
# 403 — no auth (default auth is fail-closed when IAM absent)
# ---------------------------------------------------------------------------

def test_list_applied_no_iam_returns_503():
    """Without IAM loaded, get_protocol(IamService) returns None → 503."""
    # No monkeypatch: default get_protocol returns None (test env has no modules).
    client = TestClient(_app())
    resp = client.get("/admin/presets/applied", params={"scope_key": "platform"})
    # In tests without IAM, get_protocol(IamService) returns None → 503.
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# state filter forwarded correctly
# ---------------------------------------------------------------------------

def test_list_applied_state_filter_forwarded(monkeypatch):
    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol_with_iam,
    )

    with patch(
        "dynastore.modules.iam.applied_presets_service.AppliedPresetsService.list_for_scope",
        new_callable=AsyncMock,
        return_value=([], None),
    ) as mock_svc:
        client = TestClient(_app())
        resp = client.get(
            "/admin/presets/applied",
            params={"scope_key": "platform", "state": "revoked"},
        )

    assert resp.status_code == 200
    call_kwargs = mock_svc.await_args[1]
    assert call_kwargs["state"] == "revoked"


def test_list_applied_invalid_state_returns_400(monkeypatch):
    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol_with_iam,
    )
    client = TestClient(_app())
    resp = client.get(
        "/admin/presets/applied",
        params={"scope_key": "platform", "state": "totally_invalid"},
    )
    assert resp.status_code == 400
