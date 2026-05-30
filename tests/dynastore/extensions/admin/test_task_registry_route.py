"""The /admin/task-registry read view serializes registry rows. Auth is
enforced by IamMiddleware policy (covered by the admin policy suite), so this
test pins the handler's serialization path only — no DB or app needed."""
from __future__ import annotations

import pytest

from dynastore.extensions.admin import admin_service


@pytest.mark.asyncio
async def test_list_task_registry_serializes_rows(monkeypatch):
    fake_rows = [
        {"service": "worker", "task_key": "gdal", "kind": "process",
         "modes": ["async"], "mandatory": False, "affinity_tier": None,
         "service_commit": "c1"},
    ]

    async def _fake_list_all(_engine):
        return fake_rows

    monkeypatch.setattr(admin_service, "_registry_list_all", _fake_list_all)
    monkeypatch.setattr(admin_service, "_platform_engine", lambda: object())

    out = await admin_service.list_task_registry()
    assert out == fake_rows


@pytest.mark.asyncio
async def test_list_task_registry_503_when_engine_unavailable(monkeypatch):
    from fastapi import HTTPException

    monkeypatch.setattr(admin_service, "_platform_engine", lambda: None)

    with pytest.raises(HTTPException) as exc:
        await admin_service.list_task_registry()
    assert exc.value.status_code == 503
