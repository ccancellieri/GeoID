"""Routing-preset admin endpoint (#847).

Covers:
- GET /admin/presets — list + tier filter (no DB required)
- Tier-mismatch 409 on POST/DELETE (no DB required, resolved before dispatch)
- 404 for unknown preset names (no DB required)
- Unknown-collection 404 on collection-scoped apply (uses CatalogsProtocol stub)

Apply and revoke (POST/DELETE) tests that exercise the audit-backed lifecycle
(``AppliedPresetsService``) require a live PostgreSQL instance and live in
``tests/dynastore/extensions/admin/integration/test_preset_apply_configs_service.py``.
The unit tests below only cover paths that short-circuit before the DB layer.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.admin.admin_service import AdminService


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(AdminService.router)
    return app


def test_list_routing_presets_returns_registered_names():
    client = TestClient(_app())
    resp = client.get("/admin/presets")
    assert resp.status_code == 200
    body = resp.json()
    names = {p["name"] for p in body["presets"]}
    assert {"public_catalog", "private_catalog"}.issubset(names)
    for entry in body["presets"]:
        assert entry["description"]
        # PR-2: every preset advertises its tier + catalog_scopable flag.
        assert entry["tier"]
        assert "catalog_scopable" in entry


def test_list_routing_presets_includes_multitier_presets():
    """The shipped platform + collection presets surface with their tiers."""
    import dynastore.extensions.geoid  # noqa: F401 — register geoid preset

    client = TestClient(_app())
    body = client.get("/admin/presets").json()
    by_name = {p["name"]: p for p in body["presets"]}
    assert by_name["defaults_postgres"]["tier"] == "platform"
    assert by_name["private_collection"]["tier"] == "collection"
    assert by_name["public_catalog"]["tier"] == "catalog"


def test_list_routing_presets_filters_by_tier():
    client = TestClient(_app())
    resp = client.get("/admin/presets", params={"tier": "catalog"})
    assert resp.status_code == 200
    tiers = {p["tier"] for p in resp.json()["presets"]}
    assert tiers == {"catalog"}

    resp = client.get("/admin/presets", params={"tier": "platform"})
    names = {p["name"] for p in resp.json()["presets"]}
    assert "defaults_postgres" in names
    assert "public_catalog" not in names


def test_list_routing_presets_unknown_tier_returns_400():
    client = TestClient(_app())
    resp = client.get("/admin/presets", params={"tier": "bogus"})
    assert resp.status_code == 400
    assert "bogus" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Tier-mismatch and unknown-preset 409/404 — no DB required
# ---------------------------------------------------------------------------

def test_apply_unknown_preset_returns_404():
    """Unknown preset name → 404 before any DB access."""
    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-x/presets/does_not_exist")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]


def test_delete_unknown_preset_returns_404():
    """Unknown preset name on DELETE → 404 before any DB access."""
    client = TestClient(_app())
    resp = client.delete("/admin/catalogs/cat-x/presets/does_not_exist")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]


def test_apply_catalog_preset_at_platform_url_returns_409():
    """A CATALOG-tier preset applied at the platform URL family is a
    scope/tier mismatch → 409, not 404 (the preset exists)."""
    client = TestClient(_app())
    resp = client.post("/admin/presets/public_catalog")
    assert resp.status_code == 409, resp.text
    assert "platform" in resp.json()["detail"]


def test_apply_unknown_platform_preset_returns_404():
    client = TestClient(_app())
    resp = client.post("/admin/presets/does_not_exist")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]


def test_apply_catalog_preset_at_collection_url_returns_409():
    """A CATALOG-tier preset applied at the collection URL family → 409."""
    client = TestClient(_app())
    resp = client.post(
        "/admin/catalogs/cat-a/collections/col-1/presets/public_catalog"
    )
    assert resp.status_code == 409, resp.text
    assert "collection" in resp.json()["detail"]


def test_apply_collection_preset_at_catalog_url_returns_409():
    """A COLLECTION-tier preset applied at the catalog URL family → 409."""
    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-a/presets/private_collection")
    assert resp.status_code == 409, resp.text
    assert "catalog" in resp.json()["detail"]


def test_apply_collection_preset_unknown_collection_returns_404(monkeypatch):
    """Unknown collection segment → 404 before any config write."""
    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=MagicMock())
    catalogs_mock.collections.get_collection = AsyncMock(return_value=None)

    def _fake_get_protocol(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        if proto is CatalogsProtocol:
            return catalogs_mock
        return None

    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol,
    )

    client = TestClient(_app())
    resp = client.post(
        "/admin/catalogs/cat-a/collections/ghost/presets/private_collection"
    )
    assert resp.status_code == 404
    assert "ghost" in resp.json()["detail"]
