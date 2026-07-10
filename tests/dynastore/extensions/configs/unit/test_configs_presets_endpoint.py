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

"""Routing-preset admin endpoint (#847).

Covers:
- GET /configs/presets — list + tier filter (no DB required)
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

from dynastore.extensions.configs.service import ConfigsService


def _app() -> FastAPI:
    app = FastAPI()
    svc = ConfigsService(app)
    app.include_router(svc.router)
    return app


def test_list_routing_presets_returns_registered_names():
    client = TestClient(_app())
    resp = client.get("/configs/presets")
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
    body = client.get("/configs/presets").json()
    by_name = {p["name"]: p for p in body["presets"]}
    assert by_name["defaults_postgres"]["tier"] == "platform"
    assert by_name["private_collection"]["tier"] == "collection"
    assert by_name["public_catalog"]["tier"] == "catalog"


def test_list_routing_presets_filters_by_tier():
    client = TestClient(_app())
    resp = client.get("/configs/presets", params={"tier": "catalog"})
    assert resp.status_code == 200
    tiers = {p["tier"] for p in resp.json()["presets"]}
    assert tiers == {"catalog"}

    resp = client.get("/configs/presets", params={"tier": "platform"})
    names = {p["name"] for p in resp.json()["presets"]}
    assert "defaults_postgres" in names
    assert "public_catalog" not in names


def test_list_routing_presets_unknown_tier_returns_400():
    client = TestClient(_app())
    resp = client.get("/configs/presets", params={"tier": "bogus"})
    assert resp.status_code == 400
    assert "bogus" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Tier-mismatch and unknown-preset 409/404 — no DB required
# ---------------------------------------------------------------------------

def test_apply_unknown_preset_returns_404():
    """Unknown preset name → 404 before any DB access."""
    client = TestClient(_app())
    resp = client.post("/configs/catalogs/cat-x/presets/does_not_exist")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]


def test_delete_unknown_preset_returns_404():
    """Unknown preset name on DELETE → 404 before any DB access."""
    client = TestClient(_app())
    resp = client.delete("/configs/catalogs/cat-x/presets/does_not_exist")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]


def test_apply_catalog_preset_at_platform_url_returns_409():
    """A CATALOG-tier preset applied at the platform URL family is a
    scope/tier mismatch → 409, not 404 (the preset exists)."""
    client = TestClient(_app())
    resp = client.post("/configs/presets/public_catalog")
    assert resp.status_code == 409, resp.text
    assert "platform" in resp.json()["detail"]


def test_apply_unknown_platform_preset_returns_404():
    client = TestClient(_app())
    resp = client.post("/configs/presets/does_not_exist")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]


def test_apply_catalog_preset_at_collection_url_returns_409():
    """A CATALOG-tier preset applied at the collection URL family → 409."""
    client = TestClient(_app())
    resp = client.post(
        "/configs/catalogs/cat-a/collections/col-1/presets/public_catalog"
    )
    assert resp.status_code == 409, resp.text
    assert "collection" in resp.json()["detail"]


def test_apply_collection_preset_at_catalog_url_returns_409():
    """A COLLECTION-tier preset applied at the catalog URL family → 409."""
    client = TestClient(_app())
    resp = client.post("/configs/catalogs/cat-a/presets/private_collection")
    assert resp.status_code == 409, resp.text
    assert "catalog" in resp.json()["detail"]


def test_platform_preset_catalog_scopable_reachable_at_catalog_url():
    """A PLATFORM-tier preset with ``catalog_scopable=True`` (the composite
    bundles) is reachable from the catalog URL family; without the flag it
    stays platform-only."""
    from dynastore.extensions.configs.presets_api import _preset_reachable_at
    from dynastore.modules.storage.presets import PresetTier

    class _Scopable:
        tier = PresetTier.PLATFORM
        catalog_scopable = True

    class _PlatformOnly:
        tier = PresetTier.PLATFORM
        catalog_scopable = False

    assert _preset_reachable_at(_Scopable(), PresetTier.CATALOG) is True
    assert _preset_reachable_at(_Scopable(), PresetTier.PLATFORM) is True
    assert _preset_reachable_at(_Scopable(), PresetTier.COLLECTION) is False
    assert _preset_reachable_at(_PlatformOnly(), PresetTier.CATALOG) is False


def test_apply_collection_preset_unknown_collection_returns_404(monkeypatch):
    """Unknown collection segment → 404 before any config write."""
    catalogs_mock = MagicMock()
    # deleted_at=None explicitly: a bare MagicMock() auto-mocks .deleted_at to
    # a truthy attribute, which the resolver's tombstone check (#3166) would
    # otherwise misread as a soft-deleted catalog and 404 before this test's
    # actual target (the unknown collection) is ever reached.
    catalogs_mock.get_catalog_model = AsyncMock(return_value=MagicMock(deleted_at=None))
    catalogs_mock.collections.get_collection = AsyncMock(return_value=None)

    def _fake_get_protocol(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol

        if proto is CatalogsProtocol:
            return catalogs_mock
        return None

    monkeypatch.setattr(
        "dynastore.extensions.configs.presets_api.get_protocol",
        _fake_get_protocol,
    )

    client = TestClient(_app())
    resp = client.post(
        "/configs/catalogs/cat-a/collections/ghost/presets/private_collection"
    )
    assert resp.status_code == 404
    assert "ghost" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Optional params body — drives the preset; validated against params_model
# ---------------------------------------------------------------------------

def test_coerce_params_no_body_uses_defaults():
    """No body (or an empty body) → None, so the lifecycle applies defaults."""
    from dynastore.extensions.configs.presets_api import _coerce_params
    from dynastore.modules.storage.presets import get_preset

    preset = get_preset("stac_storage")
    assert _coerce_params(preset, None) is None
    assert _coerce_params(preset, {}) is None


def test_coerce_params_valid_body_builds_model():
    """A valid body is validated into the preset's params_model instance."""
    from dynastore.extensions.configs.presets_api import _coerce_params
    from dynastore.modules.storage.presets import get_preset

    preset = get_preset("stac_storage")
    params = _coerce_params(preset, {"stac_level": "items", "stac_storage": "ES"})
    assert params is not None
    assert params.stac_level.value == "items"
    assert params.stac_storage.value == "ES"


def test_coerce_params_invalid_body_raises_422():
    """A body that fails params_model validation → HTTP 422 before any write."""
    from fastapi import HTTPException

    from dynastore.extensions.configs.presets_api import _coerce_params
    from dynastore.modules.storage.presets import get_preset

    preset = get_preset("stac_storage")
    with pytest.raises(HTTPException) as exc_info:
        _coerce_params(preset, {"stac_level": "BOGUS_LEVEL"})
    assert exc_info.value.status_code == 422


def test_apply_catalog_preset_invalid_params_body_returns_422():
    """End-to-end: POSTing an invalid params body to a catalog-tier preset
    returns 422 before reaching the (DB-backed) apply lifecycle."""
    client = TestClient(_app())
    resp = client.post(
        "/configs/catalogs/cat-x/presets/stac_storage",
        json={"stac_level": "BOGUS_LEVEL"},
    )
    assert resp.status_code == 422, resp.text


# ---------------------------------------------------------------------------
# Dry-run params body — mirrors the apply endpoint (#2711)
#
# The dry-run endpoints used to hardcode ``NoParams()``, so a preview always
# ran a different operation than the corresponding apply and a preset with
# required params could not be previewed at all. ``_DummyCollectionPreset``
# below is a test-only COLLECTION-tier preset with a required ``column``
# param and no DB dependency in ``dry_run`` (registered lazily, once per
# process, the same way extension presets self-register on import) — it
# proves the caller-supplied body now reaches the plan, with the same 422
# semantics as apply.
# ---------------------------------------------------------------------------


def _register_dummy_collection_preset() -> None:
    from typing import ClassVar, List, Tuple, Type

    from pydantic import BaseModel, Field

    from dynastore.modules.storage.presets.preset import (
        AppliedDescriptor,
        PresetContext,
        PresetPlan,
        PresetPlanEntry,
    )
    from dynastore.modules.storage.presets.protocol import PresetTier
    from dynastore.modules.storage.presets.registry import get_preset, register_preset

    class _DummyCollectionPresetParams(BaseModel):
        column: str = Field(...)

    class _DummyCollectionPreset:
        name: ClassVar[str] = "test_dummy_collection_preset"
        description: ClassVar[str] = "Test-only collection preset with a required param."
        keywords: ClassVar[Tuple[str, ...]] = ()
        tier: ClassVar[PresetTier] = PresetTier.COLLECTION
        catalog_scopable: ClassVar[bool] = False
        params_model: ClassVar[Type[BaseModel]] = _DummyCollectionPresetParams

        async def dry_run(self, params: BaseModel, scope: str, ctx: PresetContext) -> PresetPlan:
            p = _DummyCollectionPresetParams.model_validate(params.model_dump())
            entries: List[PresetPlanEntry] = [
                PresetPlanEntry(kind="upsert_claim", target=p.column, detail={}),
            ]
            return PresetPlan(preset_name=self.name, scope_key=scope, entries=tuple(entries))

        async def apply(self, params: BaseModel, scope: str, ctx: PresetContext) -> AppliedDescriptor:
            return AppliedDescriptor(payload={})

        async def revoke(self, applied_descriptor: AppliedDescriptor, ctx: PresetContext) -> None:
            return None

    try:
        get_preset(_DummyCollectionPreset.name)
    except KeyError:
        register_preset(_DummyCollectionPreset())


def test_dry_run_collection_preset_uses_caller_params():
    """The dry-run plan reflects the caller-supplied ``column``, not a
    hardcoded ``NoParams()`` preview of a different operation."""
    _register_dummy_collection_preset()

    client = TestClient(_app())
    resp = client.post(
        "/configs/catalogs/cat-a/collections/col-1/presets/test_dummy_collection_preset/dry-run",
        json={"column": "iso3"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["preset_name"] == "test_dummy_collection_preset"
    claim_targets = {e["target"] for e in body["entries"] if e["kind"] == "upsert_claim"}
    assert "iso3" in claim_targets


def test_dry_run_collection_preset_invalid_params_body_returns_422():
    """A body missing the required ``column`` field returns 422 — the same
    status the apply endpoint returns for the identical body."""
    _register_dummy_collection_preset()

    client = TestClient(_app())
    dry_run_resp = client.post(
        "/configs/catalogs/cat-a/collections/col-1/presets/test_dummy_collection_preset/dry-run",
        json={"alias": "iso_a3"},
    )
    apply_resp = client.post(
        "/configs/catalogs/cat-a/collections/col-1/presets/test_dummy_collection_preset",
        json={"alias": "iso_a3"},
    )
    assert dry_run_resp.status_code == 422, dry_run_resp.text
    assert dry_run_resp.status_code == apply_resp.status_code
    assert dry_run_resp.json() == apply_resp.json()
