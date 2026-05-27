"""Routing-preset admin endpoint (#847)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.admin.admin_service import AdminService
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
)


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


@pytest.fixture
def _patched_protocols(monkeypatch):
    """Stub ``CatalogsProtocol`` (so _assert_catalog_exists passes) and
    ``ConfigsProtocol`` (so set_config records calls)."""
    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=MagicMock())
    catalogs_mock.collections.get_collection = AsyncMock(return_value=MagicMock())

    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(return_value=None)

    def _fake_get_protocol(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol

        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is ConfigsProtocol:
            return configs_mock
        return None

    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol,
    )
    return configs_mock


def test_apply_public_catalog_preset_walks_all_three_routing_tiers(_patched_protocols):
    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-pub/presets/public_catalog")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["preset"] == "public_catalog"
    assert body["catalog_id"] == "cat-pub"
    assert body["applied"] == [
        "catalog_routing",
        "collection_template",
        "items_template",
    ]

    classes = [call.args[0] for call in _patched_protocols.set_config.await_args_list]
    assert classes == [
        CatalogRoutingConfig,
        CollectionRoutingConfig,
        ItemsRoutingConfig,
    ]
    for call in _patched_protocols.set_config.await_args_list:
        assert call.kwargs["catalog_id"] == "cat-pub"


def test_apply_private_catalog_preset_includes_only_routing_no_audiences(_patched_protocols):
    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-priv/presets/private_catalog")
    assert resp.status_code == 200, resp.text
    assert resp.json()["applied"] == [
        "catalog_routing",
        "collection_template",
        "items_template",
        "asset_template",
    ]


def test_apply_unknown_preset_returns_404(_patched_protocols):
    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-x/presets/does_not_exist")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]


def test_apply_geoid_preset_flows_audience_configs_through_set_config(_patched_protocols):
    """Geoid preset emits one audience config (lookup-only) in addition to
    the three routing tiers. Pin that the apply loop walks the audience dict
    and invokes ``set_config`` for it — otherwise the operator-PUT path
    silently skips the anonymous lookup opt-in and the catalog ships with a
    private-only posture (no anonymous lookup).

    The preset must NOT emit ``collection_write_audience``: lookup-only mode
    cannot coexist with anonymous create (un-fao/GeoID#1204).
    """
    # Geoid preset must be registered (extension auto-registers on import).
    import dynastore.extensions.geoid  # noqa: F401
    from dynastore.modules.iam.audience_configs import (
        CatalogLookupAudience,
        CollectionWriteAudience,
    )

    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-fao/presets/geoid")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["applied"] == [
        "catalog_routing",
        "collection_template",
        "items_template",
        "asset_template",
        "audience:catalog_lookup_audience",
    ]

    classes = [call.args[0] for call in _patched_protocols.set_config.await_args_list]
    assert CatalogLookupAudience in classes
    assert CollectionWriteAudience not in classes

    # The audience set_config call must scope to the catalog tier.
    audience_calls = [
        c for c in _patched_protocols.set_config.await_args_list
        if c.args[0] is CatalogLookupAudience
    ]
    for call in audience_calls:
        assert call.kwargs["catalog_id"] == "cat-fao"
        assert "collection_id" not in call.kwargs


def test_apply_preset_is_idempotent_under_repeated_calls(_patched_protocols):
    """Re-applying the same preset must succeed — the notebook re-runs
    section 2a and operators may rerun apply during incident triage.
    The endpoint defers idempotency to ``set_config`` itself; this test
    just pins that two back-to-back applies don't raise."""
    client = TestClient(_app())
    r1 = client.post("/admin/catalogs/cat-pub/presets/public_catalog")
    r2 = client.post("/admin/catalogs/cat-pub/presets/public_catalog")
    assert r1.status_code == 200
    assert r2.status_code == 200
    # Six set_config calls total — three per apply.
    assert _patched_protocols.set_config.await_count == 6


# ---------------------------------------------------------------------------
# DELETE /admin/catalogs/{catalog_id}/presets/{preset_name} — #971 rollback
# ---------------------------------------------------------------------------


@pytest.fixture
def _patched_protocols_with_persistence(monkeypatch):
    """Like ``_patched_protocols`` but also models ``get_persisted_config``
    and ``delete_config`` against an in-memory ``{class: dict}`` store so
    DELETE can drive the byte-for-byte equality check end-to-end."""
    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=MagicMock())
    catalogs_mock.collections.get_collection = AsyncMock(return_value=MagicMock())

    store: dict[type, dict] = {}

    configs_mock = MagicMock()
    configs_mock._store = store

    async def _set_config(config_cls, config, **kwargs):
        store[config_cls] = config.model_dump(mode="json")
        return config

    async def _get_persisted(config_cls, **kwargs):
        return store.get(config_cls)

    async def _delete(config_cls, **kwargs):
        store.pop(config_cls, None)

    configs_mock.set_config = AsyncMock(side_effect=_set_config)
    configs_mock.get_persisted_config = AsyncMock(side_effect=_get_persisted)
    configs_mock.delete_config = AsyncMock(side_effect=_delete)

    def _fake_get_protocol(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol

        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is ConfigsProtocol:
            return configs_mock
        return None

    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol,
    )
    return configs_mock


def test_delete_unknown_preset_returns_404(_patched_protocols_with_persistence):
    client = TestClient(_app())
    resp = client.delete("/admin/catalogs/cat-x/presets/does_not_exist")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]


def test_delete_preset_with_no_persisted_rows_returns_empty_deleted_list(
    _patched_protocols_with_persistence,
):
    """No persisted rows at the catalog scope → nothing to delete, but
    the call still succeeds (idempotent rollback)."""
    client = TestClient(_app())
    resp = client.delete("/admin/catalogs/cat-pub/presets/public_catalog")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["preset"] == "public_catalog"
    assert body["catalog_id"] == "cat-pub"
    assert body["deleted"] == []
    _patched_protocols_with_persistence.delete_config.assert_not_awaited()


def test_delete_after_apply_round_trip_clears_all_slots(
    _patched_protocols_with_persistence,
):
    """Apply public_catalog → every slot is persisted → DELETE removes
    every slot and reports them in leaf-first order."""
    client = TestClient(_app())
    apply_resp = client.post("/admin/catalogs/cat-pub/presets/public_catalog")
    assert apply_resp.status_code == 200, apply_resp.text

    del_resp = client.delete("/admin/catalogs/cat-pub/presets/public_catalog")
    assert del_resp.status_code == 200, del_resp.text
    body = del_resp.json()
    assert body["deleted"] == [
        "items_template",
        "collection_template",
        "catalog_routing",
    ]
    # All three slots were removed from the in-memory store.
    assert _patched_protocols_with_persistence._store == {}


def test_delete_geoid_preset_walks_audiences_leaf_first(
    _patched_protocols_with_persistence,
):
    """Geoid preset emits one audience config in addition to the three
    routing tiers. Rollback must also unapply the audience and it must
    trail the routing tiers in the response."""
    import dynastore.extensions.geoid  # noqa: F401

    client = TestClient(_app())
    apply_resp = client.post("/admin/catalogs/cat-fao/presets/geoid")
    assert apply_resp.status_code == 200, apply_resp.text

    del_resp = client.delete("/admin/catalogs/cat-fao/presets/geoid")
    assert del_resp.status_code == 200, del_resp.text
    deleted = del_resp.json()["deleted"]
    # Routing tiers leaf-first (items + asset share the leaf priority and
    # keep insertion order), then audiences in bundle order.
    assert deleted[:4] == [
        "items_template",
        "asset_template",
        "collection_template",
        "catalog_routing",
    ]
    assert set(deleted[4:]) == {
        "audience:catalog_lookup_audience",
    }
    assert _patched_protocols_with_persistence._store == {}


def test_delete_preset_with_diverged_row_skips_diverged_and_deletes_matches(
    _patched_protocols_with_persistence,
):
    """Persisted row that no longer matches the preset bundle is left in
    place (operator edits are preserved); matching slots are still deleted.
    Each diverged slot is reported in ``skipped`` with reason ``diverged``."""
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig,
        CollectionRoutingConfig,
        ItemsRoutingConfig,
    )

    client = TestClient(_app())
    apply_resp = client.post("/admin/catalogs/cat-drift/presets/public_catalog")
    assert apply_resp.status_code == 200, apply_resp.text

    # Manually corrupt the items_template row to simulate operator drift.
    store = _patched_protocols_with_persistence._store
    drifted = dict(store[ItemsRoutingConfig])
    drifted["__drift_marker__"] = "operator-edit"
    store[ItemsRoutingConfig] = drifted

    del_resp = client.delete("/admin/catalogs/cat-drift/presets/public_catalog")
    assert del_resp.status_code == 200, del_resp.text
    body = del_resp.json()
    # Matching slots were removed, diverged slot retained.
    assert body["deleted"] == ["collection_template", "catalog_routing"]
    skipped = body["skipped"]
    assert len(skipped) == 1
    assert skipped[0]["slot"] == "items_template"
    assert skipped[0]["class"] == "ItemsRoutingConfig"
    assert skipped[0]["reason"] == "diverged"
    assert "persisted" in skipped[0] and "expected" in skipped[0]

    # The diverged row stays, the matching rows are gone.
    assert ItemsRoutingConfig in store
    assert store[ItemsRoutingConfig].get("__drift_marker__") == "operator-edit"
    assert CatalogRoutingConfig not in store
    assert CollectionRoutingConfig not in store


def test_delete_preset_skips_missing_slots(_patched_protocols_with_persistence):
    """Operator partially removed some preset rows manually; rollback
    silently skips missing slots and deletes what remains."""
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig,
        CollectionRoutingConfig,
        ItemsRoutingConfig,
    )

    client = TestClient(_app())
    apply_resp = client.post("/admin/catalogs/cat-partial/presets/public_catalog")
    assert apply_resp.status_code == 200

    # Drop the catalog_routing row to simulate a partial manual cleanup.
    _patched_protocols_with_persistence._store.pop(CatalogRoutingConfig)

    del_resp = client.delete("/admin/catalogs/cat-partial/presets/public_catalog")
    assert del_resp.status_code == 200, del_resp.text
    assert del_resp.json()["deleted"] == ["items_template", "collection_template"]
    assert ItemsRoutingConfig not in _patched_protocols_with_persistence._store
    assert CollectionRoutingConfig not in _patched_protocols_with_persistence._store


# ---------------------------------------------------------------------------
# Platform tier — POST/DELETE /admin/presets/{name}  (#972)
# ---------------------------------------------------------------------------


def test_apply_platform_preset_walks_bundle_without_scope(_patched_protocols):
    """A PLATFORM-tier preset applies at the unscoped platform level:
    no catalog_id / collection_id reaches ``set_config``."""
    client = TestClient(_app())
    resp = client.post("/admin/presets/defaults_postgres")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["preset"] == "defaults_postgres"
    assert body["applied"] == [
        "catalog_routing",
        "collection_template",
        "items_template",
    ]
    # No scope keys are echoed back and none reach set_config.
    assert "catalog_id" not in body
    assert "collection_id" not in body
    for call in _patched_protocols.set_config.await_args_list:
        assert "catalog_id" not in call.kwargs
        assert "collection_id" not in call.kwargs


def test_apply_catalog_preset_at_platform_url_returns_409(_patched_protocols):
    """A CATALOG-tier preset applied at the platform URL family is a
    scope/tier mismatch → 409, not 404 (the preset exists)."""
    client = TestClient(_app())
    resp = client.post("/admin/presets/public_catalog")
    assert resp.status_code == 409, resp.text
    assert "platform" in resp.json()["detail"]
    _patched_protocols.set_config.assert_not_awaited()


def test_apply_unknown_platform_preset_returns_404(_patched_protocols):
    client = TestClient(_app())
    resp = client.post("/admin/presets/does_not_exist")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]


def test_delete_platform_preset_round_trip(_patched_protocols_with_persistence):
    client = TestClient(_app())
    apply_resp = client.post("/admin/presets/defaults_postgres")
    assert apply_resp.status_code == 200, apply_resp.text

    del_resp = client.delete("/admin/presets/defaults_postgres")
    assert del_resp.status_code == 200, del_resp.text
    assert del_resp.json()["deleted"] == [
        "items_template",
        "collection_template",
        "catalog_routing",
    ]
    assert _patched_protocols_with_persistence._store == {}


# ---------------------------------------------------------------------------
# Collection tier — POST/DELETE /admin/catalogs/{c}/collections/{col}/...
# ---------------------------------------------------------------------------


def test_apply_collection_preset_scopes_to_catalog_and_collection(_patched_protocols):
    client = TestClient(_app())
    resp = client.post(
        "/admin/catalogs/cat-a/collections/col-1/presets/private_collection"
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["preset"] == "private_collection"
    assert body["catalog_id"] == "cat-a"
    assert body["collection_id"] == "col-1"
    assert body["applied"] == ["items_template"]

    # The set_config call carries both scope keys.
    call = _patched_protocols.set_config.await_args_list[0]
    assert call.args[0] is ItemsRoutingConfig
    assert call.kwargs["catalog_id"] == "cat-a"
    assert call.kwargs["collection_id"] == "col-1"


def test_apply_catalog_preset_at_collection_url_returns_409(_patched_protocols):
    """A CATALOG-tier preset applied at the collection URL family → 409."""
    client = TestClient(_app())
    resp = client.post(
        "/admin/catalogs/cat-a/collections/col-1/presets/public_catalog"
    )
    assert resp.status_code == 409, resp.text
    assert "collection" in resp.json()["detail"]
    _patched_protocols.set_config.assert_not_awaited()


def test_apply_collection_preset_at_catalog_url_returns_409(_patched_protocols):
    """A COLLECTION-tier preset applied at the catalog URL family → 409."""
    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-a/presets/private_collection")
    assert resp.status_code == 409, resp.text
    assert "catalog" in resp.json()["detail"]
    _patched_protocols.set_config.assert_not_awaited()


def test_apply_collection_preset_unknown_collection_returns_404(monkeypatch):
    """Unknown collection segment → 404 before any config write."""
    from unittest.mock import AsyncMock, MagicMock

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=MagicMock())
    catalogs_mock.collections.get_collection = AsyncMock(return_value=None)
    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(return_value=None)

    def _fake_get_protocol(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol

        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is ConfigsProtocol:
            return configs_mock
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
    configs_mock.set_config.assert_not_awaited()


def test_delete_collection_preset_round_trip(_patched_protocols_with_persistence):
    client = TestClient(_app())
    apply_resp = client.post(
        "/admin/catalogs/cat-a/collections/col-1/presets/private_collection"
    )
    assert apply_resp.status_code == 200, apply_resp.text

    del_resp = client.delete(
        "/admin/catalogs/cat-a/collections/col-1/presets/private_collection"
    )
    assert del_resp.status_code == 200, del_resp.text
    body = del_resp.json()
    assert body["deleted"] == ["items_template"]
    assert body["catalog_id"] == "cat-a"
    assert body["collection_id"] == "col-1"
    assert _patched_protocols_with_persistence._store == {}
