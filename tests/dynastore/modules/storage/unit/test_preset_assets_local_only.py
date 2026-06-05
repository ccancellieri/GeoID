"""assets_local_only preset (#972) — assets-tier local-disk upload, on-prem gate.

Covers:
- Bundle shape when the preset is instantiated directly (regardless of the
  registration gate, so the test runs in any environment).
- ``catalog_scopable`` dispatch at catalog AND collection URL families.
- The 409 path when the preset is applied at the platform scope.
- The on-prem gate: ``assets_local_only`` must NOT be present in the
  registry when ``google.cloud.storage`` is importable (cloud mode).
"""
from __future__ import annotations

import importlib.util
import sys
from unittest import mock

import pytest
from fastapi import HTTPException

from dynastore.extensions.admin.admin_service import (
    _preset_reachable_at,
    _resolve_preset_for_scope,
)
from dynastore.modules.storage.presets import PresetTier
from dynastore.modules.storage.presets.assets_local_only import (
    AssetsLocalOnlyPreset,
    _local_backend_available,
)
from dynastore.modules.storage.routing_config import AssetRoutingConfig, Operation


# ---------------------------------------------------------------------------
# Bundle shape (direct instantiation — environment-independent)
# ---------------------------------------------------------------------------


def test_assets_local_only_metadata():
    p = AssetsLocalOnlyPreset()
    assert p.name == "assets_local_only"
    assert p.tier == PresetTier.ASSETS
    assert p.catalog_scopable is True
    assert p.description, "preset must carry a non-empty description"


def test_assets_local_only_bundle_has_single_asset_entry():
    bundle = AssetsLocalOnlyPreset().build()
    entries = list(bundle.iter_apply())
    assert len(entries) == 1
    entry = entries[0]
    assert entry.slot == "asset_template"
    assert entry.config_cls is AssetRoutingConfig
    assert isinstance(entry.instance, AssetRoutingConfig)
    assert dict(entry.scope) == {}


def test_assets_local_only_pins_local_upload_module_driver():
    bundle = AssetsLocalOnlyPreset().build()
    asset_template = bundle.asset_template
    assert isinstance(asset_template, AssetRoutingConfig)
    upload_entries = asset_template.operations.get(str(Operation.UPLOAD), [])
    assert len(upload_entries) == 1
    entry = upload_entries[0]
    assert entry.driver_ref == "local_upload_module"
    assert entry.source == "operator", "UPLOAD entry must be operator-managed"


# --- catalog_scopable dispatch ----------------------------------------------


def test_reachable_at_collection_and_catalog_when_scopable():
    """A catalog_scopable ASSETS preset reaches BOTH URL families."""
    preset = AssetsLocalOnlyPreset()
    assert _preset_reachable_at(preset, PresetTier.COLLECTION) is True
    assert _preset_reachable_at(preset, PresetTier.CATALOG) is True


def test_not_reachable_at_platform_scope():
    preset = AssetsLocalOnlyPreset()
    assert _preset_reachable_at(preset, PresetTier.PLATFORM) is False


def test_resolve_preset_for_scope_409_at_platform():
    """Applying the preset at PLATFORM scope raises 409."""
    with pytest.raises(HTTPException) as exc:
        _resolve_preset_for_scope("assets_local_only", PresetTier.PLATFORM)
    assert exc.value.status_code == 409
    assert "assets" in exc.value.detail.lower()


# --- On-prem gate ----------------------------------------------------------


def test_local_backend_available_when_gcs_absent():
    """When google.cloud.storage is not importable we report local-only mode."""
    with mock.patch.dict(sys.modules, {"google.cloud.storage": None}):
        # find_spec returns None for a module patched to None in sys.modules
        with mock.patch.object(importlib.util, "find_spec", return_value=None):
            assert _local_backend_available() is True


def test_local_backend_not_available_when_gcs_present():
    """When google.cloud.storage IS importable we are in cloud mode."""
    fake_spec = mock.MagicMock()
    with mock.patch.object(importlib.util, "find_spec", return_value=fake_spec):
        assert _local_backend_available() is False


def test_assets_local_only_absent_from_registry_in_cloud_mode():
    """When ``google.cloud.storage`` is importable the preset must NOT appear
    in the registry — a cloud operator cannot accidentally select local-disk
    upload through the presets API.

    Strategy: reload the presets registry module inside a context where
    ``_local_backend_available()`` reports cloud mode, then confirm the
    preset is absent.  We use ``monkeypatch`` / ``importlib.reload`` to
    avoid cross-test registry pollution.
    """
    import importlib
    from dynastore.modules.storage.presets import registry as _reg

    # Snapshot the registry before the reload so we can restore it.
    original_registry = dict(_reg._REGISTRY)

    try:
        # Simulate cloud mode: gcs available → _local_backend_available() == False
        with mock.patch(
            "dynastore.modules.storage.presets.assets_local_only._local_backend_available",
            return_value=False,
        ):
            # Clear and re-run the __init__ registration block by reloading.
            _reg._REGISTRY.clear()
            import dynastore.modules.storage.presets as _presets_pkg
            importlib.reload(_presets_pkg)

            # The preset must not appear in the registry.
            assert "assets_local_only" not in _reg._REGISTRY, (
                "assets_local_only must not be registered when GCS is available "
                "(cloud deployment mode)"
            )
            # The cloud GCS preset must still be registered.
            assert "assets_gcs_uploads" in _reg._REGISTRY
    finally:
        # Restore the original registry state to avoid polluting other tests.
        _reg._REGISTRY.clear()
        _reg._REGISTRY.update(original_registry)
