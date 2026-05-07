"""Closes the worker-B race where ``_coerce_pg_sidecar`` failed with
``sidecar_type 'stac_metadata' not registered`` because the STAC extension's
config module hadn't been imported on the request path.

Two layers under test:

1. Eager-import side effect — importing ``dynastore.extensions.stac`` must
   populate ``SidecarConfigRegistry`` for ``stac_metadata`` without anyone
   touching ``SidecarRegistry`` first.

2. Defensive backstop — even if the registry is empty, calling
   ``resolve_config_class("stac_metadata")`` self-heals via
   ``_ensure_defaults`` instead of returning the base ``SidecarConfig``.
"""

import pytest

from dynastore.modules.storage.drivers.pg_sidecars.base import (
    SidecarConfig,
    SidecarConfigRegistry,
    _coerce_pg_sidecar,
)


class TestEagerImportRegistersStacMetadata:
    def test_extension_package_import_populates_registry(self):
        import dynastore.extensions.stac  # noqa: F401
        from dynastore.extensions.stac.stac_metadata_config import (
            StacItemsSidecarConfig,
        )

        cls = SidecarConfigRegistry.resolve_config_class("stac_metadata")
        assert cls is StacItemsSidecarConfig


class TestBackstopRecovers:
    def test_resolve_self_heals_when_registry_empty(self, monkeypatch):
        monkeypatch.setitem(
            SidecarConfigRegistry.__dict__, "_defaults_loaded", False
        )
        monkeypatch.setattr(
            SidecarConfigRegistry, "_registry", {}, raising=False
        )

        cls = SidecarConfigRegistry.resolve_config_class("stac_metadata")
        from dynastore.extensions.stac.stac_metadata_config import (
            StacItemsSidecarConfig,
        )
        assert cls is StacItemsSidecarConfig

    def test_coerce_pg_sidecar_resolves_stac_metadata_dict(self):
        out = _coerce_pg_sidecar({"sidecar_type": "stac_metadata"})
        assert isinstance(out, SidecarConfig)
        assert out.sidecar_type == "stac_metadata"
