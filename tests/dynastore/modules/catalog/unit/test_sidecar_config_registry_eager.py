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
        # Simulate a cold worker where stac hasn't been imported. Drop
        # every cached stac extension module so `_ensure_defaults`'s
        # internal `from dynastore.extensions.stac import stac_metadata_config`
        # re-executes the module-level `SidecarConfigRegistry.register(...)`
        # side effect against the empty registry below.
        import sys
        for mod in list(sys.modules):
            if mod.startswith("dynastore.extensions.stac"):
                monkeypatch.delitem(sys.modules, mod, raising=False)

        monkeypatch.setattr(
            SidecarConfigRegistry, "_defaults_loaded", False, raising=False
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


class TestSlimServicePassthrough:
    """A slim service (e.g. ``maps``/``tiles``) ships without the STAC
    extension but reads the same DB-persisted ``ItemsPostgresqlDriverConfig``
    rows authored by the STAC-enabled catalog service.  Loading such a config
    must NOT fail just because ``stac_metadata`` can't be resolved here — it
    is retained as an opaque base config so the config still loads and the
    consuming pipeline skips the sidecar it can't use.
    """

    def _simulate_stac_not_installed(self, monkeypatch):
        """Make ``stac_metadata`` unresolvable, as in a SCOPE without STAC."""
        core_only = {
            k: v
            for k, v in SidecarConfigRegistry._registry.items()
            if k != "stac_metadata"
        }
        monkeypatch.setattr(
            SidecarConfigRegistry, "_registry", core_only, raising=False
        )
        # Neutralise the backstop import so it can't re-register stac_metadata
        # (in the test env the extension *is* importable; production maps is
        # not — this reproduces the not-installed path).
        monkeypatch.setattr(
            SidecarConfigRegistry, "_ensure_defaults", classmethod(lambda cls: None)
        )

    def test_unresolved_optional_type_is_retained_as_opaque_config(
        self, monkeypatch
    ):
        self._simulate_stac_not_installed(monkeypatch)

        raw = {
            "sidecar_type": "stac_metadata",
            "enabled": True,
            "indexing": {"mode": "full"},
            "external_assets_field": "assets",  # extension-specific extra field
        }
        out = _coerce_pg_sidecar(raw)

        # Loaded, not rejected — and as the base type (no STAC class here).
        assert type(out) is SidecarConfig
        assert out.sidecar_type == "stac_metadata"
        assert out.enabled is True
        # extra="allow" preserves the extension-specific field for round-trip.
        dumped = out.model_dump()
        assert dumped["sidecar_type"] == "stac_metadata"
        assert dumped["external_assets_field"] == "assets"

    def test_unknown_typo_type_still_fails_loud(self, monkeypatch):
        self._simulate_stac_not_installed(monkeypatch)

        import pytest

        # A near-miss typo is NOT in OPTIONAL_EXTENSION_SIDECAR_TYPES, so it
        # must still raise — the passthrough only covers known optional types.
        with pytest.raises(ValueError, match="stac_metdata.*not registered"):
            _coerce_pg_sidecar({"sidecar_type": "stac_metdata"})

    def test_full_driver_config_loads_with_unresolved_stac_sidecar(
        self, monkeypatch
    ):
        """The end-to-end symptom: ItemsPostgresqlDriverConfig.model_validate
        must succeed on a slim service even when a sidecar references STAC."""
        self._simulate_stac_not_installed(monkeypatch)

        from dynastore.modules.storage.driver_config import (
            ItemsPostgresqlDriverConfig,
        )

        cfg = ItemsPostgresqlDriverConfig.model_validate(
            {
                "physical_table": "t_abc123",
                "sidecars": [
                    {"sidecar_type": "geometries"},
                    {"sidecar_type": "stac_metadata", "enabled": True},
                ],
            }
        )
        types = [sc.sidecar_type for sc in cfg.sidecars]
        assert "geometries" in types
        assert "stac_metadata" in types
