"""STAC items sidecar is gated on ``stac_plugin_config.enabled`` (#1075).

Before this gate, ``SidecarRegistry.get_injected_sidecar_configs`` looped
every registered sidecar and called ``get_default_config(context)``
unconditionally, so a collection whose STAC extension is disabled at its
scope still received a ``StacItemsSidecarConfig`` in its composed /
materialised sidecar list.

The injection call sites now resolve ``StacPluginConfig.enabled`` from the
config waterfall and plumb the boolean into the injection context as
``stac_enabled``; ``StacItemsSidecar.get_default_config`` drops itself when
that flag is explicitly ``False``.
"""

import pytest

# Importing the extension package registers the ``stac_metadata`` sidecar
# (config + impl) via its module-level ``SidecarRegistry.register`` /
# ``SidecarConfigRegistry.register`` side effects.
import dynastore.extensions.stac  # noqa: F401
from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar
from dynastore.extensions.stac.stac_metadata_config import (
    StacItemsSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.registry import (
    SidecarRegistry,
)


class TestGetDefaultConfigGate:
    """Unit-level gate on the STAC sidecar's ``get_default_config``."""

    def test_returns_config_when_flag_absent(self):
        # No ``stac_enabled`` key → keep the always-inject default
        # (mirrors ``StacPluginConfig.enabled`` defaulting to True).
        cfg = StacItemsSidecar.get_default_config({"collection_type": "VECTOR"})
        assert isinstance(cfg, StacItemsSidecarConfig)

    def test_returns_config_when_flag_true(self):
        cfg = StacItemsSidecar.get_default_config(
            {"collection_type": "VECTOR", "stac_enabled": True}
        )
        assert isinstance(cfg, StacItemsSidecarConfig)

    def test_returns_none_when_flag_false(self):
        cfg = StacItemsSidecar.get_default_config(
            {"collection_type": "VECTOR", "stac_enabled": False}
        )
        assert cfg is None

    def test_records_collection_skipped_regardless_of_flag(self):
        assert (
            StacItemsSidecar.get_default_config(
                {"collection_type": "RECORDS", "stac_enabled": True}
            )
            is None
        )


class TestInjectedSidecarConfigsRespectFlag:
    """End-to-end through ``SidecarRegistry.get_injected_sidecar_configs``."""

    def _types(self, configs):
        return {getattr(c, "sidecar_type", None) for c in configs}

    def test_stac_enabled_collection_gets_stac_sidecar(self):
        injected = SidecarRegistry.get_injected_sidecar_configs(
            {"collection_type": "VECTOR", "stac_enabled": True}
        )
        assert "stac_metadata" in self._types(injected)

    def test_default_collection_gets_stac_sidecar(self):
        # Flag absent → STAC sidecar still injected (default-enabled).
        injected = SidecarRegistry.get_injected_sidecar_configs(
            {"collection_type": "VECTOR"}
        )
        assert "stac_metadata" in self._types(injected)

    def test_non_stac_collection_excludes_stac_sidecar(self):
        injected = SidecarRegistry.get_injected_sidecar_configs(
            {"collection_type": "VECTOR", "stac_enabled": False}
        )
        types = self._types(injected)
        assert "stac_metadata" not in types
        # Core, non-STAC sidecars are unaffected by the gate.
        assert "geometries" in types
        assert "attributes" in types


class TestResolveStacEnabled:
    """``resolve_stac_enabled`` reads ``StacPluginConfig.enabled``."""

    @pytest.mark.asyncio
    async def test_returns_true_when_no_config_service(self, monkeypatch):
        from dynastore.modules.storage.drivers.pg_sidecars import resolver
        import dynastore.tools.discovery as discovery

        # No ConfigsProtocol registered → default-enabled posture.
        monkeypatch.setattr(discovery, "get_protocol", lambda *_a, **_k: None)
        assert await resolver.resolve_stac_enabled("cat", "col") is True

    @pytest.mark.asyncio
    async def test_returns_config_enabled_value(self, monkeypatch):
        from dynastore.modules.stac.stac_config import StacPluginConfig
        from dynastore.modules.storage.drivers.pg_sidecars import resolver
        import dynastore.tools.discovery as discovery

        class _FakeConfigs:
            async def get_config(self, cls, catalog_id=None, collection_id=None, **_kw):
                assert cls is StacPluginConfig
                return StacPluginConfig(enabled=False)

        monkeypatch.setattr(
            discovery, "get_protocol", lambda *_a, **_k: _FakeConfigs()
        )
        assert await resolver.resolve_stac_enabled("cat", "col") is False

    @pytest.mark.asyncio
    async def test_returns_true_on_resolution_error(self, monkeypatch):
        from dynastore.modules.storage.drivers.pg_sidecars import resolver
        import dynastore.tools.discovery as discovery

        class _BoomConfigs:
            async def get_config(self, *_a, **_kw):
                raise RuntimeError("config service down")

        monkeypatch.setattr(
            discovery, "get_protocol", lambda *_a, **_k: _BoomConfigs()
        )
        assert await resolver.resolve_stac_enabled("cat", "col") is True
