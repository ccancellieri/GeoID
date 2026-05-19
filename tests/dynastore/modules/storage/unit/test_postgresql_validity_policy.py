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

"""Issue #974 — ItemsWritePolicy.enable_validity is the SSOT for the
sidecar's validity column.

The PG driver overlays ``ItemsWritePolicy.enable_validity`` onto
``FeatureAttributeSidecarConfig.enable_validity`` at ``ensure_storage``
time and persists the result, so every read path that rehydrates the
collection's driver config sees the policy-aligned value. These tests
pin the SSOT contract.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
    ItemsWritePolicy,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry


class TestSidecarConfigDefaults:
    """Sidecar config defaults must match the policy default so collections
    that never set the field on either side stay consistent."""

    def test_attributes_config_enable_validity_defaults_match_policy(self):
        """The sidecar field defaults to ``False`` — identical to
        :attr:`ItemsWritePolicy.enable_validity` default. A collection
        configured with neither side set therefore has validity OFF
        everywhere (was previously divergent: sidecar=True / policy=False)."""
        sc = FeatureAttributeSidecarConfig()
        policy = ItemsWritePolicy()
        assert sc.enable_validity is False
        assert policy.enable_validity is False
        assert sc.enable_validity == policy.enable_validity

    def test_has_validity_property_mirrors_field(self):
        sc_off = FeatureAttributeSidecarConfig(enable_validity=False)
        sc_on = FeatureAttributeSidecarConfig(enable_validity=True)
        assert sc_off.has_validity is False
        assert sc_on.has_validity is True

    def test_sidecar_factory_propagates_config_value(self):
        """Sidecar ``has_validity()`` reads ``self.config.enable_validity``
        — the value the driver overlaid from policy at DDL time."""
        sc = FeatureAttributeSidecarConfig(enable_validity=True)
        impl = SidecarRegistry.get_sidecar(sc)
        assert impl is not None
        assert impl.has_validity() is True

        sc_off = FeatureAttributeSidecarConfig(enable_validity=False)
        impl_off = SidecarRegistry.get_sidecar(sc_off)
        assert impl_off is not None
        assert impl_off.has_validity() is False

    def test_factory_absorbs_unknown_kwargs(self):
        """Ctor accepts forward-compatible kwargs (e.g. ``policy=...``)
        without erroring — keeps room for full SSOT threading later."""
        sc = FeatureAttributeSidecarConfig()
        # The factory itself does not pass policy today; verify the
        # ctor itself tolerates it for direct callers.
        from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
            FeatureAttributeSidecar,
        )

        instance = FeatureAttributeSidecar(sc, policy=MagicMock(enable_validity=True))
        assert instance.config.enable_validity is False  # policy is not consumed by ctor


class TestResolveWritePolicy:
    """The PG driver's ``_resolve_write_policy`` helper looks up
    :class:`ItemsWritePolicy` through the configs waterfall and
    falls back to defaults when the configs service or config is missing.
    Mirrors :meth:`IcebergItemsDriver._resolve_write_policy`."""

    @pytest.mark.asyncio
    async def test_resolves_policy_from_configs(self):
        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver

        mock_configs = MagicMock()
        mock_configs.get_config = AsyncMock(
            return_value=ItemsWritePolicy(enable_validity=True)
        )
        with patch(
            "dynastore.tools.discovery.get_protocol", return_value=mock_configs
        ):
            policy = await ItemsPostgresqlDriver._resolve_write_policy("c", "col")
        assert policy.enable_validity is True

    @pytest.mark.asyncio
    async def test_returns_defaults_when_configs_missing(self):
        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver

        with patch(
            "dynastore.tools.discovery.get_protocol", return_value=None
        ):
            policy = await ItemsPostgresqlDriver._resolve_write_policy("c", "col")
        assert policy.enable_validity is False

    @pytest.mark.asyncio
    async def test_swallows_configs_errors(self):
        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver

        mock_configs = MagicMock()
        mock_configs.get_config = AsyncMock(side_effect=RuntimeError("boom"))
        with patch(
            "dynastore.tools.discovery.get_protocol", return_value=mock_configs
        ):
            policy = await ItemsPostgresqlDriver._resolve_write_policy("c", "col")
        # Falls back to defaults — divergence resolution must never raise.
        assert policy.enable_validity is False


class TestPolicyOverlay:
    """The overlay logic at ``ensure_storage`` updates each
    :class:`FeatureAttributeSidecarConfig` so its ``enable_validity``
    matches the resolved policy. The same overlay is applied to the
    persisted col_config so reads see the policy-aligned value."""

    @staticmethod
    def _apply_overlay(
        col_config: ItemsPostgresqlDriverConfig, policy: ItemsWritePolicy
    ) -> ItemsPostgresqlDriverConfig:
        """Inlines the overlay block from ``ItemsPostgresqlDriver.ensure_storage``
        so the contract can be pinned without spinning a DB."""
        overlay_sidecars = []
        any_overlay = False
        for sc in col_config.sidecars:
            if isinstance(sc, FeatureAttributeSidecarConfig) and (
                sc.enable_validity != policy.enable_validity
            ):
                overlay_sidecars.append(
                    sc.model_copy(update={"enable_validity": policy.enable_validity})
                )
                any_overlay = True
            else:
                overlay_sidecars.append(sc)
        if any_overlay:
            return col_config.model_copy(update={"sidecars": overlay_sidecars})
        return col_config

    def test_overlay_flips_sidecar_when_policy_says_true(self):
        """Divergent case: sidecar=False, policy=True → sidecar becomes True."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB, enable_validity=False
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(enable_validity=True)

        overlaid = self._apply_overlay(col, policy)
        attr_sc = next(
            s
            for s in overlaid.sidecars
            if isinstance(s, FeatureAttributeSidecarConfig)
        )
        assert attr_sc.enable_validity is True

    def test_overlay_flips_sidecar_when_policy_says_false(self):
        """Divergent case: sidecar=True, policy=False → sidecar becomes False."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB, enable_validity=True
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(enable_validity=False)

        overlaid = self._apply_overlay(col, policy)
        attr_sc = next(
            s
            for s in overlaid.sidecars
            if isinstance(s, FeatureAttributeSidecarConfig)
        )
        assert attr_sc.enable_validity is False

    def test_overlay_no_op_when_aligned(self):
        """Sidecar and policy already agree → no model_copy churn."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB, enable_validity=True
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(enable_validity=True)

        overlaid = self._apply_overlay(col, policy)
        assert overlaid is col  # Same object — no overlay applied.

    def test_overlay_leaves_non_attribute_sidecars_alone(self):
        """Geometries / item_metadata sidecars don't get touched."""
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
            TargetDimension,
        )

        attr_sc = FeatureAttributeSidecarConfig(enable_validity=False)
        geom_sc = GeometriesSidecarConfig(
            target_srid=4326,
            target_dimension=TargetDimension.FORCE_2D,
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[geom_sc, attr_sc])
        policy = ItemsWritePolicy(enable_validity=True)

        overlaid = self._apply_overlay(col, policy)
        geom_after = next(
            s for s in overlaid.sidecars if isinstance(s, GeometriesSidecarConfig)
        )
        attr_after = next(
            s
            for s in overlaid.sidecars
            if isinstance(s, FeatureAttributeSidecarConfig)
        )
        assert geom_after is geom_sc  # Untouched
        assert attr_after.enable_validity is True  # Overlaid
