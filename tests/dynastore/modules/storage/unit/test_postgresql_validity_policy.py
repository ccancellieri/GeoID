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

"""Issue #957/#974/#1126/#1168 — ``ItemsWritePolicy.validity`` (:class:`ValiditySpec`)
is the null-object SSOT for temporal validity.

Validity is a driver-abstracted **concept** expressed as a first-class
null-object :class:`ValiditySpec` on ``ItemsWritePolicy.validity``. Its presence
IS the toggle: a spec enables validity tracking, ``None`` disables it.
``start_from`` / ``end_from`` select where the validity VALUES come from. The
spec carries no physical column name — each driver owns its storage layout
(#1168). ``enable_validity`` is the derived read-only toggle.

The PG driver supplies its own fixed ``validity`` tstzrange column on
``FeatureAttributeSidecarConfig.validity_column`` (and overlays the value sources
onto ``validity_start_from`` / ``validity_end_from``) at ``ensure_storage`` time
and persists the result, so every read path that rehydrates the collection's
driver config sees the policy-aligned value. These tests pin the SSOT contract.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
    ItemsWritePolicy,
)
from dynastore.modules.storage.validity import ValiditySpec
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

# The fixed physical column the PG driver owns for temporal validity (#1168).
_PG_VALIDITY_COLUMN = "validity"


class TestPolicyNullObject:
    """``ItemsWritePolicy.validity`` is the null-object SSOT; ``enable_validity``
    is derived and not independently settable. The policy never carries a
    physical column name (#1168)."""

    def test_default_validity_is_none(self):
        policy = ItemsWritePolicy()
        assert policy.validity is None
        assert policy.enable_validity is False

    def test_set_validity_enables_validity(self):
        policy = ItemsWritePolicy(validity=ValiditySpec())
        assert policy.validity is not None
        assert policy.enable_validity is True

    def test_enable_validity_is_not_a_settable_field(self):
        """The bool is a derived property — authoring it is rejected so it can
        never diverge from ``validity`` (PluginConfig is ``extra=forbid``)."""
        with pytest.raises(Exception):
            ItemsWritePolicy(enable_validity=True)

    def test_validity_column_is_not_a_policy_field(self):
        """The policy exposes no physical column name (#1168); authoring
        ``validity_column`` on the policy is rejected by ``extra=forbid``."""
        with pytest.raises(Exception):
            ItemsWritePolicy(validity_column="x")  # type: ignore[call-arg]


class TestValiditySpec:
    """The :class:`ValiditySpec` value object — validation and defaults."""

    def test_defaults(self):
        spec = ValiditySpec()
        assert spec.start_from == "context"
        assert spec.end_from is None
        assert spec.close_on_new_version is True

    def test_explicit_paths(self):
        spec = ValiditySpec(
            start_from="properties.start_date",
            end_from="properties.end_date",
            close_on_new_version=False,
        )
        assert spec.start_from == "properties.start_date"
        assert spec.end_from == "properties.end_date"
        assert spec.close_on_new_version is False

    def test_frozen(self):
        spec = ValiditySpec()
        with pytest.raises(Exception):
            spec.start_from = "other"  # type: ignore[misc]

    def test_extra_forbidden(self):
        with pytest.raises(Exception):
            ValiditySpec(bogus=1)  # type: ignore[call-arg]

    def test_column_is_no_longer_accepted(self):
        """#1168 — the physical column name is no longer part of the contract;
        passing ``column`` is rejected by ``extra=forbid``."""
        with pytest.raises(Exception):
            ValiditySpec(column="valid_from")  # type: ignore[call-arg]

    def test_presence_is_the_toggle(self):
        """A bare ``ValiditySpec()`` is valid — presence enables validity."""
        spec = ValiditySpec()
        assert spec.start_from == "context"

    def test_start_from_none_is_open_lower_bound(self):
        """#1172 — ``start_from=None`` expresses an open lower bound; the two
        bounds are fully independent so end-only / open-lower windows exist."""
        spec = ValiditySpec(start_from=None, end_from="context")
        assert spec.start_from is None
        assert spec.end_from == "context"

    def test_fully_open_window_is_expressible(self):
        """#1172 — both bounds open (neither start nor end) is a valid state."""
        spec = ValiditySpec(start_from=None, end_from=None)
        assert spec.start_from is None
        assert spec.end_from is None


class TestSidecarNullObject:
    """The sidecar carries the PG driver's own ``validity_column`` storage-shape
    field. Its default (None) means validity OFF; the driver sets the fixed
    ``validity`` column when the policy enables it."""

    def test_sidecar_default_validity_is_off(self):
        sc = FeatureAttributeSidecarConfig()
        policy = ItemsWritePolicy()
        assert sc.validity_column is None
        assert sc.enable_validity is False
        assert policy.validity is None

    def test_enable_validity_property_derives_from_column(self):
        sc_off = FeatureAttributeSidecarConfig(validity_column=None)
        sc_on = FeatureAttributeSidecarConfig(validity_column=_PG_VALIDITY_COLUMN)
        assert sc_off.enable_validity is False
        assert sc_off.has_validity is False
        assert sc_on.enable_validity is True
        assert sc_on.has_validity is True

    def test_bool_cannot_drive_behaviour(self):
        """A stray ``enable_validity=True`` (no column) does NOT enable validity
        — only ``validity_column`` does."""
        sc = FeatureAttributeSidecarConfig(enable_validity=True)
        assert sc.validity_column is None
        assert sc.enable_validity is False
        assert sc.has_validity is False

    def test_sidecar_factory_propagates_config_value(self):
        """Sidecar ``has_validity()`` reads ``self.config.validity_column`` via
        the derived property — the value the driver set from policy presence."""
        sc = FeatureAttributeSidecarConfig(validity_column=_PG_VALIDITY_COLUMN)
        impl = SidecarRegistry.get_sidecar(sc)
        assert impl is not None
        assert impl.has_validity() is True

        sc_off = FeatureAttributeSidecarConfig(validity_column=None)
        impl_off = SidecarRegistry.get_sidecar(sc_off)
        assert impl_off is not None
        assert impl_off.has_validity() is False

    def test_factory_absorbs_unknown_kwargs(self):
        """Ctor accepts forward-compatible kwargs (e.g. ``policy=...``)
        without erroring — keeps room for full SSOT threading later."""
        sc = FeatureAttributeSidecarConfig()
        from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
            FeatureAttributeSidecar,
        )

        instance = FeatureAttributeSidecar(sc, policy=MagicMock(enable_validity=True))
        # policy is not consumed by the ctor — config remains the SSOT source.
        assert instance.config.validity_column is None


class TestResolveWritePolicy:
    """The PG driver's ``_resolve_write_policy`` helper looks up
    :class:`ItemsWritePolicy` through the configs waterfall and
    falls back to defaults when the configs service or config is missing."""

    @pytest.mark.asyncio
    async def test_resolves_policy_from_configs(self):
        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver

        mock_configs = MagicMock()
        mock_configs.get_config = AsyncMock(
            return_value=ItemsWritePolicy(validity=ValiditySpec())
        )
        with patch(
            "dynastore.tools.discovery.get_protocol", return_value=mock_configs
        ):
            policy = await ItemsPostgresqlDriver._resolve_write_policy("c", "col")
        assert policy.validity is not None
        assert policy.enable_validity is True

    @pytest.mark.asyncio
    async def test_returns_defaults_when_configs_missing(self):
        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver

        with patch(
            "dynastore.tools.discovery.get_protocol", return_value=None
        ):
            policy = await ItemsPostgresqlDriver._resolve_write_policy("c", "col")
        assert policy.validity is None
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
        assert policy.validity is None
        assert policy.enable_validity is False


class TestPolicyOverlay:
    """The overlay logic at ``ensure_storage`` updates each
    :class:`FeatureAttributeSidecarConfig` so its ``validity_column`` (and value
    sources) match the resolved policy. The column is the driver's own fixed
    ``validity`` (gated on policy presence), NOT a policy-configured name (#1168).
    The same overlay is applied to the persisted col_config so reads see the
    policy-aligned value."""

    @staticmethod
    def _apply_overlay(
        col_config: ItemsPostgresqlDriverConfig, policy: ItemsWritePolicy
    ) -> ItemsPostgresqlDriverConfig:
        """Inlines the validity overlay block from
        ``ItemsPostgresqlDriver.ensure_storage`` so the contract can be pinned
        without spinning a DB."""
        policy_column = _PG_VALIDITY_COLUMN if policy.validity is not None else None
        policy_start = (
            policy.validity.start_from if policy.validity is not None else "context"
        )
        policy_end = (
            policy.validity.end_from if policy.validity is not None else None
        )
        overlay_sidecars = []
        any_overlay = False
        for sc in col_config.sidecars:
            if isinstance(sc, FeatureAttributeSidecarConfig):
                updates: dict = {}
                if sc.validity_column != policy_column:
                    updates["validity_column"] = policy_column
                if sc.validity_start_from != policy_start:
                    updates["validity_start_from"] = policy_start
                if sc.validity_end_from != policy_end:
                    updates["validity_end_from"] = policy_end
                if updates:
                    overlay_sidecars.append(sc.model_copy(update=updates))
                    any_overlay = True
                else:
                    overlay_sidecars.append(sc)
            else:
                overlay_sidecars.append(sc)
        if any_overlay:
            return col_config.model_copy(update={"sidecars": overlay_sidecars})
        return col_config

    def test_overlay_enables_sidecar_when_policy_sets_validity(self):
        """Divergent case: sidecar=None, policy has validity → sidecar gets the
        driver's fixed ``validity`` column."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB, validity_column=None
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(validity=ValiditySpec())

        overlaid = self._apply_overlay(col, policy)
        attr_sc = next(
            s
            for s in overlaid.sidecars
            if isinstance(s, FeatureAttributeSidecarConfig)
        )
        assert attr_sc.validity_column == _PG_VALIDITY_COLUMN
        assert attr_sc.enable_validity is True

    def test_overlay_disables_sidecar_when_policy_clears_validity(self):
        """Divergent case: sidecar=column, policy=None → sidecar cleared."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB,
            validity_column=_PG_VALIDITY_COLUMN,
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(validity=None)

        overlaid = self._apply_overlay(col, policy)
        attr_sc = next(
            s
            for s in overlaid.sidecars
            if isinstance(s, FeatureAttributeSidecarConfig)
        )
        assert attr_sc.validity_column is None
        assert attr_sc.enable_validity is False

    def test_overlay_no_op_when_aligned(self):
        """Sidecar and policy already agree → no model_copy churn."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB,
            validity_column=_PG_VALIDITY_COLUMN,
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(validity=ValiditySpec())

        overlaid = self._apply_overlay(col, policy)
        assert overlaid is col  # Same object — no overlay applied.

    def test_overlay_propagates_value_sources(self):
        """The overlay carries the start/end value sources — the sidecar mirror
        keeps the policy's source-path selection (#1126); the column itself is
        the driver's fixed ``validity`` (#1168)."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB, validity_column=None
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(
            validity=ValiditySpec(
                start_from="properties.start_date",
                end_from="properties.end_date",
            )
        )

        overlaid = self._apply_overlay(col, policy)
        attr_sc = next(
            s
            for s in overlaid.sidecars
            if isinstance(s, FeatureAttributeSidecarConfig)
        )
        assert attr_sc.validity_column == _PG_VALIDITY_COLUMN
        assert attr_sc.validity_start_from == "properties.start_date"
        assert attr_sc.validity_end_from == "properties.end_date"

    def test_overlay_leaves_non_attribute_sidecars_alone(self):
        """Geometries / item_metadata sidecars don't get touched."""
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
            TargetDimension,
        )

        attr_sc = FeatureAttributeSidecarConfig(validity_column=None)
        geom_sc = GeometriesSidecarConfig(
            target_srid=4326,
            target_dimension=TargetDimension.FORCE_2D,
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[geom_sc, attr_sc])
        policy = ItemsWritePolicy(validity=ValiditySpec())

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
        assert attr_after.validity_column == _PG_VALIDITY_COLUMN  # Overlaid


class TestValidityValueExtraction:
    """#1126 — the validity start/end VALUES come from ``start_from`` /
    ``end_from``. ``"context"`` keeps the write-context default; a dotted source
    path extracts the value from the feature in ``prepare_upsert_payload``."""

    @staticmethod
    def _sidecar(**overrides):
        from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
            FeatureAttributeSidecar,
        )

        cfg = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB,
            validity_column=_PG_VALIDITY_COLUMN,
            **overrides,
        )
        return FeatureAttributeSidecar(cfg)

    def test_context_default_does_not_override_from_feature(self):
        """With both sources = "context" the path-extraction branch is skipped;
        the heuristic resolution (properties.valid_from) flows to context."""
        sc = self._sidecar()  # start/end default to "context"/None
        ctx: dict = {"geoid": "g1"}
        sc.prepare_upsert_payload(
            {"id": "f1", "properties": {"valid_from": "2020-01-01T00:00:00Z"}}, ctx
        )
        assert "valid_from" in ctx  # resolved from properties heuristic

    def test_start_from_path_extracts_value(self):
        sc = self._sidecar(validity_start_from="properties.start_date")
        ctx: dict = {"geoid": "g1"}
        sc.prepare_upsert_payload(
            {
                "id": "f1",
                "properties": {
                    "start_date": "2021-06-01T00:00:00Z",
                    "valid_from": "1999-01-01T00:00:00Z",  # ignored — path wins
                },
            },
            ctx,
        )
        # The dotted path value (not the heuristic valid_from) is what landed.
        assert ctx["valid_from"].year == 2021
        assert ctx["valid_from"].month == 6

    def test_end_from_path_extracts_value(self):
        sc = self._sidecar(
            validity_start_from="properties.start_date",
            validity_end_from="properties.end_date",
        )
        ctx: dict = {"geoid": "g1"}
        sc.prepare_upsert_payload(
            {
                "id": "f1",
                "properties": {
                    "start_date": "2021-06-01T00:00:00Z",
                    "end_date": "2022-06-01T00:00:00Z",
                },
            },
            ctx,
        )
        assert ctx["valid_from"].year == 2021
        assert ctx["valid_to"].year == 2022

    def test_end_from_none_is_open_ended(self):
        """end_from=None → no upper bound is extracted from the feature."""
        sc = self._sidecar(validity_start_from="properties.start_date")
        ctx: dict = {"geoid": "g1"}
        sc.prepare_upsert_payload(
            {"id": "f1", "properties": {"start_date": "2021-06-01T00:00:00Z"}}, ctx
        )
        assert "valid_to" not in ctx  # open-ended

    def test_missing_path_value_leaves_context_unset(self):
        """A start_from path that resolves to nothing does not populate the
        context (downstream falls back to the write-context / open default)."""
        sc = self._sidecar(validity_start_from="properties.start_date")
        ctx: dict = {"geoid": "g1"}
        sc.prepare_upsert_payload({"id": "f1", "properties": {}}, ctx)
        assert "valid_from" not in ctx

    def test_start_from_none_drops_heuristic_start(self):
        """#1172 — ``validity_start_from=None`` (open lower bound) suppresses any
        heuristically resolved start so no ``valid_from`` reaches the context,
        even when the feature carries a ``valid_from`` property."""
        sc = self._sidecar(validity_start_from=None)
        ctx: dict = {"geoid": "g1"}
        sc.prepare_upsert_payload(
            {
                "id": "f1",
                "properties": {
                    "valid_from": "2020-01-01T00:00:00Z",  # ignored — open lower
                    "valid_to": "2022-01-01T00:00:00Z",
                },
            },
            ctx,
        )
        assert "valid_from" not in ctx
        assert ctx["valid_to"].year == 2022  # end bound still resolved


class TestOpenLowerBoundFinalize:
    """#1172 — ``finalize_upsert_payload`` must thread an open lower bound
    (``tstzrange(NULL, v_to)``) when the start is configured as open, instead of
    defaulting the lower bound to the Hub ``transaction_time``."""

    _PG_VALIDITY_COLUMN = "validity"

    @staticmethod
    def _sidecar(**overrides):
        from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
            FeatureAttributeSidecar,
        )

        cfg = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB,
            validity_column="validity",
            **overrides,
        )
        return FeatureAttributeSidecar(cfg)

    @staticmethod
    def _bounds(rng):
        """Read (lower, upper) from whichever tstzrange wrapper finalize built."""
        lower = getattr(rng, "lower", None)
        upper = getattr(rng, "upper", None)
        return lower, upper

    def test_open_lower_bound_when_start_is_none(self):
        from datetime import datetime, timezone

        sc = self._sidecar(validity_start_from=None)
        hub = {"transaction_time": datetime(2020, 1, 1, tzinfo=timezone.utc)}
        ctx = {"valid_to": datetime(2021, 1, 1, tzinfo=timezone.utc)}

        out = sc.finalize_upsert_payload({"geoid": "g1"}, hub, ctx)

        lower, upper = self._bounds(out["validity"])
        # Open lower bound — NOT defaulted to transaction_time.
        assert lower is None
        assert upper == datetime(2021, 1, 1, tzinfo=timezone.utc)

    def test_default_start_still_syncs_to_transaction_time(self):
        """Regression guard: ``validity_start_from='context'`` (the default)
        keeps the historical behaviour — a missing start syncs to the Hub
        transaction_time, never an open lower bound."""
        from datetime import datetime, timezone

        sc = self._sidecar()  # validity_start_from defaults to "context"
        hub = {"transaction_time": datetime(2020, 1, 1, tzinfo=timezone.utc)}
        ctx = {"valid_to": datetime(2021, 1, 1, tzinfo=timezone.utc)}

        out = sc.finalize_upsert_payload({"geoid": "g1"}, hub, ctx)

        lower, upper = self._bounds(out["validity"])
        assert lower == datetime(2020, 1, 1, tzinfo=timezone.utc)
        assert upper == datetime(2021, 1, 1, tzinfo=timezone.utc)


class TestByTimePartitionValidation:
    """``BY_TIME`` partitioning requires validity. The validation reads the
    null-object field via the derived property."""

    def test_by_time_requires_validity(self):
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            AttributePartitionStrategyPreset,
        )

        sc = FeatureAttributeSidecarConfig(
            validity_column=None,
            partition_strategy=AttributePartitionStrategyPreset.BY_TIME,
        )
        with pytest.raises(ValueError, match="BY_TIME partitioning requires"):
            _ = sc.partition_key_contributions

    def test_by_time_ok_when_validity_set(self):
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            AttributePartitionStrategyPreset,
        )

        sc = FeatureAttributeSidecarConfig(
            validity_column=_PG_VALIDITY_COLUMN,
            partition_strategy=AttributePartitionStrategyPreset.BY_TIME,
        )
        assert sc.partition_key_contributions == {"validity": "TSTZRANGE"}
