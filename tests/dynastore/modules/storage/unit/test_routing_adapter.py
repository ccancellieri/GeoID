"""RoutingPresetAdapter — round-trip apply/revoke for routing presets.

Tests verify:
* Adapter exposes back-compat properties (name, description, tier, build).
* apply() calls set_config for each bundle entry and the on_applied hook.
* revoke() calls delete_config for matching entries; skips diverged entries.
* on_revoked hook is called when provided.
* Behaviour parity with the existing ``_apply_preset_bundle`` / ``_unapply_preset_bundle`` logic.
"""
from __future__ import annotations

from typing import ClassVar, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    NoParams,
    PresetContext,
    PresetPlan,
)
from dynastore.modules.storage.presets.protocol import PresetBundle, PresetTier
from dynastore.modules.storage.presets.routing_adapter import (
    RoutingPresetAdapter,
    _scope_to_kwargs,
    _scope_to_string,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_context() -> PresetContext:
    return PresetContext(
        db=MagicMock(),
        iam=MagicMock(),
        policy=MagicMock(),
        config=MagicMock(),
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope="catalog:cat-test",
    )


class _SimpleRoutingPreset:
    """Minimal RoutingPreset-shaped object for adapter tests."""

    name = "simple-routing"
    description = "simple routing preset"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = False
    keywords: ClassVar[tuple] = ("routing",)

    def build(self, catalog_id: str = "", **_: str) -> PresetBundle:
        from dynastore.modules.storage.routing_config import (
            CatalogRoutingConfig,
            FailurePolicy,
            Operation,
            OperationDriverEntry,
        )
        return PresetBundle(
            catalog_routing=CatalogRoutingConfig(
                operations={
                    Operation.READ: [
                        OperationDriverEntry(
                            driver_ref="catalog_postgresql_driver",
                            on_failure=FailurePolicy.FATAL,
                        )
                    ],
                }
            ),
        )


class _RoutingPresetWithHook:
    name = "routing-with-hook"
    description = "routing preset with on_applied"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = False
    keywords: ClassVar[tuple] = ("routing",)

    def __init__(self) -> None:
        self._applied: list[str] = []

    def build(self, catalog_id: str = "", **_: str) -> PresetBundle:
        from dynastore.modules.storage.routing_config import (
            CatalogRoutingConfig,
            FailurePolicy,
            Operation,
            OperationDriverEntry,
        )
        return PresetBundle(
            catalog_routing=CatalogRoutingConfig(
                operations={
                    Operation.READ: [
                        OperationDriverEntry(
                            driver_ref="catalog_postgresql_driver",
                            on_failure=FailurePolicy.FATAL,
                        )
                    ],
                }
            ),
        )

    async def on_applied(self, catalog_id: str = "", **_: str) -> None:
        self._applied.append(catalog_id)


# ---------------------------------------------------------------------------
# Scope helpers
# ---------------------------------------------------------------------------

def test_scope_to_kwargs_platform():
    assert _scope_to_kwargs("platform") == {}


def test_scope_to_kwargs_catalog():
    kw = _scope_to_kwargs("catalog:cat-7")
    assert kw == {"catalog_id": "cat-7"}


def test_scope_to_kwargs_collection():
    kw = _scope_to_kwargs("catalog:cat-7/collection:coll-3")
    assert kw == {"catalog_id": "cat-7", "collection_id": "coll-3"}


def test_scope_to_string_roundtrip():
    for scope in ("platform", "catalog:foo", "catalog:foo/collection:bar"):
        assert _scope_to_string(_scope_to_kwargs(scope)) == scope


# ---------------------------------------------------------------------------
# Back-compat delegation
# ---------------------------------------------------------------------------

def test_adapter_delegates_name_description_tier():
    rp = _SimpleRoutingPreset()
    adapter = RoutingPresetAdapter(rp)
    assert adapter.name == rp.name
    assert adapter.description == rp.description
    assert adapter.tier == PresetTier.CATALOG
    assert adapter.catalog_scopable is False


def test_adapter_build_delegates_to_wrapped():
    rp = _SimpleRoutingPreset()
    adapter = RoutingPresetAdapter(rp)
    bundle = adapter.build(catalog_id="x")
    assert bundle.catalog_routing is not None


# ---------------------------------------------------------------------------
# apply / revoke round trip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_calls_set_config_for_each_entry():
    rp = _SimpleRoutingPreset()
    adapter = RoutingPresetAdapter(rp)

    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(side_effect=lambda cls, inst, **kw: inst)

    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        ctx = _make_context()
        result = await adapter.apply(NoParams(), "catalog:cat-test", ctx)

    assert isinstance(result, AppliedDescriptor)
    # At least one set_config call per bundle entry.
    assert configs_mock.set_config.call_count >= 1
    assert "slots" in result.payload


@pytest.mark.asyncio
async def test_apply_calls_on_applied_hook():
    rp = _RoutingPresetWithHook()
    adapter = RoutingPresetAdapter(rp)

    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(side_effect=lambda cls, inst, **kw: inst)

    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        ctx = _make_context()
        await adapter.apply(NoParams(), "catalog:cat-hook", ctx)

    assert "cat-hook" in rp._applied


@pytest.mark.asyncio
async def test_revoke_calls_delete_config_for_matching_slots():
    rp = _SimpleRoutingPreset()
    adapter = RoutingPresetAdapter(rp)

    # Build the descriptor that apply() would have produced.
    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(side_effect=lambda cls, inst, **kw: inst)

    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        ctx = _make_context()
        descriptor = await adapter.apply(NoParams(), "catalog:cat-rev", ctx)
        assert isinstance(descriptor, AppliedDescriptor)

    # Now revoke: get_persisted_config returns the same value the preset emitted.
    bundle = rp.build(catalog_id="cat-rev")

    async def _mock_get_persisted(cls, **kw):
        for entry in bundle.iter_apply():
            if entry.config_cls is cls:
                return entry.instance.model_dump(mode="json")
        return None

    configs_mock.get_persisted_config = _mock_get_persisted
    configs_mock.delete_config = AsyncMock()

    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        await adapter.revoke(descriptor, ctx)

    assert configs_mock.delete_config.call_count >= 1


@pytest.mark.asyncio
async def test_revoke_skips_diverged_slots():
    """If a slot's persisted value differs from what the preset emits, revoke skips it."""
    rp = _SimpleRoutingPreset()
    adapter = RoutingPresetAdapter(rp)

    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(side_effect=lambda cls, inst, **kw: inst)

    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        ctx = _make_context()
        descriptor = await adapter.apply(NoParams(), "catalog:cat-diverge", ctx)

    # Return a *different* value so everything diverges.
    configs_mock.get_persisted_config = AsyncMock(return_value={"operations": {}})
    configs_mock.delete_config = AsyncMock()

    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        await adapter.revoke(descriptor, ctx)

    # No deletes because everything diverged.
    configs_mock.delete_config.assert_not_called()


@pytest.mark.asyncio
async def test_on_revoked_hook_called_after_revoke():
    """The on_revoked callable is invoked after config entries are deleted."""
    rp = _SimpleRoutingPreset()
    on_revoked = AsyncMock()
    adapter = RoutingPresetAdapter(rp, on_revoked=on_revoked)

    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(side_effect=lambda cls, inst, **kw: inst)

    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        ctx = _make_context()
        descriptor = await adapter.apply(NoParams(), "catalog:cat-hook-rev", ctx)

    bundle = rp.build(catalog_id="cat-hook-rev")

    async def _get(cls, **kw):
        for entry in bundle.iter_apply():
            if entry.config_cls is cls:
                return entry.instance.model_dump(mode="json")
        return None

    configs_mock.get_persisted_config = _get
    configs_mock.delete_config = AsyncMock()

    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        await adapter.revoke(descriptor, ctx)

    on_revoked.assert_awaited_once_with(catalog_id="cat-hook-rev")


# ---------------------------------------------------------------------------
# Builtin preset parity
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_public_catalog_adapter_dry_run():
    """Dry-run of the public_catalog routing preset via adapter produces a plan."""
    from dynastore.modules.storage.presets import get_preset

    preset = get_preset("public_catalog")
    adapter = RoutingPresetAdapter(preset)
    ctx = _make_context()
    plan = await adapter.dry_run(NoParams(), "catalog:cat-x", ctx)
    assert isinstance(plan, PresetPlan)
    assert plan.preset_name == "public_catalog"
    assert len(plan.entries) > 0
