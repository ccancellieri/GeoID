"""BundlePreset — apply/revoke/dry_run lifecycle for build-based presets.

Covers the behaviour previously verified for ``RoutingPresetAdapter`` (now
the ``BundlePreset`` base class):

* scope-string <-> kwargs helpers,
* ``apply()`` writes each bundle entry via ``set_config`` and fires
  ``on_applied``,
* ``revoke()`` deletes matching entries and skips diverged ones (the
  byte-comparison guard),
* the ``on_revoked`` override fires after revoke,
* ``dry_run()`` produces a plan,
* a ``CompositePreset`` whose child subclasses ``BundlePreset`` applies
  cleanly (no auto-wrap needed — the child has ``apply`` natively).
"""
from __future__ import annotations

import uuid
from typing import ClassVar, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.presets.bundle_preset import (
    BundlePreset,
    _scope_to_kwargs,
    _scope_to_string,
)
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    CompositePreset,
    NoParams,
    PresetContext,
    PresetPlan,
)
from dynastore.modules.storage.presets.protocol import (
    PresetBundle,
    PresetBundleEntry,
    PresetTier,
)
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    FailurePolicy,
    Operation,
    OperationDriverEntry,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


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


def _catalog_routing() -> CatalogRoutingConfig:
    return CatalogRoutingConfig(
        operations={
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                )
            ],
        }
    )


class _SimpleBundlePreset(BundlePreset):
    """Minimal BundlePreset subclass for lifecycle tests."""

    name = "simple-bundle"
    description = "simple bundle preset"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = False

    def build(self, catalog_id: str = "", **_: str) -> PresetBundle:
        return PresetBundle(entries=(
            PresetBundleEntry(
                slot="catalog_routing",
                config_cls=CatalogRoutingConfig,
                instance=_catalog_routing(),
                rollback_priority=30,
            ),
        ))


class _BundlePresetWithHooks(_SimpleBundlePreset):
    name = "bundle-with-hooks"
    description = "bundle preset with on_applied/on_revoked"

    def __init__(self) -> None:
        self.applied: list[str] = []
        self.revoked: list[str] = []

    async def on_applied(self, catalog_id: str = "", **_: str) -> None:
        self.applied.append(catalog_id)

    async def on_revoked(self, catalog_id: str = "", **_: str) -> None:
        self.revoked.append(catalog_id)


# ---------------------------------------------------------------------------
# Scope helpers
# ---------------------------------------------------------------------------

def test_scope_to_kwargs_platform():
    assert _scope_to_kwargs("platform") == {}


def test_scope_to_kwargs_catalog():
    assert _scope_to_kwargs("catalog:cat-7") == {"catalog_id": "cat-7"}


def test_scope_to_kwargs_collection():
    assert _scope_to_kwargs("catalog:cat-7/collection:coll-3") == {
        "catalog_id": "cat-7",
        "collection_id": "coll-3",
    }


def test_scope_to_string_roundtrip():
    for scope in ("platform", "catalog:foo", "catalog:foo/collection:bar"):
        assert _scope_to_string(_scope_to_kwargs(scope)) == scope


# ---------------------------------------------------------------------------
# Metadata + abstract build
# ---------------------------------------------------------------------------

def test_bundle_preset_exposes_metadata():
    p = _SimpleBundlePreset()
    assert p.name == "simple-bundle"
    assert p.tier == PresetTier.CATALOG
    assert p.catalog_scopable is False
    assert p.keywords == ("routing",)
    assert p.params_model is NoParams


def test_base_build_not_implemented():
    class _NoBuild(BundlePreset):
        name = "no-build"
        description = "x"
        tier = PresetTier.PLATFORM

    with pytest.raises(NotImplementedError):
        _NoBuild().build()


# ---------------------------------------------------------------------------
# apply / revoke / dry_run round trip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_calls_set_config_for_each_entry():
    p = _SimpleBundlePreset()
    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(side_effect=lambda cls, inst, **kw: inst)
    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        result = await p.apply(NoParams(), "catalog:cat-test", _make_context())
    assert isinstance(result, AppliedDescriptor)
    assert configs_mock.set_config.call_count == 1
    assert "catalog_routing" in result.payload["slots"]


@pytest.mark.asyncio
async def test_apply_calls_on_applied_hook():
    p = _BundlePresetWithHooks()
    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(side_effect=lambda cls, inst, **kw: inst)
    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        await p.apply(NoParams(), "catalog:cat-hook", _make_context())
    assert "cat-hook" in p.applied


@pytest.mark.asyncio
async def test_apply_raises_when_configs_unavailable():
    p = _SimpleBundlePreset()
    with patch("dynastore.modules.get_protocol", return_value=None):
        with pytest.raises(RuntimeError, match="ConfigsProtocol"):
            await p.apply(NoParams(), "catalog:cat-x", _make_context())


@pytest.mark.asyncio
async def test_revoke_calls_delete_config_for_matching_slots():
    p = _SimpleBundlePreset()
    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(side_effect=lambda cls, inst, **kw: inst)
    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        descriptor = await p.apply(NoParams(), "catalog:cat-rev", _make_context())

    bundle = p.build(catalog_id="cat-rev")

    async def _get(cls, **kw):
        for entry in bundle.iter_apply():
            if entry.config_cls is cls:
                return entry.instance.model_dump(mode="json")
        return None

    configs_mock.get_persisted_config = _get
    configs_mock.delete_config = AsyncMock()
    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        await p.revoke(descriptor, _make_context())
    assert configs_mock.delete_config.call_count == 1


@pytest.mark.asyncio
async def test_revoke_skips_diverged_slots():
    """A slot whose persisted value differs from the preset emit is skipped."""
    p = _SimpleBundlePreset()
    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(side_effect=lambda cls, inst, **kw: inst)
    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        descriptor = await p.apply(NoParams(), "catalog:cat-div", _make_context())

    # Return a different value so the slot diverges.
    configs_mock.get_persisted_config = AsyncMock(return_value={"operations": {}})
    configs_mock.delete_config = AsyncMock()
    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        await p.revoke(descriptor, _make_context())
    configs_mock.delete_config.assert_not_called()


@pytest.mark.asyncio
async def test_on_revoked_hook_called_after_revoke():
    p = _BundlePresetWithHooks()
    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(side_effect=lambda cls, inst, **kw: inst)
    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        descriptor = await p.apply(NoParams(), "catalog:cat-hr", _make_context())

    bundle = p.build(catalog_id="cat-hr")

    async def _get(cls, **kw):
        for entry in bundle.iter_apply():
            if entry.config_cls is cls:
                return entry.instance.model_dump(mode="json")
        return None

    configs_mock.get_persisted_config = _get
    configs_mock.delete_config = AsyncMock()
    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        await p.revoke(descriptor, _make_context())
    assert "cat-hr" in p.revoked


@pytest.mark.asyncio
async def test_dry_run_produces_plan():
    p = _SimpleBundlePreset()
    plan = await p.dry_run(NoParams(), "catalog:cat-x", _make_context())
    assert isinstance(plan, PresetPlan)
    assert plan.preset_name == "simple-bundle"
    assert len(plan.entries) == 1
    assert plan.entries[0].kind == "set_config"


@pytest.mark.asyncio
async def test_builtin_public_catalog_dry_run():
    """The migrated public_catalog preset (a BundlePreset) dry-runs to a plan."""
    from dynastore.modules.storage.presets import get_preset

    preset = get_preset("public_catalog")
    plan = await preset.dry_run(NoParams(), "catalog:cat-x", _make_context())
    assert isinstance(plan, PresetPlan)
    assert plan.preset_name == "public_catalog"
    assert len(plan.entries) > 0


# ---------------------------------------------------------------------------
# Composite with a BundlePreset child applies (no auto-wrap)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_composite_with_bundle_child_applies():
    from dynastore.modules.storage.presets.registry import (
        _REGISTRY,
        get_preset,
        register_preset,
    )

    child_name = _uid("bundle-child")

    class _Child(_SimpleBundlePreset):
        pass

    _Child.name = child_name  # type: ignore[attr-defined]
    _REGISTRY.pop(child_name, None)
    register_preset(_Child())

    registered_child = get_preset(child_name)
    assert isinstance(registered_child, BundlePreset)
    assert hasattr(registered_child, "apply")

    comp_name = _uid("comp")

    class _Comp(CompositePreset):
        description = "composite with bundle child"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.CATALOG
        catalog_scopable = False
        compose = (child_name,)

    _Comp.name = comp_name
    _REGISTRY.pop(comp_name, None)
    register_preset(_Comp())

    comp = get_preset(comp_name)
    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(side_effect=lambda cls, inst, **kw: inst)
    with patch("dynastore.modules.get_protocol", return_value=configs_mock):
        result = await comp.apply(NoParams(), "catalog:cat-comp", _make_context())
    assert isinstance(result, AppliedDescriptor)

    _REGISTRY.pop(child_name, None)
    _REGISTRY.pop(comp_name, None)
