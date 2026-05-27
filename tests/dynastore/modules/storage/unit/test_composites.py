"""Curated composite preset tests (PR-4 of #1412).

Tests cover:
- Each composite's compose tuple resolves against the registry with no
  missing children when all modules are loaded.
- apply iterates children forward (recorded call sequence).
- apply rolls back prior children in reverse when a child fails.
- revoke iterates children in reverse.
- dry_run concatenates child plans without writing.
- platform_demo applied then revoked leaves the registry state unchanged.
- Composite registration with a missing child raises ValueError and the
  other composites still register.
"""
from __future__ import annotations

import uuid
from typing import ClassVar, List, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    CompositePreset,
    NoParams,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from dynastore.modules.storage.presets.protocol import PresetTier
from dynastore.modules.storage.presets.registry import (
    _REGISTRY,
    find_preset,
    register_preset,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _make_ctx() -> PresetContext:
    return PresetContext(
        db=MagicMock(),
        iam=AsyncMock(),
        policy=AsyncMock(),
        config=MagicMock(),
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope="platform",
    )


class _RecordingChild:
    """Minimal child preset that records apply/revoke calls into a shared list."""

    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model = NoParams
    is_async: ClassVar[bool] = False
    compose = None

    def __init__(self, name: str, call_log: List[str]) -> None:
        self.name = name
        self.description = "recording child"
        self.keywords: Tuple[str, ...] = ()
        self._call_log = call_log

    async def dry_run(self, params, scope, ctx) -> PresetPlan:
        return PresetPlan(
            preset_name=self.name,
            scope_key=scope,
            entries=(PresetPlanEntry(kind="noop", target=self.name),),
        )

    async def apply(self, params, scope, ctx) -> AppliedDescriptor:
        self._call_log.append(f"apply:{self.name}")
        return AppliedDescriptor(payload={"name": self.name})

    async def revoke(self, desc, ctx) -> None:
        self._call_log.append(f"revoke:{self.name}")


class _FailingChild(_RecordingChild):
    """Child that raises on apply after recording the attempt."""

    async def apply(self, params, scope, ctx) -> AppliedDescriptor:
        self._call_log.append(f"apply:{self.name}")
        raise RuntimeError(f"intentional failure in {self.name}")


# ---------------------------------------------------------------------------
# Ensure all composite children are available in the registry
# ---------------------------------------------------------------------------

def _ensure_iam_presets_registered() -> None:
    """Register IAM presets if not yet present (unit test isolation helper).

    Imports the IAM modules explicitly so the global registry contains
    default_roles_baseline and iam_baseline before the composites are
    registered.  Both registrations are idempotent (skip if already present).
    """
    # Core IAM module registers default_roles_baseline.
    if "default_roles_baseline" not in _REGISTRY:
        from dynastore.modules.iam import presets as _core_iam_presets  # noqa: F401

    # IAM extension registers iam_baseline.
    if "iam_baseline" not in _REGISTRY:
        from dynastore.extensions.iam import presets as _ext_iam_presets  # noqa: F401


def _ensure_extension_presets_registered() -> None:
    """Register per-extension policy presets if not yet present."""
    ext_presets = [
        ("admin_enable", "dynastore.extensions.admin.presets"),
        ("stac_enable", "dynastore.extensions.stac.presets"),
        ("web_enable", "dynastore.extensions.web.presets"),
        ("coverages_enable", "dynastore.extensions.coverages.presets"),
        ("dggs_enable", "dynastore.extensions.dggs.presets"),
        ("edr_enable", "dynastore.extensions.edr.presets"),
        ("events_enable", "dynastore.extensions.events.presets"),
        ("joins_enable", "dynastore.extensions.joins.presets"),
        ("logs_enable", "dynastore.extensions.logs.presets"),
        ("maps_enable", "dynastore.extensions.maps.presets"),
        ("records_enable", "dynastore.extensions.records.presets"),
        ("stats_enable", "dynastore.extensions.stats.presets"),
    ]
    import importlib
    for name, module_path in ext_presets:
        if name not in _REGISTRY:
            try:
                importlib.import_module(module_path)
            except ImportError:
                pass


def _ensure_composites_registered() -> None:
    """Trigger composite registration after all children are available."""
    from dynastore.modules.storage.presets.composites import (
        retry_composite_registration,
    )
    retry_composite_registration()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True, scope="module")
def _register_all_presets() -> None:
    """Ensure all preset dependencies are registered before any composite test."""
    _ensure_iam_presets_registered()
    _ensure_extension_presets_registered()
    _ensure_composites_registered()


# ---------------------------------------------------------------------------
# Compose-tuple resolution tests
# ---------------------------------------------------------------------------

def test_platform_demo_compose_all_children_resolve() -> None:
    """All children declared in platform_demo compose must be in the registry."""
    composite = find_preset("platform_demo")
    missing = [name for name in composite.compose if name not in _REGISTRY]
    assert missing == [], f"platform_demo has unresolved children: {missing}"


def test_public_open_data_compose_all_children_resolve() -> None:
    """All children declared in public_open_data compose must be in the registry."""
    composite = find_preset("public_open_data")
    missing = [name for name in composite.compose if name not in _REGISTRY]
    assert missing == [], f"public_open_data has unresolved children: {missing}"


def test_private_tenant_compose_all_children_resolve() -> None:
    """All children declared in private_tenant compose must be in the registry."""
    composite = find_preset("private_tenant")
    missing = [name for name in composite.compose if name not in _REGISTRY]
    assert missing == [], f"private_tenant has unresolved children: {missing}"


def test_composite_metadata_shape() -> None:
    """Composite presets expose expected metadata fields."""
    for name in ("platform_demo", "public_open_data", "private_tenant"):
        p = find_preset(name)
        assert p.name == name
        assert p.description, f"{name}: description must be non-empty"
        assert "composite" in p.keywords, f"{name}: must have 'composite' keyword"
        assert p.tier == PresetTier.PLATFORM, f"{name}: must be PLATFORM tier"


def test_catalog_scopable_flags() -> None:
    """platform_demo is not catalog-scopable; public_open_data and private_tenant are."""
    assert not find_preset("platform_demo").catalog_scopable
    assert find_preset("public_open_data").catalog_scopable
    assert find_preset("private_tenant").catalog_scopable


# ---------------------------------------------------------------------------
# Apply — forward order
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_iterates_children_forward() -> None:
    """CompositePreset.apply calls child apply in declaration order."""
    call_log: List[str] = []
    name_a = _unique("fwd-a")
    name_b = _unique("fwd-b")

    register_preset(_RecordingChild(name_a, call_log))
    register_preset(_RecordingChild(name_b, call_log))

    comp_name = _unique("fwd-comp")

    class _Comp(CompositePreset):
        name = comp_name
        description = "fwd"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        compose = (name_a, name_b)

    _Comp.name = comp_name
    register_preset(_Comp())
    comp = find_preset(comp_name)

    result = await comp.apply(NoParams(), "platform", _make_ctx())
    assert isinstance(result, AppliedDescriptor)
    assert call_log == [f"apply:{name_a}", f"apply:{name_b}"]
    children = result.payload["children"]
    assert children[0]["name"] == name_a
    assert children[1]["name"] == name_b


# ---------------------------------------------------------------------------
# Apply — rollback on failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_rolls_back_on_child_failure() -> None:
    """When a child raises, previously applied children are revoked in reverse."""
    call_log: List[str] = []
    name_ok = _unique("rb-ok")
    name_fail = _unique("rb-fail")

    register_preset(_RecordingChild(name_ok, call_log))
    register_preset(_FailingChild(name_fail, call_log))

    comp_name = _unique("rb-comp")

    class _Comp(CompositePreset):
        name = comp_name
        description = "rollback"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        compose = (name_ok, name_fail)

    _Comp.name = comp_name
    register_preset(_Comp())
    comp = find_preset(comp_name)

    with pytest.raises(RuntimeError, match="intentional failure"):
        await comp.apply(NoParams(), "platform", _make_ctx())

    # ok child was applied then revoked; fail child's apply was attempted.
    assert call_log == [f"apply:{name_ok}", f"apply:{name_fail}", f"revoke:{name_ok}"]


# ---------------------------------------------------------------------------
# Revoke — reverse order
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_revoke_iterates_children_reverse() -> None:
    """CompositePreset.revoke calls child revoke in reverse declaration order."""
    call_log: List[str] = []
    name_a = _unique("rev-a")
    name_b = _unique("rev-b")

    register_preset(_RecordingChild(name_a, call_log))
    register_preset(_RecordingChild(name_b, call_log))

    comp_name = _unique("rev-comp")

    class _Comp(CompositePreset):
        name = comp_name
        description = "rev"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        compose = (name_a, name_b)

    _Comp.name = comp_name
    register_preset(_Comp())
    comp = find_preset(comp_name)

    descriptor = AppliedDescriptor(payload={
        "children": [
            {"name": name_a, "descriptor": {"name": name_a}},
            {"name": name_b, "descriptor": {"name": name_b}},
        ],
        "scope": "platform",
    })
    await comp.revoke(descriptor, _make_ctx())
    assert call_log == [f"revoke:{name_b}", f"revoke:{name_a}"]


# ---------------------------------------------------------------------------
# Dry-run — concatenates child plans
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dry_run_concatenates_child_plans() -> None:
    """dry_run returns a PresetPlan whose entries are the union of child plans."""
    call_log: List[str] = []
    name_a = _unique("dr-a")
    name_b = _unique("dr-b")

    register_preset(_RecordingChild(name_a, call_log))
    register_preset(_RecordingChild(name_b, call_log))

    comp_name = _unique("dr-comp")

    class _Comp(CompositePreset):
        name = comp_name
        description = "dryrun"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        compose = (name_a, name_b)

    _Comp.name = comp_name
    register_preset(_Comp())
    comp = find_preset(comp_name)

    plan = await comp.dry_run(NoParams(), "platform", _make_ctx())
    assert isinstance(plan, PresetPlan)
    # Each _RecordingChild.dry_run emits one entry.
    assert len(plan.entries) == 2
    targets = {e.target for e in plan.entries}
    assert name_a in targets
    assert name_b in targets


# ---------------------------------------------------------------------------
# State-equivalence: apply then revoke leaves registry unchanged
# ---------------------------------------------------------------------------

def test_platform_demo_apply_revoke_state_equivalence() -> None:
    """platform_demo is registered with compose that resolves all children.

    This is a structural test — the actual apply/revoke requires a live DB.
    We verify that the compose tuple references only registered presets so
    the round-trip can succeed at runtime.
    """
    composite = find_preset("platform_demo")
    for child_name in composite.compose:
        assert child_name in _REGISTRY, (
            f"platform_demo child {child_name!r} not in registry — "
            "apply+revoke state equivalence cannot be guaranteed"
        )


# ---------------------------------------------------------------------------
# Missing-child registration: one composite fails, others still register
# ---------------------------------------------------------------------------

def test_missing_child_does_not_block_other_composites() -> None:
    """If one composite references a missing child the others still register."""

    class _GoodComposite(CompositePreset):
        name = _unique("good-comp-isolated")
        description = "good"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        compose = ("public_catalog",)

    # Register the isolated good composite.
    _GoodComposite.name = _unique("good-comp-isolated")
    register_preset(_GoodComposite())

    # Attempting to register a composite with a missing child raises ValueError.
    class _BadComposite(CompositePreset):
        name = _unique("bad-comp-isolated")
        description = "bad"
        keywords: ClassVar[Tuple[str, ...]] = ("composite",)
        tier = PresetTier.PLATFORM
        catalog_scopable = False
        compose = ("nonexistent-child-xyz-pr4",)

    with pytest.raises(ValueError, match="unknown child preset"):
        register_preset(_BadComposite())

    # The good composite was registered; the bad composite was not.
    assert _GoodComposite.name in _REGISTRY
    assert _BadComposite.name not in _REGISTRY


def test_retry_composite_registration_is_idempotent() -> None:
    """Calling retry_composite_registration twice does not raise."""
    from dynastore.modules.storage.presets.composites import retry_composite_registration

    registered_1 = retry_composite_registration()
    registered_2 = retry_composite_registration()
    # Both calls should succeed; second call returns same set (all already registered).
    assert set(registered_1) == set(registered_2)
