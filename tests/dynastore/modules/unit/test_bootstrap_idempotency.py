"""Local repro + regression test for the module-bootstrap idempotency bug.

Production symptom (2026-04-22): `get_protocol(StorageProtocol)` returned
None on some pods, causing `gcp_provision_catalog` tasks to fail with
"StorageProtocol not available - GCP module not loaded".

Hypothesis under test: the production app has multiple callers of
`discover_modules()` (main, bootstrap, extensions.bootstrap,
tasks.bootstrap). On re-entry the old `discover_modules()` unconditionally
reset `_DYNASTORE_MODULES[name] = ModuleConfig(cls=cls)`, wiping the live
`.instance` reference. If a later `instantiate_modules(include_only=…)`
was narrow enough to skip GCP, the dict would show `.instance = None`
for GCP.

This test pins the hypothesis by simulating the bootstrap sequence that
the serving container actually runs:

    discover_modules() → instantiate_modules() →         # main.py
    discover_modules() → instantiate_modules(subset) →   # extensions/bootstrap.py
    discover_modules() → instantiate_modules(subset)     # tasks/bootstrap.py

After all three passes we should still be able to resolve every module
via both `_DYNASTORE_MODULES[name].instance` AND `get_protocol(...)`.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from dynastore import modules as modules_mod
from dynastore.modules import (
    ModuleConfig,
    _DYNASTORE_MODULES,
    discover_modules,
    instantiate_modules,
)
from dynastore.tools.discovery import (
    _DYNASTORE_PLUGINS,
    get_protocol,
    _get_protocol_cached,
)


# Minimal fake protocol + fake module ----------------------------------

from typing import Protocol, runtime_checkable


@runtime_checkable
class _FakeStorageProtocol(Protocol):
    async def ensure_storage(self, catalog_id: str) -> str: ...


class _FakeGCPModule:
    """Stand-in for GCPModule — implements the fake storage protocol."""

    priority = 30
    init_count = 0

    def __init__(self, app_state=None):
        _FakeGCPModule.init_count += 1
        self._credentials = object()  # pretend creds OK

    async def ensure_storage(self, catalog_id: str) -> str:
        return f"bucket-for-{catalog_id}"


class _FakeOtherModule:
    """Unrelated module — just to make the registry realistic."""

    priority = 100

    def __init__(self, app_state=None):
        pass


@pytest.fixture(autouse=True)
def clean_registry():
    """Isolate each test from real plugin state."""
    saved_modules = dict(_DYNASTORE_MODULES)
    saved_plugins = list(_DYNASTORE_PLUGINS)
    _DYNASTORE_MODULES.clear()
    _DYNASTORE_PLUGINS.clear()
    _get_protocol_cached.cache_clear()
    _FakeGCPModule.init_count = 0
    try:
        yield
    finally:
        _DYNASTORE_MODULES.clear()
        _DYNASTORE_MODULES.update(saved_modules)
        _DYNASTORE_PLUGINS.clear()
        _DYNASTORE_PLUGINS.extend(saved_plugins)
        _get_protocol_cached.cache_clear()


def _fake_entry_points(_group):
    return {"fake_gcp": _FakeGCPModule, "fake_other": _FakeOtherModule}


def test_single_bootstrap_pass_resolves_storage():
    """Baseline: one discover + one instantiate → protocol resolves."""
    with patch(
        "dynastore.tools.discovery.discover_and_load_plugins",
        side_effect=_fake_entry_points,
    ):
        discover_modules()
    instantiate_modules(app_state=object())

    assert _FakeGCPModule.init_count == 1
    storage = get_protocol(_FakeStorageProtocol)
    assert storage is not None
    assert isinstance(storage, _FakeGCPModule)


def _run_three_passes() -> "_FakeGCPModule":
    app_state = object()
    with patch(
        "dynastore.tools.discovery.discover_and_load_plugins",
        side_effect=_fake_entry_points,
    ):
        # Pass 1 (main.py) — full discover + instantiate
        discover_modules()
        instantiate_modules(app_state)

        first_instance = _DYNASTORE_MODULES["fake_gcp"].instance
        assert first_instance is not None

        # Pass 2 (extensions/bootstrap.py) — discover again, then
        # instantiate only a subset that EXCLUDES gcp
        discover_modules()
        instantiate_modules(app_state, include_only=["fake_other"])

        # Pass 3 (tasks/bootstrap.py) — same pattern
        discover_modules()
        instantiate_modules(app_state, include_only=["fake_other"])

    return first_instance


def test_three_passes_module_registry_instance_preserved():
    """INVARIANT 1: _DYNASTORE_MODULES[name].instance survives re-entries."""
    first_instance = _run_three_passes()
    assert _DYNASTORE_MODULES["fake_gcp"].instance is first_instance, (
        "discover_modules() wiped _DYNASTORE_MODULES['fake_gcp'].instance "
        "on re-entry."
    )


def test_three_passes_no_duplicate_instantiation():
    """INVARIANT 2: GCPModule.__init__ runs exactly once."""
    _run_three_passes()
    assert _FakeGCPModule.init_count == 1, (
        f"GCPModule was re-instantiated ({_FakeGCPModule.init_count}x)."
    )


def test_three_passes_get_protocol_still_resolves():
    """INVARIANT 3 — **this is the production symptom**:
    after 3 bootstrap passes, get_protocol(StorageProtocol) must still
    return the live module instance, not None.

    If this test FAILS against the pre-91190ae code, it confirms the
    root cause of the production 'StorageProtocol not available'
    cascade. If it PASSES against pre-91190ae code, then 91190ae is
    fixing an upstream issue that never manifested as the observed
    production symptom — and we should keep looking.
    """
    first_instance = _run_three_passes()
    resolved = get_protocol(_FakeStorageProtocol)
    assert resolved is not None, (
        "get_protocol(StorageProtocol) returned None after 3 bootstrap "
        "passes — exact production symptom."
    )
    assert resolved is first_instance, (
        f"get_protocol returned a different instance "
        f"(id={id(resolved)}) than the original (id={id(first_instance)})."
    )


def test_plugins_list_not_polluted_on_reinit():
    """Even a correct idempotency guard must not add duplicate entries to
    _DYNASTORE_PLUGINS when discover+instantiate re-runs.
    """
    app_state = object()

    with patch(
        "dynastore.tools.discovery.discover_and_load_plugins",
        side_effect=_fake_entry_points,
    ):
        discover_modules()
        instantiate_modules(app_state)
        discover_modules()
        instantiate_modules(app_state)
        discover_modules()
        instantiate_modules(app_state)

    gcp_instances = [p for p in _DYNASTORE_PLUGINS if isinstance(p, _FakeGCPModule)]
    assert len(gcp_instances) == 1, (
        f"_DYNASTORE_PLUGINS has {len(gcp_instances)} GCPModule entries "
        "— multi-bootstrap created duplicates."
    )
