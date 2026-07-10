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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Behavioural tests for ``IdentityProviderReconcileService`` (geoid#3231).

Follow-up to geoid#3199/#3227: ``IamModule.lifespan`` registers the OIDC
identity provider exactly once per process. Two gaps survive that:

* First-ever deployment — a process that boots into the t=0 window before
  the fleet-once cold-boot leader has seeded a fresh ``IdpConfig`` row sees
  no config, registers nothing, and never gets a second look.
* Runtime rotation — a PATCH to ``IdpConfig`` (issuer/audience/client
  change) never reaches an already-running process.

``IdentityProviderReconcileService`` is a ``PeriodicService`` registered on
the IAM module's existing ``BackgroundSupervisor`` loop. Each tick fingerprints
the registration-relevant ``IdpConfig`` fields and only calls
``_register_identity_provider`` when that fingerprint changed since the last
successful call — an unchanged fingerprint costs one config read and nothing
else (no OIDC discovery, no JWKS fetch, no provider rebuild).
"""

import asyncio
import os

import pytest

os.environ.setdefault(
    "JWT_SECRET", "test-secret-padded-to-enough-chars-for-fernet-xx"
)

iam_module = pytest.importorskip(
    "dynastore.modules.iam.module",
    reason="dynastore-ext-iam distribution not installed — skipping IdP reconcile tests",
    exc_type=ImportError,
)
IdpConfig = pytest.importorskip(
    "dynastore.modules.iam.idp_config",
    reason="dynastore-ext-iam distribution not installed",
    exc_type=ImportError,
).IdpConfig
IamModule = iam_module.IamModule
IdentityProviderReconcileService = iam_module.IdentityProviderReconcileService
_idp_registration_fingerprint = iam_module._idp_registration_fingerprint

from dynastore.tools.background_service import ServiceContext  # noqa: E402


class _FakeConfigs:
    """PlatformConfigsProtocol stub returning a pinned IdpConfig."""

    def __init__(self, cfg) -> None:
        self._cfg = cfg

    async def get_config(self, cls):
        return self._cfg


class _BoomConfigs:
    """PlatformConfigsProtocol stub that always fails to read."""

    async def get_config(self, cls):
        raise RuntimeError("db unavailable")


class _RaceConfigs:
    """PlatformConfigsProtocol stub reproducing the reviewer-found race:
    the FIRST ``get_config`` call (from the first-issued
    ``_register_identity_provider`` coroutine) resolves SLOWLY and returns
    the OLD config; the SECOND call resolves immediately and returns the
    NEW config — so the first-issued call finishes last unless the two
    calls are serialized."""

    def __init__(self, old_cfg, new_cfg, *, slow_delay: float = 0.05) -> None:
        self._old_cfg = old_cfg
        self._new_cfg = new_cfg
        self._slow_delay = slow_delay
        self.calls = 0

    async def get_config(self, cls):
        self.calls += 1
        if self.calls == 1:
            await asyncio.sleep(self._slow_delay)
            return self._old_cfg
        return self._new_cfg


def _patch_configs(monkeypatch, configs) -> None:
    monkeypatch.setattr(
        iam_module, "get_protocol", lambda _proto: configs, raising=True
    )


def _ctx() -> ServiceContext:
    return ServiceContext(
        engine=None, shutdown=asyncio.Event(), is_ephemeral=False, name="test"
    )


def _configured_cfg(issuer: str = "https://kc.example.org/realms/fao") -> "IdpConfig":
    return IdpConfig(
        issuer_url=issuer,
        client_id="my-client",
        roles_claim_path="realm_access.roles",
    )


@pytest.mark.asyncio
async def test_first_boot_gap_config_appears_on_next_tick(monkeypatch):
    """A process that ticks while no ``IdpConfig`` row exists registers
    nothing (correct) but must register on the FIRST later tick that
    observes a resolved config — closing the first-boot gap left by the
    once-only lifespan call."""
    captured: list = []
    monkeypatch.setattr(
        iam_module, "register_plugin", lambda obj: captured.append(obj), raising=True
    )
    monkeypatch.setattr(
        iam_module, "unregister_plugin", lambda obj: None, raising=True
    )

    mod = object.__new__(IamModule)
    svc = IdentityProviderReconcileService(mod)

    # No PlatformConfigsProtocol registered yet == no config row seen.
    _patch_configs(monkeypatch, None)
    await svc.tick(_ctx())
    assert captured == [], "no config row present must register nothing"
    assert mod._identity_provider_fingerprint is None

    # The fleet-once cold-boot leader seeds the row; a sibling process that
    # missed the boot-time window must pick it up on its next tick.
    cfg = _configured_cfg()
    _patch_configs(monkeypatch, _FakeConfigs(cfg))
    await svc.tick(_ctx())

    assert len(captured) == 1
    assert captured[0].issuer_url == cfg.issuer_url
    assert mod._identity_provider is captured[0]
    assert mod._identity_provider_fingerprint is not None


@pytest.mark.asyncio
async def test_unchanged_fingerprint_skips_reregistration(monkeypatch):
    """Unchanged registration-relevant fields must not trigger a rebuild —
    no OIDC discovery, no JWKS fetch, no provider rebuild."""
    registered: list = []
    monkeypatch.setattr(
        iam_module, "register_plugin", lambda obj: registered.append(obj), raising=True
    )
    monkeypatch.setattr(
        iam_module, "unregister_plugin", lambda obj: None, raising=True
    )

    cfg = _configured_cfg()
    _patch_configs(monkeypatch, _FakeConfigs(cfg))

    mod = object.__new__(IamModule)
    await mod._register_identity_provider()
    assert len(registered) == 1
    first_provider = mod._identity_provider
    first_fingerprint = mod._identity_provider_fingerprint

    # Spy directly on the heavier call to prove the reconcile tick never
    # reaches it when the fingerprint is unchanged.
    reregister_calls: list = []
    original = IamModule._register_identity_provider

    async def spy(self):
        reregister_calls.append(1)
        return await original(self)

    monkeypatch.setattr(IamModule, "_register_identity_provider", spy, raising=True)

    svc = IdentityProviderReconcileService(mod)
    await svc.tick(_ctx())

    assert reregister_calls == [], "unchanged fingerprint must skip re-registration"
    assert registered == [first_provider], "no new provider must be built"
    assert mod._identity_provider is first_provider
    assert mod._identity_provider_fingerprint == first_fingerprint


@pytest.mark.asyncio
async def test_changed_fingerprint_reregisters_make_before_break(monkeypatch):
    """A registration-relevant field change (issuer rotation) must
    re-register make-before-break: the new provider is registered before
    the old one is unregistered."""
    calls: list = []
    monkeypatch.setattr(
        iam_module, "register_plugin", lambda obj: calls.append(("register", obj)), raising=True
    )
    monkeypatch.setattr(
        iam_module, "unregister_plugin", lambda obj: calls.append(("unregister", obj)), raising=True
    )

    cfg1 = _configured_cfg("https://kc.example.org/realms/fao")
    _patch_configs(monkeypatch, _FakeConfigs(cfg1))

    mod = object.__new__(IamModule)
    await mod._register_identity_provider()
    first = mod._identity_provider
    assert calls == [("register", first)]

    cfg2 = _configured_cfg("https://kc.example.org/realms/rotated")
    _patch_configs(monkeypatch, _FakeConfigs(cfg2))

    svc = IdentityProviderReconcileService(mod)
    await svc.tick(_ctx())

    second = mod._identity_provider
    assert second is not None and second is not first
    assert calls == [
        ("register", first),
        ("register", second),
        ("unregister", first),
    ], calls
    assert mod._identity_provider_fingerprint is not None


@pytest.mark.asyncio
async def test_config_read_failure_keeps_provider_and_fingerprint(monkeypatch, caplog):
    """A transient read failure during a reconcile tick must leave the
    previously-registered provider AND the stored fingerprint untouched —
    never treat a read failure as "not configured"."""
    registered: list = []
    monkeypatch.setattr(
        iam_module, "register_plugin", lambda obj: registered.append(obj), raising=True
    )
    monkeypatch.setattr(
        iam_module, "unregister_plugin", lambda obj: registered.remove(obj) if obj in registered else None,
        raising=True,
    )

    cfg = _configured_cfg()
    _patch_configs(monkeypatch, _FakeConfigs(cfg))

    mod = object.__new__(IamModule)
    await mod._register_identity_provider()
    first = mod._identity_provider
    first_fingerprint = mod._identity_provider_fingerprint
    assert registered == [first]

    _patch_configs(monkeypatch, _BoomConfigs())

    svc = IdentityProviderReconcileService(mod)
    with caplog.at_level("WARNING"):
        await svc.tick(_ctx())

    assert mod._identity_provider is first
    assert registered == [first]
    assert mod._identity_provider_fingerprint == first_fingerprint
    assert "IdpConfig read failed" in caplog.text


@pytest.mark.asyncio
async def test_config_row_deleted_removes_provider(monkeypatch):
    """A deleted ``IdpConfig`` row (resolved-but-absent config, geoid#3227)
    must eventually deregister the provider via the reconcile tick — the
    same "resolved but not configured" semantic
    ``_register_identity_provider`` already applies at boot must hold when
    reached through the periodic reconciler too."""
    registered: list = []
    monkeypatch.setattr(
        iam_module, "register_plugin", lambda obj: registered.append(obj), raising=True
    )
    monkeypatch.setattr(
        iam_module, "unregister_plugin", lambda obj: registered.remove(obj) if obj in registered else None,
        raising=True,
    )

    cfg = _configured_cfg()
    _patch_configs(monkeypatch, _FakeConfigs(cfg))

    mod = object.__new__(IamModule)
    await mod._register_identity_provider()
    assert len(registered) == 1

    # Row deleted: PlatformConfigsProtocol resolves the zero-arg default
    # (unconfigured) — the documented "no row" contract.
    _patch_configs(monkeypatch, _FakeConfigs(IdpConfig()))

    svc = IdentityProviderReconcileService(mod)
    await svc.tick(_ctx())

    assert registered == []
    assert mod._identity_provider is None
    assert mod._identity_provider_fingerprint is None


@pytest.mark.asyncio
async def test_concurrent_registration_serializes_latest_wins(monkeypatch):
    """Regression for the reviewer-found race on PR #3233: three callers
    (lifespan boot, the reconcile tick, and the cold-boot leader's step-7
    self-heal) can all invoke ``_register_identity_provider`` on the same
    instance with no ordering guarantee. Without serialization, a
    first-issued call whose config read is slow can finish LAST and
    overwrite a second-issued call's faster, more current registration —
    and because the fingerprint is updated in the same call that set it,
    the stale result looks self-consistent.

    Two overlapping calls race here via ``asyncio.gather`` against
    ``_RaceConfigs``, which resolves the FIRST read slowly (old config) and
    the SECOND read immediately (new config) — reproducing exactly that
    ordering. The final registered provider and fingerprint must reflect
    the most-recently-resolved (new) config, never the stale (old) one.
    """
    registered: list = []
    monkeypatch.setattr(
        iam_module, "register_plugin", lambda obj: registered.append(obj), raising=True
    )
    monkeypatch.setattr(
        iam_module, "unregister_plugin", lambda obj: registered.remove(obj) if obj in registered else None,
        raising=True,
    )

    old_cfg = _configured_cfg("https://kc.example.org/realms/old")
    new_cfg = _configured_cfg("https://kc.example.org/realms/new")
    configs = _RaceConfigs(old_cfg, new_cfg)
    _patch_configs(monkeypatch, configs)

    mod = object.__new__(IamModule)

    await asyncio.gather(
        mod._register_identity_provider(),
        mod._register_identity_provider(),
    )

    assert configs.calls == 2
    assert mod._identity_provider is not None
    assert mod._identity_provider.issuer_url == new_cfg.issuer_url, (
        "the most-recently-resolved config must win; a slow first-issued "
        "call must not overwrite a faster later one"
    )
    assert mod._identity_provider_fingerprint == _idp_registration_fingerprint(new_cfg)
    assert len(registered) == 1, "exactly one provider must remain registered"
