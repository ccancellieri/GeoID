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

"""Behavioural tests for ``IamModule._register_identity_provider`` (geoid#1500,
geoid#3199).

The IdP factory is config-first (``IdpConfig`` via ``PlatformConfigsProtocol``):

- when ``IdpConfig`` selects an implemented + addressed backend, the provider
  is registered from the config;
- when the config does not select one (no row → zero-arg default, or a row
  with ``issuer_url`` unset), no provider is registered;
- ``type=saml2`` registers nothing (reserved placeholder).

``IamModule._register_identity_provider`` touches module-level
``get_protocol`` / ``register_plugin`` plus one piece of instance state
(``self._identity_provider``, tracked so a re-registration in the same
process replaces its own previous provider instead of accumulating
duplicates — geoid#3199). Most tests below exercise it on a bare
``object.__new__(IamModule)`` with ``get_protocol`` / ``register_plugin``
monkeypatched; the idempotency test additionally exercises the real
``dynastore.tools.discovery`` registry.

Re-registration is make-before-break (geoid#3199 follow-up): the new
provider is registered before the old one is unregistered, so an in-flight
request never sees zero providers; and a transient ``IdpConfig`` read
failure leaves the previously-registered provider in place rather than
tearing it down.
"""

import os

import pytest

os.environ.setdefault(
    "JWT_SECRET", "test-secret-padded-to-enough-chars-for-fernet-xx"
)

iam_module = pytest.importorskip(
    "dynastore.modules.iam.module",
    reason="dynastore-ext-iam distribution not installed — skipping IDP lifespan tests",
    exc_type=ImportError,
)
IdpConfig = pytest.importorskip(
    "dynastore.modules.iam.idp_config",
    reason="dynastore-ext-iam distribution not installed",
    exc_type=ImportError,
).IdpConfig
IamModule = iam_module.IamModule


class _FakeConfigs:
    """PlatformConfigsProtocol stub returning a pinned IdpConfig."""

    def __init__(self, cfg) -> None:
        self._cfg = cfg

    async def get_config(self, cls):
        return self._cfg


def _patch_runtime(monkeypatch, configs, captured):
    """Route get_protocol → configs (or None) and capture register_plugin."""
    monkeypatch.setattr(
        iam_module, "get_protocol", lambda _proto: configs, raising=True
    )
    monkeypatch.setattr(
        iam_module, "register_plugin", lambda obj: captured.append(obj), raising=True
    )


@pytest.mark.asyncio
async def test_registers_from_config_when_configured(monkeypatch, caplog):
    captured: list = []
    cfg = IdpConfig(
        issuer_url="https://kc.example.org/realms/fao",
        client_id="my-client",
        client_secret="topsecret",
        roles_claim_path="realm_access.roles",
    )
    _patch_runtime(monkeypatch, _FakeConfigs(cfg), captured)

    mod = object.__new__(IamModule)
    with caplog.at_level("WARNING"):
        await mod._register_identity_provider()

    assert len(captured) == 1
    provider = captured[0]
    assert provider.issuer_url == "https://kc.example.org/realms/fao"
    assert provider.client_id == "my-client"
    assert provider.client_secret == "topsecret"  # revealed for the provider
    assert provider.roles_claim_path == "realm_access.roles"


@pytest.mark.asyncio
async def test_registers_nothing_when_config_unconfigured(monkeypatch):
    """When IdpConfig has no issuer_url set, no provider is registered."""
    captured: list = []
    _patch_runtime(monkeypatch, _FakeConfigs(IdpConfig()), captured)

    mod = object.__new__(IamModule)
    await mod._register_identity_provider()

    assert captured == []


@pytest.mark.asyncio
async def test_registers_nothing_when_no_configs_protocol(monkeypatch):
    """When PlatformConfigsProtocol is unregistered (get_protocol → None),
    no provider is registered and the startup continues without crashing."""
    captured: list = []
    _patch_runtime(monkeypatch, None, captured)

    mod = object.__new__(IamModule)
    await mod._register_identity_provider()

    assert captured == []


@pytest.mark.asyncio
async def test_saml2_config_registers_nothing(monkeypatch, caplog):
    captured: list = []
    cfg = IdpConfig(type="saml2", issuer_url="https://kc/x")
    _patch_runtime(monkeypatch, _FakeConfigs(cfg), captured)

    mod = object.__new__(IamModule)
    with caplog.at_level("WARNING"):
        await mod._register_identity_provider()

    assert captured == []


@pytest.mark.asyncio
async def test_second_registration_replaces_first_without_duplicate(monkeypatch):
    """Registering twice in the same process (geoid#3199) must not leave two
    providers registered in the process-local plugin registry.

    Both ``IamModule.lifespan`` (unconditional, every process) and
    ``IamColdBootContributor`` (self-heal, fleet-once leader only, after
    ``auth_bootstrap`` seeds a fresh ``IdpConfig`` row) can call
    ``_register_identity_provider`` on the same process — the second call
    must unregister the first call's provider rather than accumulate.
    """
    from dynastore.modules.iam.interfaces import IdentityProviderProtocol
    from dynastore.tools.discovery import get_protocols, unregister_plugin

    cfg = IdpConfig(
        issuer_url="https://kc.example.org/realms/fao",
        client_id="my-client",
        roles_claim_path="realm_access.roles",
    )
    monkeypatch.setattr(
        iam_module, "get_protocol", lambda _proto: _FakeConfigs(cfg), raising=True
    )

    mod = object.__new__(IamModule)
    try:
        await mod._register_identity_provider()
        first = mod._identity_provider
        assert first is not None

        await mod._register_identity_provider()
        second = mod._identity_provider
        assert second is not None
        assert second is not first, (
            "re-registration should produce a fresh provider instance"
        )

        registered = get_protocols(IdentityProviderProtocol)
        assert first not in registered, (
            "stale provider from the first call must be unregistered"
        )
        assert registered.count(second) == 1, (
            "exactly one provider must remain registered after two calls"
        )
    finally:
        if mod._identity_provider is not None:
            unregister_plugin(mod._identity_provider)


@pytest.mark.asyncio
async def test_reregistration_registers_new_before_unregistering_old(monkeypatch):
    """Make-before-break (geoid#3199 follow-up): on a re-call the new
    provider must be registered BEFORE the old one is unregistered, so
    there is never a zero-provider window. A previous version unregistered
    first, resolved the config second — any bearer-token request landing
    in that window saw ``get_identity_providers() == []`` and 401'd, a
    brief reproduction of the bug this method exists to fix.
    """
    calls: list = []

    def fake_register(obj):
        calls.append(("register", obj))

    def fake_unregister(obj):
        calls.append(("unregister", obj))

    monkeypatch.setattr(iam_module, "register_plugin", fake_register, raising=True)
    monkeypatch.setattr(iam_module, "unregister_plugin", fake_unregister, raising=True)

    cfg = IdpConfig(
        issuer_url="https://kc.example.org/realms/fao",
        client_id="my-client",
        roles_claim_path="realm_access.roles",
    )
    monkeypatch.setattr(
        iam_module, "get_protocol", lambda _proto: _FakeConfigs(cfg), raising=True
    )

    mod = object.__new__(IamModule)
    await mod._register_identity_provider()
    first = mod._identity_provider
    assert calls == [("register", first)]

    await mod._register_identity_provider()
    second = mod._identity_provider
    assert second is not None and second is not first

    assert calls == [
        ("register", first),
        ("register", second),
        ("unregister", first),
    ], calls


@pytest.mark.asyncio
async def test_config_read_failure_keeps_previous_provider_registered(monkeypatch, caplog):
    """A transient ``IdpConfig`` read failure during self-heal must NOT tear
    down the previously-registered provider — doing so would turn a
    transient config-store blip into a real auth outage on this process
    until restart (geoid#3199 follow-up). Failure is logged at WARNING
    (not DEBUG) since it now has an operator-visible consequence: self-heal
    was skipped.
    """
    registered: list = []

    def fake_register(obj):
        registered.append(obj)

    def fake_unregister(obj):
        if obj in registered:
            registered.remove(obj)

    monkeypatch.setattr(iam_module, "register_plugin", fake_register, raising=True)
    monkeypatch.setattr(iam_module, "unregister_plugin", fake_unregister, raising=True)

    cfg = IdpConfig(
        issuer_url="https://kc.example.org/realms/fao",
        client_id="my-client",
        roles_claim_path="realm_access.roles",
    )
    monkeypatch.setattr(
        iam_module, "get_protocol", lambda _proto: _FakeConfigs(cfg), raising=True
    )

    mod = object.__new__(IamModule)
    await mod._register_identity_provider()
    first = mod._identity_provider
    assert registered == [first]

    class _BoomConfigs:
        async def get_config(self, cls):
            raise RuntimeError("db unavailable")

    monkeypatch.setattr(
        iam_module, "get_protocol", lambda _proto: _BoomConfigs(), raising=True
    )

    with caplog.at_level("WARNING"):
        await mod._register_identity_provider()

    assert mod._identity_provider is first, (
        "the previously-registered provider must still be tracked after a "
        "failed config read"
    )
    assert registered == [first], (
        "the previously-registered provider must still be registered after "
        "a failed config read"
    )
    assert "IdpConfig read failed" in caplog.text


@pytest.mark.asyncio
async def test_lifespan_registers_identity_provider_unconditionally(monkeypatch):
    """``IamModule.lifespan`` must call ``_register_identity_provider`` on
    every process, unconditionally — no leader election, no fleet lease, no
    ``run_cold_boot`` gate (geoid#3199 root cause). Before the fix,
    registration only happened inside ``IamColdBootContributor``, reachable
    exclusively through ``main.py``'s fleet-once
    ``_ColdBootReconciliationService`` (a single lease-winning process per
    service revision).

    Forces the heavier iam-schema bootstrap that follows to fail
    immediately so the test needs neither a real database nor background
    services; the call-order assertion proves registration runs first and
    does not depend on anything after it (including any leadership check).
    """
    import dynastore.modules.iam.postgres_iam_storage as pg_storage_mod

    calls: list = []

    async def fake_register(self):
        calls.append("register_identity_provider")

    class _BoomStorage:
        def __init__(self, *args, **kwargs):
            calls.append("storage_init")
            raise RuntimeError("stop-here")

    monkeypatch.setattr(
        IamModule, "_register_identity_provider", fake_register, raising=True
    )
    monkeypatch.setattr(
        pg_storage_mod, "PostgresIamStorage", _BoomStorage, raising=True
    )

    mod = object.__new__(IamModule)
    with pytest.raises(RuntimeError, match="stop-here"):
        async with mod.lifespan(object()):
            pass

    assert calls == ["register_identity_provider", "storage_init"], calls
