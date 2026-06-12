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

"""Behavioural tests for ``IamModule._register_identity_provider`` (geoid#1500).

The IdP factory is config-first (``IdpConfig`` via ``PlatformConfigsProtocol``):

- when ``IdpConfig`` selects an implemented + addressed backend, the provider
  is registered from the config;
- when the config does not select one (no row → zero-arg default, or a row
  with ``issuer_url`` unset), no provider is registered;
- ``type=saml2`` registers nothing (reserved placeholder).

``IamModule._register_identity_provider`` only touches module-level
``get_protocol`` / ``register_plugin`` (not instance state), so we exercise it
on a bare ``object.__new__(IamModule)`` with those two names monkeypatched.
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
    assert "saml2" in caplog.text
