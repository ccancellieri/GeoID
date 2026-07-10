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

"""Unit tests for the shared access-envelope helpers (#2687).

Covers ``collection_uses_access_aware_driver`` (both detection branches +
fail-open on routing error) and the two config resolvers
(``resolve_catalog_visibility`` / ``resolve_attribute_stamping_paths``),
including their "raise, don't guess" contract used by the drain's
fail-closed recompute.
"""
from __future__ import annotations

import pytest

from dynastore.modules.storage.access_envelope import (
    collection_uses_access_aware_driver,
    resolve_attribute_stamping_paths,
    resolve_catalog_visibility,
)


class _StubResolved:
    def __init__(self, driver):
        self.driver = driver


class _EnvelopeDriver:
    applies_access_filter = True


class _PlainDriver:
    applies_access_filter = False


def _wire_write_drivers(monkeypatch, resolved):
    async def _get_write_drivers(catalog_id, collection_id):
        return resolved

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_write_drivers",
        _get_write_drivers,
    )


# ---------------------------------------------------------------------------
# collection_uses_access_aware_driver
# ---------------------------------------------------------------------------


async def test_detects_es_envelope_branch(monkeypatch):
    _wire_write_drivers(monkeypatch, [_StubResolved(_EnvelopeDriver())])
    assert await collection_uses_access_aware_driver("c", "col") is True


async def test_false_when_no_access_aware_driver(monkeypatch):
    _wire_write_drivers(monkeypatch, [_StubResolved(_PlainDriver())])
    assert await collection_uses_access_aware_driver("c", "col") is False


async def test_detects_pg_sidecar_branch(monkeypatch):
    from dynastore.modules.storage.drivers.pg_sidecars.access_envelope_config import (
        AccessEnvelopeSidecarConfig,
    )

    class _Config:
        sidecars = [AccessEnvelopeSidecarConfig()]

    class _PgDriver:
        async def get_driver_config(self, catalog_id, collection_id=None, **_kw):
            return _Config()

    _wire_write_drivers(monkeypatch, [_StubResolved(_PgDriver())])
    assert await collection_uses_access_aware_driver("c", "col") is True


async def test_fails_open_on_routing_error(monkeypatch):
    async def _boom(catalog_id, collection_id):
        raise RuntimeError("routing unavailable")

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_write_drivers", _boom,
    )
    assert await collection_uses_access_aware_driver("c", "col") is False


# ---------------------------------------------------------------------------
# resolve_catalog_visibility
# ---------------------------------------------------------------------------


def _patch_configs(monkeypatch, get_config):
    class _Configs:
        async def get_config(self, model, *, catalog_id=None, collection_id=None, **k):
            return await get_config(model, catalog_id, collection_id)

    from dynastore.models.protocols import ConfigsProtocol

    def _get_protocol(proto, *a, **k):
        return _Configs() if proto is ConfigsProtocol else None

    monkeypatch.setattr("dynastore.tools.discovery.get_protocol", _get_protocol)


async def test_resolve_visibility_public(monkeypatch):
    from dynastore.modules.iam.audience_configs import CatalogLookupAudience

    class _Audience:
        is_public = True

    async def _get_config(model, catalog_id, collection_id):
        assert model is CatalogLookupAudience
        return _Audience()

    _patch_configs(monkeypatch, _get_config)
    assert await resolve_catalog_visibility("c") == "public"


async def test_resolve_visibility_private_default(monkeypatch):
    async def _get_config(model, catalog_id, collection_id):
        return None

    _patch_configs(monkeypatch, _get_config)
    assert await resolve_catalog_visibility("c") == "private"


async def test_resolve_visibility_raises_without_configs_protocol(monkeypatch):
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", lambda *a, **k: None,
    )
    with pytest.raises(RuntimeError):
        await resolve_catalog_visibility("c")


async def test_resolve_visibility_propagates_config_error(monkeypatch):
    async def _get_config(model, catalog_id, collection_id):
        raise ValueError("boom")

    _patch_configs(monkeypatch, _get_config)
    with pytest.raises(ValueError):
        await resolve_catalog_visibility("c")


# ---------------------------------------------------------------------------
# resolve_attribute_stamping_paths
# ---------------------------------------------------------------------------


async def test_resolve_attribute_paths_present(monkeypatch):
    from dynastore.modules.iam.stamping_config import AttributeStampingPolicy

    class _Policy:
        attribute_paths = {"dept": "$.properties.department"}

    async def _get_config(model, catalog_id, collection_id):
        assert model is AttributeStampingPolicy
        return _Policy()

    _patch_configs(monkeypatch, _get_config)
    paths = await resolve_attribute_stamping_paths("c", "col")
    assert paths == {"dept": "$.properties.department"}


async def test_resolve_attribute_paths_absent_policy_returns_empty(monkeypatch):
    async def _get_config(model, catalog_id, collection_id):
        return None

    _patch_configs(monkeypatch, _get_config)
    assert await resolve_attribute_stamping_paths("c", "col") == {}


async def test_resolve_attribute_paths_raises_without_configs_protocol(monkeypatch):
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", lambda *a, **k: None,
    )
    with pytest.raises(RuntimeError):
        await resolve_attribute_stamping_paths("c", "col")
