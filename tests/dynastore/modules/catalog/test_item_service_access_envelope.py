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

"""Access-envelope write stamping in ``ItemService._dispatch_index_upsert`` (#1285).

The standardized row-level-ABAC envelope driver reads typed access fields
(``_visibility`` / ``_owner`` / ``_grant_subjects``) off the index payload. The
dispatcher stamps them — but ONLY when the collection routes WRITE to an
access-aware driver (``applies_access_filter=True``). For every other collection
the payload is unchanged, so existing stored docs (public / private indexes) are
byte-for-byte what they were before.

These are pure-unit tests: stub resolved drivers, a captured dispatcher, and no
DB.
"""
from __future__ import annotations

from dynastore.models.ogc import Feature
from dynastore.modules.catalog.item_service import ItemService


class _StubResolved:
    def __init__(self, driver):
        self.driver = driver


class _PublicDriver:
    applies_access_filter = False


class _EnvelopeDriver:
    applies_access_filter = True


def _wire_write_drivers(monkeypatch, resolved):
    async def _get_write_drivers(catalog_id, collection_id):
        return resolved

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_write_drivers",
        _get_write_drivers,
    )


def _capture_dispatcher(monkeypatch):
    captured: dict = {}

    class _Dispatcher:
        async def fan_out_bulk(self, ctx, ops):
            captured["ops"] = ops

    monkeypatch.setattr(
        "dynastore.modules.storage.index_dispatcher.get_index_dispatcher",
        lambda: _Dispatcher(),
    )
    return captured


# ---------------------------------------------------------------------------
# _collection_uses_access_aware_driver
# ---------------------------------------------------------------------------

async def test_detects_access_aware_driver(monkeypatch):
    svc = ItemService()
    _wire_write_drivers(
        monkeypatch, [_StubResolved(_PublicDriver()), _StubResolved(_EnvelopeDriver())],
    )
    assert await svc._collection_uses_access_aware_driver("c", "col") is True


async def test_no_access_aware_driver(monkeypatch):
    svc = ItemService()
    _wire_write_drivers(monkeypatch, [_StubResolved(_PublicDriver())])
    assert await svc._collection_uses_access_aware_driver("c", "col") is False


async def test_access_aware_detection_fails_closed_on_error(monkeypatch):
    svc = ItemService()

    async def _boom(catalog_id, collection_id):
        raise RuntimeError("routing unavailable")

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_write_drivers", _boom,
    )
    assert await svc._collection_uses_access_aware_driver("c", "col") is False


# ---------------------------------------------------------------------------
# _resolve_access_envelope — gating + value sourcing
# ---------------------------------------------------------------------------

def _patch_audience(monkeypatch, is_public):
    """Patch ConfigsProtocol so CatalogLookupAudience.is_public resolves."""
    class _Audience:
        def __init__(self, pub):
            self.is_public = pub

    class _Configs:
        async def get_config(self, model, *, catalog_id=None, collection_id=None, **k):
            return _Audience(is_public)

    from dynastore.models.protocols import ConfigsProtocol

    def _get_protocol(proto, *a, **k):
        return _Configs() if proto is ConfigsProtocol else None

    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.get_protocol", _get_protocol,
    )


async def test_envelope_none_when_not_access_aware(monkeypatch):
    svc = ItemService()
    _wire_write_drivers(monkeypatch, [_StubResolved(_PublicDriver())])
    env = await svc._resolve_access_envelope("c", "col", {"owner": "alice"})
    assert env is None


async def test_envelope_visibility_public(monkeypatch):
    svc = ItemService()
    _wire_write_drivers(monkeypatch, [_StubResolved(_EnvelopeDriver())])
    _patch_audience(monkeypatch, is_public=True)
    env = await svc._resolve_access_envelope("c", "col", {"owner": "alice"})
    assert env is not None
    assert env["_visibility"] == "public"
    assert env["_owner"] == "alice"
    # _grant_subjects retired by #1441; _attrs absent when no stamping policy.
    assert "_grant_subjects" not in env


async def test_envelope_visibility_private(monkeypatch):
    svc = ItemService()
    _wire_write_drivers(monkeypatch, [_StubResolved(_EnvelopeDriver())])
    _patch_audience(monkeypatch, is_public=False)
    env = await svc._resolve_access_envelope("c", "col", None)
    assert env is not None
    assert env["_visibility"] == "private"
    assert env["_owner"] is None  # no principal in context


async def test_envelope_visibility_defaults_private_without_audience(monkeypatch):
    """No audience config available → closed default for the isolated index."""
    svc = ItemService()
    _wire_write_drivers(monkeypatch, [_StubResolved(_EnvelopeDriver())])

    def _get_protocol(proto, *a, **k):
        return None  # no ConfigsProtocol registered

    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.get_protocol", _get_protocol,
    )
    env = await svc._resolve_access_envelope("c", "col", {"principal_id": "bob"})
    assert env is not None
    assert env["_visibility"] == "private"
    assert env["_owner"] == "bob"  # sourced from principal_id


# ---------------------------------------------------------------------------
# End-to-end stamping via _dispatch_index_upsert
# ---------------------------------------------------------------------------

async def test_dispatch_stamps_access_fields_for_envelope_target(monkeypatch):
    svc = ItemService()
    _wire_write_drivers(monkeypatch, [_StubResolved(_EnvelopeDriver())])
    _patch_audience(monkeypatch, is_public=False)
    captured = _capture_dispatcher(monkeypatch)

    async def _no_external_id(catalog_id, collection_id):
        return None

    monkeypatch.setattr(svc, "_resolve_external_id_path", _no_external_id)
    # No engine → non-atomic enqueue path (no DB).
    monkeypatch.setattr(svc, "engine", None)

    results = [Feature(type="Feature", id="g1", geometry=None, properties={})]
    await svc._dispatch_index_upsert(
        "c", "col", results, processing_context={"owner": "alice"},
    )

    ops = captured["ops"]
    assert len(ops) == 1
    payload = ops[0].payload
    assert payload["_visibility"] == "private"
    assert payload["_owner"] == "alice"
    # _grant_subjects retired by #1441; not stamped on new docs.
    assert "_grant_subjects" not in payload


async def test_dispatch_does_not_stamp_for_public_target(monkeypatch):
    svc = ItemService()
    _wire_write_drivers(monkeypatch, [_StubResolved(_PublicDriver())])
    captured = _capture_dispatcher(monkeypatch)

    async def _no_external_id(catalog_id, collection_id):
        return None

    monkeypatch.setattr(svc, "_resolve_external_id_path", _no_external_id)
    monkeypatch.setattr(svc, "engine", None)

    results = [Feature(type="Feature", id="g1", geometry=None, properties={})]
    await svc._dispatch_index_upsert(
        "c", "col", results, processing_context={"owner": "alice"},
    )

    payload = captured["ops"][0].payload
    assert "_visibility" not in payload
    assert "_owner" not in payload
    assert "_grant_subjects" not in payload
