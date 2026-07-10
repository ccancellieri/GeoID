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

"""Access-envelope write stamping in ``ItemService._dispatch_index_upsert`` (#1285).

The standardized row-level-ABAC envelope driver reads typed access fields
(``_visibility`` / ``_owner``) off the index payload. The
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
        async def fan_out_bulk(self, ctx, ops, *, tx_factory=None):
            captured["ops"] = ops
            captured["tx_factory"] = tx_factory

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

    # #2687: visibility resolution now lives in
    # ``dynastore.modules.storage.access_envelope`` (shared with drain-time
    # recompute), which resolves ``get_protocol`` freshly from
    # ``dynastore.tools.discovery`` at call time rather than through
    # ``item_service``'s module-level import.
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", _get_protocol,
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
    # _grant_subjects is never stamped; _attrs absent when no stamping policy.
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
        "dynastore.tools.discovery.get_protocol", _get_protocol,
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
    # _grant_subjects is never stamped on new docs.
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


# ---------------------------------------------------------------------------
# _resolve_write_owner (#2687) — shared by _resolve_access_envelope and the
# unconditional hub ``access_owner`` stamp
# ---------------------------------------------------------------------------


def test_resolve_write_owner_prefers_owner_key():
    assert ItemService._resolve_write_owner(
        {"owner": "alice", "principal_id": "bob", "subject_id": "carol"},
    ) == "alice"


def test_resolve_write_owner_falls_back_to_principal_id():
    assert ItemService._resolve_write_owner(
        {"principal_id": "bob", "subject_id": "carol"},
    ) == "bob"


def test_resolve_write_owner_falls_back_to_subject_id():
    assert ItemService._resolve_write_owner({"subject_id": "carol"}) == "carol"


def test_resolve_write_owner_none_without_principal():
    assert ItemService._resolve_write_owner({}) is None
    assert ItemService._resolve_write_owner(None) is None


# ---------------------------------------------------------------------------
# #3175 — write-time _attrs stamping threads the real per-item feature
#
# Before this fix, ``_resolve_index_stamp_context`` resolved
# ``_resolve_access_envelope`` once per dispatch with no feature, so
# ``_attrs`` fell back to ``processing_context`` (which never carries a
# Feature's "properties") — write-time ``_attrs`` was effectively always
# ``{}``. The fix carries the resolved ``AttributeStampingPolicy`` paths on
# ``_IndexStampContext`` and stamps ``_attrs`` per item, from that item's own
# payload, in ``_apply_index_stamp``.
# ---------------------------------------------------------------------------


def _patch_stamping_policy(monkeypatch, attribute_paths: dict, is_public: bool = False):
    """Patch ``ConfigsProtocol`` so both ``CatalogLookupAudience`` and
    ``AttributeStampingPolicy`` resolve for the access-envelope base."""
    class _Audience:
        def __init__(self, pub):
            self.is_public = pub

    class _Policy:
        def __init__(self, paths):
            self.attribute_paths = paths

    class _Configs:
        async def get_config(self, model, *, catalog_id=None, collection_id=None, **k):
            from dynastore.modules.iam.audience_configs import CatalogLookupAudience
            from dynastore.modules.iam.stamping_config import AttributeStampingPolicy
            if model is CatalogLookupAudience:
                return _Audience(is_public)
            if model is AttributeStampingPolicy:
                return _Policy(attribute_paths)
            return None

    def _get_protocol(proto, *a, **k):
        from dynastore.models.protocols import ConfigsProtocol
        return _Configs() if proto is ConfigsProtocol else None

    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol", _get_protocol,
    )


async def test_dispatch_stamps_attrs_from_each_items_own_feature_properties(monkeypatch):
    """(a) A write to an access-aware collection with a stamping policy
    indexes ``_attrs`` populated from each item's own Feature properties —
    not frozen once for the whole batch."""
    svc = ItemService()
    _wire_write_drivers(monkeypatch, [_StubResolved(_EnvelopeDriver())])
    _patch_stamping_policy(
        monkeypatch, attribute_paths={"dept": "$.properties.department"},
    )
    captured = _capture_dispatcher(monkeypatch)

    async def _no_external_id(catalog_id, collection_id):
        return None

    monkeypatch.setattr(svc, "_resolve_external_id_path", _no_external_id)
    monkeypatch.setattr(svc, "engine", None)

    results = [
        Feature(type="Feature", id="g1", geometry=None, properties={"department": "finance"}),
        Feature(type="Feature", id="g2", geometry=None, properties={"department": "legal"}),
    ]
    await svc._dispatch_index_upsert(
        "c", "col", results, processing_context={"owner": "alice"},
    )

    ops = {op.entity_id: op for op in captured["ops"]}
    assert ops["g1"].payload["_attrs"] == {"dept": "finance"}
    assert ops["g2"].payload["_attrs"] == {"dept": "legal"}


async def test_dispatch_no_attrs_key_without_stamping_policy(monkeypatch):
    """(c) Access-aware collection with no stamping policy (empty paths) →
    behaviour unchanged: no ``_attrs`` key."""
    svc = ItemService()
    _wire_write_drivers(monkeypatch, [_StubResolved(_EnvelopeDriver())])
    _patch_audience(monkeypatch, is_public=False)
    captured = _capture_dispatcher(monkeypatch)

    async def _no_external_id(catalog_id, collection_id):
        return None

    monkeypatch.setattr(svc, "_resolve_external_id_path", _no_external_id)
    monkeypatch.setattr(svc, "engine", None)

    results = [Feature(type="Feature", id="g1", geometry=None, properties={"department": "finance"})]
    await svc._dispatch_index_upsert(
        "c", "col", results, processing_context={"owner": "alice"},
    )

    payload = captured["ops"][0].payload
    assert "_attrs" not in payload


async def test_dispatch_not_access_aware_ignores_stamping_policy(monkeypatch):
    """(d) Non-access-aware collections are completely unaffected, even with
    an ``AttributeStampingPolicy`` configured."""
    svc = ItemService()
    _wire_write_drivers(monkeypatch, [_StubResolved(_PublicDriver())])
    _patch_stamping_policy(
        monkeypatch, attribute_paths={"dept": "$.properties.department"},
    )
    captured = _capture_dispatcher(monkeypatch)

    async def _no_external_id(catalog_id, collection_id):
        return None

    monkeypatch.setattr(svc, "_resolve_external_id_path", _no_external_id)
    monkeypatch.setattr(svc, "engine", None)

    results = [Feature(type="Feature", id="g1", geometry=None, properties={"department": "finance"})]
    await svc._dispatch_index_upsert(
        "c", "col", results, processing_context={"owner": "alice"},
    )

    payload = captured["ops"][0].payload
    assert "_visibility" not in payload
    assert "_owner" not in payload
    assert "_attrs" not in payload


async def test_resolve_access_envelope_base_carries_paths_not_baked_attrs(monkeypatch):
    """``_resolve_access_envelope_base`` resolves ``attrs_paths`` once but never
    bakes a resolved ``_attrs`` value into the batch-level result — there is
    no feature at this point to derive one from."""
    from dynastore.modules.catalog.item_service import _AccessEnvelopeBase

    svc = ItemService()
    _wire_write_drivers(monkeypatch, [_StubResolved(_EnvelopeDriver())])
    _patch_stamping_policy(
        monkeypatch, attribute_paths={"dept": "$.properties.department"},
    )

    base = await svc._resolve_access_envelope_base("c", "col", {"owner": "alice"})
    assert isinstance(base, _AccessEnvelopeBase)
    assert base.visibility == "private"
    assert base.owner == "alice"
    assert base.attrs_paths == {"dept": "$.properties.department"}


async def test_write_and_drain_attrs_parity(monkeypatch):
    """(b) Write-time ``_apply_index_stamp`` and drain-time
    ``_apply_access_envelope`` derive identical ``_attrs`` for the same item's
    properties (#3175 — the write and drain paths must agree)."""
    from dynastore.modules.catalog.canonical_index_read import _apply_access_envelope
    from dynastore.modules.catalog.item_service import _IndexStampContext

    attrs_paths = {"dept": "$.properties.department", "region": "$.properties.region"}
    properties = {"department": "finance", "region": "EU", "irrelevant": "x"}

    # Write-time: the per-item index payload already carries the item's own
    # properties; _apply_index_stamp derives _attrs from it.
    svc = ItemService()
    ctx = _IndexStampContext(
        external_id_path=None,
        asset_id=None,
        access_envelope={"_visibility": "private", "_owner": "alice"},
        access_envelope_attrs_paths=attrs_paths,
    )
    write_payload = svc._apply_index_stamp(
        {"id": "g1", "properties": dict(properties)}, ctx,
    )

    # Drain-time: the storage-plane recompute derives _attrs from the item's
    # stored user_properties using the same declared paths.
    drain_envelope = _apply_access_envelope(
        {"geoid": "g1", "access_owner": "alice"}, properties,
        is_access_aware=True, visibility="private", attrs_paths=attrs_paths,
    )

    assert drain_envelope is not None
    assert write_payload["_attrs"] == drain_envelope["_attrs"]
    assert write_payload["_attrs"] == {"dept": "finance", "region": "EU"}
