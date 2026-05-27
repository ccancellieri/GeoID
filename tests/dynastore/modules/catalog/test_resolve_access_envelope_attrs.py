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

"""Unit tests for _resolve_access_envelope with attribute stamping (#1441).

Covers:
- Policy present → _attrs populated from feature properties
- Policy absent → no _attrs key
- _grant_subjects no longer in output (retired)
- feature passed through feature kwarg
- Unsupported path format → field silently skipped
"""
from __future__ import annotations

from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules.catalog.item_service import ItemService


class _StubEnvelopeDriver:
    applies_access_filter = True


class _StubResolved:
    def __init__(self, driver):
        self.driver = driver


def _wire_envelope_driver(monkeypatch):
    async def _get_write_drivers(catalog_id, collection_id):
        return [_StubResolved(_StubEnvelopeDriver())]

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_write_drivers",
        _get_write_drivers,
    )


def _patch_audience(monkeypatch, is_public: bool = False):
    class _Audience:
        def __init__(self, pub):
            self.is_public = pub

    class _Configs:
        async def get_config(self, model, *, catalog_id=None, collection_id=None, **k):
            # Return audience for CatalogLookupAudience, None for anything else.
            from dynastore.modules.iam.audience_configs import CatalogLookupAudience
            from dynastore.modules.iam.stamping_config import AttributeStampingPolicy
            if model is CatalogLookupAudience:
                return _Audience(is_public)
            if model is AttributeStampingPolicy:
                return None  # no stamping policy
            return None

    def _get_protocol(proto, *a, **k):
        return _Configs() if proto is ConfigsProtocol else None

    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.get_protocol", _get_protocol,
    )


def _patch_stamping_policy(monkeypatch, attribute_paths: dict, is_public: bool = False):
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
        return _Configs() if proto is ConfigsProtocol else None

    monkeypatch.setattr(
        "dynastore.modules.catalog.item_service.get_protocol", _get_protocol,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_envelope_no_stamping_policy_no_attrs_key(monkeypatch):
    """Without an AttributeStampingPolicy the _attrs key is absent."""
    _wire_envelope_driver(monkeypatch)
    _patch_audience(monkeypatch, is_public=False)

    svc = ItemService()
    env = await svc._resolve_access_envelope("c", "col", {"owner": "alice"})
    assert env is not None
    assert "_attrs" not in env


async def test_envelope_grant_subjects_not_in_output(monkeypatch):
    """_grant_subjects must NOT appear in the envelope (retired by #1441)."""
    _wire_envelope_driver(monkeypatch)
    _patch_audience(monkeypatch, is_public=False)

    svc = ItemService()
    env = await svc._resolve_access_envelope("c", "col", {"owner": "alice"})
    assert env is not None
    assert "_grant_subjects" not in env


async def test_envelope_stamping_policy_populates_attrs(monkeypatch):
    """With a policy, feature properties are lifted into _attrs."""
    _wire_envelope_driver(monkeypatch)
    _patch_stamping_policy(
        monkeypatch,
        attribute_paths={"dept": "$.properties.department"},
    )

    feature = {
        "type": "Feature",
        "id": "g1",
        "properties": {"department": "finance", "irrelevant": "x"},
    }

    svc = ItemService()
    env = await svc._resolve_access_envelope(
        "c", "col", {}, feature=feature,
    )
    assert env is not None
    assert "_attrs" in env
    assert env["_attrs"]["dept"] == "finance"
    assert "irrelevant" not in env["_attrs"]


async def test_envelope_stamping_multiple_paths(monkeypatch):
    """Multiple paths → multiple keys in _attrs."""
    _wire_envelope_driver(monkeypatch)
    _patch_stamping_policy(
        monkeypatch,
        attribute_paths={
            "dept": "$.properties.department",
            "region": "$.properties.region",
        },
    )

    feature = {
        "properties": {"department": "legal", "region": "EU"},
    }

    svc = ItemService()
    env = await svc._resolve_access_envelope("c", "col", {}, feature=feature)
    assert env is not None
    assert env["_attrs"] == {"dept": "legal", "region": "EU"}


async def test_envelope_stamping_missing_property_skipped(monkeypatch):
    """A declared path whose property is absent in the Feature is skipped."""
    _wire_envelope_driver(monkeypatch)
    _patch_stamping_policy(
        monkeypatch,
        attribute_paths={
            "dept": "$.properties.department",
            "region": "$.properties.region",
        },
    )

    feature = {"properties": {"department": "hr"}}  # region absent

    svc = ItemService()
    env = await svc._resolve_access_envelope("c", "col", {}, feature=feature)
    assert env is not None
    assert env["_attrs"] == {"dept": "hr"}


async def test_envelope_stamping_unsupported_path_format_skipped(monkeypatch):
    """A path not starting with $.properties. is silently skipped."""
    _wire_envelope_driver(monkeypatch)
    _patch_stamping_policy(
        monkeypatch,
        attribute_paths={
            "dept": "$.metadata.department",  # unsupported path
            "region": "$.properties.region",  # supported
        },
    )

    feature = {"properties": {"region": "US"}}

    svc = ItemService()
    env = await svc._resolve_access_envelope("c", "col", {}, feature=feature)
    assert env is not None
    assert "_attrs" in env
    assert "dept" not in env["_attrs"]
    assert env["_attrs"]["region"] == "US"


async def test_envelope_empty_attribute_paths_no_attrs_key(monkeypatch):
    """AttributeStampingPolicy with empty paths dict → no _attrs key."""
    _wire_envelope_driver(monkeypatch)
    _patch_stamping_policy(monkeypatch, attribute_paths={})

    svc = ItemService()
    env = await svc._resolve_access_envelope("c", "col", {"owner": "alice"})
    assert env is not None
    assert "_attrs" not in env
