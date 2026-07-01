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

"""Private items driver — tenant-scoped manual mapping overlay (#1295 slice 3).

The private (per-tenant) ES items driver is the exception to PR #1431's
default cap-safe shared shape: because each tenant gets its own index
with no cross-tenant alias contract, the operator is allowed to pin a
typed manual mapping at the catalog (tenant) tier — declared keys land
at their typed ``properties.<key>`` path; undeclared keys still route
through the cap-safe ``properties.extras`` (``flattened``) +
``_search_text`` lane.

These tests pin:

* the ``mapping`` field + ``_freeze_at = "catalog"`` on the config;
* the validate handler (shape + reserved-root-field rejection);
* the empty-overlay backward-compatibility path (legacy
  ``TENANT_FEATURE_MAPPING``);
* the strict-overlay shape (cap-safe extras lane + root
  ``_search_text``);
* the projection helper (declared/undeclared routing,
  ``_search_text`` population, idempotence).
"""
from __future__ import annotations

import pytest

from dynastore.modules.storage.driver_config import (
    ItemsElasticsearchPrivateDriverConfig,
    _validate_items_es_private_driver_config,
)
from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
    TENANT_FEATURE_MAPPING,
    build_private_item_mapping,
    project_private_doc,
)


# ---------------------------------------------------------------------------
# Config — field + freeze tier
# ---------------------------------------------------------------------------


class TestPrivateConfigShape:
    def test_mapping_field_default_is_empty_dict(self):
        cfg = ItemsElasticsearchPrivateDriverConfig()
        assert cfg.mapping == {}

    def test_mapping_field_accepts_typed_overlay(self):
        overlay = {"my:custom": {"type": "keyword"}}
        cfg = ItemsElasticsearchPrivateDriverConfig(mapping=overlay)
        assert cfg.mapping == overlay

    def test_freeze_at_is_catalog_tier(self):
        # The per-tenant index is shared across all collections of a
        # catalog, so the mapping snapshot is meaningful at the catalog
        # tier (not collection). #1295 locked design.
        assert ItemsElasticsearchPrivateDriverConfig._freeze_at == "catalog"


# ---------------------------------------------------------------------------
# Validate handler
# ---------------------------------------------------------------------------


class TestValidateHandler:
    @pytest.mark.asyncio
    async def test_empty_overlay_accepted(self):
        cfg = ItemsElasticsearchPrivateDriverConfig(mapping={})
        # No raise.
        await _validate_items_es_private_driver_config(cfg, "cat", "col", None)

    @pytest.mark.asyncio
    async def test_typed_overlay_accepted(self):
        cfg = ItemsElasticsearchPrivateDriverConfig(
            mapping={"my:custom": {"type": "keyword"}}
        )
        await _validate_items_es_private_driver_config(cfg, "cat", "col", None)

    @pytest.mark.asyncio
    async def test_entry_without_type_rejected(self):
        cfg = ItemsElasticsearchPrivateDriverConfig(
            mapping={"my:custom": {"analyzer": "standard"}}  # missing "type"
        )
        with pytest.raises(ValueError, match="must be a dict with at least a 'type'"):
            await _validate_items_es_private_driver_config(cfg, "cat", "col", None)

    @pytest.mark.asyncio
    async def test_non_dict_entry_rejected_at_validate(self):
        # Pydantic catches a string-typed value at construction; pass a
        # bypass cfg (raw .__dict__ assignment) to exercise the validator
        # itself.
        cfg = ItemsElasticsearchPrivateDriverConfig()
        # Bypass the model validator by assigning straight to __dict__.
        cfg.__dict__["mapping"] = "not-a-dict"
        with pytest.raises(ValueError, match="must be a dict of"):
            await _validate_items_es_private_driver_config(cfg, "cat", "col", None)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reserved_key",
        ["geoid", "catalog_id", "geometry", "bbox", "properties", "extras"],
    )
    async def test_reserved_root_field_rejected(self, reserved_key):
        cfg = ItemsElasticsearchPrivateDriverConfig(
            mapping={reserved_key: {"type": "keyword"}}
        )
        with pytest.raises(ValueError, match="reserved root field"):
            await _validate_items_es_private_driver_config(cfg, "cat", "col", None)

    @pytest.mark.asyncio
    async def test_handler_skips_wrong_config_type(self):
        # Validator must be a no-op for unrelated configs (handlers are
        # registered as a chain).
        class Other:
            mapping = {"foo": "bar"}  # malformed, but not our type

        await _validate_items_es_private_driver_config(
            Other(),  # type: ignore[arg-type]
            "cat",
            "col",
            None,
        )


# ---------------------------------------------------------------------------
# build_private_item_mapping
# ---------------------------------------------------------------------------


class TestBuildPrivateItemMapping:
    def test_empty_overlay_returns_legacy_shape(self):
        # Backward compatibility: tenants that never set ``mapping`` get
        # the pre-#1295-slice-3 fully-dynamic ``properties`` sub-tree.
        m = build_private_item_mapping({})
        assert m is TENANT_FEATURE_MAPPING
        assert m["properties"]["properties"] == {"type": "object", "dynamic": True}

    def test_none_overlay_returns_legacy_shape(self):
        assert build_private_item_mapping(None) is TENANT_FEATURE_MAPPING

    def test_strict_overlay_pins_root_shape(self):
        m = build_private_item_mapping({"my:custom": {"type": "keyword"}})
        assert m["dynamic"] is False
        # Reserved root fields still present.
        for k in ("geoid", "catalog_id", "geometry", "bbox", "asset_id"):
            assert k in m["properties"]
        # Root analyzed catch-all for the undeclared tail.
        assert m["properties"]["_search_text"] == {
            "type": "text", "analyzer": "standard",
        }

    def test_strict_overlay_pins_properties_subtree(self):
        m = build_private_item_mapping({"my:custom": {"type": "keyword"}})
        props_subtree = m["properties"]["properties"]
        assert props_subtree["dynamic"] is False
        assert props_subtree["properties"]["my:custom"] == {"type": "keyword"}
        # Cap-safe extras lane for undeclared keys.
        assert props_subtree["properties"]["extras"] == {"type": "flattened"}

    def test_strict_overlay_strips_reserved_root_fields_defensively(self):
        # The validator already rejects these, but a bypass shouldn't
        # corrupt the doc-root mapping by injecting under properties.<root>.
        m = build_private_item_mapping({
            "geoid": {"type": "text"},  # reserved root
            "my:custom": {"type": "keyword"},
        })
        props_subtree = m["properties"]["properties"]["properties"]
        assert "geoid" not in props_subtree
        assert props_subtree["my:custom"] == {"type": "keyword"}


# ---------------------------------------------------------------------------
# project_private_doc
# ---------------------------------------------------------------------------


class TestProjectPrivateDoc:
    def test_empty_overlay_passthrough(self):
        # Legacy mode = no projection (fully-dynamic ``properties``).
        doc = {
            "geoid": "g1",
            "properties": {"known": 1, "my:unknown": "x"},
        }
        out = project_private_doc(doc, {})
        assert out == doc

    def test_declared_key_stays_at_typed_path(self):
        known = {"my:declared": {"type": "keyword"}}
        doc = {"properties": {"my:declared": "v"}}
        out = project_private_doc(doc, known)
        assert out["properties"]["my:declared"] == "v"
        assert "extras" not in out["properties"]
        # No tail → no _search_text.
        assert "_search_text" not in out

    def test_undeclared_key_routes_to_extras(self):
        known = {"my:declared": {"type": "keyword"}}
        doc = {"properties": {"my:declared": "v", "my:other": "z"}}
        out = project_private_doc(doc, known)
        assert out["properties"]["my:declared"] == "v"
        assert out["properties"]["extras"] == {"my:other": "z"}

    def test_undeclared_key_populates_search_text(self):
        known = {"my:declared": {"type": "keyword"}}
        doc = {"properties": {"my:declared": "v", "my:other": "z", "more": 42}}
        out = project_private_doc(doc, known)
        assert "z" in out["_search_text"]
        assert "42" in out["_search_text"]

    def test_existing_extras_merged_idempotently(self):
        known = {"my:declared": {"type": "keyword"}}
        doc = {
            "properties": {
                "my:declared": "v",
                "extras": {"prior": "a"},
                "fresh": "b",
            }
        }
        out = project_private_doc(doc, known)
        assert out["properties"]["extras"] == {"prior": "a", "fresh": "b"}

    def test_reserved_member_keys_quarantined_into_extras(self):
        # If a tenant attribute leaks under ``properties`` using a
        # GeoJSON/STAC structural name (``id``, ``geometry``, …), route it
        # into ``properties.extras.<member>`` instead of dropping it — the
        # canonical value still lives at the doc root, but the tenant's
        # colliding attribute is preserved losslessly (#2642).
        known = {"my:declared": {"type": "keyword"}}
        doc = {
            "geoid": "g1",
            "properties": {
                "my:declared": "v",
                "id": "leak",
                "geometry": {"type": "Point", "coordinates": [0, 0]},
                "my:other": "z",
            },
        }
        out = project_private_doc(doc, known)
        assert "id" not in out["properties"]
        assert "geometry" not in out["properties"]
        assert out["properties"]["extras"] == {
            "id": "leak",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "my:other": "z",
        }
        # The doc-root ``geoid`` is untouched — no shadowing occurred.
        assert out["geoid"] == "g1"

    def test_reserved_member_key_round_trips_via_extras(self):
        # Round-trip: ingest a feature carrying ``properties.collection``,
        # assert it lands under ``properties.extras.collection`` and the
        # value is intact (not mangled/stringified) (#2642).
        known = {"my:declared": {"type": "keyword"}}
        doc = {
            "geoid": "g1",
            "properties": {"my:declared": "v", "collection": "external-value"},
        }
        out = project_private_doc(doc, known)
        assert "collection" not in out["properties"]
        assert out["properties"]["extras"]["collection"] == "external-value"
        # Re-projecting the already-projected doc (idempotent — the read
        # path may feed a stored doc back through the same helper) keeps
        # the quarantined value intact.
        out2 = project_private_doc(out, known)
        assert out2["properties"]["extras"]["collection"] == "external-value"

    def test_input_not_mutated(self):
        known = {"my:declared": {"type": "keyword"}}
        doc = {"properties": {"my:declared": "v", "my:other": "z"}}
        snapshot = {"properties": {"my:declared": "v", "my:other": "z"}}
        project_private_doc(doc, known)
        assert doc == snapshot


# ---------------------------------------------------------------------------
# Driver wiring — ensure_storage + write_entities consume the overlay
# ---------------------------------------------------------------------------


class _StubIndices:
    def __init__(self, exists=False):
        self.exists_result = exists
        self.create_calls: list = []

    async def exists(self, *, index, **kwargs):
        return self.exists_result

    async def create(self, *, index, body=None, **kwargs):
        self.create_calls.append({"index": index, "body": body})


class _StubEs:
    def __init__(self, exists=False):
        self.indices = _StubIndices(exists=exists)
        self.bulk_calls: list = []

    async def bulk(self, *, body, params=None, **kwargs):
        self.bulk_calls.append({"body": body})
        return {"items": []}


class TestDriverConsumesOverlay:
    @pytest.mark.asyncio
    async def test_ensure_storage_uses_overlay_when_present(self, monkeypatch):
        from dynastore.modules.storage.drivers.elasticsearch_private import (
            driver as drv_mod,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )

        es = _StubEs(exists=False)

        async def _resolve(catalog_id):
            return {"my:custom": {"type": "keyword"}}

        async def _no_deny(*a, **kw):
            return None

        async def _settings():
            return {"index.mapping.total_fields.limit": 1500}

        monkeypatch.setattr(
            "dynastore.modules.storage.drivers.elasticsearch_private.mappings."
            "resolve_catalog_private_known_fields",
            _resolve,
        )
        monkeypatch.setattr(
            "dynastore.modules.elasticsearch.index_config."
            "get_private_items_index_settings",
            _settings,
        )
        monkeypatch.setattr(
            ItemsElasticsearchPrivateDriver, "_apply_deny_policy", _no_deny,
        )

        driver = ItemsElasticsearchPrivateDriver()
        monkeypatch.setattr(driver, "_get_client", lambda: es)

        await driver.ensure_storage("cat1")

        assert len(es.indices.create_calls) == 1
        body = es.indices.create_calls[0]["body"]
        # Strict shape (overlay non-empty): _search_text on root + extras
        # as flattened.
        assert "_search_text" in body["mappings"]["properties"]
        assert body["mappings"]["properties"]["properties"]["properties"][
            "my:custom"
        ] == {"type": "keyword"}
        assert body["mappings"]["properties"]["properties"]["properties"][
            "extras"
        ] == {"type": "flattened"}
        # And the driver invokes the lookup-side discovery cleanly —
        # nothing else writes to driver-internal state.
        assert drv_mod is not None  # import sanity

    @pytest.mark.asyncio
    async def test_ensure_storage_legacy_shape_when_overlay_empty(self, monkeypatch):
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )

        es = _StubEs(exists=False)

        async def _resolve(catalog_id):
            return {}

        async def _no_deny(*a, **kw):
            return None

        async def _settings():
            return {"index.mapping.total_fields.limit": 1500}

        monkeypatch.setattr(
            "dynastore.modules.storage.drivers.elasticsearch_private.mappings."
            "resolve_catalog_private_known_fields",
            _resolve,
        )
        monkeypatch.setattr(
            "dynastore.modules.elasticsearch.index_config."
            "get_private_items_index_settings",
            _settings,
        )
        monkeypatch.setattr(
            ItemsElasticsearchPrivateDriver, "_apply_deny_policy", _no_deny,
        )

        driver = ItemsElasticsearchPrivateDriver()
        monkeypatch.setattr(driver, "_get_client", lambda: es)

        await driver.ensure_storage("cat1")

        body = es.indices.create_calls[0]["body"]
        # Legacy shape — properties sub-tree fully dynamic.
        # Post-#1800: _search_text IS present in the root (mirrors COMMON_PROPERTIES
        # so project_item_for_es can populate it for extras full-text search).
        assert body["mappings"] is TENANT_FEATURE_MAPPING
        # Verify the legacy-mode properties sub-tree is still dynamic.
        assert body["mappings"]["properties"]["properties"] == {
            "type": "object", "dynamic": True,
        }
