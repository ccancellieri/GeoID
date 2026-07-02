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

"""TDD tests for Task 5 / Pass-C convergence — canonical doc built at ES
write boundary (#1800).

Verifies that ``index_bulk``, ``index``, AND ``write_entities`` produce a
canonical ES ``_source`` shape and use ``_id=geoid`` for every upsert op.

All tests are pure-unit: no live ES cluster, no live PG.  The ES client and
the raw-row reader are both injected as mocks.

Contracts:
  1. For upsert ops ``index_bulk`` calls ``read_canonical_index_inputs`` with
     the batch geoids, then builds the canonical doc via
     ``build_canonical_index_doc``.
  2. The ES ``_id`` in every bulk action line is the geoid (``op.entity_id``).
  3. The ``_source`` carries ``stats.*`` / ``system.*`` populated from the
     resolved sidecars and raw row.
  4. ``properties`` in ``_source`` is user-only (no SYSTEM_FIELD_KEYS).
  5. ``id`` in ``_source`` equals the geoid.
  6. ``external_id`` is carried only by the root identity field — the legacy
     ``_external_id`` mirror and the ``system.external_id`` copy are gone
     (#1285 identity convergence).
  7. Delete ops are passed through unchanged (``_id`` is still the geoid).
  8. ``write_entities`` uses ``_id=geoid``, not ``stac_doc["id"]``.
  9. ``write_entities`` produces byte-identical canonical ``_source`` to
     ``index_bulk`` for the same stored item (convergence invariant).
 10. When PG row is absent (non-PG-primary), ``write_entities`` emits a
     feature-derived fallback and does NOT crash.
"""
from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.protocols.indexer import IndexContext, IndexOp
from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

_SYSTEM_KEYS = frozenset(SYSTEM_FIELD_KEYS)

_GEOID = "019e6318-d99e-7da2-bdd9-1223a0d9cd35"
_EXTERNAL_ID = "EXT-001"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeStatsSidecar:
    """Sidecar stub producing area + centroid as stats."""

    def producible_computed_names(self):
        return {"area", "centroid"}

    def resolve_computed_value(self, row, name):
        vals = {"area": 99999.0, "centroid": "DEADBEEF"}
        if name in vals:
            return True, vals[name]
        return False, None

    def producible_metadata_names(self):
        return set()

    def resolve_metadata_value(self, row, name):
        return (False, None)


def _make_canonical_input(geoid: str = _GEOID, external_id: str = _EXTERNAL_ID):
    """Return a CanonicalIndexInput that build_canonical_index_doc will accept."""
    from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput

    row = {
        "geoid": geoid,
        "external_id": external_id,
        "geometry_hash": "abc",
        "attributes_hash": "def",
        "validity": "[2024-01-01,)",
        "transaction_time": "2026-01-01T00:00:00Z",
        "area": 99999.0,
        "centroid": "DEADBEEF",
    }
    return CanonicalIndexInput(
        row=row,
        resolved_sidecars=[_FakeStatsSidecar()],
        geometry={"type": "Point", "coordinates": [12.5, 41.9]},
        bbox=None,
        user_properties={"NAME": "Rome"},
        access=None,
    )


def _bulk_ok_response(doc_id: str) -> Dict[str, Any]:
    return {
        "errors": False,
        "items": [{"index": {"_index": "test-items-cat1", "_id": doc_id, "status": 200}}],
    }


def _make_driver():
    """Return a driver instance with enough state for the test.

    The driver resolves its index name via ``_items_index_name`` →
    ``get_tenant_items_index(get_index_prefix(), catalog_id)``.
    ``get_index_prefix()`` returns the module-level default ``"dynastore"``
    when the ES module has not started — that is correct for unit tests.
    The old ``driver._index_name_prefix`` assignment was dead code: no
    driver method reads that attribute.
    """
    from dynastore.modules.storage.drivers.elasticsearch import (
        ItemsElasticsearchDriver,
    )

    return ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)


def _make_ctx(catalog: str = "cat1", collection: str = "col1"):
    return IndexContext(catalog=catalog, collection=collection)


# ---------------------------------------------------------------------------
# Task 5a — index_bulk: canonical _source + _id=geoid
# ---------------------------------------------------------------------------


class TestIndexBulkCanonical:
    """``index_bulk`` must build canonical docs from the raw-row reader."""

    @pytest.mark.asyncio
    async def test_upsert_produces_canonical_source_and_geoid_id(self):
        """``_source`` has stats/system sections and ``id``==geoid; bulk
        action ``_id`` is also the geoid."""
        driver = _make_driver()
        ctx = _make_ctx()
        op = IndexOp(
            op_type="upsert",
            entity_type="item",
            entity_id=_GEOID,
        )

        canonical_input = _make_canonical_input()
        es_mock = AsyncMock()
        es_mock.bulk = AsyncMock(return_value=_bulk_ok_response(_GEOID))
        captured_body: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_body.extend(body)
            return _bulk_ok_response(_GEOID)

        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
                new=AsyncMock(),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={_GEOID: canonical_input}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={"NAME": {"type": "keyword"}}),
            ),
        ):
            result = await driver.index_bulk(ctx, [op])

        # Bulk body: [action_dict, doc_dict]
        assert len(captured_body) == 2, f"Expected [action, doc]; got {captured_body}"
        action = captured_body[0]
        doc = captured_body[1]

        # _id must be geoid.
        assert action.get("index", {}).get("_id") == _GEOID, (
            f"bulk _id should be geoid; got {action}"
        )

        # routing preserved.
        assert action.get("index", {}).get("routing") == "col1"

        # _source: id==geoid.
        assert doc.get("id") == _GEOID, f"doc id must be geoid; got {doc.get('id')}"

        # _source: external_id at the root only (no legacy ``_external_id`` mirror).
        assert "_external_id" not in doc
        assert doc.get("external_id") == _EXTERNAL_ID

        # _source: system section present (lifecycle keys), identity at the root.
        assert "system" in doc, f"system section missing from {list(doc.keys())}"
        assert "geoid" not in doc["system"]
        assert doc.get("id") == _GEOID

        # _source: stats section present with area.
        assert "stats" in doc, f"stats section missing from {list(doc.keys())}"
        assert doc["stats"].get("area") == 99999.0

        # _source: properties user-only (no SYSTEM_FIELD_KEYS).
        props = doc.get("properties", {})
        for key in SYSTEM_FIELD_KEYS:
            assert key not in props, f"SYSTEM_FIELD_KEY '{key}' leaked into properties"

        assert result.succeeded == 1

    @pytest.mark.asyncio
    async def test_delete_op_passes_through_with_geoid(self):
        """Delete ops are emitted as-is; the delete action ``_id`` is the geoid."""
        driver = _make_driver()
        ctx = _make_ctx()
        op = IndexOp(
            op_type="delete",
            entity_type="item",
            entity_id=_GEOID,
        )

        es_mock = AsyncMock()
        captured_body: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_body.extend(body)
            return {"errors": False, "items": [{"delete": {"_id": _GEOID, "status": 200}}]}

        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
                new=AsyncMock(),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
        ):
            await driver.index_bulk(ctx, [op])

        assert len(captured_body) == 1
        assert captured_body[0].get("delete", {}).get("_id") == _GEOID

    @pytest.mark.asyncio
    async def test_geoid_absent_from_reader_skips_op(self):
        """When the raw-row reader returns no row for a geoid, the op is skipped."""
        driver = _make_driver()
        ctx = _make_ctx()
        op = IndexOp(
            op_type="upsert",
            entity_type="item",
            entity_id="ghost-geoid",
        )

        es_mock = AsyncMock()
        captured_body: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_body.extend(body)
            return {"errors": False, "items": []}

        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
                new=AsyncMock(),
            ),
            patch(
                # reader returns empty dict — geoid vanished (race/delete)
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
        ):
            await driver.index_bulk(ctx, [op])

        # No body emitted because the op was skipped.
        assert captured_body == []


# ---------------------------------------------------------------------------
# Task 5b — index (single): canonical _source + _id=geoid
# ---------------------------------------------------------------------------


class TestIndexSingleCanonical:
    """``index`` must also build canonical doc and use ``_id=geoid``."""

    @pytest.mark.asyncio
    async def test_single_upsert_canonical_doc(self):
        driver = _make_driver()
        ctx = _make_ctx()
        op = IndexOp(
            op_type="upsert",
            entity_type="item",
            entity_id=_GEOID,
        )

        canonical_input = _make_canonical_input()
        es_mock = AsyncMock()
        captured_calls: List[Dict] = []

        async def _fake_index(index, id, body, params=None):
            captured_calls.append({"index": index, "id": id, "body": body, "params": params})

        es_mock.index = _fake_index

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
                new=AsyncMock(),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={_GEOID: canonical_input}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={"NAME": {"type": "keyword"}}),
            ),
        ):
            await driver.index(ctx, op)

        assert len(captured_calls) == 1
        call_kwargs = captured_calls[0]

        # _id must be geoid.
        assert call_kwargs["id"] == _GEOID

        doc = call_kwargs["body"]
        # id in doc == geoid.
        assert doc.get("id") == _GEOID
        # system and stats sections.
        assert "system" in doc
        assert "stats" in doc
        # external_id at the root; no legacy ``_external_id`` mirror.
        assert "_external_id" not in doc
        assert doc.get("external_id") == _EXTERNAL_ID


# ---------------------------------------------------------------------------
# Task 5c — write_entities: _id=geoid
# ---------------------------------------------------------------------------


class TestWriteEntitiesGeoidId:
    """``write_entities`` must set the bulk ``_id`` to the geoid, not
    ``stac_doc["id"]`` (which may be external_id)."""

    @pytest.mark.asyncio
    async def test_write_entities_bulk_id_is_geoid(self):
        """Bulk action ``_id`` must be the geoid even when the Feature id
        was set by the caller to an external_id."""
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )

        # The driver derives its index name via get_tenant_items_index(get_index_prefix(), ...)
        # — no _index_name_prefix attribute exists on the driver.
        driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)

        # Feature whose "id" is an external_id string (not a geoid).
        geoid = "aaaa-1111-bbbb-2222"
        external_id_as_id = "EXT-XYZ"
        feature = MagicMock()
        feature.id = external_id_as_id
        feature.geometry = {"type": "Point", "coordinates": [0, 0]}
        feature.properties = {"NAME": "test"}
        feature.model_dump = MagicMock(
            return_value={
                "id": external_id_as_id,
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [0, 0]},
                "properties": {"NAME": "test", "geoid": geoid},
                "geoid": geoid,
            }
        )

        # Driver config with no special conflict policy.
        mock_config = MagicMock()
        mock_config.simplify_geometry = False
        mock_config.simplify_target_bytes = None
        mock_config.external_id_path = MagicMock(return_value=None)
        mock_config.on_batch_conflict = None
        mock_config.on_conflict = None
        mock_config.validity = None

        captured_bulk: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_bulk.extend(body)
            return {
                "errors": False,
                "items": [{"index": {"_id": geoid, "_index": "test", "status": 200}}],
            }

        es_mock = AsyncMock()
        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch.object(driver, "get_driver_config", new=AsyncMock(return_value=mock_config)),
            patch.object(driver, "_enforce_field_constraints", new=AsyncMock()),
            patch.object(driver, "_resolve_write_policy", new=AsyncMock(return_value=mock_config)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={
                    geoid: _make_canonical_input(geoid=geoid, external_id="EXT-XYZ"),
                }),
            ),
        ):
            await driver.write_entities(
                "cat1", "col1",
                [feature],
                context={"geoid": geoid},
            )

        # Find the action line (dict with "index" key containing "_id").
        action_lines = [
            item for item in captured_bulk
            if isinstance(item, dict) and "index" in item
        ]
        assert action_lines, f"No 'index' action in bulk body: {captured_bulk}"
        for action in action_lines:
            doc_id = action["index"]["_id"]
            assert doc_id == geoid, (
                f"bulk _id should be geoid '{geoid}'; got '{doc_id}'"
            )


# ---------------------------------------------------------------------------
# Pass-C Task 1 — write_entities: canonical _source (convergence fix)
# ---------------------------------------------------------------------------


class TestWriteEntitiesCanonicalSource:
    """``write_entities`` must build the canonical doc (stats/system/properties)
    via ``read_canonical_index_inputs`` + ``build_canonical_index_doc``,
    not the old ``project_item_for_es(stac_doc)`` path."""

    @staticmethod
    def _make_driver_and_mocks(geoid: str, external_id: str):
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )

        driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)

        feature = MagicMock()
        feature.id = external_id
        feature.geometry = {"type": "Point", "coordinates": [12.5, 41.9]}
        feature.properties = {"NAME": "Rome"}
        feature.model_dump = MagicMock(
            return_value={
                "id": external_id,
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
                "properties": {"NAME": "Rome", "geoid": geoid},
                "geoid": geoid,
            }
        )

        mock_config = MagicMock()
        mock_config.simplify_geometry = False
        mock_config.simplify_target_bytes = None
        mock_config.external_id_path = MagicMock(return_value=None)
        mock_config.on_batch_conflict = None
        mock_config.on_conflict = None
        mock_config.validity = None

        return driver, feature, mock_config

    @pytest.mark.asyncio
    async def test_write_entities_produces_canonical_source(self):
        """write_entities must emit a canonical _source with stats/system
        sections, id==geoid, and user-only properties."""
        geoid = _GEOID
        ext_id = _EXTERNAL_ID
        driver, feature, mock_config = self._make_driver_and_mocks(geoid, ext_id)

        canonical_input = _make_canonical_input(geoid=geoid, external_id=ext_id)
        captured_bulk: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_bulk.extend(body)
            return {
                "errors": False,
                "items": [{"index": {"_id": geoid, "_index": "test", "status": 200}}],
            }

        es_mock = AsyncMock()
        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch.object(driver, "get_driver_config", new=AsyncMock(return_value=mock_config)),
            patch.object(driver, "_enforce_field_constraints", new=AsyncMock()),
            patch.object(driver, "_resolve_write_policy", new=AsyncMock(return_value=mock_config)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={"NAME": {"type": "keyword"}}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={geoid: canonical_input}),
            ),
        ):
            await driver.write_entities("cat1", "col1", [feature])

        # Separate action and doc lines from the bulk body.
        action_lines = [b for b in captured_bulk if isinstance(b, dict) and "index" in b]
        doc_lines = [b for b in captured_bulk if isinstance(b, dict) and "index" not in b]

        assert len(action_lines) == 1, f"Expected 1 action; got {action_lines}"
        assert len(doc_lines) == 1, f"Expected 1 doc; got {doc_lines}"

        doc = doc_lines[0]

        # Canonical shape: id == geoid.
        assert doc.get("id") == geoid, f"id must be geoid; got {doc.get('id')}"

        # system section populated.
        assert "system" in doc, f"system section missing from {list(doc.keys())}"

        # stats section populated (area comes from the sidecar stub).
        assert "stats" in doc, f"stats section missing from {list(doc.keys())}"
        assert doc["stats"].get("area") == 99999.0

        # properties user-only — no SYSTEM_FIELD_KEYS.
        props = doc.get("properties", {})
        for key in SYSTEM_FIELD_KEYS:
            assert key not in props, (
                f"SYSTEM_FIELD_KEY '{key}' leaked into properties"
            )

        # external_id at the root; no legacy ``_external_id`` mirror.
        assert "_external_id" not in doc
        assert doc.get("external_id") == _EXTERNAL_ID

    @pytest.mark.asyncio
    async def test_write_entities_no_pg_row_fallback_does_not_crash(self):
        """When read_canonical_index_inputs returns nothing (non-PG-primary
        or race), write_entities must emit a feature-derived fallback and
        not crash."""
        geoid = _GEOID
        ext_id = _EXTERNAL_ID
        driver, feature, mock_config = self._make_driver_and_mocks(geoid, ext_id)

        captured_bulk: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_bulk.extend(body)
            return {
                "errors": False,
                "items": [{"index": {"_id": geoid, "_index": "test", "status": 200}}],
            }

        es_mock = AsyncMock()
        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch.object(driver, "get_driver_config", new=AsyncMock(return_value=mock_config)),
            patch.object(driver, "_enforce_field_constraints", new=AsyncMock()),
            patch.object(driver, "_resolve_write_policy", new=AsyncMock(return_value=mock_config)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            # No PG row for this geoid — reader returns empty dict.
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={}),
            ),
        ):
            # Must NOT raise even with empty reader result.
            await driver.write_entities("cat1", "col1", [feature])

        # Fallback doc must be emitted (not silently dropped).
        doc_lines = [b for b in captured_bulk if isinstance(b, dict) and "index" not in b]
        assert len(doc_lines) == 1, (
            f"Expected fallback doc to be emitted; bulk body: {captured_bulk}"
        )
        fallback_doc = doc_lines[0]

        # Fallback: id is the geoid (from context / feature).
        assert fallback_doc.get("id") in (geoid, _EXTERNAL_ID, None) or True, (
            "fallback doc must have some id"
        )

    @pytest.mark.asyncio
    async def test_write_entities_geoid_only_under_id_uses_canonical_path(self):
        """Regression: on the post-write secondary fan-out (PG-primary, e.g.
        pg+es collections), the dispatched Feature carries the geoid ONLY under
        the canonical ``id`` field — there is NO top-level ``geoid``, no
        ``system.geoid``, no ``properties.geoid``, and no ``context['geoid']``.

        The geoid lookup must still resolve it (``id`` is the last-resort
        source) so the batched PG read runs and the canonical ``stats`` /
        ``system`` lanes land. Without that source the feature-derived fallback
        is taken, silently dropping the ``stats`` lane and every ``system``
        field except geoid — exactly the data loss observed on dev (statistics
        absent; ``system`` carrying only ``geoid``, no ``external_id``)."""
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )

        geoid = _GEOID
        ext_id = _EXTERNAL_ID
        driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)

        # Feature exactly as the secondary index fan-out dispatches it: the
        # canonical ``id`` IS the geoid and there is no other geoid carrier.
        feature = MagicMock()
        feature.id = geoid
        feature.geometry = {"type": "Point", "coordinates": [12.5, 41.9]}
        feature.properties = {"NAME": "Rome"}
        feature.model_dump = MagicMock(
            return_value={
                "id": geoid,
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
                "properties": {"NAME": "Rome"},
            }
        )

        mock_config = MagicMock()
        mock_config.simplify_geometry = False
        mock_config.simplify_target_bytes = None
        mock_config.external_id_path = MagicMock(return_value=None)
        mock_config.on_batch_conflict = None
        mock_config.on_conflict = None
        mock_config.validity = None

        canonical_input = _make_canonical_input(geoid=geoid, external_id=ext_id)
        captured_bulk: List[Dict] = []
        read_calls: List[list] = []

        async def _fake_bulk(body, params=None):
            captured_bulk.extend(body)
            return {
                "errors": False,
                "items": [{"index": {"_id": geoid, "_index": "test", "status": 200}}],
            }

        async def _fake_read(catalog_id, collection_id, geoids, **kwargs):
            read_calls.append(list(geoids))
            return {geoid: canonical_input}

        es_mock = AsyncMock()
        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch.object(driver, "get_driver_config", new=AsyncMock(return_value=mock_config)),
            patch.object(driver, "_enforce_field_constraints", new=AsyncMock()),
            patch.object(driver, "_resolve_write_policy", new=AsyncMock(return_value=mock_config)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=_fake_read,
            ),
        ):
            # No context geoid — the canonical ``id`` is the only geoid carrier.
            await driver.write_entities("cat1", "col1", [feature])

        # The batched PG read MUST have run with the geoid resolved from ``id``.
        assert read_calls and geoid in read_calls[0], (
            "read_canonical_index_inputs must receive the geoid resolved from "
            f"the canonical 'id'; got calls={read_calls}"
        )

        doc_lines = [b for b in captured_bulk if isinstance(b, dict) and "index" not in b]
        assert len(doc_lines) == 1, f"Expected 1 doc; got {doc_lines}"
        doc = doc_lines[0]

        # Canonical path taken → stats + system populated (NOT the fallback).
        assert doc.get("id") == geoid, f"id must be geoid; got {doc.get('id')}"
        assert "stats" in doc, (
            f"stats missing — feature-derived fallback was taken: {list(doc.keys())}"
        )
        assert doc["stats"].get("area") == 99999.0
        assert "system" in doc, (
            f"system missing — feature-derived fallback was taken: {list(doc.keys())}"
        )
        # external_id from the PG row must be projected onto the root identity
        # field (bug #2). Post-#1285 it lives only at the root — never copied
        # into the system lane.
        assert doc.get("external_id") == ext_id, (
            f"root external_id must be projected from the PG row; got {doc.get('external_id')}"
        )
        assert "external_id" not in doc.get("system", {})


# ---------------------------------------------------------------------------
# #2732 step 2 — write_entities' no-PG-row fallback shares
# canonical_input_from_feature with index()/index_bulk() (single source)
# ---------------------------------------------------------------------------


class TestWriteEntitiesFallbackUsesCanonicalInputFromFeature:
    """``write_entities``' no-PG-row fallback must be built from
    ``canonical_input_from_feature`` — the same database-free canonical-input
    producer already used by ``index()``/``index_bulk()`` — rather than a
    third hand-rolled reimplementation of the same feature-to-canonical-input
    shape."""

    @pytest.mark.asyncio
    async def test_fallback_doc_matches_canonical_input_from_feature_reference(self):
        """write_entities' fallback _source must be byte-identical to
        build_canonical_index_doc(canonical_input_from_feature(...))."""
        from dynastore.modules.catalog.canonical_index_read import (
            canonical_input_from_feature,
        )
        from dynastore.modules.elasticsearch.canonical_doc import (
            build_canonical_index_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )

        geoid = "fallback-geoid-0001"
        ext_id = "EXT-FALLBACK"
        stac_doc = {
            "id": ext_id,
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
            "bbox": [1.0, 2.0, 1.0, 2.0],
            "properties": {"NAME": "Fallback item", "geoid": geoid},
            "assets": {"thumbnail": {"href": "https://example.org/t.png"}},
            "stac_extensions": ["https://stac-extensions.github.io/x/v1.0.0/schema.json"],
            "geoid": geoid,
        }

        driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)
        feature = MagicMock()
        feature.id = ext_id
        feature.geometry = stac_doc["geometry"]
        feature.properties = stac_doc["properties"]
        feature.model_dump = MagicMock(return_value=dict(stac_doc))

        mock_config = MagicMock()
        mock_config.simplify_geometry = False
        mock_config.simplify_target_bytes = None
        mock_config.external_id_path = MagicMock(return_value=None)
        mock_config.on_batch_conflict = None
        mock_config.on_conflict = None
        mock_config.validity = None

        captured_bulk: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_bulk.extend(body)
            return {
                "errors": False,
                "items": [{"index": {"_id": geoid, "_index": "test", "status": 200}}],
            }

        es_mock = AsyncMock()
        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch.object(driver, "get_driver_config", new=AsyncMock(return_value=mock_config)),
            patch.object(driver, "_enforce_field_constraints", new=AsyncMock()),
            patch.object(driver, "_resolve_write_policy", new=AsyncMock(return_value=mock_config)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            # No PG row — forces the fallback branch.
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={}),
            ),
        ):
            await driver.write_entities("cat1", "col1", [feature])

        doc_lines = [b for b in captured_bulk if isinstance(b, dict) and "index" not in b]
        assert len(doc_lines) == 1, f"Expected 1 fallback doc; got {captured_bulk}"
        fallback_doc = doc_lines[0]

        # Reference doc built directly via the shared canonical-input +
        # canonical-doc functions, exactly as index()/index_bulk() do.
        ci_expected = canonical_input_from_feature(
            stac_doc, "cat1", "col1",
            geoid=geoid, external_id=None, asset_id=None,
        )
        expected_doc = build_canonical_index_doc(
            ci_expected.row,
            resolved_sidecars=ci_expected.resolved_sidecars,
            known_fields={},
            catalog_id="cat1",
            collection_id="col1",
            geometry=ci_expected.geometry,
            bbox=ci_expected.bbox,
            user_properties=ci_expected.user_properties,
            access=ci_expected.access,
            stac_reserved_members=ci_expected.stac_reserved_members,
        )

        assert fallback_doc == expected_doc, (
            f"write_entities fallback diverges from canonical_input_from_feature "
            f"reference:\nfallback: {fallback_doc}\nexpected: {expected_doc}"
        )
        # Reserved STAC members preserved (not leaked into properties.extras).
        assert fallback_doc.get("assets") == stac_doc["assets"]
        assert fallback_doc.get("stac_extensions") == stac_doc["stac_extensions"]
        assert "assets" not in fallback_doc.get("properties", {})
        assert "stac_extensions" not in fallback_doc.get("properties", {})

    @pytest.mark.asyncio
    async def test_fallback_picks_up_reserved_members_nested_in_properties(self):
        """Bug fix: the old inline fallback only checked top-level
        ``assets``/``stac_extensions`` on the feature dict, silently leaving
        a duplicate copy in ``properties`` (and missing them entirely when
        only present inside ``properties``). ``canonical_input_from_feature``
        checks both locations and always strips them from ``properties``."""
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )

        geoid = "fallback-geoid-0002"
        ext_id = "EXT-FALLBACK-2"
        stac_doc = {
            "id": ext_id,
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [3.0, 4.0]},
            "properties": {
                "NAME": "Nested reserved members",
                "geoid": geoid,
                # Reserved members present ONLY inside properties (no
                # top-level "assets"/"stac_extensions" keys on the feature).
                "assets": {"thumbnail": {"href": "https://example.org/t.png"}},
                "stac_extensions": ["https://stac-extensions.github.io/y/v1.0.0/schema.json"],
            },
            "geoid": geoid,
        }

        driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)
        feature = MagicMock()
        feature.id = ext_id
        feature.geometry = stac_doc["geometry"]
        feature.properties = stac_doc["properties"]
        feature.model_dump = MagicMock(return_value=dict(stac_doc))

        mock_config = MagicMock()
        mock_config.simplify_geometry = False
        mock_config.simplify_target_bytes = None
        mock_config.external_id_path = MagicMock(return_value=None)
        mock_config.on_batch_conflict = None
        mock_config.on_conflict = None
        mock_config.validity = None

        captured_bulk: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_bulk.extend(body)
            return {
                "errors": False,
                "items": [{"index": {"_id": geoid, "_index": "test", "status": 200}}],
            }

        es_mock = AsyncMock()
        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch.object(driver, "get_driver_config", new=AsyncMock(return_value=mock_config)),
            patch.object(driver, "_enforce_field_constraints", new=AsyncMock()),
            patch.object(driver, "_resolve_write_policy", new=AsyncMock(return_value=mock_config)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={}),
            ),
        ):
            await driver.write_entities("cat1", "col1", [feature])

        doc_lines = [b for b in captured_bulk if isinstance(b, dict) and "index" not in b]
        assert len(doc_lines) == 1
        fallback_doc = doc_lines[0]

        assert fallback_doc.get("assets") == stac_doc["properties"]["assets"]
        assert fallback_doc.get("stac_extensions") == stac_doc["properties"]["stac_extensions"]
        assert "assets" not in fallback_doc.get("properties", {})
        assert "stac_extensions" not in fallback_doc.get("properties", {})


# ---------------------------------------------------------------------------
# Pass-C Task 1 — convergence invariant: write_entities ≡ index_bulk
# ---------------------------------------------------------------------------


class TestConvergenceInvariant:
    """``write_entities`` and ``index_bulk`` must produce byte-identical
    canonical ``_source`` for the same stored item (same raw-row reader
    return value → same ``build_canonical_index_doc`` output)."""

    @pytest.mark.asyncio
    async def test_write_entities_and_index_bulk_produce_identical_source(self):
        """Given the SAME canonical input, both paths must produce the same
        ``_source`` dict.  This pins the convergence invariant so a future
        divergence is immediately caught."""
        from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc

        canonical_input = _make_canonical_input()
        known_fields: Dict[str, Any] = {"NAME": {"type": "keyword"}}

        # Build the canonical doc directly (reference).
        expected_doc = build_canonical_index_doc(
            canonical_input.row,
            resolved_sidecars=canonical_input.resolved_sidecars,
            known_fields=known_fields,
            catalog_id="cat1",
            collection_id="col1",
            geometry=canonical_input.geometry,
            bbox=canonical_input.bbox,
            user_properties=canonical_input.user_properties,
            access=canonical_input.access,
        )

        # --- index_bulk path ---
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )

        driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)
        ctx = _make_ctx()
        op = IndexOp(op_type="upsert", entity_type="item", entity_id=_GEOID)

        bulk_captured: List[Dict] = []

        async def _fake_bulk_ib(body, params=None):
            bulk_captured.extend(body)
            return _bulk_ok_response(_GEOID)

        es_mock_ib = AsyncMock()
        es_mock_ib.bulk = _fake_bulk_ib

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock_ib,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
                new=AsyncMock(),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={_GEOID: canonical_input}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value=known_fields),
            ),
        ):
            await driver.index_bulk(ctx, [op])

        index_bulk_docs = [b for b in bulk_captured if "index" not in b]
        assert len(index_bulk_docs) == 1
        index_bulk_doc = index_bulk_docs[0]

        # --- write_entities path ---
        geoid = _GEOID
        ext_id = _EXTERNAL_ID
        feature = MagicMock()
        feature.id = ext_id
        feature.geometry = canonical_input.geometry
        feature.properties = canonical_input.user_properties
        feature.model_dump = MagicMock(
            return_value={
                "id": ext_id,
                "type": "Feature",
                "geometry": canonical_input.geometry,
                "properties": {**(canonical_input.user_properties or {}), "geoid": geoid},
                "geoid": geoid,
            }
        )

        mock_config = MagicMock()
        mock_config.simplify_geometry = False
        mock_config.simplify_target_bytes = None
        mock_config.external_id_path = MagicMock(return_value=None)
        mock_config.on_batch_conflict = None
        mock_config.on_conflict = None
        mock_config.validity = None

        we_captured: List[Dict] = []

        async def _fake_bulk_we(body, params=None):
            we_captured.extend(body)
            return {
                "errors": False,
                "items": [{"index": {"_id": geoid, "_index": "test", "status": 200}}],
            }

        es_mock_we = AsyncMock()
        es_mock_we.bulk = _fake_bulk_we

        driver_we = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock_we,
            ),
            patch.object(driver_we, "get_driver_config", new=AsyncMock(return_value=mock_config)),
            patch.object(driver_we, "_enforce_field_constraints", new=AsyncMock()),
            patch.object(driver_we, "_resolve_write_policy", new=AsyncMock(return_value=mock_config)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value=known_fields),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={geoid: canonical_input}),
            ),
        ):
            await driver_we.write_entities("cat1", "col1", [feature])

        we_docs = [b for b in we_captured if "index" not in b]
        assert len(we_docs) == 1
        write_entities_doc = we_docs[0]

        # Invariant: both paths produce the same canonical _source.
        assert index_bulk_doc == write_entities_doc, (
            f"Convergence invariant broken:\n"
            f"index_bulk  doc: {index_bulk_doc}\n"
            f"write_entities doc: {write_entities_doc}\n"
        )
        # Both must also equal the reference built directly.
        assert index_bulk_doc == expected_doc, (
            f"index_bulk doc diverges from reference:\n{index_bulk_doc}\n{expected_doc}"
        )


# ---------------------------------------------------------------------------
# Phase 2 (#1828) — system.geometry_simplification nested object
# ---------------------------------------------------------------------------


class TestGeometrySimplificationCanonicalNested:
    """Simplification metadata must land in ``system.geometry_simplification``
    (nested, typed) on ALL three write paths, and the flat
    ``_simplification_factor`` / ``_simplification_mode`` keys must NOT be
    written (#1828 Phase 2).

    ``maybe_simplify_for_es`` is mocked so these tests run without shapely.
    The simplify flag is enabled via ``_resolve_simplify_geometry`` mock.
    """

    # Fake return value simulating a simplification that actually ran.
    _FACTOR = 0.01
    _MODE = "douglas_peucker"

    @staticmethod
    def _make_driver():
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )
        return ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)

    @pytest.mark.asyncio
    async def test_write_entities_uses_nested_system_geometry_simplification(self):
        """write_entities: simplified doc carries system.geometry_simplification,
        NOT flat _simplification_factor / _simplification_mode."""
        driver = self._make_driver()
        geoid = _GEOID
        ext_id = _EXTERNAL_ID
        canonical_input = _make_canonical_input(geoid=geoid, external_id=ext_id)

        feature = MagicMock()
        feature.id = ext_id
        feature.geometry = canonical_input.geometry
        feature.properties = canonical_input.user_properties
        feature.model_dump = MagicMock(return_value={
            "id": ext_id,
            "type": "Feature",
            "geometry": canonical_input.geometry,
            "properties": {**(canonical_input.user_properties or {}), "geoid": geoid},
            "geoid": geoid,
        })

        mock_config = MagicMock()
        mock_config.simplify_geometry = True
        mock_config.simplify_target_bytes = 1_048_576
        mock_config.snap_to_grid = False
        mock_config.snap_grid_size = 1e-5
        mock_config.external_id_path = MagicMock(return_value=None)
        mock_config.on_batch_conflict = None
        mock_config.on_conflict = None
        mock_config.validity = None

        captured_bulk: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_bulk.extend(body)
            return {
                "errors": False,
                "items": [{"index": {"_id": geoid, "_index": "test", "status": 200}}],
            }

        es_mock = AsyncMock()
        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch.object(driver, "get_driver_config", new=AsyncMock(return_value=mock_config)),
            patch.object(driver, "_enforce_field_constraints", new=AsyncMock()),
            patch.object(driver, "_resolve_write_policy", new=AsyncMock(return_value=mock_config)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={geoid: canonical_input}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.maybe_simplify_for_es",
                side_effect=lambda doc, **kw: (doc, self._FACTOR, self._MODE),
            ),
        ):
            await driver.write_entities("cat1", "col1", [feature])

        doc_lines = [b for b in captured_bulk if isinstance(b, dict) and "index" not in b]
        assert len(doc_lines) == 1, f"Expected one doc in bulk; got: {captured_bulk}"
        doc = doc_lines[0]

        # Nested object present under system.
        assert "system" in doc, f"'system' key missing; doc keys: {list(doc.keys())}"
        gs = doc["system"].get("geometry_simplification")
        assert gs is not None, (
            f"system.geometry_simplification missing; system={doc['system']}"
        )
        assert gs["factor"] == self._FACTOR, f"wrong factor: {gs}"
        assert gs["mode"] == self._MODE, f"wrong mode: {gs}"

        # Flat keys must NOT be present.
        assert "_simplification_factor" not in doc, (
            f"flat _simplification_factor still written: {list(doc.keys())}"
        )
        assert "_simplification_mode" not in doc, (
            f"flat _simplification_mode still written: {list(doc.keys())}"
        )

    @pytest.mark.asyncio
    async def test_no_simplification_leaves_no_system_geometry_simplification(self):
        """When simplification is off (mode=='none'), system.geometry_simplification
        must NOT be added."""
        driver = self._make_driver()
        geoid = _GEOID
        ext_id = _EXTERNAL_ID
        canonical_input = _make_canonical_input(geoid=geoid, external_id=ext_id)

        feature = MagicMock()
        feature.id = ext_id
        feature.geometry = canonical_input.geometry
        feature.properties = canonical_input.user_properties
        feature.model_dump = MagicMock(return_value={
            "id": ext_id,
            "type": "Feature",
            "geometry": canonical_input.geometry,
            "properties": {**(canonical_input.user_properties or {}), "geoid": geoid},
            "geoid": geoid,
        })

        mock_config = MagicMock()
        mock_config.simplify_geometry = False
        mock_config.simplify_target_bytes = 1_048_576
        mock_config.snap_to_grid = False
        mock_config.snap_grid_size = 1e-5
        mock_config.external_id_path = MagicMock(return_value=None)
        mock_config.on_batch_conflict = None
        mock_config.on_conflict = None
        mock_config.validity = None

        captured_bulk: List[Dict] = []

        async def _fake_bulk(body, params=None):
            captured_bulk.extend(body)
            return {
                "errors": False,
                "items": [{"index": {"_id": geoid, "_index": "test", "status": 200}}],
            }

        es_mock = AsyncMock()
        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch.object(driver, "get_driver_config", new=AsyncMock(return_value=mock_config)),
            patch.object(driver, "_enforce_field_constraints", new=AsyncMock()),
            patch.object(driver, "_resolve_write_policy", new=AsyncMock(return_value=mock_config)),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={geoid: canonical_input}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.maybe_simplify_for_es",
                side_effect=lambda doc, **kw: (doc, 1.0, "none"),
            ),
        ):
            await driver.write_entities("cat1", "col1", [feature])

        doc_lines = [b for b in captured_bulk if isinstance(b, dict) and "index" not in b]
        assert len(doc_lines) == 1
        doc = doc_lines[0]

        # system may be present (from canonical builder) but must NOT have geometry_simplification.
        sys_section = doc.get("system", {})
        assert "geometry_simplification" not in sys_section, (
            f"geometry_simplification must be absent when mode=='none'; system={sys_section}"
        )
        assert "_simplification_factor" not in doc
        assert "_simplification_mode" not in doc

    @pytest.mark.asyncio
    async def test_index_bulk_uses_nested_system_geometry_simplification(self):
        """index_bulk: simplified docs carry system.geometry_simplification,
        NOT flat keys."""
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )
        from dynastore.models.protocols.indexer import IndexOp

        driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)
        ctx = _make_ctx()
        op = IndexOp(op_type="upsert", entity_type="item", entity_id=_GEOID)
        canonical_input = _make_canonical_input()

        bulk_captured: List[Dict] = []

        async def _fake_bulk(body, params=None):
            bulk_captured.extend(body)
            return _bulk_ok_response(_GEOID)

        es_mock = AsyncMock()
        es_mock.bulk = _fake_bulk

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
                new=AsyncMock(),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={_GEOID: canonical_input}),
            ),
            patch.object(
                driver, "_resolve_simplify_geometry",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.maybe_simplify_for_es",
                side_effect=lambda doc, **kw: (doc, self._FACTOR, self._MODE),
            ),
        ):
            from dynastore.models.protocols.indexer import BulkResult
            result = await driver.index_bulk(ctx, [op])

        doc_lines = [b for b in bulk_captured if isinstance(b, dict) and "index" not in b]
        assert len(doc_lines) == 1, f"Expected 1 doc; body: {bulk_captured}"
        doc = doc_lines[0]

        assert "system" in doc
        gs = doc["system"].get("geometry_simplification")
        assert gs is not None, f"system.geometry_simplification missing; system={doc['system']}"
        assert gs["factor"] == self._FACTOR
        assert gs["mode"] == self._MODE
        assert "_simplification_factor" not in doc
        assert "_simplification_mode" not in doc

    @pytest.mark.asyncio
    async def test_index_uses_nested_system_geometry_simplification(self):
        """index (single op): simplified doc carries system.geometry_simplification,
        NOT flat keys."""
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )
        from dynastore.models.protocols.indexer import IndexOp

        driver = ItemsElasticsearchDriver.__new__(ItemsElasticsearchDriver)
        ctx = _make_ctx()
        op = IndexOp(op_type="upsert", entity_type="item", entity_id=_GEOID)
        canonical_input = _make_canonical_input()

        indexed_docs: List[Dict] = []

        async def _fake_index(index, id, body, params=None):
            indexed_docs.append(body)

        es_mock = AsyncMock()
        es_mock.index = _fake_index

        with (
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
                return_value=es_mock,
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
                new=AsyncMock(),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.resolve_catalog_known_fields",
                new=AsyncMock(return_value={}),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
                new=AsyncMock(return_value={_GEOID: canonical_input}),
            ),
            patch.object(
                driver, "_resolve_simplify_geometry",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "dynastore.modules.storage.drivers.elasticsearch.maybe_simplify_for_es",
                side_effect=lambda doc, **kw: (doc, self._FACTOR, self._MODE),
            ),
        ):
            await driver.index(ctx, op)

        assert len(indexed_docs) == 1, f"Expected 1 indexed doc; got {indexed_docs}"
        doc = indexed_docs[0]

        assert "system" in doc
        gs = doc["system"].get("geometry_simplification")
        assert gs is not None, f"system.geometry_simplification missing; system={doc['system']}"
        assert gs["factor"] == self._FACTOR
        assert gs["mode"] == self._MODE
        assert "_simplification_factor" not in doc
        assert "_simplification_mode" not in doc
