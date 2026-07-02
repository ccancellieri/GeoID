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

"""Unit tests for the routing-driven bulk reindex tasks.

The bulk reindex implementation was redesigned to resolve both the reader and
writer via the storage routing layer rather than hardcoding driver references.

Coverage in this file:

- Routing resolution: reader comes from get_items_search_driver (GEOMETRY_EXACT
  hint → PG primary); writer comes from get_write_drivers filtered to the first
  secondary-index (is_item_indexer) driver distinct from the reader.
- Streaming contract: features from reader.read_entities are written via
  writer.write_entities in chunks.
- Error propagation: a write_entities failure propagates (no silent success).
- Skip condition: collections not routing through the public ES driver are skipped.
- driver_hint input: an explicit driver_ref in task inputs selects the WRITE target.
- Pre-reindex wipe: delete_by_query still fires before the routing-driven reindex.
- Supersedes ``test_bypass_matches_dispatcher_bulk_contract``: the hardcoded-bypass
  path (issue #507 Option B) has been replaced by routing-resolved read/write; the
  contract that the reindex produces the same shape as the dispatcher is no longer
  load-bearing because write_entities (the normal write path) IS the dispatcher entry
  point — both paths are identical by construction.
"""

from __future__ import annotations

from typing import AsyncIterator, Dict, List, Optional
from unittest.mock import patch

import pytest

pytest.importorskip("opensearchpy")  # optional dep — skip when SCOPE excludes it

from dynastore.modules.elasticsearch.bulk_reindex import ReindexResult
from dynastore.tasks.elasticsearch_indexer.tasks import (
    BulkCatalogReindexInputs,
    BulkCatalogReindexTask,
    BulkCollectionReindexInputs,
    BulkCollectionReindexTask,
)


# ---------------------------------------------------------------------------
# Fakes shared across tests
# ---------------------------------------------------------------------------

class _FakeEs:
    """Minimal fake AsyncElasticsearch for pre-reindex delete_by_query calls."""

    def __init__(self):
        self.delete_by_query_calls: list = []

    async def delete_by_query(self, *, index, body, params=None, **kwargs):
        self.delete_by_query_calls.append({
            "index": index, "body": body, "params": params,
        })
        return {"deleted": 0}


class _FakeCatalogs:
    """CatalogsProtocol stub that lists a fixed set of collections."""

    def __init__(self, collection_ids: List[str]):
        self._collection_ids = collection_ids

    async def list_collections(self, catalog_id, *, limit, offset):
        if offset > 0:
            return []
        return [
            type("C", (), {"id": cid})
            for cid in self._collection_ids
        ]


def _make_feature(item_id: str) -> dict:
    return {
        "id": item_id,
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {},
    }


class _FakeReader:
    """Fake CollectionItemsStore implementing read_entities as an async iterator."""

    driver_id = "fake_reader_driver"
    preferred_chunk_size: int = 0
    is_item_indexer: bool = False

    def __init__(self, features_by_collection: Dict[str, List[dict]]):
        self._features = features_by_collection
        self._calls: list = []

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
        **kwargs,
    ) -> AsyncIterator:
        self._calls.append((catalog_id, collection_id, limit, offset))
        page = self._features.get(collection_id, [])
        # Return items from offset up to offset+limit (simulate pagination).
        slice_ = page[offset: offset + limit]
        for f in slice_:
            yield f


class _FakeWriter:
    """Fake CollectionItemsStore implementing write_entities."""

    driver_id = "fake_writer_driver"
    preferred_chunk_size: int = 0
    is_item_indexer: bool = True  # marks as secondary-index / ES-like target

    def __init__(self, raise_on_write: Optional[Exception] = None, fail_calls: int = -1):
        """``fail_calls``: -1 (default) raises ``raise_on_write`` on every call
        (matches the pre-#2764 "always fails" contract). A positive N raises
        only for the first N calls, then succeeds — used to simulate a single
        rejected sub-chunk followed by recovery."""
        self._raise = raise_on_write
        self._fail_calls = fail_calls
        self._call_count = 0
        self.written_batches: list = []

    async def write_entities(self, catalog_id, collection_id, entities, **kwargs):
        self._call_count += 1
        if self._raise is not None and (
            self._fail_calls == -1 or self._call_count <= self._fail_calls
        ):
            raise self._raise
        self.written_batches.append(list(entities))
        return list(entities)


class _FakeResolvedDriver:
    """Minimal stand-in for ResolvedDriver."""

    def __init__(self, driver, driver_ref: str):
        self.driver = driver
        self.driver_ref = driver_ref


def _make_routing_config(driver_ref: str = "items_elasticsearch_driver", secondary_index: bool = True):
    return type("Routing", (), {
        "operations": {"WRITE": [
            type("Entry", (), {
                "driver_ref": driver_ref,
                "secondary_index": secondary_index,
            })()
        ]},
    })()


def _routing_without_es():
    return type("Routing", (), {
        "operations": {"WRITE": [
            type("Entry", (), {"driver_ref": "other_driver", "secondary_index": False})()
        ]},
    })()


def _make_payload(model_inputs):
    from uuid import uuid4
    from dynastore.modules.tasks.models import TaskPayload
    return TaskPayload(
        task_id=uuid4(),
        caller_id="tests:bulk-reindex",
        inputs=model_inputs.model_dump(),
    )


# ---------------------------------------------------------------------------
# Helper: build the routing-layer patch context for reindex_collection_into_index
# ---------------------------------------------------------------------------

def _build_router_patches(
    reader: _FakeReader,
    writer: _FakeWriter,
    reader_ref: str = "items_postgresql_driver",
    writer_ref: str = "items_elasticsearch_driver",
):
    """Return a context-manager that patches the routing layer for bulk_reindex."""
    resolved_reader = _FakeResolvedDriver(reader, reader_ref)
    resolved_writer = _FakeResolvedDriver(writer, writer_ref)

    async def _fake_get_items_search_driver(catalog_id, collection_id, *, hints=frozenset()):
        return resolved_reader

    async def _fake_get_write_drivers(catalog_id, collection_id, *, hints=frozenset()):
        return [resolved_writer]

    import contextlib

    @contextlib.asynccontextmanager
    async def _ctx():
        with patch(
            "dynastore.modules.elasticsearch.bulk_reindex.get_items_search_driver",
            side_effect=_fake_get_items_search_driver,
        ), patch(
            "dynastore.modules.elasticsearch.bulk_reindex.get_write_drivers",
            side_effect=_fake_get_write_drivers,
        ):
            yield

    return _ctx()


# ---------------------------------------------------------------------------
# Tests: routing resolution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reindex_reader_is_routing_resolved_not_hardcoded():
    """Reindex resolves the reader via get_items_search_driver, not a hardcoded
    catalogs_proto.search call.  The reader is distinct from the writer."""
    reader = _FakeReader({"col1": [_make_feature("f1"), _make_feature("f2")]})
    writer = _FakeWriter()

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _make_routing_config()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
        async with _build_router_patches(reader, writer):
            with patch(
                "dynastore.modules.elasticsearch.bulk_reindex.add_index_to_public_alias",
                return_value=None,
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_tenant_items_index",
                return_value="dynastore-cat1-items",
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_index_prefix",
                return_value="dynastore",
            ):
                from dynastore.modules.elasticsearch.bulk_reindex import reindex_collection_into_index
                result = await reindex_collection_into_index("cat1", "col1")

    assert result.total_written == 2
    assert result.rejected == 0
    # Reader was called, not a catalogs_proto path.
    assert reader._calls, "read_entities was never called"
    assert reader._calls[0][0] == "cat1"
    assert reader._calls[0][1] == "col1"


@pytest.mark.asyncio
async def test_reindex_writer_is_secondary_index_driver_not_reader():
    """Writer is the is_item_indexer driver; it must not equal the reader."""
    reader = _FakeReader({"col1": [_make_feature("f1")]})
    writer = _FakeWriter()

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _make_routing_config()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
        async with _build_router_patches(
            reader,
            writer,
            reader_ref="items_postgresql_driver",
            writer_ref="items_elasticsearch_driver",
        ):
            with patch(
                "dynastore.modules.elasticsearch.bulk_reindex.add_index_to_public_alias",
                return_value=None,
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_tenant_items_index",
                return_value="dynastore-cat1-items",
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_index_prefix",
                return_value="dynastore",
            ):
                from dynastore.modules.elasticsearch.bulk_reindex import reindex_collection_into_index
                await reindex_collection_into_index("cat1", "col1")

    assert writer.written_batches, "write_entities was never called"
    # Writer must have received the features from the reader.
    written_ids = [f["id"] for f in writer.written_batches[0]]
    assert "f1" in written_ids


@pytest.mark.asyncio
async def test_reindex_raises_when_reader_equals_writer():
    """If the only WRITE driver matches the reader, reindex raises ValueError
    rather than silently looping reads back to the source."""
    from dynastore.modules.elasticsearch.bulk_reindex import _select_writer

    # Simulate reader and writer resolving to the same driver_ref.
    reader_ref = "items_postgresql_driver"
    writer_fake = _FakeResolvedDriver(_FakeWriter(), reader_ref)

    with pytest.raises(ValueError, match="same driver"):
        _select_writer([writer_fake], reader_ref, driver_hint=reader_ref)


@pytest.mark.asyncio
async def test_reindex_raises_when_no_secondary_index_writer():
    """When no WRITE driver with is_item_indexer=True exists (distinct from reader),
    reindex raises ValueError rather than silently no-oping."""
    from dynastore.modules.elasticsearch.bulk_reindex import _select_writer

    class _NonIndexerWriter:
        is_item_indexer = False

    writer_entry = _FakeResolvedDriver(_NonIndexerWriter(), "other_driver")

    with pytest.raises(ValueError, match="secondary-index"):
        _select_writer([writer_entry], "items_postgresql_driver", driver_hint=None)


# ---------------------------------------------------------------------------
# Tests: read-page / write-chunk size resolution (#2750)
# ---------------------------------------------------------------------------

def test_resolve_read_page_explicit_value_wins_over_writer_preference():
    """An operator-supplied page_size governs verbatim, even when smaller
    than the writer's preferred_chunk_size — the read page must never be
    silently overridden upward (the #2750 regression)."""
    from dynastore.modules.elasticsearch.bulk_reindex import _resolve_read_page

    assert _resolve_read_page(50, 500) == 50


def test_resolve_read_page_none_falls_back_to_writer_preference():
    """page_size=None defers to the writer's preferred_chunk_size when it
    declares one."""
    from dynastore.modules.elasticsearch.bulk_reindex import _resolve_read_page

    assert _resolve_read_page(None, 500) == 500


def test_resolve_read_page_none_and_no_writer_preference_uses_default():
    """page_size=None with no writer preference (0) falls back to the
    module default."""
    from dynastore.modules.elasticsearch.bulk_reindex import (
        _DEFAULT_READ_PAGE_SIZE,
        _resolve_read_page,
    )

    assert _resolve_read_page(None, 0) == _DEFAULT_READ_PAGE_SIZE


def test_resolve_read_page_custom_default_override():
    """The default parameter is honoured when both page_size and the writer
    preference are absent."""
    from dynastore.modules.elasticsearch.bulk_reindex import _resolve_read_page

    assert _resolve_read_page(None, 0, default=42) == 42


@pytest.mark.asyncio
async def test_reindex_explicit_page_size_survives_larger_writer_preference():
    """End-to-end: an explicit small page_size reaches reader.read_entities
    verbatim even when the writer declares a larger preferred_chunk_size —
    the read page must not be widened by the writer preference."""
    features = [_make_feature(f"f{i}") for i in range(3)]
    reader = _FakeReader({"col1": features})
    writer = _FakeWriter()
    writer.preferred_chunk_size = 500  # writer prefers a much larger chunk

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _make_routing_config()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
        async with _build_router_patches(reader, writer):
            with patch(
                "dynastore.modules.elasticsearch.bulk_reindex.add_index_to_public_alias",
                return_value=None,
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_tenant_items_index",
                return_value="dynastore-cat1-items",
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_index_prefix",
                return_value="dynastore",
            ):
                from dynastore.modules.elasticsearch.bulk_reindex import reindex_collection_into_index
                result = await reindex_collection_into_index("cat1", "col1", page_size=50)

    assert result.total_written == 3
    # The read page passed to read_entities must be the explicit 50, not
    # max(50, writer.preferred_chunk_size)=500.
    assert reader._calls[0][2] == 50


# ---------------------------------------------------------------------------
# Tests: streaming and chunk delivery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reindex_features_are_delivered_to_writer_in_chunks():
    """Features streamed from read_entities arrive at write_entities in pages."""
    features = [_make_feature(f"f{i}") for i in range(5)]
    reader = _FakeReader({"col1": features})
    writer = _FakeWriter()

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _make_routing_config()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
        async with _build_router_patches(reader, writer):
            with patch(
                "dynastore.modules.elasticsearch.bulk_reindex.add_index_to_public_alias",
                return_value=None,
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_tenant_items_index",
                return_value="dynastore-cat1-items",
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_index_prefix",
                return_value="dynastore",
            ):
                from dynastore.modules.elasticsearch.bulk_reindex import reindex_collection_into_index
                result = await reindex_collection_into_index("cat1", "col1", page_size=3)

    assert result.total_written == 5
    all_written = [f for batch in writer.written_batches for f in batch]
    written_ids = sorted(f["id"] for f in all_written)
    assert written_ids == sorted(f["id"] for f in features)


# ---------------------------------------------------------------------------
# Tests: error propagation (silent-loss invariant)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reindex_rejected_subchunk_does_not_abort_run():
    """#2764: a sub-chunk rejected by ES (EsBulkWriteError) must not abort the
    run. The loop continues with the next sub-chunk/page, the rejected doc is
    surfaced in ``rejected_docs``, and ``total_written`` reflects only the
    accepted documents — including the ones ES accepted in later sub-chunks."""
    reader = _FakeReader(
        {"col1": [_make_feature("f1"), _make_feature("f2"), _make_feature("f3")]}
    )

    from dynastore.modules.storage.errors import EsBulkWriteError
    err = EsBulkWriteError(
        "bulk failure",
        failures=[("f1", "400 document_parsing_exception: known geo_shape divergence")],
    )
    # Only the first write_entities call (f1's sub-chunk) fails; f2 and f3
    # succeed — proves the run keeps going past the rejected sub-chunk.
    writer = _FakeWriter(raise_on_write=err, fail_calls=1)

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _make_routing_config()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
        async with _build_router_patches(reader, writer):
            with patch(
                "dynastore.modules.elasticsearch.bulk_reindex.add_index_to_public_alias",
                return_value=None,
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_tenant_items_index",
                return_value="dynastore-cat1-items",
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_index_prefix",
                return_value="dynastore",
            ):
                from dynastore.modules.elasticsearch.bulk_reindex import reindex_collection_into_index
                # page_size=1 forces one sub-chunk per document so the test can
                # pin the failure to exactly f1's sub-chunk.
                result = await reindex_collection_into_index("cat1", "col1", page_size=1)

    assert result.total_written == 2  # f2 + f3; f1 was rejected
    assert result.rejected == 1
    assert result.rejected_docs == [
        ("f1", "400 document_parsing_exception: known geo_shape divergence")
    ]
    written_ids = sorted(f["id"] for batch in writer.written_batches for f in batch)
    assert written_ids == ["f2", "f3"]
    assert writer._call_count == 3  # all three sub-chunks were attempted


@pytest.mark.asyncio
async def test_reindex_credits_only_acknowledged_ids_on_silent_sibling_drop():
    """#2799: the id-level reconcile audit found that ES's ``_bulk`` response
    can acknowledge FEWER docs than ``len(sub_chunk) - len(failures)`` —
    sibling documents in the same rejected sub-chunk silently failed to
    persist while the old formula still credited them as written. Crediting
    must come from ``exc.acknowledged`` only, and the shortfall must be
    reported, not silently dropped."""
    reader = _FakeReader(
        {"col1": [_make_feature(f"f{i}") for i in range(1, 5)]}  # f1..f4
    )

    from dynastore.modules.storage.errors import EsBulkWriteError
    # ES explicitly rejected f2; only f1 and f3 were actually acknowledged —
    # f4 vanished from the bulk response entirely (the silent-sibling-drop
    # case), so it is neither acknowledged nor an explicit failure.
    err = EsBulkWriteError(
        "bulk failure",
        failures=[("f2", "400 document_parsing_exception: known geo_shape divergence")],
        acknowledged=["f1", "f3"],
    )
    writer = _FakeWriter(raise_on_write=err, fail_calls=1)

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _make_routing_config()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
        async with _build_router_patches(reader, writer):
            with patch(
                "dynastore.modules.elasticsearch.bulk_reindex.add_index_to_public_alias",
                return_value=None,
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_tenant_items_index",
                return_value="dynastore-cat1-items",
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_index_prefix",
                return_value="dynastore",
            ):
                from dynastore.modules.elasticsearch.bulk_reindex import reindex_collection_into_index
                # page_size=4 keeps all 4 docs in a single sub-chunk/one
                # write_entities call so the whole silent-drop is exercised
                # inside one EsBulkWriteError.
                result = await reindex_collection_into_index("cat1", "col1", page_size=4)

    # Only the two truly-acknowledged docs are credited — never the
    # over-count of len(sub_chunk) - len(failures) == 3 the old formula
    # would have produced.
    assert result.total_written == 2
    # f2 (explicit ES rejection) + f4 (unaccounted/silently dropped).
    assert result.rejected == 2
    assert ("f2", "400 document_parsing_exception: known geo_shape divergence") in result.rejected_docs
    unaccounted = [r for r in result.rejected_docs if r[0] != "f2"]
    assert len(unaccounted) == 1
    assert "unacknowledged" in unaccounted[0][1]
    # Self-report invariant: acknowledged + reported-failures == docs read.
    assert result.total_written + result.rejected == 4


@pytest.mark.asyncio
async def test_reindex_ladder_recovered_docs_are_credited_as_written():
    """#2799/#2769: a doc recovered by the geo_shape degradation ladder is
    reported on ``EsBulkWriteError.acknowledged`` (or not raised at all when
    every rejection recovers) — the reindex accounting must credit it as
    written, not fold it into ``rejected_docs`` just because ES's raw
    response initially rejected it."""
    reader = _FakeReader(
        {"col1": [_make_feature("f1"), _make_feature("f2"), _make_feature("f3")]}
    )

    from dynastore.modules.storage.errors import EsBulkWriteError
    # f1 passed cleanly, f2 recovered on a degraded geometry rung (so it is
    # acknowledged despite originating as a poison rejection), f3 exhausted
    # every rung and is still a real failure.
    err = EsBulkWriteError(
        "bulk failure",
        failures=[("f3", "400 document_parsing_exception: still invalid after ladder")],
        acknowledged=["f1", "f2"],
    )
    writer = _FakeWriter(raise_on_write=err, fail_calls=1)

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _make_routing_config()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
        async with _build_router_patches(reader, writer):
            with patch(
                "dynastore.modules.elasticsearch.bulk_reindex.add_index_to_public_alias",
                return_value=None,
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_tenant_items_index",
                return_value="dynastore-cat1-items",
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_index_prefix",
                return_value="dynastore",
            ):
                from dynastore.modules.elasticsearch.bulk_reindex import reindex_collection_into_index
                result = await reindex_collection_into_index("cat1", "col1", page_size=3)

    assert result.total_written == 2  # f1 + f2 (ladder-recovered)
    assert result.rejected == 1  # only f3, the truly-exhausted rejection
    assert result.rejected_docs == [
        ("f3", "400 document_parsing_exception: still invalid after ladder")
    ]
    # No unaccounted/silent gap — acknowledged + failures == docs read.
    assert result.total_written + result.rejected == 3


@pytest.mark.asyncio
async def test_reindex_generic_write_failure_propagates():
    """Any write_entities exception (not just EsBulkWriteError) propagates."""
    reader = _FakeReader({"col1": [_make_feature("f1")]})
    writer = _FakeWriter(raise_on_write=RuntimeError("transport error"))

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _make_routing_config()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
        async with _build_router_patches(reader, writer):
            with patch(
                "dynastore.modules.elasticsearch.bulk_reindex.add_index_to_public_alias",
                return_value=None,
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_tenant_items_index",
                return_value="dynastore-cat1-items",
            ), patch(
                "dynastore.modules.elasticsearch.bulk_reindex.get_index_prefix",
                return_value="dynastore",
            ):
                from dynastore.modules.elasticsearch.bulk_reindex import reindex_collection_into_index
                with pytest.raises(RuntimeError, match="transport error"):
                    await reindex_collection_into_index("cat1", "col1")


# ---------------------------------------------------------------------------
# Tests: skip condition
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_collection_reindex_skips_when_es_not_in_routing(monkeypatch):
    """Collections not routed through the public ES driver are skipped."""
    es = _FakeEs()

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _routing_without_es()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    # Mock the router to return a valid reader/writer pair — but is_es_active_for
    # should short-circuit before they are used.
    reader = _FakeReader({"col1": [_make_feature("f1")]})
    writer = _FakeWriter()

    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        return_value="dynastore",
    ), patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ):
        async with _build_router_patches(reader, writer):
            task = BulkCollectionReindexTask()
            result = await task.run(_make_payload(
                BulkCollectionReindexInputs(catalog_id="cat1", collection_id="col1"),
            ))

    assert result["total_indexed"] == 0
    assert not writer.written_batches
    # delete_by_query still ran (pre-reindex wipe is unconditional).
    assert es.delete_by_query_calls


# ---------------------------------------------------------------------------
# Tests: task-level wiring
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_collection_reindex_task_passes_driver_hint():
    """The driver field in BulkCollectionReindexInputs reaches driver_hint in
    reindex_collection_into_index."""
    captured_hints: list = []

    async def _fake_reindex(catalog_id, collection_id, *, driver_hint=None, page_size=500):
        captured_hints.append(driver_hint)
        return ReindexResult(total_written=1)

    es = _FakeEs()

    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        return_value="dynastore",
    ), patch(
        "dynastore.tasks.elasticsearch_indexer.tasks._reindex_collection",
        side_effect=_fake_reindex,
    ):
        task = BulkCollectionReindexTask()
        await task.run(_make_payload(
            BulkCollectionReindexInputs(
                catalog_id="cat1",
                collection_id="col1",
                driver="items_elasticsearch_driver",
            ),
        ))

    assert captured_hints == ["items_elasticsearch_driver"]


@pytest.mark.asyncio
async def test_collection_reindex_task_surfaces_rejected_docs():
    """#2764: BulkCollectionReindexTask's result dict must surface the
    rejected count and per-doc reasons alongside total_indexed, not just a
    bare success count."""

    async def _fake_reindex(catalog_id, collection_id, *, driver_hint=None, page_size=500):
        return ReindexResult(
            total_written=5842,
            rejected_docs=[("019f2160-5a24-75b7-85b5-d51eb3c011ed", "400 document_parsing_exception")],
        )

    es = _FakeEs()

    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        return_value="dynastore",
    ), patch(
        "dynastore.tasks.elasticsearch_indexer.tasks._reindex_collection",
        side_effect=_fake_reindex,
    ):
        task = BulkCollectionReindexTask()
        result = await task.run(_make_payload(
            BulkCollectionReindexInputs(catalog_id="cat1", collection_id="col1"),
        ))

    assert result["total_indexed"] == 5842
    assert result["rejected"] == 1
    assert result["rejected_docs"] == [
        {"id": "019f2160-5a24-75b7-85b5-d51eb3c011ed", "reason": "400 document_parsing_exception"}
    ]


@pytest.mark.asyncio
async def test_catalog_reindex_task_iterates_collections_and_passes_driver_hint():
    """BulkCatalogReindexTask calls _reindex_collection for each collection and
    forwards the driver hint from inputs."""
    captured_calls: list = []

    async def _fake_reindex(catalog_id, collection_id, *, driver_hint=None, page_size=500):
        captured_calls.append((collection_id, driver_hint))
        return ReindexResult(total_written=2)

    es = _FakeEs()
    catalogs = _FakeCatalogs(["col1", "col2"])

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "CatalogsProtocol" in name:
            return catalogs
        return None

    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        return_value="dynastore",
    ), patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ), patch(
        "dynastore.tasks.elasticsearch_indexer.tasks._reindex_collection",
        side_effect=_fake_reindex,
    ):
        task = BulkCatalogReindexTask()
        result = await task.run(_make_payload(
            BulkCatalogReindexInputs(
                catalog_id="cat1",
                driver="items_elasticsearch_driver",
            ),
        ))

    assert result["total_indexed"] == 4
    assert sorted(c for c, _ in captured_calls) == ["col1", "col2"]
    assert all(hint == "items_elasticsearch_driver" for _, hint in captured_calls)


@pytest.mark.asyncio
async def test_catalog_reindex_task_aggregates_rejected_docs_across_collections():
    """#2764: BulkCatalogReindexTask sums rejected counts across collections
    and tags each rejected doc with its collection_id."""

    async def _fake_reindex(catalog_id, collection_id, *, driver_hint=None, page_size=500):
        if collection_id == "col1":
            return ReindexResult(total_written=10, rejected_docs=[("bad1", "400 reason")])
        return ReindexResult(total_written=20)

    es = _FakeEs()
    catalogs = _FakeCatalogs(["col1", "col2"])

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "CatalogsProtocol" in name:
            return catalogs
        return None

    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        return_value="dynastore",
    ), patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ), patch(
        "dynastore.tasks.elasticsearch_indexer.tasks._reindex_collection",
        side_effect=_fake_reindex,
    ):
        task = BulkCatalogReindexTask()
        result = await task.run(_make_payload(
            BulkCatalogReindexInputs(catalog_id="cat1"),
        ))

    assert result["total_indexed"] == 30
    assert result["rejected"] == 1
    assert result["rejected_docs"] == [
        {"collection_id": "col1", "id": "bad1", "reason": "400 reason"}
    ]


@pytest.mark.asyncio
async def test_collection_task_pre_reindex_wipe_collection_scoped():
    """Pre-reindex delete_by_query for BulkCollectionReindexTask uses a
    collection-scoped term query and carries routing."""
    es = _FakeEs()

    async def _fake_reindex(*args, **kwargs):
        return ReindexResult(total_written=0)

    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        return_value="dynastore",
    ), patch(
        "dynastore.tasks.elasticsearch_indexer.tasks._reindex_collection",
        side_effect=_fake_reindex,
    ):
        task = BulkCollectionReindexTask()
        await task.run(_make_payload(
            BulkCollectionReindexInputs(catalog_id="cat1", collection_id="col1"),
        ))

    assert es.delete_by_query_calls
    dbq = es.delete_by_query_calls[0]
    assert dbq["index"] == "dynastore-cat1-items"
    assert dbq["body"] == {"query": {"term": {"collection": "col1"}}}
    assert dbq["params"]["routing"] == "col1"


@pytest.mark.asyncio
async def test_catalog_task_pre_reindex_wipe_catalog_scoped():
    """Pre-reindex delete_by_query for BulkCatalogReindexTask uses a match_all
    against the per-tenant index (no collection routing)."""
    es = _FakeEs()
    catalogs = _FakeCatalogs([])  # no collections → zero iterations

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "CatalogsProtocol" in name:
            return catalogs
        return None

    with patch(
        "dynastore.modules.elasticsearch.client.get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        return_value="dynastore",
    ), patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ):
        task = BulkCatalogReindexTask()
        result = await task.run(_make_payload(
            BulkCatalogReindexInputs(catalog_id="cat1"),
        ))

    assert result["total_indexed"] == 0
    assert es.delete_by_query_calls
    dbq = es.delete_by_query_calls[0]
    assert dbq["index"] == "dynastore-cat1-items"
    assert dbq["body"] == {"query": {"match_all": {}}}
    # No collection-scope routing for catalog-level wipe.
    assert "routing" not in (dbq["params"] or {})


# ---------------------------------------------------------------------------
# Tests: schema regression
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_inputs_drop_mode_field():
    """Schema regression — the mode field must be absent from inputs."""
    inputs = BulkCatalogReindexInputs(catalog_id="cat1")
    assert "mode" not in inputs.model_dump()
    inputs2 = BulkCollectionReindexInputs(catalog_id="cat1", collection_id="col1")
    assert "mode" not in inputs2.model_dump()


# ---------------------------------------------------------------------------
# Tests: is_es_active_for (unchanged guard, tested independently)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_is_es_active_for_matches_snake_case_driver_id():
    """PR-1e regression guard: ``OperationDriverEntry.driver_ref`` is always
    snake_case after the validator coerces it. ``is_es_active_for`` must
    compare against ``"items_elasticsearch_driver"`` — pre-PR-1e it compared
    against ``"ItemsElasticsearchDriver"`` and silently returned False for
    every collection, breaking ES-aware downstream code paths.
    """
    from dynastore.modules.elasticsearch.bulk_reindex import is_es_active_for
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig,
        OperationDriverEntry,
        Operation,
    )
    from dynastore.tools import discovery

    routing = ItemsRoutingConfig(
        operations={Operation.READ: [OperationDriverEntry(driver_ref="items_elasticsearch_driver")]},
    )

    async def _get_config(model, *, catalog_id, collection_id=None):
        return routing

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch.object(discovery, "get_protocol", side_effect=_get_protocol):
        assert await is_es_active_for("cat1", "col1") is True

    # And conversely: a routing without ES returns False.
    routing_pg_only = ItemsRoutingConfig(
        operations={Operation.READ: [OperationDriverEntry(driver_ref="items_postgresql_driver")]},
    )

    async def _get_config_pg(model, *, catalog_id, collection_id=None):
        return routing_pg_only

    fake_configs_pg = type("C", (), {"get_config": staticmethod(_get_config_pg)})()

    def _get_protocol_pg(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs_pg
        return None

    with patch.object(discovery, "get_protocol", side_effect=_get_protocol_pg):
        assert await is_es_active_for("cat1", "col1") is False


@pytest.mark.asyncio
async def test_is_es_active_for_returns_false_for_private_only_routing():
    """#733 privacy safety: a collection whose items routing pins
    ONLY ``items_elasticsearch_private_driver`` (the routing-config
    expression of "this collection is private") must return False
    from ``is_es_active_for``.

    Without this property, the catalog bulk-reindex pipeline
    (``BulkCatalogReindexTask``) would fan out the collection's items
    into the per-tenant **public** index ``{prefix}-{cat}-items``,
    leaking private item geometry that should only live in the
    per-tenant **private** index ``{prefix}-{cat}-private-items``.
    """
    from dynastore.modules.elasticsearch.bulk_reindex import is_es_active_for
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig,
        Operation,
        OperationDriverEntry,
    )
    from dynastore.tools import discovery

    routing = ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(driver_ref="items_postgresql_driver"),
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    secondary_index=True,
                ),
            ],
        },
    )

    async def _get_config(model, *, catalog_id, collection_id=None):
        return routing

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch.object(discovery, "get_protocol", side_effect=_get_protocol):
        # Private driver pinned, but public driver IS NOT — bulk
        # reindex must skip this collection to honour the privacy
        # cascade.
        assert await is_es_active_for("cat1", "col1") is False


@pytest.mark.asyncio
async def test_is_es_active_for_returns_true_when_public_and_private_both_pinned():
    """A collection that pins BOTH the public and private items
    drivers (e.g. an operator transitioning OUT of private mode by
    layering the public driver before dropping the private driver)
    is "es-active" — the bulk-reindex into the public per-tenant
    index is the right action.  The cascade validator allows this
    shape (#733): items-private + collection-public is OK.
    """
    from dynastore.modules.elasticsearch.bulk_reindex import is_es_active_for
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig,
        Operation,
        OperationDriverEntry,
    )
    from dynastore.tools import discovery

    routing = ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(driver_ref="items_postgresql_driver"),
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    secondary_index=True,
                ),
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    secondary_index=True,
                ),
            ],
        },
    )

    async def _get_config(model, *, catalog_id, collection_id=None):
        return routing

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    with patch.object(discovery, "get_protocol", side_effect=_get_protocol):
        assert await is_es_active_for("cat1", "col1") is True


# ---------------------------------------------------------------------------
# NOTE: test_bypass_matches_dispatcher_bulk_contract has been deliberately
# superseded by the tests above.
#
# The original contract (issue #507 Option B) pinned that the bulk-reindex
# bypass path and the IndexDispatcher path produced the same es.bulk body
# shape. That bypass no longer exists: reindex_collection_into_index now
# calls writer.write_entities(), which IS the normal write/dispatch entry
# point. The two paths are structurally identical by construction — there is
# no separate bypass to pin against. Any regression in write_entities
# body shape would surface in the item_service / dispatcher tests, not here.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests: explicit reader_ref override (file-backed collections, #376)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reindex_reader_ref_override_resolves_from_registry():
    """A file-backed collection names its file driver via reader_ref; the reindex
    resolves it straight from the DriverRegistry instead of the GEOMETRY_EXACT hint,
    then streams it into the ES writer."""
    reader = _FakeReader({"col1": [_make_feature("f1"), _make_feature("f2")]})
    writer = _FakeWriter()

    async def _get_config(model, *, catalog_id, collection_id=None):
        return _make_routing_config()

    fake_configs = type("C", (), {"get_config": staticmethod(_get_config)})()

    def _get_protocol(proto):
        name = getattr(proto, "__name__", str(proto))
        if "ConfigsProtocol" in name:
            return fake_configs
        return None

    search_driver_calls: list = []

    async def _fake_search_driver(catalog_id, collection_id, *, hints=frozenset()):
        search_driver_calls.append((catalog_id, collection_id))
        raise AssertionError("get_items_search_driver must NOT be called when reader_ref is set")

    async def _fake_get_write_drivers(catalog_id, collection_id, *, hints=frozenset()):
        return [_FakeResolvedDriver(writer, "items_elasticsearch_driver")]

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol), patch(
        "dynastore.modules.storage.driver_registry.DriverRegistry.get_collection",
        return_value=reader,
    ), patch(
        "dynastore.modules.elasticsearch.bulk_reindex.get_items_search_driver",
        side_effect=_fake_search_driver,
    ), patch(
        "dynastore.modules.elasticsearch.bulk_reindex.get_write_drivers",
        side_effect=_fake_get_write_drivers,
    ), patch(
        "dynastore.modules.elasticsearch.bulk_reindex.add_index_to_public_alias",
        return_value=None,
    ), patch(
        "dynastore.modules.elasticsearch.bulk_reindex.get_tenant_items_index",
        return_value="dynastore-cat1-items",
    ), patch(
        "dynastore.modules.elasticsearch.bulk_reindex.get_index_prefix",
        return_value="dynastore",
    ):
        from dynastore.modules.elasticsearch.bulk_reindex import reindex_collection_into_index
        result = await reindex_collection_into_index(
            "cat1", "col1", reader_ref="items_duckdb_driver",
        )

    assert result.total_written == 2
    assert reader._calls, "file-driver read_entities was never called"
    assert not search_driver_calls, "hint resolution should be bypassed by reader_ref"


@pytest.mark.asyncio
async def test_reindex_reader_ref_unknown_raises():
    """An unregistered reader_ref is a hard error, not a silent skip."""
    writer = _FakeWriter()

    async def _fake_get_write_drivers(catalog_id, collection_id, *, hints=frozenset()):
        return [_FakeResolvedDriver(writer, "items_elasticsearch_driver")]

    with patch(
        "dynastore.modules.storage.driver_registry.DriverRegistry.get_collection",
        return_value=None,
    ), patch(
        "dynastore.modules.elasticsearch.bulk_reindex.get_write_drivers",
        side_effect=_fake_get_write_drivers,
    ):
        from dynastore.modules.elasticsearch.bulk_reindex import reindex_collection_into_index
        with pytest.raises(ValueError):
            await reindex_collection_into_index(
                "cat1", "col1", reader_ref="nonexistent_driver",
            )
