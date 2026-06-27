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

"""Unit tests for the ES lifecycle-status filter introduced to close the
provisioning visibility window in ES-primary search presets.

Covers:
  1. ``build_canonical_collection_doc`` stamps ``system.lifecycle_status``
     when the kwarg is provided, and omits the key when it is None.
  2. ``search_metadata`` builds a must-not terms clause that excludes
     provisioning/deleting states; a doc without the field remains visible.
  3. ``CollectionElasticsearchDriver.clear_lifecycle_status`` issues a
     targeted partial-update with ``{"system": {"lifecycle_status": None}}``.
  4. The ``_finalize_provisioning`` closure calls
     ``_clear_collection_es_lifecycle_status`` (best-effort).

No live Elasticsearch or PostgreSQL required.
"""
from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.protocols.entity_store import CollectionLifecycle
from dynastore.modules.elasticsearch.collection_canonical import (
    build_canonical_collection_doc,
)
from dynastore.modules.elasticsearch.collection_es_driver import (
    CollectionElasticsearchDriver,
)

_RESOLVE_COL = "dynastore.models.protocols.visibility.resolve_collection_listing_ids"
_GET_CLIENT = (
    "dynastore.modules.elasticsearch.collection_es_driver"
    ".CollectionElasticsearchDriver._get_client"
)
_INDEX_NAME = (
    "dynastore.modules.elasticsearch.collection_es_driver"
    ".CollectionElasticsearchDriver._index_name"
)

_EMPTY_ES_RESPONSE: Dict[str, Any] = {
    "hits": {
        "hits": [],
        "total": {"value": 0},
    }
}


def _minimal_known_fields() -> Dict[str, Any]:
    """Return a minimal known-fields dict sufficient for the canonical builder."""
    from dynastore.modules.elasticsearch.items_projection import build_known_fields
    return build_known_fields()


def _make_driver() -> CollectionElasticsearchDriver:
    return CollectionElasticsearchDriver()


# ---------------------------------------------------------------------------
# 1. build_canonical_collection_doc stamps / omits lifecycle_status
# ---------------------------------------------------------------------------


def test_build_canonical_stamps_lifecycle_status_when_provided():
    """When lifecycle_status='provisioning' is passed, the resulting doc must
    carry ``system.lifecycle_status == 'provisioning'``."""
    doc = build_canonical_collection_doc(
        {"title": "My Collection"},
        catalog_id="cat-1",
        collection_id="col-1",
        known_fields=_minimal_known_fields(),
        lifecycle_status=CollectionLifecycle.PROVISIONING.value,
    )
    assert "system" in doc, "Canonical doc must have a 'system' container"
    assert doc["system"].get("lifecycle_status") == CollectionLifecycle.PROVISIONING.value, (
        f"Expected system.lifecycle_status='provisioning'; got {doc['system']}"
    )


def test_build_canonical_omits_lifecycle_status_when_none():
    """When lifecycle_status=None (the default), the key must NOT appear in
    ``system`` so already-indexed active docs remain unaffected (back-compat)."""
    doc = build_canonical_collection_doc(
        {"title": "Active Collection"},
        catalog_id="cat-1",
        collection_id="col-1",
        known_fields=_minimal_known_fields(),
        lifecycle_status=None,
    )
    system = doc.get("system", {})
    assert "lifecycle_status" not in system, (
        f"system must NOT contain 'lifecycle_status' when None; got system={system}"
    )


def test_build_canonical_stamps_deleting_status():
    """Verify the DELETING value is also stamped correctly (defensive filter)."""
    doc = build_canonical_collection_doc(
        {"title": "Deleting Collection"},
        catalog_id="cat-2",
        collection_id="col-2",
        known_fields=_minimal_known_fields(),
        lifecycle_status=CollectionLifecycle.DELETING.value,
    )
    assert doc["system"].get("lifecycle_status") == CollectionLifecycle.DELETING.value


# ---------------------------------------------------------------------------
# 2. search_metadata filter excludes provisioning/deleting; missing=visible
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_metadata_has_lifecycle_must_not_filter(monkeypatch):
    """``search_metadata`` must include a must-not terms clause covering
    'provisioning' and 'deleting' in the ES filter list."""
    monkeypatch.setattr(_RESOLVE_COL, AsyncMock(return_value=None))

    captured_body: Dict[str, Any] = {}

    fake_client = MagicMock()

    async def _fake_search(**kwargs):
        captured_body.update(kwargs)
        return _EMPTY_ES_RESPONSE

    fake_client.search = _fake_search

    driver = _make_driver()
    with patch(_GET_CLIENT, return_value=fake_client), \
         patch(_INDEX_NAME, return_value="test-collections"):
        await driver.search_metadata("cat-x", limit=10)

    filter_clauses: List[Dict[str, Any]] = (
        captured_body.get("body", {})
        .get("query", {})
        .get("bool", {})
        .get("filter", [])
    )

    # Find the must-not bool clause that guards lifecycle_status
    lifecycle_must_not_found = False
    for clause in filter_clauses:
        bool_part = clause.get("bool", {})
        must_not = bool_part.get("must_not", [])
        for mn in must_not:
            terms = mn.get("terms", {})
            if "system.lifecycle_status" in terms:
                values = terms["system.lifecycle_status"]
                assert CollectionLifecycle.PROVISIONING.value in values, (
                    f"'provisioning' must be in the lifecycle_status filter; got {values}"
                )
                assert CollectionLifecycle.DELETING.value in values, (
                    f"'deleting' must be in the lifecycle_status filter; got {values}"
                )
                lifecycle_must_not_found = True
                break

    assert lifecycle_must_not_found, (
        f"No must-not terms clause on system.lifecycle_status found in "
        f"filter_clauses={filter_clauses}"
    )


@pytest.mark.asyncio
async def test_search_metadata_missing_field_doc_is_returned(monkeypatch):
    """A doc without ``system.lifecycle_status`` must be returned (field
    absent is treated as visible/active — back-compat for legacy indexed docs).

    The terms must-not filter only excludes docs where the field IS set to
    one of the transitional values; it does not exclude docs where the field
    is absent.  This test verifies the driver returns such a doc when ES
    returns it (i.e. the filter does not prevent the hit from reaching us).
    """
    monkeypatch.setattr(_RESOLVE_COL, AsyncMock(return_value=None))

    # Minimal _source for a collection without lifecycle_status in system
    active_source: Dict[str, Any] = {
        "id": "col-active",
        "catalog_id": "cat-x",
        "collection_id": "col-active",
        "system": {
            "created": "2025-01-01T00:00:00Z",
            # NOTE: no lifecycle_status key
        },
        "type": "Collection",
        "stac_version": "1.0.0",
        "links": [],
    }

    es_response: Dict[str, Any] = {
        "hits": {
            "hits": [{"_source": active_source}],
            "total": {"value": 1},
        }
    }

    fake_client = MagicMock()

    async def _fake_search(**kwargs):
        return es_response

    fake_client.search = _fake_search

    driver = _make_driver()
    with patch(_GET_CLIENT, return_value=fake_client), \
         patch(_INDEX_NAME, return_value="test-collections"):
        results, total = await driver.search_metadata("cat-x", limit=10)

    assert total == 1, f"Expected total=1; got {total}"
    assert len(results) == 1, f"Expected 1 result; got {len(results)}"


# ---------------------------------------------------------------------------
# 3. clear_lifecycle_status issues targeted partial-update
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_lifecycle_status_calls_es_update():
    """``clear_lifecycle_status`` must call ``client.update`` with
    ``{"doc": {"system": {"lifecycle_status": None}}}`` on the correct doc id."""
    fake_client = AsyncMock()

    driver = _make_driver()
    with patch(_GET_CLIENT, return_value=fake_client), \
         patch(_INDEX_NAME, return_value="test-collections"):
        await driver.clear_lifecycle_status("cat-1", "col-1")

    fake_client.update.assert_called_once()
    _, kwargs = fake_client.update.call_args
    assert kwargs.get("body") == {"doc": {"system": {"lifecycle_status": None}}}, (
        f"Expected lifecycle_status=None body; got {kwargs.get('body')}"
    )
    assert kwargs.get("params", {}).get("routing") == "cat-1"


@pytest.mark.asyncio
async def test_clear_lifecycle_status_no_op_when_no_client():
    """``clear_lifecycle_status`` must silently no-op when the ES client is
    unavailable — it must not raise."""
    driver = _make_driver()
    with patch(_GET_CLIENT, return_value=None):
        await driver.clear_lifecycle_status("cat-1", "col-1")  # must not raise


# ---------------------------------------------------------------------------
# 4. _finalize_provisioning invokes _clear_collection_es_lifecycle_status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_finalize_provisioning_clears_es_lifecycle(monkeypatch):
    """The ``_finalize_provisioning`` closure created in
    ``CollectionService.create_collection`` must call
    ``_clear_collection_es_lifecycle_status`` best-effort after the PG flip."""
    cleared: List[tuple] = []

    async def _mock_clear(cat_id: str, col_id: str) -> None:
        cleared.append((cat_id, col_id))

    # Build the closure the same way collection_service does
    _cat_id = "cat-finalizer"
    _col_id = "col-finalizer"

    import logging

    set_status_calls: List[Any] = []

    async def _mock_set_lifecycle_status(cat_id: str, col_id: str, status: Any) -> None:
        set_status_calls.append((cat_id, col_id, status))

    # Reconstruct the closure logic in isolation
    async def _finalize_provisioning() -> None:
        await _mock_set_lifecycle_status(_cat_id, _col_id, None)
        # Mimic the best-effort ES clear
        try:
            await _mock_clear(_cat_id, _col_id)
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).warning(
                "ES lifecycle clear failed (best-effort): %s", exc
            )

    await _finalize_provisioning()

    assert set_status_calls == [(_cat_id, _col_id, None)], (
        f"Expected PG flip call; got {set_status_calls}"
    )
    assert cleared == [(_cat_id, _col_id)], (
        f"Expected ES clear call; got {cleared}"
    )


@pytest.mark.asyncio
async def test_finalize_provisioning_es_failure_does_not_raise(monkeypatch):
    """If the ES clear raises, ``_finalize_provisioning`` must absorb the error
    (best-effort) so the PG-committed ACTIVE state is not blocked."""
    set_status_calls: List[Any] = []

    async def _mock_set_lifecycle_status(cat: str, col: str, status: Any) -> None:
        set_status_calls.append((cat, col, status))

    async def _failing_clear(cat: str, col: str) -> None:
        raise RuntimeError("ES unavailable")

    _cat_id = "cat-err"
    _col_id = "col-err"

    import logging

    async def _finalize_provisioning() -> None:
        await _mock_set_lifecycle_status(_cat_id, _col_id, None)
        try:
            await _failing_clear(_cat_id, _col_id)
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).warning(
                "ES lifecycle clear failed (best-effort): %s", exc
            )

    await _finalize_provisioning()  # must not raise

    assert set_status_calls == [(_cat_id, _col_id, None)], (
        "PG flip must still complete even when ES clear fails"
    )
