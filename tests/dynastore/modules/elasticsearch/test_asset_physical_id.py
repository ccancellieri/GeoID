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

"""Tests for asset_physical_id rename-safety in Elasticsearch (#2296 Gap 2).

Covers:
  1. ``build_canonical_index_doc`` stamps ``asset_physical_id`` from
     ``row["asset_id"]`` (the PG sidecar stores physical_id there) and does
     NOT write the mutable logical ``asset_id`` to ``_source``.
  2. ``build_canonical_index_doc`` prefers ``row["physical_id"]`` when
     present (forward-compatible: a future SELECT that aliases
     ``assets.physical_id`` into the item row).
  3. ``asset_physical_id`` is declared as ``keyword`` in the items mapping;
     the mutable ``asset_id`` is NOT in the items mapping (only the assets
     index retains ``asset_id`` as the asset's own identity field).
  4. The ITEM_MAPPING root-level keyword block includes ``asset_physical_id``.
  5. ``asset_physical_id`` is absent from ``_source`` when ``asset_id`` is not
     in the row (no parent asset for this item).
  6. The assets visibility filter translates logical visible_ids → physical_ids
     and emits a ``terms`` clause on ``asset_physical_id``.
"""
from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# 1-2. build_canonical_index_doc stamps asset_physical_id
# ---------------------------------------------------------------------------


def _build_doc(row: Dict[str, Any]) -> Dict[str, Any]:
    """Call build_canonical_index_doc with a minimal row and empty sidecars."""
    from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc

    return build_canonical_index_doc(
        row,
        resolved_sidecars=[],
        known_fields={},
        catalog_id="cat1",
        collection_id="col1",
    )


def test_asset_physical_id_stamped_from_asset_id_column() -> None:
    """When the row carries ``asset_id`` (the PG sidecar column that stores
    the physical_id post-#2296), ``asset_physical_id`` must be stamped from it.
    The mutable logical ``asset_id`` must NOT appear in the item _source."""
    _phys_uuid = "01920000-0000-7000-8000-000000000001"
    doc = _build_doc({"geoid": "g1", "asset_id": _phys_uuid})

    assert "asset_id" not in doc, (
        f"logical asset_id must NOT be written to item _source; "
        f"got {doc.get('asset_id')}"
    )
    assert doc.get("asset_physical_id") == _phys_uuid, (
        f"asset_physical_id must be stamped from asset_id column; "
        f"got {doc.get('asset_physical_id')}"
    )


def test_asset_physical_id_prefers_physical_id_key_when_present() -> None:
    """When the row carries a separate ``physical_id`` key (e.g. from a future
    SELECT that aliases assets.physical_id), ``asset_physical_id`` must be
    taken from that key.  The mutable logical ``asset_id`` must NOT appear
    in the item _source regardless."""
    _logical = "some-logical-id"
    _phys_uuid = "01920000-0000-7000-8000-000000000002"
    doc = _build_doc({"geoid": "g1", "asset_id": _logical, "physical_id": _phys_uuid})

    assert "asset_id" not in doc, (
        f"logical asset_id must NOT be written to item _source; "
        f"got {doc.get('asset_id')}"
    )
    assert doc.get("asset_physical_id") == _phys_uuid, (
        f"asset_physical_id must be taken from physical_id key when present; "
        f"got {doc.get('asset_physical_id')}"
    )


def test_asset_physical_id_absent_when_no_asset_id_in_row() -> None:
    """Items that have no parent asset carry no ``asset_id``.  Neither
    ``asset_id`` nor ``asset_physical_id`` should appear in ``_source``."""
    doc = _build_doc({"geoid": "g1"})

    assert "asset_id" not in doc, f"asset_id should be absent; got {doc.get('asset_id')}"
    assert "asset_physical_id" not in doc, (
        f"asset_physical_id should be absent; got {doc.get('asset_physical_id')}"
    )


def test_asset_physical_id_not_in_properties() -> None:
    """``asset_physical_id`` is an identity field stamped at the document root;
    it must NOT bleed into ``properties``."""
    _phys_uuid = "01920000-0000-7000-8000-000000000003"
    doc = _build_doc({"geoid": "g1", "asset_id": _phys_uuid})

    props = doc.get("properties", {})
    assert "asset_physical_id" not in props, (
        f"asset_physical_id must not leak into properties; props keys: {list(props)}"
    )


# ---------------------------------------------------------------------------
# 3-4. Mapping declarations
# ---------------------------------------------------------------------------


def test_item_mapping_declares_asset_physical_id_keyword() -> None:
    """``ITEM_MAPPING`` must declare ``asset_physical_id`` as a root-level
    ``keyword`` so terms filters on it are indexed (not silently unindexed)."""
    from dynastore.modules.elasticsearch.mappings import ITEM_MAPPING

    root_props = ITEM_MAPPING["properties"]
    assert root_props.get("asset_physical_id") == {"type": "keyword"}, (
        f"ITEM_MAPPING must declare asset_physical_id as keyword; "
        f"got {root_props.get('asset_physical_id')}"
    )


def test_item_mapping_does_not_declare_asset_id() -> None:
    """The mutable logical ``asset_id`` must NOT appear in ITEM_MAPPING.
    Only the immutable ``asset_physical_id`` UUID is stored in item _source."""
    from dynastore.modules.elasticsearch.mappings import ITEM_MAPPING

    assert "asset_id" not in ITEM_MAPPING["properties"], (
        f"asset_id must not be in ITEM_MAPPING; "
        f"found {ITEM_MAPPING['properties'].get('asset_id')}"
    )


def test_asset_mapping_declares_asset_physical_id_keyword() -> None:
    """``ASSET_MAPPING`` must declare ``asset_physical_id`` as a ``keyword``
    for the assets-index visibility filter."""
    from dynastore.modules.elasticsearch.mappings import ASSET_MAPPING

    props = ASSET_MAPPING["properties"]
    assert props.get("asset_physical_id") == {"type": "keyword"}, (
        f"ASSET_MAPPING must declare asset_physical_id as keyword; "
        f"got {props.get('asset_physical_id')}"
    )


# ---------------------------------------------------------------------------
# 6. Assets visibility filter translates logical → physical
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_assets_visibility_filter_uses_asset_physical_id() -> None:
    """When ``resolve_asset_listing_ids`` returns a non-None visible set,
    ``search_assets`` must translate each logical id → physical_id via
    ``AssetsProtocol.resolve_asset_physical_id`` and emit a ``terms`` clause
    on ``asset_physical_id``, NOT on ``asset_id``."""
    from dynastore.modules.storage.drivers.elasticsearch import AssetElasticsearchDriver

    driver = AssetElasticsearchDriver.__new__(AssetElasticsearchDriver)

    _logical_id = "my-asset"
    _physical_id = "01920000-0000-7000-8000-000000000099"

    captured_queries: list = []

    async def _fake_search(index, query, size, from_):
        captured_queries.append(query)
        return {"hits": {"hits": []}}

    es_mock = AsyncMock()
    es_mock.search = _fake_search
    es_mock.indices = AsyncMock()
    es_mock.indices.exists = AsyncMock(return_value=True)

    _mock_assets = AsyncMock()
    _mock_assets.resolve_asset_physical_id = AsyncMock(return_value=_physical_id)

    with (
        patch(
            "dynastore.modules.storage.drivers.elasticsearch._es_client_required",
            return_value=es_mock,
        ),
        patch.object(
            driver, "_asset_index_name",
            new=AsyncMock(return_value="test-assets-cat1"),
        ),
        # Patch the visibility helper at its canonical module path (it is
        # imported via ``from ... import`` inside the function body, so the
        # mock must live on the source module, not the caller's namespace).
        patch(
            "dynastore.models.protocols.visibility.resolve_asset_listing_ids",
            new=AsyncMock(return_value=frozenset({_logical_id})),
        ),
        patch(
            "dynastore.modules.storage.drivers.elasticsearch.build_es_query",
            return_value={"match_all": {}},
        ),
        # get_protocol is also imported inside the function body; patch at
        # its canonical home in dynastore.tools.discovery so the local alias
        # receives the mock.
        patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=_mock_assets,
        ),
    ):
        await driver.search_assets("cat1", collection_id=None)

    assert captured_queries, "ES search was never called"
    query = captured_queries[0]

    # Verify a terms filter is present on asset_physical_id.
    filter_clauses = query.get("bool", {}).get("filter", [])
    terms_clause = next(
        (c for c in filter_clauses if "terms" in c), None
    )
    assert terms_clause is not None, (
        f"Expected a terms filter clause in query; filter={filter_clauses}, query={query}"
    )
    assert "asset_physical_id" in terms_clause["terms"], (
        f"terms clause must be on asset_physical_id; got {terms_clause}"
    )
    assert _physical_id in terms_clause["terms"]["asset_physical_id"], (
        f"physical_id must appear in the terms values; got {terms_clause}"
    )
    assert _logical_id not in terms_clause["terms"]["asset_physical_id"], (
        f"logical id must NOT appear in the terms values (must be translated); "
        f"got {terms_clause}"
    )
