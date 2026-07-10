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

"""Unit tests for the drain-time access-envelope recompute (#2687).

Covers ``_resolve_access_context`` / ``_apply_access_envelope`` directly
(pure, no DB) plus one end-to-end check that
``read_canonical_index_inputs`` wires ``CanonicalIndexInput.access``
correctly for an access-aware collection and leaves it ``None`` for an
ordinary one.
"""
from __future__ import annotations

import json
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.canonical_index_read import (
    _apply_access_envelope,
    _resolve_access_context,
)


# ---------------------------------------------------------------------------
# _resolve_access_context
# ---------------------------------------------------------------------------


async def test_context_not_access_aware_short_circuits():
    with patch(
        "dynastore.modules.storage.access_envelope.collection_uses_access_aware_driver",
        new=AsyncMock(return_value=False),
    ):
        is_access_aware, visibility, attrs_paths = await _resolve_access_context("c", "col")
    assert is_access_aware is False
    assert visibility is None
    assert attrs_paths == {}


async def test_context_access_aware_resolves_visibility_and_paths():
    with (
        patch(
            "dynastore.modules.storage.access_envelope.collection_uses_access_aware_driver",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.modules.storage.access_envelope.resolve_catalog_visibility",
            new=AsyncMock(return_value="public"),
        ),
        patch(
            "dynastore.modules.storage.access_envelope.resolve_attribute_stamping_paths",
            new=AsyncMock(return_value={"dept": "$.properties.department"}),
        ),
    ):
        is_access_aware, visibility, attrs_paths = await _resolve_access_context("c", "col")
    assert is_access_aware is True
    assert visibility == "public"
    assert attrs_paths == {"dept": "$.properties.department"}


async def test_context_detection_error_degrades_to_not_access_aware():
    with patch(
        "dynastore.modules.storage.access_envelope.collection_uses_access_aware_driver",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ):
        is_access_aware, visibility, attrs_paths = await _resolve_access_context("c", "col")
    assert is_access_aware is False
    assert visibility is None


async def test_context_recompute_error_keeps_access_aware_but_no_visibility():
    """A config-lookup failure AFTER detection must NOT silently flip
    ``is_access_aware`` to False — the drain's fail-closed guard
    (``StorageDrainTask._build_canonical_doc``) relies on
    ``visibility is None`` (not ``is_access_aware``) to detect "envelope
    recompute failed, retry"."""
    with (
        patch(
            "dynastore.modules.storage.access_envelope.collection_uses_access_aware_driver",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.modules.storage.access_envelope.resolve_catalog_visibility",
            new=AsyncMock(side_effect=RuntimeError("ConfigsProtocol unavailable")),
        ),
    ):
        is_access_aware, visibility, attrs_paths = await _resolve_access_context("c", "col")
    assert is_access_aware is True
    assert visibility is None
    assert attrs_paths == {}


# ---------------------------------------------------------------------------
# _apply_access_envelope
# ---------------------------------------------------------------------------


def test_apply_envelope_none_when_not_access_aware():
    row = {"geoid": "g1", "access_owner": "alice"}
    assert _apply_access_envelope(
        row, {}, is_access_aware=False, visibility="private", attrs_paths={},
    ) is None


def test_apply_envelope_none_when_visibility_missing():
    """Fail-closed signal: access-aware but visibility recompute failed."""
    row = {"geoid": "g1", "access_owner": "alice"}
    assert _apply_access_envelope(
        row, {}, is_access_aware=True, visibility=None, attrs_paths={},
    ) is None


def test_apply_envelope_owner_from_hub_row_column():
    row = {"geoid": "g1", "access_owner": "alice"}
    env = _apply_access_envelope(
        row, {}, is_access_aware=True, visibility="private", attrs_paths={},
    )
    assert env == {"_visibility": "private", "_owner": "alice"}


def test_apply_envelope_owner_none_when_column_null():
    row = {"geoid": "g1", "access_owner": None}
    env = _apply_access_envelope(
        row, {}, is_access_aware=True, visibility="public", attrs_paths={},
    )
    assert env == {"_visibility": "public", "_owner": None}


def test_apply_envelope_attrs_extracted_from_user_properties():
    row = {"geoid": "g1", "access_owner": "alice"}
    user_properties = {"department": "finance", "irrelevant": "x"}
    env = _apply_access_envelope(
        row, user_properties, is_access_aware=True, visibility="private",
        attrs_paths={"dept": "$.properties.department"},
    )
    assert env is not None
    assert env["_attrs"] == {"dept": "finance"}


def test_apply_envelope_no_attrs_key_when_paths_empty():
    row = {"geoid": "g1", "access_owner": "alice"}
    env = _apply_access_envelope(
        row, {"department": "finance"}, is_access_aware=True,
        visibility="private", attrs_paths={},
    )
    assert env is not None
    assert "_attrs" not in env


# ---------------------------------------------------------------------------
# read_canonical_index_inputs — end-to-end wiring
# ---------------------------------------------------------------------------


def _make_col_config():
    cfg = MagicMock()
    cfg.sidecars = []
    cfg.collection_type = "VECTOR"
    return cfg


def _raw_row(geoid: str, owner: Any = "alice") -> Dict[str, Any]:
    """Shaped like ``test_canonical_index_read._make_raw_row`` — the real
    (unmocked) ``ItemService.map_row_to_feature`` call inside
    ``_extract_feature_parts`` resolves the default VECTOR sidecar pipeline
    from ``col_config`` and expects these keys, plus the new
    ``access_owner`` hub column (#2687)."""
    return {
        "geoid": geoid,
        "access_owner": owner,
        "external_id": "EXT-001",
        "geometry_hash": "h1",
        "attributes_hash": "h2",
        "validity": "[2024-01-01,)",
        "transaction_time": "2026-01-01T00:00:00Z",
        "attributes": json.dumps({"NAME": "TestItem"}),
        "geom": {"type": "Point", "coordinates": [12.5, 41.9]},
    }


@pytest.mark.asyncio
async def test_read_canonical_index_inputs_populates_access_for_aware_collection():
    from dynastore.modules.catalog.canonical_index_read import (
        read_canonical_index_inputs,
    )

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-1": _raw_row("gid-1")}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
        patch(
            "dynastore.modules.storage.access_envelope.collection_uses_access_aware_driver",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.modules.storage.access_envelope.resolve_catalog_visibility",
            new=AsyncMock(return_value="private"),
        ),
        patch(
            "dynastore.modules.storage.access_envelope.resolve_attribute_stamping_paths",
            new=AsyncMock(return_value={}),
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-1"])

    assert result["gid-1"].access == {"_visibility": "private", "_owner": "alice"}


@pytest.mark.asyncio
async def test_read_canonical_index_inputs_leaves_access_none_for_ordinary_collection():
    from dynastore.modules.catalog.canonical_index_read import (
        read_canonical_index_inputs,
    )

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-1": _raw_row("gid-1", owner=None)}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
        patch(
            "dynastore.modules.storage.access_envelope.collection_uses_access_aware_driver",
            new=AsyncMock(return_value=False),
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-1"])

    assert result["gid-1"].access is None
