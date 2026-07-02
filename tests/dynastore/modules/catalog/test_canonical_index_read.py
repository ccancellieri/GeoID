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

"""Unit tests for :func:`read_canonical_index_inputs` (Task 4, #1800).

These are pure-unit tests — they mock out the DB read path and sidecar
resolution so no PG instance is required.

Contracts verified:
  - Returns a ``CanonicalIndexInput`` per geoid.
  - ``row`` carries all sidecar columns (area / centroid / hashes / validity).
  - ``resolved_sidecars`` is non-empty (sidecars resolved from col_config).
  - ``user_properties`` contains only user attrs — no stats keys (area,
    centroid, s2_*, h3_*, geohash_*) and no SYSTEM_FIELD_KEYS.
  - ``geometry`` is present and is a plain dict (not a Pydantic object).
  - ``access`` is ``None`` (not yet wired).
  - Missing geoid → absent from result dict.
  - No read-policy applied: ``id`` in the returned row is the geoid, not
    external_id.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

_SYSTEM_KEYS = frozenset(SYSTEM_FIELD_KEYS)


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _FakeSidecar:
    """Minimal sidecar stub that produces one stats computed value.

    Declares its computed names as internal columns so they are excluded
    from the JSONB-mode attributes fallback in ``map_row_to_feature``.
    """

    def __init__(self, sidecar_id: str, computed: Dict[str, Any]):
        self.sidecar_id = sidecar_id
        self._computed = computed

    def get_internal_columns(self) -> set:
        # Declare all computed keys as internal so the attributes sidecar
        # does not accidentally merge them into user_properties.
        return set(self._computed) | {"geom", "geom_stats", "centroid"}

    def map_row_to_feature(self, row, feature, context=None) -> None:
        """Populate geometry from a 'geom' key in row."""
        geom = row.get("geom")
        if geom and isinstance(geom, dict):
            feature.geometry = geom
        if feature.properties is None:
            feature.properties = {}

    def producible_computed_names(self) -> set:
        return set(self._computed)

    def resolve_computed_value(self, row: Dict[str, Any], name: str):
        if name in self._computed:
            return True, self._computed.get(name)
        return False, None


class _FakeAttrSidecar:
    """Attributes sidecar stub that injects user properties."""

    sidecar_id = "attributes"

    def get_internal_columns(self) -> set:
        return {"geoid", "external_id", "attributes", "asset_id"}

    def map_row_to_feature(self, row, feature, context=None) -> None:
        attrs_raw = row.get("attributes", "{}")
        if isinstance(attrs_raw, str):
            try:
                attrs = json.loads(attrs_raw)
            except Exception:
                attrs = {}
        elif isinstance(attrs_raw, dict):
            attrs = dict(attrs_raw)
        else:
            attrs = {}
        if feature.properties is None:
            feature.properties = {}
        feature.properties.update(attrs)

    def producible_computed_names(self) -> set:
        return set()

    def resolve_computed_value(self, row, name):
        return False, None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_raw_row(
    geoid: str = "test-geoid-1234",
    external_id: str = "EXT-001",
    area: float = 1234.5,
    attributes: Optional[Dict] = None,
    geometry_hash: str = "abc123",
    attributes_hash: str = "def456",
    validity: str = "[2024-01-01,)",
    transaction_time: str = "2026-01-01T00:00:00Z",
) -> Dict[str, Any]:
    attrs = attributes or {"NAME": "TestItem", "VALUE": 42}
    geom = {"type": "Point", "coordinates": [12.5, 41.9]}
    return {
        "geoid": geoid,
        "external_id": external_id,
        "area": area,
        "centroid": "0101000020E6100000...",  # WKB hex stub
        "geometry_hash": geometry_hash,
        "attributes_hash": attributes_hash,
        "validity": validity,
        "transaction_time": transaction_time,
        "attributes": json.dumps(attrs),
        "geom": geom,
    }


def _make_col_config():
    """Minimal ItemsPostgresqlDriverConfig stub with empty sidecars list."""
    cfg = MagicMock()
    cfg.sidecars = []
    cfg.collection_type = "VECTOR"
    return cfg


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_returns_input_per_geoid():
    """read_canonical_index_inputs returns one entry per requested geoid."""
    from dynastore.modules.catalog.canonical_index_read import read_canonical_index_inputs

    raw_row = _make_raw_row(geoid="gid-1")

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-1": raw_row}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            return_value=[_FakeSidecar("geom", {"area": 1234.5}), _FakeAttrSidecar()],
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-1"])

    assert "gid-1" in result
    entry = result["gid-1"]
    assert entry.row["geoid"] == "gid-1"


@pytest.mark.asyncio
async def test_row_carries_sidecar_stats_columns():
    """The raw row includes sidecar-derived columns like area/centroid/hashes."""
    from dynastore.modules.catalog.canonical_index_read import read_canonical_index_inputs

    raw_row = _make_raw_row(geoid="gid-2", area=9999.0)

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-2": raw_row}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            return_value=[_FakeSidecar("geom", {"area": 9999.0}), _FakeAttrSidecar()],
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-2"])

    entry = result["gid-2"]
    assert entry.row.get("area") == 9999.0
    assert "centroid" in entry.row
    assert "geometry_hash" in entry.row
    assert "attributes_hash" in entry.row
    assert "validity" in entry.row


@pytest.mark.asyncio
async def test_resolved_sidecars_non_empty():
    """resolved_sidecars must be non-empty for a VECTOR collection."""
    from dynastore.modules.catalog.canonical_index_read import read_canonical_index_inputs

    raw_row = _make_raw_row(geoid="gid-3")
    fake_sidecars = [_FakeSidecar("geom", {"area": 1.0}), _FakeAttrSidecar()]

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-3": raw_row}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            return_value=fake_sidecars,
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-3"])

    entry = result["gid-3"]
    assert len(entry.resolved_sidecars) > 0


@pytest.mark.asyncio
async def test_user_properties_no_stats_or_system_keys():
    """user_properties must not contain any SYSTEM_FIELD_KEYS or stats names."""
    from dynastore.modules.catalog.canonical_index_read import read_canonical_index_inputs

    attrs = {"NAME": "Nairobi", "population": 4_922_000}
    raw_row = _make_raw_row(geoid="gid-4", attributes=attrs)
    # Sidecar produces area as a stats value — must NOT appear in user_properties.
    fake_sidecars = [_FakeSidecar("geom", {"area": 12345.0}), _FakeAttrSidecar()]

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-4": raw_row}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            return_value=fake_sidecars,
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-4"])

    entry = result["gid-4"]
    props = entry.user_properties or {}
    # User attrs present.
    assert props.get("NAME") == "Nairobi"
    assert props.get("population") == 4_922_000
    # No SYSTEM_FIELD_KEYS in user_properties.
    for key in SYSTEM_FIELD_KEYS:
        assert key not in props, f"SYSTEM_FIELD_KEY '{key}' leaked into user_properties"
    # No stats keys.
    assert "area" not in props


@pytest.mark.asyncio
async def test_geometry_present_as_dict():
    """geometry is returned as a plain dict, not a Pydantic geometry object."""
    from dynastore.modules.catalog.canonical_index_read import read_canonical_index_inputs

    raw_row = _make_raw_row(geoid="gid-5")

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-5": raw_row}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            return_value=[_FakeSidecar("geom", {}), _FakeAttrSidecar()],
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-5"])

    entry = result["gid-5"]
    assert entry.geometry is not None
    assert isinstance(entry.geometry, dict)
    assert entry.geometry.get("type") == "Point"


@pytest.mark.asyncio
async def test_missing_geoid_absent_from_result():
    """A geoid with no matching DB row does not appear in the result dict."""
    from dynastore.modules.catalog.canonical_index_read import read_canonical_index_inputs

    # Only gid-6 has a row; gid-MISSING has none.
    raw_row = _make_raw_row(geoid="gid-6")

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-6": raw_row}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            return_value=[_FakeSidecar("geom", {}), _FakeAttrSidecar()],
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-6", "gid-MISSING"])

    assert "gid-6" in result
    assert "gid-MISSING" not in result


@pytest.mark.asyncio
async def test_access_is_none():
    """access field is always None (not yet wired in Task 4)."""
    from dynastore.modules.catalog.canonical_index_read import read_canonical_index_inputs

    raw_row = _make_raw_row(geoid="gid-7")

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-7": raw_row}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            return_value=[_FakeSidecar("geom", {}), _FakeAttrSidecar()],
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-7"])

    assert result["gid-7"].access is None


@pytest.mark.asyncio
async def test_no_read_policy_applied_id_stays_geoid():
    """No read-policy: id in row stays as geoid, not flipped to external_id."""
    from dynastore.modules.catalog.canonical_index_read import read_canonical_index_inputs

    raw_row = _make_raw_row(geoid="gid-8", external_id="EXTERNAL-ID-XYZ")

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-8": raw_row}),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            return_value=[_FakeSidecar("geom", {}), _FakeAttrSidecar()],
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=_make_col_config()),
        ),
    ):
        result = await read_canonical_index_inputs("cat1", "col1", ["gid-8"])

    entry = result["gid-8"]
    # Row's geoid must be used, not external_id.
    assert entry.row["geoid"] == "gid-8"
    # user_properties must not contain external_id (it's a SYSTEM key).
    props = entry.user_properties or {}
    assert "external_id" not in props


# ---------------------------------------------------------------------------
# #2731 — _fetch_raw_rows must propagate re-read failures, never swallow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_raw_rows_propagates_query_execution_failure():
    """A DB error while running the batched SELECT must propagate out of
    ``_fetch_raw_rows``, not be swallowed into ``{}``.

    Swallowing here made every geoid in the batch look identical to a
    genuinely deleted row — the drain then classified the whole batch
    auto_done with zero logging (#2731, ~5200 items silently dropped from
    the ES index on 2026-07-02).
    """
    from contextlib import asynccontextmanager

    from dynastore.modules.catalog.canonical_index_read import _fetch_raw_rows

    fake_conn = MagicMock()
    fake_conn.execute = MagicMock(side_effect=RuntimeError("simulated connection error"))

    @asynccontextmanager
    async def _fake_managed_transaction(_resource):
        yield fake_conn

    fake_col_config = MagicMock()

    with (
        patch(
            "dynastore.modules.db_config.query_executor.managed_transaction",
            new=_fake_managed_transaction,
        ),
        patch(
            "dynastore.modules.catalog.item_service.ItemService._resolve_physical_schema",
            new=AsyncMock(return_value="public"),
        ),
        patch(
            "dynastore.modules.catalog.item_service.ItemService._resolve_physical_table",
            new=AsyncMock(return_value="items_col1"),
        ),
        patch(
            "dynastore.modules.catalog.item_service.ItemService._apply_query_transformations",
            new=AsyncMock(return_value=("SELECT 1", {})),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_db_engine",
            return_value=MagicMock(),
        ),
    ):
        with pytest.raises(RuntimeError, match="simulated connection error"):
            await _fetch_raw_rows(
                "cat1", "col1", ["gid-err"], fake_col_config, db_resource=MagicMock(),
            )


@pytest.mark.asyncio
async def test_read_canonical_index_inputs_propagates_fetch_raw_rows_failure():
    """``read_canonical_index_inputs`` must not fold a ``_fetch_raw_rows``
    failure into an empty result — callers must see the failure and retry,
    never treat it as "rows deleted"."""
    from dynastore.modules.catalog.canonical_index_read import read_canonical_index_inputs

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(side_effect=RuntimeError("simulated re-read failure")),
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._resolve_sidecars_for",
            return_value=[],
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_col_config",
            new=AsyncMock(return_value=MagicMock()),
        ),
    ):
        with pytest.raises(RuntimeError, match="simulated re-read failure"):
            await read_canonical_index_inputs("cat1", "col1", ["gid-err"])
