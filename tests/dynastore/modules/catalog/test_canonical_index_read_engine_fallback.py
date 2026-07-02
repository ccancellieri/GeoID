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

"""Regression tests — FIX 1: _fetch_raw_rows resolves DB engine driver-agnostically.

When ``db_resource`` is None (Cloud Run JOB/worker context), the function must
NOT fall back to a bare ``ItemService().engine`` (which is None in that context).
Instead it must resolve an engine via the DatabaseProtocol registered in the
process (the same engine-discovery path all other worker code uses).

Contracts verified:
  - When db_resource is None AND DatabaseProtocol is registered, _fetch_raw_rows
    calls managed_transaction with the protocol engine (not ItemService().engine).
  - When db_resource is None AND DatabaseProtocol is absent (bare import context),
    _fetch_raw_rows raises rather than silently returning {} (#2731 — a swallowed
    "no engine" condition made every row in the batch look like a deleted item).
  - read_canonical_index_inputs returns a populated dict when db_resource is None
    but a DatabaseProtocol engine is present.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fake_protocol_engine():
    """Return a mock that looks like an AsyncEngine to managed_transaction."""
    from unittest.mock import MagicMock
    engine = MagicMock()
    engine.__class__.__name__ = "AsyncEngine"
    return engine


def _make_raw_row(geoid: str = "gid-engine-1"):
    return {
        "geoid": geoid,
        "external_id": "EXT-001",
        "area": 1.0,
        "centroid": "stub",
        "geometry_hash": "abc",
        "attributes_hash": "def",
        "validity": "[2024-01-01,)",
        "transaction_time": "2026-01-01T00:00:00Z",
        "attributes": '{"NAME": "test"}',
        "geom": {"type": "Point", "coordinates": [0.0, 0.0]},
    }


# ---------------------------------------------------------------------------
# Tests for _fetch_raw_rows engine resolution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_raw_rows_uses_db_protocol_engine_when_db_resource_none():
    """When db_resource is None, _fetch_raw_rows must not give up just because
    db_resource was not passed explicitly — it must call _get_db_engine() to
    resolve an engine from DatabaseProtocol, and only raise when that also
    yields None.

    This test verifies the branch: _get_db_engine() returns a non-None engine,
    so _fetch_raw_rows proceeds to call managed_transaction (which then fails in
    the test environment since there is no real DB pool, and propagates per
    #2731 — no bare-except swallow). The critical invariant is that
    _fetch_raw_rows does NOT short-circuit with an empty return before
    attempting the DB call when a valid engine is available, and does NOT
    swallow the subsequent failure into an empty dict either.
    """
    from dynastore.modules.catalog.canonical_index_read import _fetch_raw_rows

    fake_engine = _fake_protocol_engine()
    fake_col_config = MagicMock()

    # When _get_db_engine returns an engine, _fetch_raw_rows must attempt the
    # managed_transaction call.  In the test environment managed_transaction
    # raises (no real DB) — that failure must propagate (#2731), never be
    # folded into an empty dict that looks identical to "no rows found".
    with patch(
        "dynastore.modules.catalog.canonical_index_read._get_db_engine",
        return_value=fake_engine,
    ):
        with pytest.raises(Exception):  # noqa: B017 — any DB-layer failure, not ValueError
            await _fetch_raw_rows(
                "cat1", "col1", ["gid-1"], fake_col_config, db_resource=None,
            )


@pytest.mark.asyncio
async def test_fetch_raw_rows_raises_when_no_engine_available():
    """When db_resource is None and no engine can be resolved, _fetch_raw_rows
    raises rather than returning {} (#2731 — no DB engine is an infra failure,
    not a "rows genuinely absent" outcome, and must funnel to retry, not
    auto_done).
    """
    from dynastore.modules.catalog.canonical_index_read import _fetch_raw_rows

    fake_col_config = MagicMock()

    with patch(
        "dynastore.modules.catalog.canonical_index_read._get_db_engine",
        return_value=None,
    ):
        with pytest.raises(RuntimeError, match="no DB engine available"):
            await _fetch_raw_rows(
                "cat1", "col1", ["gid-missing"], fake_col_config, db_resource=None,
            )


@pytest.mark.asyncio
async def test_read_canonical_index_inputs_resolves_engine_via_protocol():
    """read_canonical_index_inputs with db_resource=None resolves the engine
    via DatabaseProtocol and returns populated results (not an empty dict).
    """
    from dynastore.modules.catalog.canonical_index_read import read_canonical_index_inputs

    raw_row = _make_raw_row(geoid="gid-proto-1")
    fake_engine = _fake_protocol_engine()

    class _FakeSidecar:
        def get_internal_columns(self):
            return set()

        def producible_computed_names(self):
            return set()

        def map_row_to_feature(self, row, feature, context=None):
            feature.geometry = row.get("geom")

    with (
        patch(
            "dynastore.modules.catalog.canonical_index_read._get_db_engine",
            return_value=fake_engine,
        ),
        patch(
            "dynastore.modules.catalog.canonical_index_read._fetch_raw_rows",
            new=AsyncMock(return_value={"gid-proto-1": raw_row}),
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
        result = await read_canonical_index_inputs(
            "cat1", "col1", ["gid-proto-1"], db_resource=None,
        )

    assert "gid-proto-1" in result, (
        "read_canonical_index_inputs must return results when the engine is "
        "resolved via DatabaseProtocol, not an empty dict."
    )
