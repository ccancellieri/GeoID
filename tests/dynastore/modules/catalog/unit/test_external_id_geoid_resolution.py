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

"""Unit coverage for ``ItemQueryMixin._resolve_external_ids_to_geoids`` (#2314).

The wire surface may carry external/producer ids that are NOT unique within a
collection — the same external id repeats to represent successive versions of a
feature, each with its own validity. ``feature_id_field_name`` is only that
external representation; internally the sidecars are always joinable by
``geoid``, the unique physical id. This resolver turns external ids into their
geoid(s) so the caller can match on ``h.geoid`` everywhere else, which:

  - removes the dynamic identifier from the bbox match (no more
    ``{alias}.{ext_field}`` interpolation), and
  - de-ambiguates versioned collections (an external id mapping to many rows).

These tests verify:
  (a) The resolution query selects DISTINCT ``s.geoid`` from the sidecar table
      and matches the (validated) external-id column with a bound ``ANY`` param.
  (b) When the sidecar manages validity, the query filters to the currently
      valid version (``validity @> NOW()``); otherwise it does not.
  (c) The external-id column name is validated before interpolation — a
      non-identifier value raises rather than reaching the SQL string.
  (d) Empty ``ext_ids`` short-circuits with no DB call.
  (e) The resolved geoids are returned as strings.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from dynastore.modules.catalog.item_service import ItemService
from dynastore.tools.db import InvalidIdentifierError


_UUID_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_UUID_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
_GUID_A = "GUID_FFF9E566-1A2B-3C4D-5E6F-7A8B9C0D1E2F"


class _FakeSidecar:
    def __init__(
        self,
        *,
        sidecar_id: str = "attributes",
        has_validity: bool = False,
        validity_column: Optional[str] = None,
    ):
        self.sidecar_id = sidecar_id
        self.has_validity = has_validity
        self.validity_column = validity_column


def _make_svc() -> "ItemService":
    return ItemService.__new__(ItemService)


def _patch_dql(return_geoids: List[Any]):
    """Patch ``DQLQuery`` in the item_query namespace to capture the SQL and
    params, returning ``return_geoids`` from ``execute``."""
    import dynastore.modules.catalog.item_query as iq

    captured: Dict[str, Any] = {}

    class _CapturingDQL:
        def __init__(self, sql, *_a, **_k):
            captured["sql"] = sql

        async def execute(self, _conn, **params):
            captured["params"] = params
            return list(return_geoids)

    return patch.object(iq, "DQLQuery", _CapturingDQL), captured


# ---------------------------------------------------------------------------
# (d) Empty ext_ids → no DB
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_ext_ids_returns_empty_without_db():
    svc = _make_svc()
    patcher, captured = _patch_dql([_UUID_A])
    with patcher:
        result = await svc._resolve_external_ids_to_geoids(
            object(), "public", "items_cat_col", _FakeSidecar(), "external_id", [],
        )
    assert result == []
    assert "sql" not in captured  # DQLQuery never constructed


# ---------------------------------------------------------------------------
# (a) + (e) Resolves to geoid, selects DISTINCT geoid, binds ANY param
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resolves_external_ids_to_geoids():
    svc = _make_svc()
    patcher, captured = _patch_dql([_UUID_A, _UUID_B])
    with patcher:
        result = await svc._resolve_external_ids_to_geoids(
            object(), "public", "items_cat_col",
            _FakeSidecar(sidecar_id="attributes"), "external_id", [_GUID_A],
        )

    # Returns the resolved geoids as strings.
    assert result == [_UUID_A, _UUID_B]

    sql = captured["sql"]
    # Selects the immutable geoid, DISTINCT, from the sidecar physical table.
    assert "DISTINCT s.geoid" in sql
    assert '"public"."items_cat_col_attributes"' in sql
    # Matches the external-id column via a bound ANY param (never the value
    # interpolated, never the geoid column for the predicate).
    assert "s.external_id = ANY(:_ext_ids)" in sql
    # External ids are bound, not interpolated.
    assert captured["params"]["_ext_ids"] == [_GUID_A]


# ---------------------------------------------------------------------------
# (b) Validity filter only when the sidecar manages validity
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_versioned_sidecar_filters_to_valid_version():
    svc = _make_svc()
    patcher, captured = _patch_dql([_UUID_A])
    sc = _FakeSidecar(has_validity=True, validity_column="validity")
    with patcher:
        await svc._resolve_external_ids_to_geoids(
            object(), "public", "items_cat_col", sc, "external_id", [_GUID_A],
        )
    sql = captured["sql"]
    assert "s.validity @> NOW()" in sql


@pytest.mark.asyncio
async def test_non_versioned_sidecar_has_no_validity_filter():
    svc = _make_svc()
    patcher, captured = _patch_dql([_UUID_A])
    sc = _FakeSidecar(has_validity=False, validity_column=None)
    with patcher:
        await svc._resolve_external_ids_to_geoids(
            object(), "public", "items_cat_col", sc, "external_id", [_GUID_A],
        )
    sql = captured["sql"]
    assert "NOW()" not in sql
    assert "validity" not in sql


# ---------------------------------------------------------------------------
# (c) Identifier validation before interpolation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_malicious_field_name_is_rejected():
    """A non-identifier ``feature_id_field_name`` must raise (validated before
    it can reach the SQL string), never silently interpolate."""
    svc = _make_svc()
    patcher, captured = _patch_dql([_UUID_A])
    with patcher:
        with pytest.raises(InvalidIdentifierError):
            await svc._resolve_external_ids_to_geoids(
                object(), "public", "items_cat_col", _FakeSidecar(),
                "external_id = '' OR 1=1); DROP TABLE items; --", [_GUID_A],
            )
    # Never reached the query.
    assert "sql" not in captured


@pytest.mark.asyncio
async def test_malicious_sidecar_id_is_rejected():
    svc = _make_svc()
    patcher, captured = _patch_dql([_UUID_A])
    sc = _FakeSidecar(sidecar_id="attributes; DROP TABLE items")
    with patcher:
        with pytest.raises(InvalidIdentifierError):
            await svc._resolve_external_ids_to_geoids(
                object(), "public", "items_cat_col", sc, "external_id", [_GUID_A],
            )
    assert "sql" not in captured
