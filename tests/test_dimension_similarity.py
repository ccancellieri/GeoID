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

"""Unit tests for the dimension Similarity search (pg_trgm) default.

Covers the pieces testable without a live Postgres:

  * ``build_similarity_query`` emits a well-formed pg_trgm ranking query
    (``similarity()`` score, ``%`` filter, ``ORDER BY score DESC``) with the
    user query carried as a bound parameter and identifiers quoted.
  * ``build_index_ddl`` emits an idempotent GIN trigram index on the member
    label column (``gin_trgm_ops``).
  * The extension ``search_route`` flips ``?similar=`` from the upstream
    501/400 stub to provider dispatch (mocking the DB-backed search), and
    delegates non-similar params to the upstream search implementation.

DB-level ranking (actual trigram scores) is integration-tested against a
Postgres with the pg_trgm migration applied — out of scope for unit tests.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

# Load similarity.py standalone, bypassing the dimensions package __init__
# (which eagerly imports heavy optional modules). The functions under test
# import their dependencies lazily and have no import-time side effects.
_SIM_PATH = (
    Path(__file__).resolve().parents[1]
    / "packages/extensions/dimensions/src/dynastore/extensions/dimensions/similarity.py"
)
_spec = importlib.util.spec_from_file_location("_dim_similarity_under_test", _SIM_PATH)
assert _spec and _spec.loader
similarity = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(similarity)


# ---------------------------------------------------------------------------
# build_similarity_query
# ---------------------------------------------------------------------------


def test_build_similarity_query_attr_table_name():
    _, attr_table = similarity.build_similarity_query("dims_schema", "t_members")
    assert attr_table == "t_members_attributes"


def test_build_similarity_query_uses_pg_trgm_ranking():
    sql, _ = similarity.build_similarity_query("dims_schema", "t_members")
    # Trigram similarity score + ordering by it descending.
    assert "similarity(s.attributes->>'title', :q) AS score" in sql
    assert "ORDER BY score DESC" in sql
    # The `%` operator is the indexable trigram-match filter.
    assert "s.attributes->>'title' % :q" in sql


def test_build_similarity_query_binds_user_input():
    sql, _ = similarity.build_similarity_query("dims_schema", "t_members")
    # The free-text query is ALWAYS a bound parameter, never interpolated.
    assert ":q" in sql
    assert ":threshold" in sql
    assert ":limit" in sql


def test_build_similarity_query_quotes_identifiers():
    sql, _ = similarity.build_similarity_query("dims_schema", "t_members")
    assert '"dims_schema"."t_members"' in sql
    assert '"dims_schema"."t_members_attributes"' in sql
    # Hub joined to attributes sidecar on geoid.
    assert "h.geoid = s.geoid" in sql
    # Soft-deleted members excluded.
    assert "h.deleted_at IS NULL" in sql


def test_build_similarity_query_custom_label_key():
    sql, _ = similarity.build_similarity_query(
        "dims_schema", "t_members", label_key="name", code_key="code",
    )
    assert "s.attributes->>'name'" in sql
    assert "s.attributes->>'code' AS id" in sql


# ---------------------------------------------------------------------------
# build_index_ddl
# ---------------------------------------------------------------------------


def test_build_index_ddl_is_idempotent_gin_trgm():
    ddl = similarity.build_index_ddl("dims_schema", "t_members")
    assert "CREATE INDEX IF NOT EXISTS" in ddl
    assert "USING gin" in ddl
    assert "gin_trgm_ops" in ddl
    # Index on the attributes sidecar's label expression.
    assert '"dims_schema"."t_members_attributes"' in ddl
    assert "(attributes->>'title')" in ddl


def test_build_index_ddl_index_name_is_table_scoped():
    ddl = similarity.build_index_ddl("dims_schema", "t_members")
    # Per-table index name avoids collisions across materialized dimensions.
    assert '"t_members_attr_title_trgm_idx"' in ddl


# ---------------------------------------------------------------------------
# Migration well-formedness
# ---------------------------------------------------------------------------


def test_pg_trgm_extension_in_bootstrap():
    """The extension bootstrap enables pg_trgm alongside the other extensions."""
    from dynastore.modules.db_config.tools import BASE_DB_EXTENSIONS

    assert "pg_trgm" in BASE_DB_EXTENSIONS




# ---------------------------------------------------------------------------
# Route dispatch — requires the optional ogc_dimensions package
# ---------------------------------------------------------------------------


def _load_extension_module():
    pytest.importorskip(
        "ogc_dimensions",
        reason="dimensions extension requires the extension_dimensions extra",
    )
    from dynastore.extensions.dimensions import dimensions_extension as ext
    return ext


class _FakeURL:
    def remove_query_params(self, keys=None):
        return "http://testserver/dimensions/world-admin/search"

    def __str__(self):
        return "http://testserver/dimensions/world-admin/search"


class _FakeRequest:
    def __init__(self):
        self.url = _FakeURL()
        self.query_params = {"similar": "brazil"}


@pytest.mark.asyncio
async def test_search_route_similar_dispatches_to_pg_trgm():
    ext = _load_extension_module()
    ranked = [
        {"id": "BRA", "name": "Brazil", "score": 0.9},
        {"id": "BRB", "name": "Barbados", "score": 0.4},
    ]
    with patch.object(ext, "search_similar", new=AsyncMock(return_value=ranked)) as m:
        body = await ext.search_route(_FakeRequest(), "world-admin", similar="brazil")

    m.assert_awaited_once()
    assert body["type"] == "FeatureCollection"
    assert body["numberReturned"] == 2
    assert [f["id"] for f in body["features"]] == ["BRA", "BRB"]
    # Ranked: similarity score carried through, descending.
    scores = [f["properties"]["dimension:similarity"] for f in body["features"]]
    assert scores == [0.9, 0.4]
    assert body["features"][0]["properties"]["title"] == "Brazil"
    assert body["features"][0]["geometry"] is None


@pytest.mark.asyncio
async def test_search_route_without_similar_delegates_upstream():
    ext = _load_extension_module()
    sentinel = {"type": "FeatureCollection", "features": [], "delegated": True}
    with patch.object(
        ext, "_upstream_search", new=AsyncMock(return_value=sentinel)
    ) as up, patch.object(ext, "search_similar", new=AsyncMock()) as sim:
        out = await ext.search_route(
            _FakeRequest(), "world-admin", similar=None, exact="BRA",
        )

    sim.assert_not_awaited()
    up.assert_awaited_once()
    assert out is sentinel


def test_similarity_feature_shape():
    ext = _load_extension_module()
    feat = ext._similarity_feature({"id": "BRA", "name": "Brazil", "score": 0.75})
    assert feat["type"] == "Feature"
    assert feat["id"] == "BRA"
    assert feat["geometry"] is None
    assert feat["properties"]["title"] == "Brazil"
    assert feat["properties"]["dimension:similarity"] == 0.75
    assert feat["properties"]["recordType"] == "dimension-member"
