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

import pytest

from dynastore.models.ogc import Feature
from dynastore.modules.joins.executor import index_secondary, run_join
from dynastore.modules.joins.models import (
    JoinRequest,
    JoinSpec,
    NamedSecondarySpec,
    PagingSpec,
    ProjectionSpec,
)


def _feat(fid, **props):
    return Feature(type="Feature", id=fid, geometry=None, properties=props)


async def _astream(items):
    for it in items:
        yield it


def _req(**overrides):
    base = dict(
        secondary=NamedSecondarySpec(ref="x"),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    base.update(overrides)
    return JoinRequest(**base)


@pytest.mark.asyncio
async def test_inner_join_drops_unmatched_features():
    primary = _astream([
        _feat("1", uid="a"),
        _feat("2", uid="b"),
        _feat("3", uid="missing"),
    ])
    secondary = {"a": {"user_id": "a", "score": 1}, "b": {"user_id": "b", "score": 2}}
    out = [f async for f in run_join(_req(), primary_stream=primary, secondary_index=secondary)]
    assert [f.id for f in out] == ["1", "2"]
    assert out[0].properties["score"] == 1


@pytest.mark.asyncio
async def test_enrichment_false_passes_features_unmodified():
    primary = _astream([_feat("1", uid="a")])
    secondary = {"a": {"user_id": "a", "score": 99}}
    req = _req(join=JoinSpec(primary_column="uid", secondary_column="user_id", enrichment=False))
    out = [f async for f in run_join(req, primary_stream=primary, secondary_index=secondary)]
    assert "score" not in out[0].properties


@pytest.mark.asyncio
async def test_projection_attributes_are_filtered():
    primary = _astream([_feat("1", uid="a", color="red", size=10)])
    secondary = {"a": {"score": 1, "name": "alice"}}
    req = _req(projection=ProjectionSpec(with_geometry=False, attributes=["color", "score"]))
    out = [f async for f in run_join(req, primary_stream=primary, secondary_index=secondary)]
    assert set(out[0].properties.keys()) == {"uid", "color", "score"}  # join key kept
    assert out[0].geometry is None


@pytest.mark.asyncio
async def test_paging_limit_and_offset():
    primary = _astream([_feat(str(i), uid=str(i)) for i in range(10)])
    secondary = {str(i): {"user_id": str(i)} for i in range(10)}
    req = _req(paging=PagingSpec(limit=3, offset=2))
    out = [f async for f in run_join(req, primary_stream=primary, secondary_index=secondary)]
    assert [f.id for f in out] == ["2", "3", "4"]


@pytest.mark.asyncio
async def test_index_secondary_drops_rows_with_null_key():
    secondary = _astream([
        _feat("a", user_id="alpha", score=1),
        _feat("b", user_id=None, score=2),
        _feat("c", user_id="gamma", score=3),
    ])
    idx = await index_secondary(secondary, secondary_column="user_id")
    assert set(idx.keys()) == {"alpha", "gamma"}
    assert idx["alpha"]["score"] == 1


@pytest.mark.asyncio
async def test_run_join_matches_when_join_key_only_in_model_extra():
    """Regression for #1818: PG-path features arrive with join-column values
    only in model_extra (properties={}). run_join must match them after
    normalize_feature_attributes lifts model_extra into properties."""
    # Simulate PG-driver output: properties empty, join column in model_extra.
    # Use dict-spread so pyright does not flag unknown extra kwargs on Feature.
    primary = _astream([
        Feature(type="Feature", id="f1", geometry=None, properties={}, **{"uid": "a"}),
        Feature(type="Feature", id="f2", geometry=None, properties={}, **{"uid": "missing"}),
    ])
    secondary = {"a": {"user_id": "a", "score": 42}}
    out = [f async for f in run_join(_req(), primary_stream=primary, secondary_index=secondary)]
    # Only "f1" matches; "f2" is correctly dropped (inner-join semantics).
    assert [f.id for f in out] == ["f1"]
    assert (out[0].properties or {})["score"] == 42
    # The join key itself must be visible in the output properties too.
    assert (out[0].properties or {})["uid"] == "a"


# ---------------------------------------------------------------------------
# Sparse-join pagination tests (issue #2587)
#
# Each primary row whose uid is NOT in the secondary is a non-match (INNER
# join drops it).  With N=20 primary rows and matches at every 5th row
# (rows 0, 5, 10, 15 → 4 total), offset-paging on *matched* features must
# produce correct, gap-free pages regardless of match density.
# ---------------------------------------------------------------------------

def _sparse_primary(n: int, match_positions: set):
    """Build a list of primary Features; only rows at match_positions carry a uid in the secondary."""
    feats = []
    for i in range(n):
        uid = f"match_{i}" if i in match_positions else f"nomatch_{i}"
        feats.append(_feat(str(i), uid=uid))
    return feats


def _sparse_secondary(match_positions: set):
    """Build a secondary index for the matching rows only."""
    return {f"match_{i}": {"user_id": f"match_{i}", "score": i} for i in match_positions}


@pytest.mark.asyncio
async def test_sparse_inner_join_page1_fills_to_limit():
    """With a sparse primary (1-in-5 match rate), page 1 returns exactly `limit`
    matched features — not fewer — when the total match count >= limit.

    Before the fix, the service pre-capped the primary stream to offset+limit
    rows, which could skip matches beyond that window and under-fill the page.
    This test targets run_join in isolation: the caller (the service) now passes
    limit+1 as the paging limit to enable the peek; we verify that run_join
    respects that limit and returns the right items.
    """
    match_pos = {0, 5, 10, 15}  # 4 matches in 20 rows
    primary = _astream(_sparse_primary(20, match_pos))
    secondary = _sparse_secondary(match_pos)

    # Service would call run_join with peek paging (limit+1=3, offset=0).
    req = _req(paging=PagingSpec(limit=3, offset=0))
    out = [f async for f in run_join(req, primary_stream=primary, secondary_index=secondary)]

    assert len(out) == 3, "run_join must yield all 3 requested matches from a sparse primary"
    assert [f.properties["uid"] for f in out] == ["match_0", "match_5", "match_10"]


@pytest.mark.asyncio
async def test_sparse_inner_join_page2_no_gaps_and_no_duplicates():
    """Page 2 (offset=2 matched features) picks up exactly where page 1 left off.

    Service passes peek_paging(limit=3, offset=2); we expect matches at positions
    10 and 15 — no gap (match_5 must not appear) and no duplicate (match_0 must
    not appear).  With only 2 matches remaining, run_join returns 2 items.
    """
    match_pos = {0, 5, 10, 15}
    primary = _astream(_sparse_primary(20, match_pos))
    secondary = _sparse_secondary(match_pos)

    req = _req(paging=PagingSpec(limit=3, offset=2))
    out = [f async for f in run_join(req, primary_stream=primary, secondary_index=secondary)]

    assert len(out) == 2, "only 2 matches remain after skipping offset=2"
    assert [f.properties["uid"] for f in out] == ["match_10", "match_15"]
    # No gaps: match_5 is gone (was page 1), no duplicates: match_0 is gone too.


@pytest.mark.asyncio
async def test_sparse_inner_join_final_page_returns_empty():
    """When offset >= total matches, run_join yields nothing (empty final page)."""
    match_pos = {0, 5, 10, 15}
    primary = _astream(_sparse_primary(20, match_pos))
    secondary = _sparse_secondary(match_pos)

    req = _req(paging=PagingSpec(limit=3, offset=4))  # 4 = all 4 matches exhausted
    out = [f async for f in run_join(req, primary_stream=primary, secondary_index=secondary)]

    assert out == [], "past the last match: empty page"


@pytest.mark.asyncio
async def test_run_join_closes_primary_stream_on_early_stop():
    """run_join must call aclose() on the primary stream when it stops before
    exhausting it (limit reached), so asyncpg cursors are released promptly.
    """
    closed = False

    async def _tracked_stream():
        nonlocal closed
        try:
            for i in range(100):
                yield _feat(str(i), uid=str(i))
        finally:
            closed = True

    secondary = {str(i): {"user_id": str(i)} for i in range(100)}
    req = _req(paging=PagingSpec(limit=2, offset=0))

    out = [f async for f in run_join(req, primary_stream=_tracked_stream(), secondary_index=secondary)]

    assert len(out) == 2
    assert closed, "primary stream's finally block must run when run_join stops early"


@pytest.mark.asyncio
async def test_dense_join_paging_regression():
    """Dense (1:1) join with paging must still return the correct slice.

    This is a regression guard: the sparse-join fix must not break the
    common dense case where every primary row has a matching secondary row.
    """
    n = 10
    primary = _astream([_feat(str(i), uid=str(i)) for i in range(n)])
    secondary = {str(i): {"user_id": str(i), "val": i * 2} for i in range(n)}

    # Service peek paging: limit=4 (user limit=3 + 1 peek)
    req = _req(paging=PagingSpec(limit=4, offset=2))
    out = [f async for f in run_join(req, primary_stream=primary, secondary_index=secondary)]

    # Offset 2 means skip matches 0,1; then yield up to 4 (peek): matches 2,3,4,5
    assert [f.id for f in out] == ["2", "3", "4", "5"]
    assert out[0].properties["val"] == 4  # 2 * 2
