#    Copyright 2025 FAO
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

"""Chain-runner unit tests for :func:`upsert_asset`.

The integration fixture stack (catalog-readiness, full app lifespan, real PG
schema) is heavy for a logic-only test of policy dispatch. Instead we use a
fake :class:`DQLQuery` that records every SQL invocation, returns
pre-programmed rows, and lets us assert:

* the matcher chain is walked in order (first match wins, no probe runs
  after a hit),
* each ``AssetWriteConflictPolicy`` action dispatches the right SQL,
* ``NEW_VERSION`` archives the existing row before inserting,
* ``REFUSE_FAIL`` raises :class:`AssetSidecarRejectedError` with the
  matcher recorded in the exception,
* the metadata-field matcher resolves dot-paths into the JSONB column.

Stage 4.1 / 4.2 add the real-DB test that complements this one.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest

from dynastore.modules.catalog import asset_distributed as ad
from dynastore.modules.catalog.asset_distributed import (
    AssetSidecarRejectedError,
    Scope,
    upsert_asset,
)
from dynastore.modules.catalog.asset_service import (
    AssetCreate,
    AssetKind,
    AssetStatus,
    AssetTypeEnum,
    VirtualAssetCreate,
)
from dynastore.modules.catalog.write_policy_assets import (
    AssetIdentityMatcher,
    AssetsWritePolicy,
    AssetWriteConflictPolicy,
)


# ---------------------------------------------------------------------------
# Fake DQLQuery — records calls + returns scripted rows
# ---------------------------------------------------------------------------


class _Recorder:
    """Holds the script (SQL pattern → row) and the call log shared between
    every fake DQLQuery instance produced during a single test."""

    def __init__(self) -> None:
        self.script: List[Tuple[Callable[[str], bool], Optional[Dict[str, Any]]]] = []
        self.calls: List[Dict[str, Any]] = []

    def when(self, predicate: Callable[[str], bool], row: Optional[Dict[str, Any]]) -> None:
        self.script.append((predicate, row))


def _make_fake_dql(recorder: _Recorder) -> Any:
    class _FakeDQL:
        def __init__(self, sql: str, *, result_handler: Any = None) -> None:
            self.sql = sql
            self.result_handler = result_handler

        async def execute(self, conn: Any, **params: Any) -> Optional[Dict[str, Any]]:
            recorder.calls.append({"sql": self.sql, "params": dict(params)})
            for predicate, row in recorder.script:
                if predicate(self.sql):
                    # SELECT — return scripted row (or None for misses).
                    if self.sql.lstrip().upper().startswith("SELECT"):
                        return row
                    # INSERT … RETURNING / UPDATE … RETURNING — return the
                    # row the predicate ships (test-controlled echo).
                    if "RETURNING" in self.sql.upper():
                        return row
                    # UPDATE … (archive) — no return needed.
                    return None
            # Unmatched call: behave like a miss so probes never error out.
            if self.sql.lstrip().upper().startswith("SELECT"):
                return None
            if "RETURNING" in self.sql.upper():
                # INSERT path with no scripted row — synthesise a minimal
                # echo from the params (mirrors RETURNING * semantics).
                return {k: v for k, v in params.items()}
            return None

    return _FakeDQL


@pytest.fixture
def fake_dql(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    recorder = _Recorder()
    monkeypatch.setattr(ad, "DQLQuery", _make_fake_dql(recorder))
    return recorder


# Helper predicates ----------------------------------------------------------


def is_select(sql: str) -> bool:
    return sql.lstrip().upper().startswith("SELECT")


def is_select_by(field: str) -> Callable[[str], bool]:
    def _p(sql: str) -> bool:
        s = sql.upper()
        if not is_select(s):
            return False
        # Use lowercase field to avoid matching SQL keywords.
        return field.lower() in sql.lower() and "FROM" in s
    return _p


def is_update_metadata(sql: str) -> bool:
    s = sql.upper()
    return s.lstrip().startswith("UPDATE") and "SET METADATA" in s


def is_update_archive(sql: str) -> bool:
    s = sql.upper()
    return s.lstrip().startswith("UPDATE") and "STATUS = 'DELETED'" in s


def is_insert(sql: str) -> bool:
    return sql.lstrip().upper().startswith("INSERT")


# ---------------------------------------------------------------------------
# Common test fixtures
# ---------------------------------------------------------------------------


SCOPE = Scope(schema="ds_test", catalog_id="cat", collection_id="col")
EXISTING_ROW: Dict[str, Any] = {
    "asset_id": "alpha",
    "catalog_id": "cat",
    "collection_id": "col",
    "asset_type": "RASTER",
    "kind": "physical",
    "status": "active",
    "filename": "alpha.tif",
    "href": None,
    "uri": "gs://bucket/alpha.tif",
    "content_hash": "deadbeef",
    "size_bytes": 1024,
    "created_at": None,
    "updated_at": None,
    "metadata": {"version": 1, "iso19115": {"fileIdentifier": "URN:1"}},
    "owned_by": "gcs",
}


def physical(asset_id: str = "alpha", filename: str = "alpha.tif", **extra: Any) -> AssetCreate:
    return AssetCreate(
        asset_id=asset_id,
        filename=filename,
        asset_type=AssetTypeEnum.RASTER,
        kind=AssetKind.PHYSICAL,
        metadata=extra.pop("metadata", {"version": 2}),
        **extra,
    )


def virtual(asset_id: str = "v1", href: str = "https://example.org/x") -> VirtualAssetCreate:
    return VirtualAssetCreate(asset_id=asset_id, href=href, kind=AssetKind.VIRTUAL)


# ---------------------------------------------------------------------------
# REFUSE_FAIL — default action
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refuse_fail_on_asset_id_match(fake_dql: _Recorder) -> None:
    fake_dql.when(is_select_by("asset_id"), EXISTING_ROW)

    policy = AssetsWritePolicy()  # defaults: REFUSE_FAIL on [ASSET_ID, FILENAME]
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=object(), scope=SCOPE, payload=physical(), policy=policy)

    err = exc_info.value
    assert err.matcher == "asset_id"
    assert err.reason == "conflict"
    assert err.existing_id == "alpha"
    # Only one SELECT executed — first matcher in the chain hit.
    assert sum(1 for c in fake_dql.calls if is_select(c["sql"])) == 1


@pytest.mark.asyncio
async def test_refuse_fail_on_filename_match(fake_dql: _Recorder) -> None:
    # Two SELECTs in order: asset_id probe misses, filename probe hits.
    rows: List[Optional[Dict[str, Any]]] = [None, EXISTING_ROW]

    def make_dql() -> Any:
        class _DQL:
            def __init__(self, sql: str, *, result_handler: Any = None) -> None:
                self.sql = sql

            async def execute(self, conn: Any, **params: Any) -> Optional[Dict[str, Any]]:
                fake_dql.calls.append({"sql": self.sql, "params": dict(params)})
                if is_select(self.sql):
                    return rows.pop(0) if rows else None
                return None

        return _DQL

    # Replace the previously-installed fake.
    import dynastore.modules.catalog.asset_distributed as mod
    mod.DQLQuery = make_dql()

    policy = AssetsWritePolicy(
        on_conflict=AssetWriteConflictPolicy.REFUSE_FAIL,
        identity_matchers=[AssetIdentityMatcher.ASSET_ID, AssetIdentityMatcher.FILENAME],
    )
    payload = physical(asset_id="brand_new", filename="alpha.tif")
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=object(), scope=SCOPE, payload=payload, policy=policy)

    assert exc_info.value.matcher == "filename"
    # Two SELECTs — chain ran ASSET_ID then FILENAME.
    select_calls = [c for c in fake_dql.calls if is_select(c["sql"])]
    assert len(select_calls) == 2


# ---------------------------------------------------------------------------
# UPDATE — mutates metadata, no INSERT
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_on_asset_id_keeps_filename(fake_dql: _Recorder) -> None:
    updated_row = dict(EXISTING_ROW, metadata={"version": 99}, status="active")
    fake_dql.when(is_select_by("asset_id"), EXISTING_ROW)
    fake_dql.when(is_update_metadata, updated_row)

    policy = AssetsWritePolicy(on_conflict=AssetWriteConflictPolicy.UPDATE)
    payload = physical(metadata={"version": 99})
    result = await upsert_asset(conn=object(), scope=SCOPE, payload=payload, policy=policy)

    assert result.action == "updated"
    assert result.matcher_hit == AssetIdentityMatcher.ASSET_ID
    # An UPDATE was issued, no INSERT.
    assert any(is_update_metadata(c["sql"]) for c in fake_dql.calls)
    assert not any(is_insert(c["sql"]) for c in fake_dql.calls)


# ---------------------------------------------------------------------------
# REFUSE — silent skip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refuse_returns_existing_silently(fake_dql: _Recorder) -> None:
    fake_dql.when(is_select_by("asset_id"), EXISTING_ROW)

    policy = AssetsWritePolicy(on_conflict=AssetWriteConflictPolicy.REFUSE)
    result = await upsert_asset(
        conn=object(), scope=SCOPE, payload=physical(), policy=policy
    )

    assert result.action == "refused"
    assert result.row["asset_id"] == "alpha"
    assert not any(is_insert(c["sql"]) for c in fake_dql.calls)
    assert not any(is_update_metadata(c["sql"]) for c in fake_dql.calls)


# ---------------------------------------------------------------------------
# Metadata-field matcher (dot-path JSON lookup)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_metadata_field_matcher(fake_dql: _Recorder) -> None:
    # The probe builds `metadata #>> '{iso19115,fileIdentifier}'` SQL — match
    # by looking for the dot-translated path literal.
    def is_metadata_select(sql: str) -> bool:
        return is_select(sql) and "{iso19115,fileIdentifier}" in sql

    fake_dql.when(is_metadata_select, EXISTING_ROW)

    policy = AssetsWritePolicy(
        on_conflict=AssetWriteConflictPolicy.REFUSE_FAIL,
        identity_matchers=[AssetIdentityMatcher.METADATA_FIELD],
        metadata_match_path="iso19115.fileIdentifier",
    )
    payload = physical(metadata={"iso19115": {"fileIdentifier": "URN:1"}})
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=object(), scope=SCOPE, payload=payload, policy=policy)

    assert exc_info.value.matcher == "metadata_field"


# ---------------------------------------------------------------------------
# Chain ordering — first-match-wins
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chain_first_match_wins(fake_dql: _Recorder) -> None:
    """Chain [ASSET_ID, FILENAME, METADATA_FIELD]; only metadata matches.

    Asserts that the chain walks all three matchers in declared order until
    the metadata probe wins, AND that the rejection records ``matcher_hit ==
    "metadata_field"`` (not the cheaper matchers that ran before it).
    """
    fake_dql.when(
        lambda s: is_select(s) and "{iso19115,fileIdentifier}" in s,
        EXISTING_ROW,
    )

    policy = AssetsWritePolicy(
        on_conflict=AssetWriteConflictPolicy.REFUSE_FAIL,
        identity_matchers=[
            AssetIdentityMatcher.ASSET_ID,
            AssetIdentityMatcher.FILENAME,
            AssetIdentityMatcher.METADATA_FIELD,
        ],
        metadata_match_path="iso19115.fileIdentifier",
    )
    payload = physical(
        asset_id="something_else",
        filename="otherwise.tif",
        metadata={"iso19115": {"fileIdentifier": "URN:1"}},
    )
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=object(), scope=SCOPE, payload=payload, policy=policy)

    assert exc_info.value.matcher == "metadata_field"
    # Three SELECT probes: the first two missed, the third hit and stopped
    # the chain.
    select_calls = [c for c in fake_dql.calls if is_select(c["sql"])]
    assert len(select_calls) == 3


# ---------------------------------------------------------------------------
# NEW_VERSION — archives old row, inserts fresh one
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_new_version_archives_old(fake_dql: _Recorder) -> None:
    """NEW_VERSION on asset_id match: existing row → status='deleted',
    new row inserted with same asset_id.

    Asserts the SQL ordering: first the asset_id probe (NEW_VERSION uses
    the dedicated probe), then the archive UPDATE, then the INSERT.
    """
    new_row = dict(EXISTING_ROW, content_hash=None, status="active", metadata={"version": 2})
    fake_dql.when(is_select_by("asset_id"), EXISTING_ROW)
    fake_dql.when(is_insert, new_row)

    policy = AssetsWritePolicy(on_conflict=AssetWriteConflictPolicy.NEW_VERSION)
    payload = physical(metadata={"version": 2})
    result = await upsert_asset(
        conn=object(), scope=SCOPE, payload=payload, policy=policy
    )

    assert result.action == "new_version"
    sql_kinds: List[str] = []
    for c in fake_dql.calls:
        s = c["sql"]
        if is_select(s):
            sql_kinds.append("SELECT")
        elif is_update_archive(s):
            sql_kinds.append("ARCHIVE")
        elif is_insert(s):
            sql_kinds.append("INSERT")
        elif is_update_metadata(s):
            sql_kinds.append("UPDATE_META")

    # Expected ordering: probe → archive → insert.
    assert sql_kinds == ["SELECT", "ARCHIVE", "INSERT"], sql_kinds
    assert "UPDATE_META" not in sql_kinds


# ---------------------------------------------------------------------------
# No match → fresh INSERT in the requested initial_status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_match_inserts_pending_when_requested(fake_dql: _Recorder) -> None:
    fake_dql.when(is_insert, dict(EXISTING_ROW, status="pending"))

    policy = AssetsWritePolicy()
    result = await upsert_asset(
        conn=object(),
        scope=SCOPE,
        payload=physical(asset_id="brand_new"),
        policy=policy,
        initial_status=AssetStatus.PENDING,
    )

    assert result.action == "inserted_pending"
    insert_calls = [c for c in fake_dql.calls if is_insert(c["sql"])]
    assert len(insert_calls) == 1
    # Status param was forwarded as PENDING.
    assert insert_calls[0]["params"]["status"] == AssetStatus.PENDING.value


# ---------------------------------------------------------------------------
# REFUSE_RETURN — idempotent return-existing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refuse_return_echoes_existing(fake_dql: _Recorder) -> None:
    fake_dql.when(is_select_by("asset_id"), EXISTING_ROW)

    policy = AssetsWritePolicy(on_conflict=AssetWriteConflictPolicy.REFUSE_RETURN)
    result = await upsert_asset(
        conn=object(), scope=SCOPE, payload=physical(), policy=policy
    )

    assert result.action == "returned_existing"
    assert result.row["asset_id"] == "alpha"
    assert not any(is_insert(c["sql"]) for c in fake_dql.calls)


# ---------------------------------------------------------------------------
# Hash gating — UPDATE collapses to REFUSE_RETURN when content_hash unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hash_gating_collapses_update_to_refuse_return(
    fake_dql: _Recorder,
) -> None:
    fake_dql.when(is_select_by("asset_id"), EXISTING_ROW)

    policy = AssetsWritePolicy(
        on_conflict=AssetWriteConflictPolicy.UPDATE,
        skip_if_unchanged_content_hash=True,
    )
    # Payload carries no content_hash field on AssetCreate today, but the
    # runner reads via getattr — exercise via a custom attribute set on the
    # payload object so the gating branch is reachable in unit-test land.
    payload = physical()
    object.__setattr__(payload, "content_hash", "deadbeef")  # same as EXISTING_ROW

    result = await upsert_asset(
        conn=object(), scope=SCOPE, payload=payload, policy=policy
    )

    assert result.action == "returned_existing"
    assert not any(is_update_metadata(c["sql"]) for c in fake_dql.calls)
