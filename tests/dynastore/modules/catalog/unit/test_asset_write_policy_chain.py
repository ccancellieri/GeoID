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
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncConnection

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
    AssetIdentityField,
    AssetIdentityKind,
    AssetIdentityRule,
    AssetsWritePolicy,
    AssetWriteConflictPolicy,
)


def _rules(*kinds_with_paths: object) -> list:
    """Build a chain of single-field rules.

    Each arg is either a bare :class:`AssetIdentityKind` or a tuple
    ``(kind, path)`` for METADATA_FIELD entries.
    """
    out = []
    for item in kinds_with_paths:
        if isinstance(item, tuple):
            kind, path = item
            field_spec = AssetIdentityField(kind=kind, path=path)
        else:
            field_spec = AssetIdentityField(kind=item)
        out.append(AssetIdentityRule(match_on=[field_spec]))
    return out


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


def is_update_refs_invalidate(sql: str) -> bool:
    """Match the UPDATE … asset_references SET valid_until = :now stamp."""
    s = sql.upper()
    return (
        s.lstrip().startswith("UPDATE")
        and "ASSET_REFERENCES" in s
        and "VALID_UNTIL" in s
    )


def is_insert(sql: str) -> bool:
    return sql.lstrip().upper().startswith("INSERT")


# ---------------------------------------------------------------------------
# Common test fixtures
# ---------------------------------------------------------------------------


SCOPE = Scope(schema="ds_test", catalog_id="cat", collection_id="col")


def _spec_conn() -> MagicMock:
    """Mock that passes `isinstance(_, AsyncConnection)` and carries a real
    PG dialect so TemplateQueryBuilder can resolve identifier quoting when
    any code path bypasses the fake-DQLQuery monkeypatch."""
    conn = MagicMock(spec=AsyncConnection)
    conn.dialect = postgresql.dialect()
    return conn


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
        await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=physical(), policy=policy)

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
        identity=_rules(AssetIdentityKind.ASSET_ID, AssetIdentityKind.FILENAME),
    )
    payload = physical(asset_id="brand_new", filename="alpha.tif")
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=payload, policy=policy)

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
    result = await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=payload, policy=policy)

    assert result.action == "updated"
    assert result.matcher_hit == AssetIdentityKind.ASSET_ID
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
        conn=_spec_conn(), scope=SCOPE, payload=physical(), policy=policy
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
        identity=_rules(
            (AssetIdentityKind.METADATA_FIELD, "iso19115.fileIdentifier"),
        ),
    )
    payload = physical(metadata={"iso19115": {"fileIdentifier": "URN:1"}})
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=payload, policy=policy)

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
        identity=_rules(
            AssetIdentityKind.ASSET_ID,
            AssetIdentityKind.FILENAME,
            (AssetIdentityKind.METADATA_FIELD, "iso19115.fileIdentifier"),
        ),
    )
    payload = physical(
        asset_id="something_else",
        filename="otherwise.tif",
        metadata={"iso19115": {"fileIdentifier": "URN:1"}},
    )
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=payload, policy=policy)

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
        conn=_spec_conn(), scope=SCOPE, payload=payload, policy=policy
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
        conn=_spec_conn(),
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
        conn=_spec_conn(), scope=SCOPE, payload=physical(), policy=policy
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
        conn=_spec_conn(), scope=SCOPE, payload=payload, policy=policy
    )

    assert result.action == "returned_existing"
    assert not any(is_update_metadata(c["sql"]) for c in fake_dql.calls)


# ---------------------------------------------------------------------------
# F1 — Concurrent-INSERT race surfaces as AssetSidecarRejectedError (409),
#       not an unhandled DB exception (500).
# ---------------------------------------------------------------------------


def _make_unique_violation(constraint: str) -> Exception:
    """Build a dynastore ``UniqueViolationError`` whose ``original_exception``
    carries the violated constraint name on a synthetic asyncpg-style stub."""
    from dynastore.modules.db_config.exceptions import UniqueViolationError

    class _AsyncpgStub(Exception):
        pass

    inner = _AsyncpgStub(f'duplicate key value violates unique constraint "{constraint}"')
    setattr(inner, "constraint_name", constraint)
    return UniqueViolationError(
        f"unique violation on {constraint}", original_exception=inner
    )


@pytest.mark.asyncio
async def test_race_filename_constraint_maps_to_filename_matcher(
    fake_dql: _Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Probes return None — the chain ran without finding a match, then a
    # concurrent writer landed first and the partial unique index fires.
    async def _raise_unique(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        raise _make_unique_violation("assets_uq_filename_ds_test")

    monkeypatch.setattr(ad, "_insert_new_row", _raise_unique)

    policy = AssetsWritePolicy()  # defaults
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=physical(), policy=policy)

    err = exc_info.value
    assert err.matcher == AssetIdentityKind.FILENAME.value
    assert err.reason == "conflict"
    assert err.asset_id == "alpha"
    assert err.existing_id is None  # we don't query the winning row


@pytest.mark.asyncio
async def test_race_href_constraint_maps_to_url_matcher(
    fake_dql: _Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _raise_unique(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        raise _make_unique_violation("assets_uq_href_ds_test")

    monkeypatch.setattr(ad, "_insert_new_row", _raise_unique)

    policy = AssetsWritePolicy()
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=virtual(), policy=policy)

    assert exc_info.value.matcher == AssetIdentityKind.URL.value
    assert exc_info.value.reason == "conflict"


@pytest.mark.asyncio
async def test_race_identity_constraint_maps_to_asset_id_matcher(
    fake_dql: _Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _raise_unique(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        raise _make_unique_violation("assets_identity_uq")

    monkeypatch.setattr(ad, "_insert_new_row", _raise_unique)

    policy = AssetsWritePolicy()
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=physical(), policy=policy)

    assert exc_info.value.matcher == AssetIdentityKind.ASSET_ID.value
    assert exc_info.value.reason == "conflict"


@pytest.mark.asyncio
async def test_race_unknown_constraint_maps_to_unknown_matcher(
    fake_dql: _Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _raise_unique(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        raise _make_unique_violation("some_other_unique_constraint")

    monkeypatch.setattr(ad, "_insert_new_row", _raise_unique)

    policy = AssetsWritePolicy()
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=physical(), policy=policy)

    assert exc_info.value.matcher == "unknown"
    assert exc_info.value.reason == "conflict"


@pytest.mark.asyncio
async def test_race_via_message_scrape_when_constraint_attr_missing(
    fake_dql: _Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the underlying driver doesn't expose ``constraint_name``, the
    message-scrape fallback recovers the constraint from the error text."""
    from dynastore.modules.db_config.exceptions import UniqueViolationError

    class _StringInner(Exception):
        pass

    inner = _StringInner(
        'duplicate key value violates unique constraint "assets_uq_filename_ds_test"'
    )
    # Note: NO constraint_name attr — only the message.
    err = UniqueViolationError("scraped", original_exception=inner)

    async def _raise_unique(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        raise err

    monkeypatch.setattr(ad, "_insert_new_row", _raise_unique)

    policy = AssetsWritePolicy()
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=physical(), policy=policy)

    assert exc_info.value.matcher == AssetIdentityKind.FILENAME.value


# ---------------------------------------------------------------------------
# F4 — NEW_VERSION archives also invalidate the asset_references rows.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_new_version_archive_stamps_asset_references_valid_until(
    fake_dql: _Recorder,
) -> None:
    """When NEW_VERSION archives an existing row, it must also stamp
    ``valid_until`` on any active asset_references for that asset_id so
    the new row inheriting the same id isn't blocked by stale references.
    """
    fake_dql.when(is_select_by("asset_id"), EXISTING_ROW)

    policy = AssetsWritePolicy(on_conflict=AssetWriteConflictPolicy.NEW_VERSION)
    result = await upsert_asset(
        conn=_spec_conn(), scope=SCOPE, payload=physical(), policy=policy
    )
    assert result.action == "new_version"

    # Both UPDATEs must be present in the captured call log:
    archived = [c for c in fake_dql.calls if is_update_archive(c["sql"])]
    refs_invalidated = [
        c for c in fake_dql.calls if is_update_refs_invalidate(c["sql"])
    ]
    assert len(archived) == 1, "expected exactly one row-archive UPDATE"
    assert len(refs_invalidated) == 1, (
        "expected exactly one asset_references invalidation UPDATE"
    )
    # The invalidation targets the archived asset_id and only active rows.
    binds = refs_invalidated[0]["params"]
    assert binds["asset_id"] == EXISTING_ROW["asset_id"]
    assert binds["catalog_id"] == SCOPE.catalog_id


# ---------------------------------------------------------------------------
# content_hash tagged-only contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_content_hash_probe_sends_tagged_bind_only(
    fake_dql: _Recorder,
) -> None:
    """The probe SQL matches the tagged form verbatim. Verify the tagged
    bind is present and no legacy ``:raw`` bind leaks through."""
    fake_dql.when(is_select_by("content_hash"), EXISTING_ROW)

    payload = physical()
    object.__setattr__(payload, "content_hash", "md5:abc==")
    policy = AssetsWritePolicy(
        identity=_rules(AssetIdentityKind.CONTENT_HASH),
    )
    with pytest.raises(AssetSidecarRejectedError):
        await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=payload, policy=policy)

    probe_call = next(c for c in fake_dql.calls if "content_hash" in c["sql"])
    assert probe_call["params"]["tagged"] == "md5:abc=="
    assert "raw" not in probe_call["params"]


@pytest.mark.asyncio
async def test_hash_gating_requires_byte_for_byte_match(
    fake_dql: _Recorder,
) -> None:
    """``skip_if_unchanged_content_hash`` compares stored and incoming
    values as-is. Tagged-vs-untagged are NOT considered equal — both
    sides MUST submit the tagged form."""
    tagged_existing = dict(EXISTING_ROW, content_hash="md5:abc==")
    fake_dql.when(is_select_by("asset_id"), tagged_existing)

    policy = AssetsWritePolicy(
        on_conflict=AssetWriteConflictPolicy.UPDATE,
        skip_if_unchanged_content_hash=True,
    )
    payload = physical()
    object.__setattr__(payload, "content_hash", "md5:abc==")

    result = await upsert_asset(
        conn=_spec_conn(), scope=SCOPE, payload=payload, policy=policy
    )
    assert result.action == "returned_existing"
    assert not any(is_update_metadata(c["sql"]) for c in fake_dql.calls)


@pytest.mark.asyncio
async def test_hash_gating_rejects_untagged_payload(
    fake_dql: _Recorder,
) -> None:
    """Untagged ``abc==`` payload does NOT short-circuit against a tagged
    ``md5:abc==`` row — the gating compare is byte-for-byte. The conflict
    flow then falls through to the UPDATE branch."""
    tagged_existing = dict(EXISTING_ROW, content_hash="md5:abc==")
    fake_dql.when(is_select_by("asset_id"), tagged_existing)

    policy = AssetsWritePolicy(
        on_conflict=AssetWriteConflictPolicy.UPDATE,
        skip_if_unchanged_content_hash=True,
    )
    payload = physical()
    object.__setattr__(payload, "content_hash", "abc==")  # untagged — no match

    result = await upsert_asset(
        conn=_spec_conn(), scope=SCOPE, payload=payload, policy=policy
    )
    assert result.action != "returned_existing"


@pytest.mark.asyncio
async def test_race_in_new_version_path_preserves_existing_id(
    fake_dql: _Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    """NEW_VERSION races: the existing row was found and archived, then the
    re-INSERT lost the race. The rejection must include the original
    ``existing_id`` so the client can correlate."""
    fake_dql.when(is_select_by("asset_id"), EXISTING_ROW)

    async def _raise_unique(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        raise _make_unique_violation("assets_uq_filename_ds_test")

    monkeypatch.setattr(ad, "_insert_new_row", _raise_unique)

    policy = AssetsWritePolicy(on_conflict=AssetWriteConflictPolicy.NEW_VERSION)
    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=physical(), policy=policy)

    err = exc_info.value
    assert err.matcher == AssetIdentityKind.FILENAME.value
    assert err.existing_id == "alpha"  # the archived row's id is preserved
    assert err.reason == "conflict"


# ---------------------------------------------------------------------------
# New-shape pins — rule-level on_match override + AND-composition + posture
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rule_on_match_overrides_policy_level_action(
    fake_dql: _Recorder,
) -> None:
    """A rule's ``on_match`` substitutes the policy-level ``on_conflict``.

    Policy default is REFUSE_FAIL; the asset_id rule overrides to
    REFUSE_RETURN — a hit on asset_id must echo the existing row instead
    of raising.
    """
    fake_dql.when(is_select_by("asset_id"), EXISTING_ROW)

    policy = AssetsWritePolicy(
        on_conflict=AssetWriteConflictPolicy.REFUSE_FAIL,
        identity=[
            AssetIdentityRule(
                match_on=[AssetIdentityField(kind=AssetIdentityKind.ASSET_ID)],
                on_match=AssetWriteConflictPolicy.REFUSE_RETURN,
            ),
        ],
    )
    result = await upsert_asset(
        conn=_spec_conn(), scope=SCOPE, payload=physical(), policy=policy
    )
    assert result.action == "returned_existing"


@pytest.mark.asyncio
async def test_multi_field_rule_requires_all_fields_to_match_same_row(
    fake_dql: _Recorder,
) -> None:
    """AND-composition: the FILENAME probe must also resolve the row that
    the ASSET_ID probe already returned. When the probes report different
    asset_ids, the rule misses and the chain falls through to a fresh
    INSERT.
    """
    # asset_id probe → row A; filename probe → row B (different asset_id).
    # The probes need disjoint predicates — both SQL statements list
    # asset_id in their SELECT clause, so match on a more specific term.
    row_a = dict(EXISTING_ROW, asset_id="alpha")
    row_b = dict(EXISTING_ROW, asset_id="beta")

    def is_select_asset_id_predicate(sql: str) -> bool:
        return is_select(sql) and "AND asset_id = :asset_id" in sql

    def is_select_filename_predicate(sql: str) -> bool:
        return is_select(sql) and "AND filename = :filename" in sql

    fake_dql.when(is_select_asset_id_predicate, row_a)
    fake_dql.when(is_select_filename_predicate, row_b)
    fake_dql.when(is_insert, dict(EXISTING_ROW, asset_id="gamma", status="active"))

    policy = AssetsWritePolicy(
        on_conflict=AssetWriteConflictPolicy.REFUSE_FAIL,
        identity=[
            AssetIdentityRule(
                match_on=[
                    AssetIdentityField(kind=AssetIdentityKind.ASSET_ID),
                    AssetIdentityField(kind=AssetIdentityKind.FILENAME),
                ],
            ),
        ],
    )
    # No raise expected — AND fails, chain falls through to INSERT.
    result = await upsert_asset(
        conn=_spec_conn(),
        scope=SCOPE,
        payload=physical(asset_id="gamma"),
        policy=policy,
    )
    assert result.action == "inserted_active"


@pytest.mark.asyncio
async def test_metadata_field_path_lives_on_field_not_policy(
    fake_dql: _Recorder,
) -> None:
    """The dot-path is read off the :class:`AssetIdentityField`, not off a
    policy-level slot. Multiple METADATA_FIELD rules with different paths
    must all dispatch against their own path.
    """
    captured_paths: List[str] = []

    def _capture(sql: str) -> bool:
        # capture the path literal embedded in the SQL — '{a,b,c}'
        if is_select(sql) and "#>>" in sql:
            import re
            m = re.search(r"#>> '\{([^}]+)\}'", sql)
            if m:
                captured_paths.append(m.group(1).replace(",", "."))
        return False  # never match — we just want to capture & let probes miss

    fake_dql.script.append((_capture, None))

    policy = AssetsWritePolicy(
        on_conflict=AssetWriteConflictPolicy.REFUSE_FAIL,
        identity=[
            AssetIdentityRule(
                match_on=[
                    AssetIdentityField(
                        kind=AssetIdentityKind.METADATA_FIELD,
                        path="iso19115.fileIdentifier",
                    ),
                ],
            ),
            AssetIdentityRule(
                match_on=[
                    AssetIdentityField(
                        kind=AssetIdentityKind.METADATA_FIELD,
                        path="external.urn",
                    ),
                ],
            ),
        ],
    )
    payload = physical(
        metadata={
            "iso19115": {"fileIdentifier": "URN:1"},
            "external": {"urn": "URN:2"},
        },
    )
    fake_dql.when(is_insert, dict(EXISTING_ROW, status="active"))
    await upsert_asset(conn=_spec_conn(), scope=SCOPE, payload=payload, policy=policy)
    # Both rule paths got dispatched.
    assert "iso19115.fileIdentifier" in captured_paths
    assert "external.urn" in captured_paths


@pytest.mark.asyncio
async def test_content_hash_rule_short_circuits_to_miss_for_pending(
    fake_dql: _Recorder,
) -> None:
    """The content_hash probe returns no hit when the incoming payload has
    no content_hash — preserves the documented "PENDING short-circuit"
    behaviour after the shape migration.
    """
    fake_dql.when(is_insert, dict(EXISTING_ROW, status="pending"))

    policy = AssetsWritePolicy(
        identity=_rules(AssetIdentityKind.CONTENT_HASH),
    )
    # AssetCreate has no content_hash field → probe miss → INSERT path.
    result = await upsert_asset(
        conn=_spec_conn(),
        scope=SCOPE,
        payload=physical(asset_id="fresh"),
        policy=policy,
        initial_status=AssetStatus.PENDING,
    )
    assert result.action == "inserted_pending"


def test_default_policy_dump_yields_byte_for_byte_chain() -> None:
    """The default identity chain dumps to a JSON shape that round-trips
    through ``model_validate`` producing the exact same chain dispatch.
    """
    p1 = AssetsWritePolicy()
    dumped = p1.model_dump(mode="json")
    p2 = AssetsWritePolicy.model_validate(dumped)
    # Two single-field rules in declared order: ASSET_ID, FILENAME.
    assert [r.match_on[0].kind for r in p2.identity] == [
        AssetIdentityKind.ASSET_ID,
        AssetIdentityKind.FILENAME,
    ]
    # And the dump is stable.
    assert p2.model_dump(mode="json") == dumped


def test_require_filename_posture_stays_orthogonal_to_identity() -> None:
    """``require_filename`` is a service-layer pre-check, not an identity
    field. Toggling it must not change ``policy.identity`` at all.
    """
    strict = AssetsWritePolicy(require_filename=True)
    loose = AssetsWritePolicy(require_filename=False)
    assert strict.identity == loose.identity
