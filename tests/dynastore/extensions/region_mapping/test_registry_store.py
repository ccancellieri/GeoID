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

"""DB-free unit tests for ``registry_store`` (dynastore#2821) -- write
orchestration (``apply_mapping``/``delete_mapping``) and the ``@cached``
read wrappers, with the SQL layer (``registry_queries``) mocked out.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Dict, List
from unittest.mock import AsyncMock

import pytest


@pytest.fixture(autouse=True)
def _reset_caches():
    from dynastore.extensions.region_mapping.registry_store import invalidate_serving_caches

    invalidate_serving_caches()
    yield
    invalidate_serving_caches()


class _FakeConn:
    """Stand-in for a SQLAlchemy ``AsyncConnection`` that only needs to
    support ``begin_nested()`` -- the SAVEPOINT ``apply_mapping`` opens
    around every ``INSERT_CLAIM`` attempt. A plain string sentinel can't
    satisfy that, so tests exercising the insert path need this instead of
    the bare ``"conn"`` default.
    """

    def __repr__(self) -> str:  # pragma: no cover -- debugging aid only
        return "<_FakeConn>"

    def begin_nested(self):
        @asynccontextmanager
        async def _savepoint():
            yield self

        return _savepoint()


def _fake_managed_transaction(monkeypatch: pytest.MonkeyPatch, conn: Any = None) -> None:
    from dynastore.extensions.region_mapping import registry_store as store

    if conn is None:
        conn = _FakeConn()

    @asynccontextmanager
    async def _mt(engine: Any):
        yield conn

    monkeypatch.setattr(store, "managed_transaction", _mt)


# ---------------------------------------------------------------------------
# apply_mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_mapping_deletes_stale_then_updates_or_inserts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import registry_store as store
    from dynastore.extensions.region_mapping import registry_queries as rq

    _fake_managed_transaction(monkeypatch)

    delete_stale = AsyncMock(return_value=[])
    # First claim ("country", primary) is a fresh insert; the rest already
    # exist under this mapping (UPDATE succeeds).
    update_calls: List[Dict[str, Any]] = []
    insert_calls: List[Dict[str, Any]] = []

    async def _update(conn: Any, **params: Any):
        update_calls.append(params)
        if params["claim_ci"] == "country":
            return None
        return {**params}

    async def _insert(conn: Any, **params: Any):
        insert_calls.append(params)
        return {**params}

    monkeypatch.setattr(rq.DELETE_STALE_CLAIMS, "execute", delete_stale)
    monkeypatch.setattr(rq.UPDATE_OWN_CLAIM, "execute", _update)
    monkeypatch.setattr(rq.INSERT_CLAIM, "execute", _insert)

    invalidated = []
    monkeypatch.setattr(store, "invalidate_serving_caches", lambda: invalidated.append(True))

    mapping_id, rows = await store.apply_mapping(
        object(),
        catalog_id="fao", collection_id="countries", column="adm0_code",
        alias="country", extra_aliases=["adm0"], title="Countries",
    )

    assert mapping_id == "fao_countries"
    assert len(rows) == len(update_calls)  # one row per claim
    assert delete_stale.await_args.kwargs["mapping_id"] == mapping_id
    assert set(delete_stale.await_args.kwargs["keep_claim_ci"]) == {
        c["claim_ci"] for c in update_calls
    }
    assert any(c["claim_ci"] == "country" for c in insert_calls)
    assert invalidated == [True]


@pytest.mark.asyncio
async def test_apply_mapping_rejects_regex_metacharacter_claims_before_any_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bad claim fails ``compute_claim_set`` before touching the DB."""
    from dynastore.extensions.region_mapping import registry_store as store

    with pytest.raises(ValueError, match="regex metacharacters"):
        await store.apply_mapping(
            object(),
            catalog_id="fao", collection_id="countries", column="adm0.code",
            alias="country", extra_aliases=[], title=None,
        )


@pytest.mark.asyncio
async def test_apply_mapping_propagates_unique_violation_for_cross_mapping_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claim already owned by a different mapping: UPDATE (scoped to this
    mapping_id) finds nothing, INSERT hits the real PK violation. The
    surviving row's re-check reports a *different* mapping_id, so this must
    propagate -- never be swallowed here (the global exception-handler chain
    maps it to HTTP 409)."""
    from dynastore.modules.db_config.exceptions import UniqueViolationError
    from dynastore.extensions.region_mapping import registry_store as store
    from dynastore.extensions.region_mapping import registry_queries as rq

    _fake_managed_transaction(monkeypatch)

    monkeypatch.setattr(rq.DELETE_STALE_CLAIMS, "execute", AsyncMock(return_value=[]))
    monkeypatch.setattr(rq.UPDATE_OWN_CLAIM, "execute", AsyncMock(return_value=None))

    async def _insert_conflict(conn: Any, **params: Any):
        raise UniqueViolationError("duplicate key value violates unique constraint")

    monkeypatch.setattr(rq.INSERT_CLAIM, "execute", _insert_conflict)
    monkeypatch.setattr(
        rq.SELECT_CLAIM_BY_CI, "execute",
        AsyncMock(return_value={"claim_ci": "region", "mapping_id": "someone_else"}),
    )

    with pytest.raises(UniqueViolationError):
        await store.apply_mapping(
            object(),
            catalog_id="who", collection_id="regions", column="region",
            alias="region_name", extra_aliases=[], title=None,
        )


@pytest.mark.asyncio
async def test_apply_mapping_absorbs_unique_violation_for_concurrent_same_mapping_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two racing first-applies of the SAME mapping: this transaction's
    UPDATE finds nothing (the peer's row isn't committed yet), INSERT then
    hits ``23505`` against the peer's now-committed row. Because the
    surviving row's ``mapping_id`` matches ours, this must resolve as an
    idempotent success -- not a 409 (dynastore#2824)."""
    from dynastore.modules.db_config.exceptions import UniqueViolationError
    from dynastore.extensions.region_mapping import registry_store as store
    from dynastore.extensions.region_mapping import registry_queries as rq

    _fake_managed_transaction(monkeypatch)

    monkeypatch.setattr(rq.DELETE_STALE_CLAIMS, "execute", AsyncMock(return_value=[]))
    monkeypatch.setattr(rq.UPDATE_OWN_CLAIM, "execute", AsyncMock(return_value=None))

    async def _insert_conflict(conn: Any, **params: Any):
        raise UniqueViolationError("duplicate key value violates unique constraint")

    monkeypatch.setattr(rq.INSERT_CLAIM, "execute", _insert_conflict)

    winning_row = {"claim_ci": "region", "mapping_id": "who_regions", "claim": "region"}
    monkeypatch.setattr(
        rq.SELECT_CLAIM_BY_CI, "execute", AsyncMock(return_value=winning_row),
    )

    mapping_id, rows = await store.apply_mapping(
        object(),
        catalog_id="who", collection_id="regions", column="region",
        alias="region_name", extra_aliases=[], title=None,
    )

    assert mapping_id == "who_regions"
    assert winning_row in rows


# ---------------------------------------------------------------------------
# delete_mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_mapping_returns_count_and_invalidates(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import registry_store as store
    from dynastore.extensions.region_mapping import registry_queries as rq

    _fake_managed_transaction(monkeypatch)
    monkeypatch.setattr(
        rq.DELETE_CLAIMS_BY_MAPPING_ID, "execute",
        AsyncMock(return_value=[{"claim_ci": "country"}, {"claim_ci": "adm0_code"}]),
    )
    invalidated = []
    monkeypatch.setattr(store, "invalidate_serving_caches", lambda: invalidated.append(True))

    deleted = await store.delete_mapping(object(), "fao_countries")

    assert deleted == 2
    assert invalidated == [True]


@pytest.mark.asyncio
async def test_delete_mapping_raises_not_found_when_no_claims_existed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import registry_store as store
    from dynastore.extensions.region_mapping import registry_queries as rq

    _fake_managed_transaction(monkeypatch)
    monkeypatch.setattr(rq.DELETE_CLAIMS_BY_MAPPING_ID, "execute", AsyncMock(return_value=[]))

    with pytest.raises(store.MappingNotFoundError):
        await store.delete_mapping(object(), "does-not-exist")


# ---------------------------------------------------------------------------
# delete_claims_by_source_collection / delete_claims_by_source_catalog
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_claims_by_source_collection_returns_count_and_invalidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import registry_store as store
    from dynastore.extensions.region_mapping import registry_queries as rq

    _fake_managed_transaction(monkeypatch)
    monkeypatch.setattr(
        rq.DELETE_CLAIMS_BY_SOURCE_COLLECTION, "execute",
        AsyncMock(return_value=[{"claim_ci": "country"}]),
    )
    invalidated = []
    monkeypatch.setattr(store, "invalidate_serving_caches", lambda: invalidated.append(True))

    deleted = await store.delete_claims_by_source_collection(object(), "fao", "countries")

    assert deleted == 1
    assert invalidated == [True]


@pytest.mark.asyncio
async def test_delete_claims_by_source_collection_noop_does_not_invalidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing was ever registered for this collection -- not an error, and
    no reason to bust the serving caches."""
    from dynastore.extensions.region_mapping import registry_store as store
    from dynastore.extensions.region_mapping import registry_queries as rq

    _fake_managed_transaction(monkeypatch)
    monkeypatch.setattr(rq.DELETE_CLAIMS_BY_SOURCE_COLLECTION, "execute", AsyncMock(return_value=[]))
    invalidated = []
    monkeypatch.setattr(store, "invalidate_serving_caches", lambda: invalidated.append(True))

    deleted = await store.delete_claims_by_source_collection(object(), "fao", "countries")

    assert deleted == 0
    assert invalidated == []


@pytest.mark.asyncio
async def test_delete_claims_by_source_catalog_returns_count_and_invalidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import registry_store as store
    from dynastore.extensions.region_mapping import registry_queries as rq

    _fake_managed_transaction(monkeypatch)
    monkeypatch.setattr(
        rq.DELETE_CLAIMS_BY_SOURCE_CATALOG, "execute",
        AsyncMock(return_value=[{"claim_ci": "country"}, {"claim_ci": "adm0_code"}]),
    )
    invalidated = []
    monkeypatch.setattr(store, "invalidate_serving_caches", lambda: invalidated.append(True))

    deleted = await store.delete_claims_by_source_catalog(object(), "fao")

    assert deleted == 2
    assert invalidated == [True]


# ---------------------------------------------------------------------------
# list_claims (uncached)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_claims_returns_empty_when_engine_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import registry_store as store

    monkeypatch.setattr(store, "get_engine", lambda: None)

    assert await store.list_claims(mapping_id="fao_countries") == []


@pytest.mark.asyncio
async def test_list_claims_delegates_to_sql_layer(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import registry_store as store
    from dynastore.extensions.region_mapping import registry_queries as rq

    engine = object()
    monkeypatch.setattr(store, "get_engine", lambda: engine)
    captured: Dict[str, Any] = {}

    async def _list_claims(passed_engine: Any, **kwargs: Any):
        captured["engine"] = passed_engine
        captured["kwargs"] = kwargs
        return [{"claim_ci": "country"}]

    monkeypatch.setattr(rq, "list_claims", _list_claims)

    rows = await store.list_claims(mapping_id="fao_countries", limit=10, offset=5)

    assert rows == [{"claim_ci": "country"}]
    assert captured["engine"] is engine
    assert captured["kwargs"]["mapping_id"] == "fao_countries"
    assert captured["kwargs"]["limit"] == 10
    assert captured["kwargs"]["offset"] == 5


# ---------------------------------------------------------------------------
# Cached reads + invalidation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_primary_records_is_cached_until_invalidated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import registry_store as store
    from dynastore.extensions.region_mapping import registry_queries as rq

    engine = object()
    monkeypatch.setattr(store, "get_engine", lambda: engine)
    call_count = {"n": 0}

    async def _list_claims(passed_engine: Any, **kwargs: Any):
        call_count["n"] += 1
        return [{"mapping_id": "fao_countries", "claim": "country"}]

    monkeypatch.setattr(rq, "list_claims", _list_claims)

    first = await store.fetch_primary_records("fao", "countries", None)
    second = await store.fetch_primary_records("fao", "countries", None)
    assert first == second
    assert call_count["n"] == 1, "second call within TTL must hit the cache"

    store.invalidate_serving_caches()
    await store.fetch_primary_records("fao", "countries", None)
    assert call_count["n"] == 2, "cache must be empty right after invalidation"


@pytest.mark.asyncio
async def test_fetch_primary_records_alias_ci_uses_claim_ci_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import registry_store as store
    from dynastore.extensions.region_mapping import registry_queries as rq

    engine = object()
    monkeypatch.setattr(store, "get_engine", lambda: engine)
    captured: Dict[str, Any] = {}

    async def _list_claims(passed_engine: Any, **kwargs: Any):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(rq, "list_claims", _list_claims)

    await store.fetch_primary_records(None, None, "country")

    assert captured["claim_ci"] == "country"
    assert "role" not in captured


@pytest.mark.asyncio
async def test_fetch_mapping_primary_returns_none_without_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import registry_store as store

    monkeypatch.setattr(store, "get_engine", lambda: None)

    assert await store.fetch_mapping_primary("fao_countries") is None
