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

"""Unit tests for the missed-obligation sweep (#2688 lane 1).

Pure-mock style — no live DB. Run with:
    PYTHONPATH=packages/core/src \
      /Users/ccancellieri/work/code/geoid/.venv/bin/python \
      -m pytest tests/dynastore/modules/storage/unit/test_obligation_sweep.py \
      --noconftest -p no:cacheprovider -q
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
)
from dynastore.modules.storage.obligation_sweep import (
    LOOKBACK_MULTIPLIER,
    _check_and_enqueue,
    _sweep_collection,
    sweep_missing_obligations,
)


# ---------------------------------------------------------------------------
# Fixtures / fakes
# ---------------------------------------------------------------------------


def _fake_sa_conn() -> Any:
    """A conn that satisfies DriverContext.db_resource's isinstance check.

    Only ``sweep_missing_obligations`` builds a ``DriverContext`` from
    ``conn`` (to thread it through the enumeration protocols); the
    ``_check_and_enqueue``/``_sweep_collection`` tests call those functions
    directly with ``ctx=None`` and never construct one, so a bare
    ``object()`` is fine there.
    """
    return MagicMock(spec=AsyncConnection)


def _pg_primary_entry() -> OperationDriverEntry:
    return OperationDriverEntry(driver_ref="items_postgresql_driver", on_failure=FailurePolicy.FATAL)


def _es_secondary_entry() -> OperationDriverEntry:
    return OperationDriverEntry(
        driver_ref="items_elasticsearch_driver",
        source="auto",
    )


def _routing_with_pg_primary_and_async_secondary() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(operations={
        Operation.WRITE: [_pg_primary_entry()],
        Operation.INDEX: [_es_secondary_entry()],
    })


def _routing_with_pg_only_no_secondary() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(operations={
        Operation.WRITE: [_pg_primary_entry()],
    })


def _routing_with_es_primary() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(operations={
        Operation.WRITE: [
            OperationDriverEntry(driver_ref="items_elasticsearch_driver", on_failure=FailurePolicy.FATAL),
        ],
        Operation.INDEX: [_es_secondary_entry()],
    })


def _legacy_on_failure_validation_error() -> ValidationError:
    """A real ``ValidationError`` shaped exactly like #3240: a stored
    routing config whose WRITE lane still carries the legacy
    ``on_failure: "outbox"`` value the shrunk ``FailurePolicy {FATAL, WARN}``
    rejects at parse time."""
    try:
        ItemsRoutingConfig.model_validate({
            "operations": {
                "WRITE": [
                    {"driver_ref": "items_postgresql_driver", "on_failure": "fatal"},
                    {"driver_ref": "legacy_outbox_driver", "on_failure": "outbox"},
                ],
            },
        })
    except ValidationError as exc:
        return exc
    raise AssertionError("expected ItemsRoutingConfig to reject on_failure='outbox'")


class _FakeConfigs:
    """Minimal ConfigsProtocol stub: returns pre-registered config instances."""

    def __init__(
        self,
        *,
        routing: Optional[ItemsRoutingConfig] = None,
        driver_cfg: Optional[ItemsPostgresqlDriverConfig] = None,
    ) -> None:
        self.routing = routing
        self.driver_cfg = driver_cfg
        self.calls: List[Any] = []

    async def get_config(self, config_cls, catalog_id=None, collection_id=None, ctx=None, config_snapshot=None):
        self.calls.append((config_cls, catalog_id, collection_id))
        if config_cls is ItemsRoutingConfig:
            return self.routing if self.routing is not None else ItemsRoutingConfig()
        if config_cls is ItemsPostgresqlDriverConfig:
            return self.driver_cfg if self.driver_cfg is not None else ItemsPostgresqlDriverConfig()
        raise AssertionError(f"unexpected config_cls {config_cls!r}")


class _FakeCatalogs:
    """Minimal CatalogsProtocol stub for schema resolution + enumeration."""

    def __init__(self, *, schema: str = "cat1", catalogs=None, collections=None) -> None:
        self.schema = schema
        self._catalogs = catalogs or []
        self._collections = collections or {}

    async def resolve_physical_schema(self, catalog_id, ctx=None, allow_missing=False):
        return self.schema

    async def list_catalogs(self, *, limit, offset, ctx=None):
        return self._catalogs[offset:offset + limit]

    async def list_collections(self, catalog_id, *, limit, offset, ctx=None):
        cols = self._collections.get(catalog_id, [])
        return cols[offset:offset + limit]


def _dql_factory(rows_by_driver: Dict[str, List[Dict[str, Any]]], captured_sql: List[str]):
    def _factory(sql, **_kw):
        captured_sql.append(sql)
        inst = MagicMock()

        async def _execute(conn, **params):
            return rows_by_driver.get(params["driver_id"], [])

        inst.execute = AsyncMock(side_effect=_execute)
        return inst

    return _factory


_WINDOW_START = datetime(2026, 7, 10, 0, 0, 0, tzinfo=timezone.utc)
_WINDOW_END = datetime(2026, 7, 10, 0, 25, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# _check_and_enqueue: classification + enqueue shape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_row_is_re_enqueued_via_id_only_producer():
    """A hub row with no matching obligation is re-enqueued through
    enqueue_storage_op_id_only — never a hand-rolled INSERT."""
    configs = _FakeConfigs(driver_cfg=ItemsPostgresqlDriverConfig(physical_table="items_c1"))
    catalogs = _FakeCatalogs()
    captured_sql: List[str] = []
    rows_by_driver = {"items_elasticsearch_driver": [{"geoid": "g1", "deleted_at": None}]}

    mock_id_only = AsyncMock()
    mock_write_id = AsyncMock()
    with (
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            side_effect=_dql_factory(rows_by_driver, captured_sql),
        ),
        patch(
            "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
            new=mock_id_only,
        ),
        patch(
            "dynastore.modules.storage.storage_emit.enqueue_storage_op_write_id",
            new=mock_write_id,
        ),
    ):
        checked, enqueued = await _check_and_enqueue(
            object(),
            catalogs=catalogs,
            configs=configs,
            catalog_id="cat1",
            collection_id="col1",
            ctx=None,
            async_entries=[_es_secondary_entry()],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    assert checked == 1
    assert enqueued == 1
    mock_id_only.assert_awaited_once()
    mock_write_id.assert_not_awaited()

    _, kwargs = mock_id_only.call_args
    assert kwargs["catalog_id"] == "cat1"
    records = kwargs["rows"]
    assert len(records) == 1
    assert records[0].item_id == "g1"
    assert records[0].op == "upsert"
    assert records[0].driver_id == "items_elasticsearch_driver"
    assert records[0].idempotency_key == "g1"


@pytest.mark.asyncio
async def test_tombstoned_row_is_enqueued_as_delete():
    """A hub row with deleted_at set produces an op='delete' obligation."""
    configs = _FakeConfigs(driver_cfg=ItemsPostgresqlDriverConfig(physical_table="items_c1"))
    catalogs = _FakeCatalogs()
    captured_sql: List[str] = []
    rows_by_driver = {
        "items_elasticsearch_driver": [{"geoid": "g2", "deleted_at": datetime(2026, 7, 10)}],
    }

    mock_id_only = AsyncMock()
    with (
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            side_effect=_dql_factory(rows_by_driver, captured_sql),
        ),
        patch(
            "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
            new=mock_id_only,
        ),
    ):
        checked, enqueued = await _check_and_enqueue(
            object(),
            catalogs=catalogs,
            configs=configs,
            catalog_id="cat1",
            collection_id="col1",
            ctx=None,
            async_entries=[_es_secondary_entry()],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    assert enqueued == 1
    records = mock_id_only.call_args.kwargs["rows"]
    assert records[0].op == "delete"


@pytest.mark.asyncio
async def test_no_missing_rows_skips_enqueue():
    """An empty result set (every hub row already has a matching obligation,
    whether via write_id or entity_id) re-enqueues nothing."""
    configs = _FakeConfigs(driver_cfg=ItemsPostgresqlDriverConfig(physical_table="items_c1"))
    catalogs = _FakeCatalogs()
    captured_sql: List[str] = []

    mock_id_only = AsyncMock()
    with (
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            side_effect=_dql_factory({}, captured_sql),
        ),
        patch(
            "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
            new=mock_id_only,
        ),
    ):
        checked, enqueued = await _check_and_enqueue(
            object(),
            catalogs=catalogs,
            configs=configs,
            catalog_id="cat1",
            collection_id="col1",
            ctx=None,
            async_entries=[_es_secondary_entry()],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    assert checked == 1
    assert enqueued == 0
    mock_id_only.assert_not_awaited()


@pytest.mark.asyncio
async def test_query_matches_on_write_id_or_entity_id():
    """The anti-join predicate covers both row shapes: write-id ledger rows
    (matched on write_id, entity_id NULL) and id-only rows (matched on
    entity_id, scoped to obligations created at-or-after the row's latest
    change — see the staleness test below) — either counts as an existing
    obligation, in any status."""
    configs = _FakeConfigs(driver_cfg=ItemsPostgresqlDriverConfig(physical_table="items_c1"))
    catalogs = _FakeCatalogs()
    captured_sql: List[str] = []

    with (
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            side_effect=_dql_factory({}, captured_sql),
        ),
        patch(
            "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
            new=AsyncMock(),
        ),
    ):
        await _check_and_enqueue(
            object(),
            catalogs=catalogs,
            configs=configs,
            catalog_id="cat1",
            collection_id="col1",
            ctx=None,
            async_entries=[_es_secondary_entry()],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    assert len(captured_sql) == 1
    sql = captured_sql[0]
    assert "NOT EXISTS" in sql
    assert 's.write_id = h."write_id"' in sql
    assert 's.entity_id = h."geoid"::text' in sql
    assert "s.driver_id = :driver_id" in sql
    assert "s.collection_id = :collection_id" in sql
    assert 'h."transaction_time" >= :window_start AND h."transaction_time" < :window_end' in sql
    # No status filter: any obligation status counts as "exists" (#2688).
    assert "status" not in sql


@pytest.mark.asyncio
async def test_entity_id_branch_scoped_to_obligations_after_latest_change():
    """The entity_id match must be scoped to s.created_at >= the hub row's
    latest change (transaction_time, or deleted_at when tombstoned) — an
    id-only row is produced by this very sweep, so an unscoped match would
    let a stale, already-drained sweep row from an earlier tick permanently
    mask a LATER missed write of the same item. The write_id branch is
    scoped implicitly (a ledger row is written once, atomically, for the
    exact batch that produced it) and carries no such condition."""
    configs = _FakeConfigs(driver_cfg=ItemsPostgresqlDriverConfig(physical_table="items_c1"))
    catalogs = _FakeCatalogs()
    captured_sql: List[str] = []

    with (
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            side_effect=_dql_factory({}, captured_sql),
        ),
        patch(
            "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
            new=AsyncMock(),
        ),
    ):
        await _check_and_enqueue(
            object(),
            catalogs=catalogs,
            configs=configs,
            catalog_id="cat1",
            collection_id="col1",
            ctx=None,
            async_entries=[_es_secondary_entry()],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    sql = captured_sql[0]
    latest_change = 'GREATEST(h."transaction_time", COALESCE(h."deleted_at", h."transaction_time"))'
    assert f's.entity_id = h."geoid"::text AND s.created_at >= {latest_change}' in sql

    # The write_id branch carries no staleness condition: nothing between
    # the write_id equality and the "OR" that opens the entity_id branch
    # mentions created_at.
    write_id_pos = sql.index('s.write_id = h."write_id"')
    or_pos = sql.index('OR (s.entity_id')
    assert "created_at" not in sql[write_id_pos:or_pos]


@pytest.mark.asyncio
async def test_soft_delete_widens_window_to_deleted_at():
    """A soft delete stamps deleted_at without bumping transaction_time
    (ItemService.soft_delete_item_query / _enqueue_index_deletes), so the
    window predicate must independently range-match on deleted_at too."""
    configs = _FakeConfigs(driver_cfg=ItemsPostgresqlDriverConfig(physical_table="items_c1"))
    catalogs = _FakeCatalogs()
    captured_sql: List[str] = []

    with (
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            side_effect=_dql_factory({}, captured_sql),
        ),
        patch(
            "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
            new=AsyncMock(),
        ),
    ):
        await _check_and_enqueue(
            object(),
            catalogs=catalogs,
            configs=configs,
            catalog_id="cat1",
            collection_id="col1",
            ctx=None,
            async_entries=[_es_secondary_entry()],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    sql = captured_sql[0]
    assert 'h."transaction_time" >= :window_start AND h."transaction_time" < :window_end' in sql
    assert 'h."deleted_at" >= :window_start AND h."deleted_at" < :window_end' in sql


@pytest.mark.asyncio
async def test_old_row_recent_soft_delete_is_enqueued_as_delete():
    """A row created long ago but soft-deleted recently — invisible to a
    transaction_time-only window — must still be re-enqueued as a delete
    once Postgres (via the widened window predicate) surfaces it."""
    configs = _FakeConfigs(driver_cfg=ItemsPostgresqlDriverConfig(physical_table="items_c1"))
    catalogs = _FakeCatalogs()
    captured_sql: List[str] = []
    # The row's transaction_time (not selected/returned) would be far
    # outside the window; only its recent deleted_at falls inside it — this
    # is exactly the row shape the widened predicate exists to surface.
    rows_by_driver = {
        "items_elasticsearch_driver": [{"geoid": "g_old", "deleted_at": _WINDOW_START + timedelta(minutes=5)}],
    }

    mock_id_only = AsyncMock()
    with (
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            side_effect=_dql_factory(rows_by_driver, captured_sql),
        ),
        patch(
            "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
            new=mock_id_only,
        ),
    ):
        checked, enqueued = await _check_and_enqueue(
            object(),
            catalogs=catalogs,
            configs=configs,
            catalog_id="cat1",
            collection_id="col1",
            ctx=None,
            async_entries=[_es_secondary_entry()],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    assert enqueued == 1
    records = mock_id_only.call_args.kwargs["rows"]
    assert records[0].op == "delete"
    assert records[0].item_id == "g_old"


@pytest.mark.asyncio
async def test_result_ordered_oldest_latest_change_first():
    """Truncation via LIMIT must prioritize rows closest to aging out of the
    lookback window — ORDER BY the latest-change expression ASC."""
    configs = _FakeConfigs(driver_cfg=ItemsPostgresqlDriverConfig(physical_table="items_c1"))
    catalogs = _FakeCatalogs()
    captured_sql: List[str] = []

    with (
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            side_effect=_dql_factory({}, captured_sql),
        ),
        patch(
            "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
            new=AsyncMock(),
        ),
    ):
        await _check_and_enqueue(
            object(),
            catalogs=catalogs,
            configs=configs,
            catalog_id="cat1",
            collection_id="col1",
            ctx=None,
            async_entries=[_es_secondary_entry()],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    sql = captured_sql[0]
    latest_change = 'GREATEST(h."transaction_time", COALESCE(h."deleted_at", h."transaction_time"))'
    assert f'ORDER BY {latest_change} ASC' in sql
    # ORDER BY must precede LIMIT, not the other way around.
    assert sql.index("ORDER BY") < sql.index("LIMIT :row_limit")


@pytest.mark.asyncio
async def test_missing_physical_table_skips_collection():
    """A collection that has not provisioned its hub table yet is skipped
    without issuing any hub-table SQL."""
    configs = _FakeConfigs(driver_cfg=ItemsPostgresqlDriverConfig(physical_table=None))
    catalogs = _FakeCatalogs()
    captured_sql: List[str] = []

    with patch(
        "dynastore.modules.db_config.query_executor.DQLQuery",
        side_effect=_dql_factory({}, captured_sql),
    ):
        checked, enqueued = await _check_and_enqueue(
            object(),
            catalogs=catalogs,
            configs=configs,
            catalog_id="cat1",
            collection_id="col1",
            ctx=None,
            async_entries=[_es_secondary_entry()],
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    assert (checked, enqueued) == (0, 0)
    assert captured_sql == []


# ---------------------------------------------------------------------------
# _sweep_collection: routing-level gates
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pg_only_skip_when_primary_is_not_postgresql_driver():
    """#3116-style exclusion: a collection whose resolved WRITE primary is
    not the PG items driver has no hub table to read — skip silently."""
    configs = _FakeConfigs(routing=_routing_with_es_primary())
    catalogs = _FakeCatalogs()

    checked, enqueued = await _sweep_collection(
        object(),
        catalogs=catalogs,
        configs=configs,
        catalog_id="cat1",
        collection_id="col1",
        ctx=None,
        window_start=_WINDOW_START,
        window_end=_WINDOW_END,
    )

    assert (checked, enqueued) == (0, 0)
    # ItemsPostgresqlDriverConfig must never be resolved for a non-PG primary.
    assert all(cls is not ItemsPostgresqlDriverConfig for cls, _, _ in configs.calls)


@pytest.mark.asyncio
async def test_no_async_secondary_entries_skips_collection():
    """A PG-only collection with no async secondary-index WRITE entries has
    nothing to reconcile."""
    configs = _FakeConfigs(routing=_routing_with_pg_only_no_secondary())
    catalogs = _FakeCatalogs()

    checked, enqueued = await _sweep_collection(
        object(),
        catalogs=catalogs,
        configs=configs,
        catalog_id="cat1",
        collection_id="col1",
        ctx=None,
        window_start=_WINDOW_START,
        window_end=_WINDOW_END,
    )

    assert (checked, enqueued) == (0, 0)
    assert configs.calls == [(ItemsRoutingConfig, "cat1", "col1")]


@pytest.mark.asyncio
async def test_collection_failure_is_isolated_and_retried_next_tick():
    """A per-collection failure (e.g. a legacy hub table missing the
    write_id column) is swallowed by the savepoint — it must never abort
    the sweep for other collections."""
    configs = _FakeConfigs(
        routing=_routing_with_pg_primary_and_async_secondary(),
        driver_cfg=ItemsPostgresqlDriverConfig(physical_table="items_c1"),
    )
    catalogs = _FakeCatalogs()

    def _boom(sql, **_kw):
        raise RuntimeError("column h.write_id does not exist")

    with patch(
        "dynastore.modules.db_config.query_executor.DQLQuery",
        side_effect=_boom,
    ):
        checked, enqueued = await _sweep_collection(
            object(),  # no begin_nested -> best_effort_savepoint runs unguarded
            catalogs=catalogs,
            configs=configs,
            catalog_id="cat1",
            collection_id="col1",
            ctx=None,
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    assert (checked, enqueued) == (0, 0)


@pytest.mark.asyncio
async def test_invalid_routing_config_is_skipped_and_logged(caplog):
    """#3240: a stored ``ItemsRoutingConfig`` that fails Pydantic validation
    (legacy ``on_failure: outbox`` rejected by the shrunk ``FailurePolicy``)
    must not raise out of ``_sweep_collection`` — it is logged at ERROR with
    the catalog/collection id and the failing field, and the collection is
    skipped for this tick."""

    class _InvalidRoutingConfigs(_FakeConfigs):
        async def get_config(self, config_cls, catalog_id=None, collection_id=None, ctx=None, config_snapshot=None):
            self.calls.append((config_cls, catalog_id, collection_id))
            if config_cls is ItemsRoutingConfig:
                raise _legacy_on_failure_validation_error()
            raise AssertionError(f"unexpected config_cls {config_cls!r}")

    configs = _InvalidRoutingConfigs()
    catalogs = _FakeCatalogs()

    with caplog.at_level(logging.ERROR, logger="dynastore.modules.storage.obligation_sweep"):
        checked, enqueued = await _sweep_collection(
            object(),
            catalogs=catalogs,
            configs=configs,
            catalog_id="cat_bad",
            collection_id="col_bad",
            ctx=None,
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
        )

    assert (checked, enqueued) == (0, 0)
    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert "cat_bad" in message
    assert "col_bad" in message
    assert "on_failure" in message


# ---------------------------------------------------------------------------
# sweep_missing_obligations: window math + top-level wiring
# ---------------------------------------------------------------------------


def _patched_get_protocol(catalogs, configs):
    from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol

    def _get_protocol(proto):
        if proto is CatalogsProtocol:
            return catalogs
        if proto is ConfigsProtocol:
            return configs
        return None

    return _get_protocol


@pytest.mark.asyncio
async def test_window_math_lookback_and_grace():
    """window_end - window_start == LOOKBACK_MULTIPLIER*interval - grace, and
    window_end sits ``grace_seconds`` behind wall-clock now.

    Captures the actual ``window_start``/``window_end`` the hub-table query
    was called with (via the DQLQuery execute kwargs) rather than asserting
    a constant in isolation, so this proves the values the function computed
    — not just the arithmetic identity.
    """
    catalog = MagicMock(id="cat1")
    collection = MagicMock(id="col1")
    catalogs = _FakeCatalogs(catalogs=[catalog], collections={"cat1": [collection]})
    configs = _FakeConfigs(
        routing=_routing_with_pg_primary_and_async_secondary(),
        driver_cfg=ItemsPostgresqlDriverConfig(physical_table="items_c1"),
    )
    captured_windows: List[Any] = []

    def _dql_factory_capture(sql, **_kw):
        inst = MagicMock()

        async def _execute(conn, **params):
            captured_windows.append((params["window_start"], params["window_end"]))
            return []

        inst.execute = AsyncMock(side_effect=_execute)
        return inst

    interval_seconds = 600
    grace_seconds = 300
    before = datetime.now(timezone.utc)
    with (
        patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=_patched_get_protocol(catalogs, configs),
        ),
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            side_effect=_dql_factory_capture,
        ),
    ):
        result = await sweep_missing_obligations(
            _fake_sa_conn(), interval_seconds=interval_seconds, grace_seconds=grace_seconds,
        )
    after = datetime.now(timezone.utc)

    assert result == 0
    assert len(captured_windows) == 1
    window_start, window_end = captured_windows[0]

    expected_span = timedelta(
        seconds=LOOKBACK_MULTIPLIER * interval_seconds - grace_seconds,
    )
    assert window_end - window_start == expected_span

    # window_end trails "now" by exactly grace_seconds, bounded by the
    # [before, after] wall-clock bracket around the call.
    assert (before - timedelta(seconds=grace_seconds)) <= window_end <= (
        after - timedelta(seconds=grace_seconds)
    )


@pytest.mark.asyncio
async def test_empty_window_when_grace_exceeds_lookback_short_circuits():
    """grace_seconds >= lookback is a pathological config — return 0 without
    ever touching CatalogsProtocol/ConfigsProtocol."""

    def _get_protocol(proto):
        raise AssertionError("get_protocol must not be called on an empty window")

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
        result = await sweep_missing_obligations(
            object(), interval_seconds=60, grace_seconds=300,
        )

    assert result == 0


@pytest.mark.asyncio
async def test_sweep_missing_obligations_totals_across_collections():
    """End-to-end wiring: one catalog, two collections, one with a miss."""
    catalog = MagicMock(id="cat1")
    col_hit = MagicMock(id="col_hit")
    col_miss = MagicMock(id="col_miss")
    catalogs = _FakeCatalogs(
        catalogs=[catalog],
        collections={"cat1": [col_hit, col_miss]},
    )

    routing = _routing_with_pg_primary_and_async_secondary()
    driver_cfg = ItemsPostgresqlDriverConfig(physical_table="items_c1")

    class _PerCollectionConfigs(_FakeConfigs):
        async def get_config(self, config_cls, catalog_id=None, collection_id=None, ctx=None, config_snapshot=None):
            self.calls.append((config_cls, catalog_id, collection_id))
            if config_cls is ItemsRoutingConfig:
                return routing
            if config_cls is ItemsPostgresqlDriverConfig:
                return driver_cfg
            raise AssertionError(config_cls)

    configs = _PerCollectionConfigs()
    captured_sql: List[str] = []

    def _rows_by_collection(collection_id: str):
        if collection_id == "col_miss":
            return {"items_elasticsearch_driver": [{"geoid": "gX", "deleted_at": None}]}
        return {}

    def _dql_factory_per_collection(sql, **_kw):
        captured_sql.append(sql)
        inst = MagicMock()

        async def _execute(conn, **params):
            return _rows_by_collection(params["collection_id"]).get(params["driver_id"], [])

        inst.execute = AsyncMock(side_effect=_execute)
        return inst

    with (
        patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=_patched_get_protocol(catalogs, configs),
        ),
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            side_effect=_dql_factory_per_collection,
        ),
        patch(
            "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
            new=AsyncMock(),
        ) as mock_enqueue,
    ):
        total = await sweep_missing_obligations(_fake_sa_conn(), interval_seconds=600)

    assert total == 1
    mock_enqueue.assert_awaited_once()
    assert mock_enqueue.call_args.kwargs["catalog_id"] == "cat1"


@pytest.mark.asyncio
async def test_sweep_continues_across_catalogs_when_one_routing_config_is_invalid(caplog):
    """#3240 regression: one catalog's stored ``ItemsRoutingConfig`` failing
    Pydantic validation (legacy ``on_failure: outbox``) must degrade to a
    single-catalog skip, not abort the whole sweep — every other catalog's
    valid config is still swept and the run does not raise."""
    catalog_ok = MagicMock(id="cat_ok")
    catalog_bad = MagicMock(id="cat_bad")
    col_ok = MagicMock(id="col_ok")
    col_bad = MagicMock(id="col_bad")
    catalogs = _FakeCatalogs(
        catalogs=[catalog_ok, catalog_bad],
        collections={"cat_ok": [col_ok], "cat_bad": [col_bad]},
    )

    routing_ok = _routing_with_pg_primary_and_async_secondary()
    driver_cfg = ItemsPostgresqlDriverConfig(physical_table="items_c1")

    class _MixedConfigs(_FakeConfigs):
        async def get_config(self, config_cls, catalog_id=None, collection_id=None, ctx=None, config_snapshot=None):
            self.calls.append((config_cls, catalog_id, collection_id))
            if config_cls is ItemsRoutingConfig:
                if catalog_id == "cat_bad":
                    raise _legacy_on_failure_validation_error()
                return routing_ok
            if config_cls is ItemsPostgresqlDriverConfig:
                return driver_cfg
            raise AssertionError(config_cls)

    configs = _MixedConfigs()

    def _dql_factory_ok_only(sql, **_kw):
        inst = MagicMock()

        async def _execute(conn, **params):
            if params["collection_id"] == "col_ok":
                return [{"geoid": "gX", "deleted_at": None}]
            return []

        inst.execute = AsyncMock(side_effect=_execute)
        return inst

    with (
        patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=_patched_get_protocol(catalogs, configs),
        ),
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            side_effect=_dql_factory_ok_only,
        ),
        patch(
            "dynastore.modules.storage.storage_emit.enqueue_storage_op_id_only",
            new=AsyncMock(),
        ) as mock_enqueue,
        caplog.at_level(logging.ERROR, logger="dynastore.modules.storage.obligation_sweep"),
    ):
        total = await sweep_missing_obligations(_fake_sa_conn(), interval_seconds=600)

    # The valid catalog was still fully swept despite the other's bad config.
    assert total == 1
    mock_enqueue.assert_awaited_once()
    assert mock_enqueue.call_args.kwargs["catalog_id"] == "cat_ok"

    # The invalid catalog's config error was logged (still alerts) rather
    # than silently dropped or left to raise out of the sweep.
    assert any("cat_bad" in r.getMessage() for r in caplog.records)
