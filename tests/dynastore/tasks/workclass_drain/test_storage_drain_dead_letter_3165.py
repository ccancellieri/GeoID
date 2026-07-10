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

"""Unit tests for the storage-drain dead-letter mechanism (#3165).

Pure unit tests — no PG / no DB fixture, mirrors
``test_storage_drain_access_envelope.py``. Every ``tasks.storage`` row is a
plain dict; ``_mark_dead`` / ``_mark_retry`` / ``_resolve_routing_membership``
are monkeypatched seams so the classification logic in
``_handle_unresolvable_rows`` is exercised directly, without a live engine.

Covers both dead-letter mechanisms:

* B — routing-config membership: a driver_id absent from every lane of the
  collection's ``ItemsRoutingConfig`` is dead-lettered immediately; a driver_id
  still listed (split-deployment: configured, just not registered in THIS
  process) is never dead-lettered by B.
* A — age/attempts backstop: rows B did not classify are dead-lettered once
  ``attempts`` or ``created_at`` age crosses the configured cutoff, otherwise
  retried exactly as before ``0`` disables a cutoff.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch
from uuid import uuid4

import pytest

from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask


def _row(
    *,
    catalog_id: str = "cat_a",
    collection_id: Optional[str] = "coll_a",
    attempts: int = 0,
    age_seconds: float = 0.0,
) -> Dict[str, Any]:
    """Build a minimal claimed-row dict — only the fields
    ``_handle_unresolvable_rows`` / ``_mark_dead`` / ``_mark_retry`` read."""
    return {
        "op_id": str(uuid4()),
        "day": None,
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "attempts": attempts,
        "created_at": datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
        "claim_version": 1,
        "claimed_by": "owner-1",
    }


class _Recorder:
    """Fakes ``_mark_dead`` / ``_mark_retry`` — records op_ids, never touches PG."""

    def __init__(self) -> None:
        self.dead: List[str] = []
        self.retried: List[Tuple[str, str]] = []

    async def mark_dead(self, *, engine, task_schema, row, owner_id) -> None:
        self.dead.append(row["op_id"])

    async def mark_retry(self, *, engine, task_schema, row, owner_id, error) -> None:
        self.retried.append((row["op_id"], error))


def _wire(task: StorageDrainTask, recorder: _Recorder, monkeypatch) -> None:
    monkeypatch.setattr(task, "_mark_dead", recorder.mark_dead)
    monkeypatch.setattr(task, "_mark_retry", recorder.mark_retry)


def _stub_membership(
    task: StorageDrainTask, monkeypatch, membership: Optional[frozenset],
) -> None:
    async def _fake(catalog_id: str, collection_id: str):
        return membership

    monkeypatch.setattr(task, "_resolve_routing_membership", _fake)


# ---------------------------------------------------------------------------
# 1. B — config present, driver absent from every lane -> dead immediately.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_b_driver_absent_from_config_dead_letters_immediately(
    monkeypatch, caplog,
):
    task = StorageDrainTask()
    recorder = _Recorder()
    _wire(task, recorder, monkeypatch)
    _stub_membership(task, monkeypatch, frozenset({"items_postgresql_driver"}))

    rows = [_row(), _row()]
    with caplog.at_level(logging.WARNING):
        counts = await task._handle_unresolvable_rows(
            engine=object(), task_schema="tasks",
            driver_id="items_elasticsearch_driver",
            driver_rows=rows, owner_id="owner-1",
            max_attempts=200, max_age_seconds=604800,
        )

    assert counts == {"retried": 0, "dead_lettered": 2}
    assert sorted(recorder.dead) == sorted(r["op_id"] for r in rows)
    assert recorder.retried == []
    assert any(
        "driver_unregistered" in r.getMessage()
        and "items_elasticsearch_driver" in r.getMessage()
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# 2. B guard — config absent (membership None) -> falls through to A;
#    a young row (few attempts, fresh) retries as today.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_b_absent_config_falls_through_to_a_young_row_retries(monkeypatch):
    task = StorageDrainTask()
    recorder = _Recorder()
    _wire(task, recorder, monkeypatch)
    _stub_membership(task, monkeypatch, None)

    row = _row(attempts=1, age_seconds=5)
    counts = await task._handle_unresolvable_rows(
        engine=object(), task_schema="tasks",
        driver_id="items_elasticsearch_driver",
        driver_rows=[row], owner_id="owner-1",
        max_attempts=200, max_age_seconds=604800,
    )

    assert counts == {"retried": 1, "dead_lettered": 0}
    assert recorder.dead == []
    assert recorder.retried == [(row["op_id"], "indexer not registered: items_elasticsearch_driver")]


# ---------------------------------------------------------------------------
# 3. B guard — the config lookup itself raises -> _resolve_routing_membership
#    swallows it to None -> rows retry (transient), never dead.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_b_config_lookup_raising_never_dead_letters(monkeypatch):
    class _RaisingConfigsMgr:
        async def get_config(self, config_cls, catalog_id=None, collection_id=None,
                              ctx=None, config_snapshot=None):
            raise RuntimeError("simulated config-store outage")

    task = StorageDrainTask()
    recorder = _Recorder()
    _wire(task, recorder, monkeypatch)

    row = _row(attempts=0, age_seconds=0)
    with patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=_RaisingConfigsMgr(),
    ):
        # Exercise the real _resolve_routing_membership (not stubbed) so the
        # exception-swallowing path itself is under test.
        counts = await task._handle_unresolvable_rows(
            engine=object(), task_schema="tasks",
            driver_id="items_elasticsearch_driver",
            driver_rows=[row], owner_id="owner-1",
            max_attempts=200, max_age_seconds=604800,
        )

    assert counts == {"retried": 1, "dead_lettered": 0}
    assert recorder.dead == []
    assert len(recorder.retried) == 1


# ---------------------------------------------------------------------------
# 4. B split-deployment guard — config CONTAINS the driver (even though
#    unregistered in this process) -> retry, never dead.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_b_split_deployment_driver_configured_elsewhere_retries(monkeypatch):
    task = StorageDrainTask()
    recorder = _Recorder()
    _wire(task, recorder, monkeypatch)
    _stub_membership(
        task, monkeypatch, frozenset({"items_elasticsearch_driver"}),
    )

    row = _row(attempts=2, age_seconds=10)
    counts = await task._handle_unresolvable_rows(
        engine=object(), task_schema="tasks",
        driver_id="items_elasticsearch_driver",
        driver_rows=[row], owner_id="owner-1",
        max_attempts=200, max_age_seconds=604800,
    )

    assert counts == {"retried": 1, "dead_lettered": 0}
    assert recorder.dead == []
    assert len(recorder.retried) == 1


# ---------------------------------------------------------------------------
# 5. A — attempts >= max_attempts (config absent) -> dead,
#    reason=unresolvable_driver_exhausted.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_attempts_exhausted_dead_letters(monkeypatch, caplog):
    task = StorageDrainTask()
    recorder = _Recorder()
    _wire(task, recorder, monkeypatch)
    _stub_membership(task, monkeypatch, None)

    row = _row(attempts=5, age_seconds=1)
    with caplog.at_level(logging.WARNING):
        counts = await task._handle_unresolvable_rows(
            engine=object(), task_schema="tasks",
            driver_id="items_elasticsearch_driver",
            driver_rows=[row], owner_id="owner-1",
            max_attempts=5, max_age_seconds=604800,
        )

    assert counts == {"retried": 0, "dead_lettered": 1}
    assert recorder.dead == [row["op_id"]]
    assert recorder.retried == []
    assert any(
        "unresolvable_driver_exhausted" in r.getMessage()
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# 6. A — created_at older than max_age_seconds -> dead.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_age_exhausted_dead_letters(monkeypatch, caplog):
    task = StorageDrainTask()
    recorder = _Recorder()
    _wire(task, recorder, monkeypatch)
    _stub_membership(task, monkeypatch, None)

    row = _row(attempts=0, age_seconds=1000)
    with caplog.at_level(logging.WARNING):
        counts = await task._handle_unresolvable_rows(
            engine=object(), task_schema="tasks",
            driver_id="items_elasticsearch_driver",
            driver_rows=[row], owner_id="owner-1",
            max_attempts=200, max_age_seconds=500,
        )

    assert counts == {"retried": 0, "dead_lettered": 1}
    assert recorder.dead == [row["op_id"]]
    assert recorder.retried == []
    assert any(
        "unresolvable_driver_exhausted" in r.getMessage()
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# 7. A disabled (0) -> forever-retry preserved regardless of attempts/age.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_disabled_never_dead_letters(monkeypatch):
    task = StorageDrainTask()
    recorder = _Recorder()
    _wire(task, recorder, monkeypatch)
    _stub_membership(task, monkeypatch, None)

    row = _row(attempts=10_000, age_seconds=10_000_000)
    counts = await task._handle_unresolvable_rows(
        engine=object(), task_schema="tasks",
        driver_id="items_elasticsearch_driver",
        driver_rows=[row], owner_id="owner-1",
        max_attempts=0, max_age_seconds=0,
    )

    assert counts == {"retried": 1, "dead_lettered": 0}
    assert recorder.dead == []
    assert len(recorder.retried) == 1


# ---------------------------------------------------------------------------
# 8. Namespace-equivalence pin: the driver_id stamped on a tasks.storage row
#    (_to_snake(type(driver).__name__)) equals an OperationDriverEntry's
#    normalized driver_ref for the same driver class — no translation is
#    needed between the two when comparing membership (#3165 B mechanism).
# ---------------------------------------------------------------------------


def test_driver_id_namespace_matches_routing_config_entry_ref():
    from dynastore.modules.storage.drivers.elasticsearch import (
        ItemsElasticsearchDriver,
    )
    from dynastore.modules.storage.routing_config import OperationDriverEntry
    from dynastore.tools.typed_store.base import _to_snake

    stamped_driver_id = _to_snake(ItemsElasticsearchDriver.__name__)
    entry = OperationDriverEntry(driver_ref="ItemsElasticsearchDriver")

    assert entry.driver_ref == stamped_driver_id == "items_elasticsearch_driver"


# ---------------------------------------------------------------------------
# Bonus: _resolve_routing_membership itself (real code, no stub) — collects
# driver_refs across every lane, and treats a config with no entries in any
# lane as unresolvable (None).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_routing_membership_collects_every_lane():
    from dynastore.modules.storage.routing_config import (
        ItemsRoutingConfig,
        OperationDriverEntry,
    )

    cfg = ItemsRoutingConfig.model_construct(
        operations={
            "WRITE": [OperationDriverEntry(driver_ref="items_postgresql_driver")],
            "READ": [OperationDriverEntry(driver_ref="items_elasticsearch_driver")],
            "INDEX": [OperationDriverEntry(driver_ref="items_elasticsearch_driver")],
        },
    )

    class _FakeConfigsMgr:
        async def get_config(self, config_cls, catalog_id=None, collection_id=None,
                              ctx=None, config_snapshot=None):
            assert config_cls is ItemsRoutingConfig
            return cfg

    task = StorageDrainTask()
    with patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=_FakeConfigsMgr(),
    ):
        membership = await task._resolve_routing_membership("cat_a", "coll_a")

    assert membership == frozenset(
        {"items_postgresql_driver", "items_elasticsearch_driver"},
    )


@pytest.mark.asyncio
async def test_resolve_routing_membership_empty_config_is_none():
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig

    cfg = ItemsRoutingConfig.model_construct(operations={})

    class _FakeConfigsMgr:
        async def get_config(self, config_cls, catalog_id=None, collection_id=None,
                              ctx=None, config_snapshot=None):
            return cfg

    task = StorageDrainTask()
    with patch(
        "dynastore.tools.discovery.get_protocol",
        return_value=_FakeConfigsMgr(),
    ):
        membership = await task._resolve_routing_membership("cat_a", "coll_a")

    assert membership is None


@pytest.mark.asyncio
async def test_resolve_routing_membership_no_collection_id_is_none():
    task = StorageDrainTask()
    assert await task._resolve_routing_membership("cat_a", "") is None


# ---------------------------------------------------------------------------
# Bonus: WARNING dedup — one per (reason, driver_id, catalog, collection)
# per run, not per row / per call.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dead_letter_warning_logged_once_per_group(monkeypatch, caplog):
    task = StorageDrainTask()
    recorder = _Recorder()
    _wire(task, recorder, monkeypatch)
    _stub_membership(task, monkeypatch, frozenset())

    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            await task._handle_unresolvable_rows(
                engine=object(), task_schema="tasks",
                driver_id="items_elasticsearch_driver",
                driver_rows=[_row()], owner_id="owner-1",
                max_attempts=200, max_age_seconds=604800,
            )

    matching = [
        r for r in caplog.records
        if "driver_unregistered" in r.getMessage()
    ]
    assert len(matching) == 1, f"expected exactly one WARNING; got {len(matching)}"


# ---------------------------------------------------------------------------
# Bonus: drain_once wiring — an unresolved indexer routes through
# _handle_unresolvable_rows (not the old unconditional _apply_retry_all),
# and dead_lettered is folded into the per-batch metrics.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_once_routes_unresolved_indexer_through_dead_letter(monkeypatch):
    task = StorageDrainTask()
    row = _row(catalog_id="tenant_a", collection_id="coll_a")
    row["driver_id"] = "items_elasticsearch_driver"

    async def _fake_claim_batch(**kwargs):
        return [row]

    async def _fake_resolve_indexer(driver_id: str):
        return None

    async def _fake_membership(catalog_id: str, collection_id: str):
        return frozenset()  # driver configured nowhere -> dead-letters

    dead: List[str] = []

    async def _fake_mark_dead(*, engine, task_schema, row, owner_id):
        dead.append(row["op_id"])

    monkeypatch.setattr(task, "_claim_batch", _fake_claim_batch)
    monkeypatch.setattr(task, "_resolve_indexer", _fake_resolve_indexer)
    monkeypatch.setattr(task, "_resolve_routing_membership", _fake_membership)
    monkeypatch.setattr(task, "_mark_dead", _fake_mark_dead)

    with patch(
        "dynastore.modules.tasks.tasks_module.get_task_schema",
        return_value="tasks",
    ):
        count = await task.drain_once(engine=object(), owner_id="owner-1")

    assert count == 1
    assert dead == [row["op_id"]]
    assert task._run_metrics["dead_lettered"] == 1
    assert task._run_metrics["retried"] == 0
