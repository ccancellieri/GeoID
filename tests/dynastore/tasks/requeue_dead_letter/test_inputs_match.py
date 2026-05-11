"""Unit tests for the URL-path → JSONB filter translator and the
``inputs_match`` argument added to ``requeue_dead_letter_tasks_by_type``.

The translator (`_build_inputs_match`) is the single piece of business
logic in the new OGC Process — everything else is wiring. We pin its
contract end-to-end so future task_types added to
:data:`TASK_TYPE_INPUTS_KEYS` automatically pick up the right behavior.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from dynastore.tasks.requeue_dead_letter.task import _build_inputs_match


def test_platform_scope_yields_no_filter():
    assert _build_inputs_match(
        task_type="index_propagation",
        catalog_id=None,
        collection_id=None,
    ) == {}


def test_catalog_scope_maps_to_jsonb_catalog_key():
    out = _build_inputs_match(
        task_type="index_propagation",
        catalog_id="c1",
        collection_id=None,
    )
    assert out == {"catalog": "c1"}


def test_collection_scope_maps_both_keys():
    out = _build_inputs_match(
        task_type="index_propagation",
        catalog_id="c1",
        collection_id="col1",
    )
    assert out == {"catalog": "c1", "collection": "col1"}


def test_scoped_call_with_unknown_task_type_rejects():
    """An entity-scoped endpoint hit with a task_type that has no
    declared JSONB keys must be rejected, not silently turned into a
    platform-wide replay."""
    with pytest.raises(ValueError, match="do not declare catalog/collection"):
        _build_inputs_match(
            task_type="some_other_task",
            catalog_id="c1",
            collection_id=None,
        )


# --- maintenance fn integration ---

@pytest.mark.asyncio
async def test_maintenance_fn_rejects_unsafe_jsonb_key():
    """The maintenance helper inlines the JSONB key literal (it must, for
    asyncpg parameter binding) so it must refuse keys that could break
    out of the literal."""
    from dynastore.modules.tasks import maintenance

    with pytest.raises(ValueError, match="unsafe JSONB key"):
        await maintenance.requeue_dead_letter_tasks_by_type(
            engine=object(),  # type: ignore[arg-type]
            task_type="index_propagation",
            inputs_match={"catalog'; DROP TABLE--": "c1"},
        )


@pytest.mark.asyncio
async def test_maintenance_fn_passes_jsonb_filter_into_sql_and_params():
    """When `inputs_match` is supplied the SQL gains the right WHERE
    clause and the param dict carries the matching `jm_<key>` binding.
    """
    from dynastore.modules.tasks import maintenance

    captured: dict = {}

    class _FakeDQL:
        def __init__(self, sql, **_kwargs):
            captured["sql"] = sql

        async def execute(self, _conn, **params):
            captured["params"] = params
            return []

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield object()

    with patch.object(maintenance, "DQLQuery", _FakeDQL), patch.object(
        maintenance, "managed_transaction", _fake_managed_transaction,
    ), patch.object(maintenance, "get_task_schema", return_value="tasks"):
        count = await maintenance.requeue_dead_letter_tasks_by_type(
            engine=object(),  # type: ignore[arg-type]
            task_type="index_propagation",
            inputs_match={"catalog": "c1", "collection": "col1"},
            limit=42,
        )

    assert count == 0
    assert "inputs->>'catalog' = :jm_catalog" in captured["sql"]
    assert "inputs->>'collection' = :jm_collection" in captured["sql"]
    assert captured["params"]["jm_catalog"] == "c1"
    assert captured["params"]["jm_collection"] == "col1"
    assert captured["params"]["task_type"] == "index_propagation"
    assert captured["params"]["lim"] == 42


@pytest.mark.asyncio
async def test_maintenance_fn_without_inputs_match_keeps_old_shape():
    """Backwards check: no `inputs_match` → no extra clauses, no jm_*
    params (the existing single-row contract is preserved)."""
    from dynastore.modules.tasks import maintenance

    captured: dict = {}

    class _FakeDQL:
        def __init__(self, sql, **_kwargs):
            captured["sql"] = sql

        async def execute(self, _conn, **params):
            captured["params"] = params
            return [{"task_id": "t1"}, {"task_id": "t2"}]

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield object()

    with patch.object(maintenance, "DQLQuery", _FakeDQL), patch.object(
        maintenance, "managed_transaction", _fake_managed_transaction,
    ), patch.object(maintenance, "get_task_schema", return_value="tasks"):
        count = await maintenance.requeue_dead_letter_tasks_by_type(
            engine=object(),  # type: ignore[arg-type]
            task_type="index_propagation",
        )

    assert count == 2
    assert "inputs->>" not in captured["sql"]
    assert not any(k.startswith("jm_") for k in captured["params"])
