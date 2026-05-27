"""Tests for the catalog_preset_delegation allowlist backfill migration (#1426).

Covers:
1. Happy path — pre-fix policy row (no allowlist) is updated with the default.
2. Idempotency — once recorded in iam.applied_presets, subsequent runs no-op.
3. Operator-customised allowlist (key present, value differs) → untouched.
4. Missing policy row — migration still records the audit row to stop retrying.
5. Pure-function ``_augment_conditions`` direct assertions.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch
import pytest

from dynastore.modules.iam.migrations import (
    tighten_catalog_preset_allowlist as m,
)


# ---------------------------------------------------------------------------
# Pure-function helper
# ---------------------------------------------------------------------------

def test_augment_adds_default_allowlist_when_missing():
    conds = [{"type": "catalog_admin_required", "config": {"required_roles": ["admin"]}}]
    out = m._augment_conditions(conds)
    assert out is not None
    cfg = out[0]["config"]
    assert cfg["required_roles"] == ["admin"]
    assert cfg["allowed_preset_names"] == m._DEFAULT_ALLOWLIST


def test_augment_no_op_when_allowlist_already_present():
    conds = [
        {
            "type": "catalog_admin_required",
            "config": {
                "required_roles": ["admin"],
                "allowed_preset_names": ["custom_preset"],
            },
        }
    ]
    assert m._augment_conditions(conds) is None


def test_augment_preserves_unrelated_conditions():
    conds = [
        {"type": "rate_limit", "config": {"limit": 100}},
        {"type": "catalog_admin_required", "config": {"required_roles": ["admin"]}},
    ]
    out = m._augment_conditions(conds)
    assert out is not None
    assert out[0] == {"type": "rate_limit", "config": {"limit": 100}}
    assert out[1]["config"]["allowed_preset_names"] == m._DEFAULT_ALLOWLIST


def test_augment_handles_missing_config_field():
    conds = [{"type": "catalog_admin_required"}]
    out = m._augment_conditions(conds)
    assert out is not None
    assert out[0]["config"]["allowed_preset_names"] == m._DEFAULT_ALLOWLIST


def test_augment_handles_non_list_input():
    assert m._augment_conditions(None) is None  # type: ignore[arg-type]
    assert m._augment_conditions({}) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Migration integration (DB-mocked)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _txn(_engine: Any):
    yield MagicMock()


@pytest.mark.asyncio
async def test_migration_updates_policy_when_allowlist_absent():
    engine = MagicMock()
    update_calls: List[Dict[str, Any]] = []
    insert_calls: List[Dict[str, Any]] = []

    async def _select_applied(_resource, **kw):
        return None  # not yet applied

    async def _select_policy(_resource, **kw):
        # Pre-fix row: condition has no allowlist key.
        return (json.dumps([{
            "type": "catalog_admin_required",
            "config": {"required_roles": ["admin"]},
        }]),)

    async def _update(_resource, **kw):
        update_calls.append(kw)

    async def _insert(_resource, **kw):
        insert_calls.append(kw)

    with patch.object(m, "managed_transaction", side_effect=_txn), \
         patch.object(m.SELECT_APPLIED, "execute", _select_applied), \
         patch.object(m.SELECT_POLICY, "execute", _select_policy), \
         patch.object(m.UPDATE_POLICY_CONDITIONS, "execute", _update), \
         patch.object(m.INSERT_APPLIED, "execute", _insert):
        await m.run_migration(engine)

    assert len(update_calls) == 1
    augmented = json.loads(update_calls[0]["conditions"])
    assert augmented[0]["config"]["allowed_preset_names"] == m._DEFAULT_ALLOWLIST
    assert len(insert_calls) == 1
    assert insert_calls[0]["preset_name"] == m._PRESET_NAME


@pytest.mark.asyncio
async def test_migration_idempotent_when_audit_row_exists():
    engine = MagicMock()
    update_calls: List[Dict[str, Any]] = []
    insert_calls: List[Dict[str, Any]] = []

    async def _select_applied(_resource, **kw):
        return (1,)  # already applied

    async def _update(_resource, **kw):
        update_calls.append(kw)

    async def _insert(_resource, **kw):
        insert_calls.append(kw)

    with patch.object(m, "managed_transaction", side_effect=_txn), \
         patch.object(m.SELECT_APPLIED, "execute", _select_applied), \
         patch.object(m.UPDATE_POLICY_CONDITIONS, "execute", _update), \
         patch.object(m.INSERT_APPLIED, "execute", _insert):
        await m.run_migration(engine)

    assert update_calls == [], "policy must NOT be updated on repeat run"
    assert insert_calls == [], "audit row must NOT be re-inserted"


@pytest.mark.asyncio
async def test_migration_leaves_operator_customised_allowlist_alone():
    engine = MagicMock()
    update_calls: List[Dict[str, Any]] = []
    insert_calls: List[Dict[str, Any]] = []

    async def _select_applied(_resource, **kw):
        return None

    async def _select_policy(_resource, **kw):
        return (json.dumps([{
            "type": "catalog_admin_required",
            "config": {
                "required_roles": ["admin"],
                "allowed_preset_names": ["custom_only"],
            },
        }]),)

    async def _update(_resource, **kw):
        update_calls.append(kw)

    async def _insert(_resource, **kw):
        insert_calls.append(kw)

    with patch.object(m, "managed_transaction", side_effect=_txn), \
         patch.object(m.SELECT_APPLIED, "execute", _select_applied), \
         patch.object(m.SELECT_POLICY, "execute", _select_policy), \
         patch.object(m.UPDATE_POLICY_CONDITIONS, "execute", _update), \
         patch.object(m.INSERT_APPLIED, "execute", _insert):
        await m.run_migration(engine)

    assert update_calls == [], "operator allowlist must not be overwritten"
    assert len(insert_calls) == 1, "still record completion to skip future runs"


@pytest.mark.asyncio
async def test_migration_records_completion_when_policy_absent():
    engine = MagicMock()
    update_calls: List[Dict[str, Any]] = []
    insert_calls: List[Dict[str, Any]] = []

    async def _select_applied(_resource, **kw):
        return None

    async def _select_policy(_resource, **kw):
        return None  # policy row absent

    async def _update(_resource, **kw):
        update_calls.append(kw)

    async def _insert(_resource, **kw):
        insert_calls.append(kw)

    with patch.object(m, "managed_transaction", side_effect=_txn), \
         patch.object(m.SELECT_APPLIED, "execute", _select_applied), \
         patch.object(m.SELECT_POLICY, "execute", _select_policy), \
         patch.object(m.UPDATE_POLICY_CONDITIONS, "execute", _update), \
         patch.object(m.INSERT_APPLIED, "execute", _insert):
        await m.run_migration(engine)

    assert update_calls == []
    assert len(insert_calls) == 1
