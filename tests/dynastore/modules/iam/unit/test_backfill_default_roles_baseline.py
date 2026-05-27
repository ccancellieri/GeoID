"""Unit tests for the ``default_roles_baseline`` backfill migration.

All DB calls are mocked via ``unittest.mock``.  Tests run serially
under ``pytest -p no:xdist -p no:logging``.
"""
from __future__ import annotations

import json
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

import dynastore.modules.iam.migrations.backfill_default_roles_baseline_audit as _m
from dynastore.models.protocols.authorization import (
    _DEFAULT_CATALOG_ROLES,
    _DEFAULT_PLATFORM_ROLES,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ALL_ROLE_NAMES = [
    s.name
    for s in list(_DEFAULT_PLATFORM_ROLES) + list(_DEFAULT_CATALOG_ROLES)
]

_ALL_EDGES = [
    [s.parent, s.name]
    for s in list(_DEFAULT_PLATFORM_ROLES) + list(_DEFAULT_CATALOG_ROLES)
    if s.parent
]


class _FakeConn:
    """Context-manager stub returned by managed_transaction."""


def _make_engine() -> MagicMock:
    """Return a fake engine that satisfies managed_transaction usage."""
    return MagicMock()


def _patch_managed_transaction(conn: Any):
    """Patch managed_transaction so it yields ``conn``."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _mgr(engine: Any):
        yield conn

    return patch(
        "dynastore.modules.iam.migrations.backfill_default_roles_baseline_audit.managed_transaction",
        side_effect=_mgr,
    )


# ---------------------------------------------------------------------------
# Clean-snapshot path: all roles present → full descriptor
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_clean_snapshot_inserts_full_descriptor():
    conn = _FakeConn()
    engine = _make_engine()
    inserted: Dict[str, Any] = {}

    # SELECT_EXISTING → no row (not yet backfilled)
    # SELECT_ROLE → row present for every canonical name
    # SELECT_HIERARCHY_EDGE → present for every canonical edge
    # INSERT_APPLIED → capture kwargs

    async def _select_existing(c, **kw):
        return None

    async def _select_role(c, **kw):
        row = MagicMock()
        row._mapping = {"name": kw["name"]}
        return row

    async def _select_edge(c, **kw):
        row = MagicMock()
        row._mapping = {"parent_role": kw["parent_role"]}
        return row

    async def _insert_applied(c, **kw):
        inserted.update(kw)
        return None

    with _patch_managed_transaction(conn):
        with patch.object(_m.SELECT_EXISTING, "execute", side_effect=_select_existing):
            with patch.object(_m.SELECT_ROLE, "execute", side_effect=_select_role):
                with patch.object(_m.SELECT_HIERARCHY_EDGE, "execute", side_effect=_select_edge):
                    with patch.object(_m.INSERT_APPLIED, "execute", side_effect=_insert_applied):
                        await _m.run_backfill(engine=engine)

    assert inserted["preset_name"] == "default_roles_baseline"
    assert inserted["scope_key"] == "platform"

    descriptor = json.loads(inserted["revoke_descriptor"])
    assert sorted(descriptor["role_names"]) == sorted(_ALL_ROLE_NAMES)
    assert len(descriptor["hierarchy_edges"]) == len(_ALL_EDGES)


# ---------------------------------------------------------------------------
# Divergence path: operator-edited roles missing from DB
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_divergence_reflects_actual_db_state():
    """When only some roles exist, the descriptor captures what is present."""
    conn = _FakeConn()
    engine = _make_engine()
    inserted: Dict[str, Any] = {}

    # Only "sysadmin" exists; all others are absent.
    _present = {"sysadmin"}

    async def _select_existing(c, **kw):
        return None

    async def _select_role(c, **kw):
        if kw["name"] in _present:
            row = MagicMock()
            row._mapping = {"name": kw["name"]}
            return row
        return None

    async def _select_edge(c, **kw):
        return None

    async def _insert_applied(c, **kw):
        inserted.update(kw)
        return None

    with _patch_managed_transaction(conn):
        with patch.object(_m.SELECT_EXISTING, "execute", side_effect=_select_existing):
            with patch.object(_m.SELECT_ROLE, "execute", side_effect=_select_role):
                with patch.object(_m.SELECT_HIERARCHY_EDGE, "execute", side_effect=_select_edge):
                    with patch.object(_m.INSERT_APPLIED, "execute", side_effect=_insert_applied):
                        await _m.run_backfill(engine=engine)

    descriptor = json.loads(inserted["revoke_descriptor"])
    # Only sysadmin present.
    assert descriptor["role_names"] == ["sysadmin"]
    # No edges present.
    assert descriptor["hierarchy_edges"] == []


# ---------------------------------------------------------------------------
# Idempotency: second run is a no-op
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_is_idempotent_when_row_exists():
    conn = _FakeConn()
    engine = _make_engine()
    insert_called = False

    async def _select_existing(c, **kw):
        # Row already present.
        row = MagicMock()
        row._mapping = {"preset_name": "default_roles_baseline"}
        return row

    async def _insert_applied(c, **kw):
        nonlocal insert_called
        insert_called = True
        return None

    with _patch_managed_transaction(conn):
        with patch.object(_m.SELECT_EXISTING, "execute", side_effect=_select_existing):
            with patch.object(_m.INSERT_APPLIED, "execute", side_effect=_insert_applied):
                await _m.run_backfill(engine=engine)

    assert not insert_called, "INSERT should not have been called on second run"
