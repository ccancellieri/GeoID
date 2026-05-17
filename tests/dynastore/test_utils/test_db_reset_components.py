#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Selective ``--components`` dispatch — see dynastore#295.

These tests exercise the dispatch wiring without touching a real DB:
calls into ``conn.execute`` and ``conn.fetch`` are intercepted via a
minimal fake connection that records issued statements.
"""
from __future__ import annotations

import re
from typing import Any

import pytest

from dynastore.scripts import db_reset as dbr
from tests.dynastore.test_utils import db_cleanup as dc


# ── Token parsing ──────────────────────────────────────────────────────────────


def test_parse_components_strips_dedupes_and_lowercases() -> None:
    assert dbr._parse_components(" Configs , iam ,iam") == ("configs", "iam")


def test_parse_components_rejects_unknown_token() -> None:
    with pytest.raises(SystemExit) as ei:
        dbr._parse_components("configs,bogus")
    assert "bogus" in str(ei.value)


def test_parse_components_rejects_empty_input() -> None:
    with pytest.raises(SystemExit):
        dbr._parse_components(",,")


def test_known_components_set_is_the_advertised_set() -> None:
    assert dbr.KNOWN_COMPONENTS == {"configs", "iam", "tasks", "catalog", "cron"}


# ── Tenant-schema pattern parity ──────────────────────────────────────────────


def test_tenant_schema_pattern_matches_test_utils() -> None:
    """``_TENANT_SCHEMA_PATTERN`` in the production script must equal the
    test-utils declaration. Drift means the catalog component would either
    skip live tenants or match unrelated schemas."""
    assert dbr._TENANT_SCHEMA_PATTERN == dc.TENANT_SCHEMA_PATTERN


# ── Dispatch (fake-connection based) ──────────────────────────────────────────


class _FakeConn:
    def __init__(self, fetch_rows: list[dict[str, Any]] | None = None) -> None:
        self.executed: list[str] = []
        self.fetched: list[tuple[str, tuple[Any, ...]]] = []
        self._fetch_rows = fetch_rows or []

    async def execute(self, sql: str, *args: Any) -> None:
        self.executed.append(sql)

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.fetched.append((sql, args))
        return self._fetch_rows


@pytest.mark.asyncio
async def test_components_configs_calls_reset_configs(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()
    calls: list[str] = []

    async def fake_reset_configs(c, dry_run):
        calls.append("configs")

    monkeypatch.setattr(dbr, "_reset_configs", fake_reset_configs)
    await dbr._reset_components(conn, ("configs",), dry_run=False)
    assert calls == ["configs"]


@pytest.mark.asyncio
async def test_components_iam_drops_iam_schema_only() -> None:
    conn = _FakeConn()
    await dbr._reset_components(conn, ("iam",), dry_run=False)
    assert any('DROP SCHEMA IF EXISTS "iam"' in s for s in conn.executed)
    assert not any('DROP SCHEMA IF EXISTS "tasks"' in s for s in conn.executed)


@pytest.mark.asyncio
async def test_components_tasks_drops_tasks_schema_only() -> None:
    conn = _FakeConn()
    await dbr._reset_components(conn, ("tasks",), dry_run=False)
    assert any('DROP SCHEMA IF EXISTS "tasks"' in s for s in conn.executed)
    assert not any('DROP SCHEMA IF EXISTS "iam"' in s for s in conn.executed)


@pytest.mark.asyncio
async def test_components_catalog_drops_tenant_schemas_only() -> None:
    conn = _FakeConn(fetch_rows=[
        {"nspname": "s_abcd1234"},
        {"nspname": "s_zzzz0000"},
    ])
    await dbr._reset_components(conn, ("catalog",), dry_run=False)
    assert any('DROP SCHEMA IF EXISTS "s_abcd1234"' in s for s in conn.executed)
    assert any('DROP SCHEMA IF EXISTS "s_zzzz0000"' in s for s in conn.executed)
    # Configs/iam/tasks left intact.
    assert not any('DROP SCHEMA IF EXISTS "configs"' in s for s in conn.executed)
    assert not any('DROP SCHEMA IF EXISTS "iam"' in s for s in conn.executed)
    # The enumerate-tenants query was issued with the canonical pattern.
    assert conn.fetched
    assert conn.fetched[0][1] == (dbr._TENANT_SCHEMA_PATTERN,)


@pytest.mark.asyncio
async def test_components_iam_tasks_combo_preserves_tenant_data() -> None:
    """The motivating case from #295: ``--components iam,tasks`` must NOT
    touch tenant ``s_<base36>`` schemas."""
    conn = _FakeConn()
    await dbr._reset_components(conn, ("iam", "tasks"), dry_run=False)
    drops = [s for s in conn.executed if s.startswith("DROP SCHEMA")]
    assert any('"iam"' in s for s in drops)
    assert any('"tasks"' in s for s in drops)
    # No tenant pattern enumeration.
    tenant_query = [q for q, _ in conn.fetched if "pg_namespace" in q]
    assert tenant_query == []


@pytest.mark.asyncio
async def test_components_dispatch_order_is_fixed_independent_of_input() -> None:
    """Caller order (cron,iam,configs) maps to canonical order
    (configs,iam,cron) — so cron wipe always runs last."""
    conn = _FakeConn()
    seen_components: list[str] = []

    real_drop = dbr._drop_named_schemas
    real_wipe = dbr._wipe_cron_jobs

    async def spy_drop(c, schemas, dry_run, label):
        seen_components.append(label)
        await real_drop(c, schemas, dry_run, label)

    async def spy_wipe(c, dry_run):
        seen_components.append("cron")
        await real_wipe(c, dry_run)

    async def spy_configs(c, dry_run):
        seen_components.append("configs")

    import dynastore.scripts.db_reset as m
    m._drop_named_schemas = spy_drop  # type: ignore[assignment]
    m._wipe_cron_jobs = spy_wipe       # type: ignore[assignment]
    m._reset_configs = spy_configs     # type: ignore[assignment]
    try:
        await dbr._reset_components(conn, ("cron", "iam", "configs"), dry_run=False)
    finally:
        m._drop_named_schemas = real_drop  # type: ignore[assignment]
        m._wipe_cron_jobs = real_wipe       # type: ignore[assignment]

    assert seen_components == ["configs", "iam", "cron"]


@pytest.mark.asyncio
async def test_dry_run_does_not_execute_drops() -> None:
    conn = _FakeConn()
    await dbr._reset_components(conn, ("iam", "tasks"), dry_run=True)
    # In dry-run, no DROP statements actually issued via execute.
    assert not any(s.startswith("DROP SCHEMA") for s in conn.executed)


# ── Backward compat ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_components_configs_matches_mode_configs_path() -> None:
    """``--components configs`` and ``--mode configs`` must converge on the
    same _reset_configs entry point so behaviour is bit-identical."""
    seen: list[str] = []

    async def fake_reset_configs(c, dry_run):
        seen.append("configs-entry")

    import dynastore.scripts.db_reset as m
    saved = m._reset_configs
    m._reset_configs = fake_reset_configs  # type: ignore[assignment]
    try:
        await dbr._reset_components(_FakeConn(), ("configs",), dry_run=False)
    finally:
        m._reset_configs = saved  # type: ignore[assignment]
    assert seen == ["configs-entry"]


# ── Component schema map sanity ───────────────────────────────────────────────


def test_component_schemas_map_only_uses_fixed_lowercase_names() -> None:
    """Each schema name must be a fixed, lowercase identifier — anything
    matching the tenant pattern would belong to the ``catalog`` component
    and is a bug in this map."""
    tenant_rx = re.compile(dc.TENANT_SCHEMA_PATTERN)
    for component, schemas in dbr._COMPONENT_SCHEMAS.items():
        for s in schemas:
            assert s == s.lower(), (component, s)
            assert not tenant_rx.fullmatch(s), (component, s)
