"""UPSERT SQL construction for the task-capability registry."""
from __future__ import annotations

from dynastore.modules.tasks.registry import repository as repo


def test_upsert_sql_is_on_conflict_pk():
    sql = repo._UPSERT_SQL.lower()
    assert "insert into configs.task_capability_registry" in sql
    assert "on conflict (service, task_key) do update" in sql
    # last_seen + updated_at refreshed on structural change
    assert "updated_at = now()" in sql
