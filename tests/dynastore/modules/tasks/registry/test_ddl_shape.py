"""The registry DDL is idempotent (CREATE ... IF NOT EXISTS) and matches the
qualified table name the repository reads. No ALTER/DROP (hard invariant)."""
from __future__ import annotations

from dynastore.modules.db_config.typed_store.ddl import TASK_CAPABILITY_REGISTRY_DDL
from dynastore.modules.tasks.registry.model import TASK_CAPABILITY_REGISTRY_TABLE


def test_ddl_is_idempotent_create_only():
    sql = TASK_CAPABILITY_REGISTRY_DDL.lower()
    assert "create table if not exists" in sql
    assert "create index if not exists" in sql
    for forbidden in (" alter table ", " drop table ", " drop column ", " rename "):
        assert forbidden not in f" {sql} ", f"forbidden in-place DDL: {forbidden!r}"


def test_ddl_targets_the_repository_table():
    assert TASK_CAPABILITY_REGISTRY_TABLE in TASK_CAPABILITY_REGISTRY_DDL
    assert "primary key (service, task_key)" in TASK_CAPABILITY_REGISTRY_DDL.lower()
