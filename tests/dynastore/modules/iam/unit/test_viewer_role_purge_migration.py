"""Unit coverage for the viewer-role purge migration (#660)."""

from __future__ import annotations

import pytest

from dynastore.modules.iam import viewer_role_purge_migration as mod


class _FakeConn:
    pass


class _FakeTxCM:
    def __init__(self, conn: _FakeConn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def patched(monkeypatch):
    """Patch managed_transaction + DQLQuery so DELETE calls are recorded and
    each one's rowcount is scriptable per-target."""
    state: dict = {
        "role_table_schemas": [],
        "delete_calls": [],
        "rowcount_by_sql_substr": {},  # SQL substring -> int rowcount
        "raises_on": set(),  # SQL substrings to raise on
    }

    def fake_managed(engine):
        return _FakeTxCM(_FakeConn())

    class _FakeDQLQuery:
        def __init__(self, sql: str, *, result_handler=None, **_):
            self.sql = sql
            self.result_handler = result_handler

        async def execute(self, conn, **kwargs):
            state["delete_calls"].append((self.sql, kwargs))
            for marker in state["raises_on"]:
                if marker in self.sql:
                    raise RuntimeError("simulated DELETE failure")
            for marker, rc in state["rowcount_by_sql_substr"].items():
                if marker in self.sql:
                    return rc
            return 0

    async def fake_find(conn, **kwargs):
        return state["role_table_schemas"]

    monkeypatch.setattr(mod, "managed_transaction", fake_managed, raising=True)
    monkeypatch.setattr(mod, "DQLQuery", _FakeDQLQuery, raising=True)
    monkeypatch.setattr(
        mod._find_role_tables_query, "execute", fake_find, raising=True,
    )
    return state


@pytest.mark.asyncio
async def test_no_engine_is_noop():
    assert await mod.purge_viewer_role_rows(None) == 0


@pytest.mark.asyncio
async def test_clean_tree_returns_zero(patched):
    patched["role_table_schemas"] = [{"schema_name": "iam"}]
    deleted = await mod.purge_viewer_role_rows(object())
    assert deleted == 0
    # one roles DELETE + one grants DELETE per schema
    assert len(patched["delete_calls"]) == 2


@pytest.mark.asyncio
async def test_returns_total_rowcount_across_schemas(patched):
    patched["role_table_schemas"] = [
        {"schema_name": "iam"},
        {"schema_name": "cat_a"},
        {"schema_name": "cat_b"},
    ]
    patched["rowcount_by_sql_substr"] = {
        '"iam".roles': 1,
        '"iam".grants': 2,
        '"cat_a".roles': 0,
        '"cat_a".grants': 3,
        '"cat_b".roles': 1,
        '"cat_b".grants': 0,
    }
    deleted = await mod.purge_viewer_role_rows(object())
    assert deleted == 7  # 1+2+0+3+1+0
    # 3 schemas × 2 deletes
    assert len(patched["delete_calls"]) == 6
    assert all(
        kwargs == {"role_name": "viewer"} for _, kwargs in patched["delete_calls"]
    )


@pytest.mark.asyncio
async def test_role_delete_is_guarded_by_inert_predicate(patched):
    patched["role_table_schemas"] = [{"schema_name": "iam"}]
    await mod.purge_viewer_role_rows(object())
    role_sql = next(
        sql for sql, _ in patched["delete_calls"] if '"iam".roles' in sql
    )
    assert "policies = '[]'::jsonb" in role_sql
    assert "metadata = '{}'::jsonb" in role_sql


@pytest.mark.asyncio
async def test_uses_named_param_binding_not_inline_literal(patched):
    patched["role_table_schemas"] = [{"schema_name": "iam"}]
    await mod.purge_viewer_role_rows(object())
    for sql, kwargs in patched["delete_calls"]:
        assert ":role_name" in sql
        assert "'viewer'" not in sql.replace("object_ref = :role_name", "")
        assert kwargs == {"role_name": "viewer"}


@pytest.mark.asyncio
async def test_one_schema_failure_does_not_block_remaining(patched):
    patched["role_table_schemas"] = [
        {"schema_name": "iam"},
        {"schema_name": "cat_a"},
    ]
    patched["rowcount_by_sql_substr"] = {
        '"iam".roles': 1,
        '"cat_a".roles': 2,
    }
    patched["raises_on"].add('"cat_a".grants')
    deleted = await mod.purge_viewer_role_rows(object())
    # iam.roles=1, iam.grants=0, cat_a.roles=2, cat_a.grants raises -> 3
    assert deleted == 3
    assert len(patched["delete_calls"]) == 4
