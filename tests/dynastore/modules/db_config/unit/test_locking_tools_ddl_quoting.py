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

"""DDL identifier-quoting regression for ``db_config.locking_tools`` (#2700).

Every schema/table/relation name these force-cleanup helpers interpolate into
DROP/DELETE DDL now goes through ``quote_ident`` (``"`` -> ``""``) instead of a
bare ``"{name}"`` f-string. A name carrying an embedded double quote must come
out escaped so it can never break out of the quoted identifier.
"""

from __future__ import annotations

import contextlib
from typing import Any, List

import pytest

from dynastore.modules.db_config import locking_tools

EVIL = 'x";DROP TABLE injected;--'
EVIL_QUOTED = '"x"";DROP TABLE injected;--"'  # embedded " doubled, whole name wrapped


class _CapturingConn:
    """Records the SQL text of every ``execute`` call."""

    def __init__(self) -> None:
        self.statements: List[str] = []

    async def execute(self, clause: Any, *args: Any, **kwargs: Any) -> None:
        self.statements.append(str(clause))


@pytest.fixture
def captured(monkeypatch) -> _CapturingConn:
    conn = _CapturingConn()

    @contextlib.asynccontextmanager
    async def _fake_managed_transaction(_resource):
        yield conn

    monkeypatch.setattr(locking_tools, "managed_transaction", _fake_managed_transaction)
    return conn


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs, must_contain",
    [
        (dict(schema=EVIL, relation="t", kind="table"), f"DROP TABLE IF EXISTS {EVIL_QUOTED}."),
        (dict(schema="s", relation=EVIL, kind="index"), f".{EVIL_QUOTED}"),
        (dict(schema="s", relation="trg", kind="trigger", on_table=EVIL), f".{EVIL_QUOTED}"),
        (dict(schema=EVIL, relation="ignored", kind="schema"), f"DROP SCHEMA IF EXISTS {EVIL_QUOTED}"),
    ],
)
async def test_safe_drop_relation_escapes_identifiers(captured, kwargs, must_contain):
    await locking_tools.safe_drop_relation(object(), **kwargs)
    drop_stmts = [s for s in captured.statements if "DROP" in s]
    assert drop_stmts, "no DROP statement was executed"
    sql = drop_stmts[-1]
    assert must_contain in sql
    # The raw, unescaped name must never appear verbatim in the DDL.
    assert f'"{EVIL}"' not in sql


class _CapturingDDLQuery:
    """Stand-in for ``DDLQuery`` that records the SQL template and no-ops execute."""

    seen: List[str] = []

    def __init__(self, sql_template: Any, *args: Any, **kwargs: Any) -> None:
        _CapturingDDLQuery.seen.append(str(sql_template))

    async def execute(self, *args: Any, **kwargs: Any) -> None:
        return None


@pytest.mark.asyncio
async def test_force_truncate_and_drop_schema_escape_identifiers(monkeypatch):
    _CapturingDDLQuery.seen = []
    monkeypatch.setattr(locking_tools, "DDLQuery", _CapturingDDLQuery)

    async def _noop(*args: Any, **kwargs: Any) -> None:
        return None

    monkeypatch.setattr(locking_tools, "terminate_backends_locking_table", _noop)
    monkeypatch.setattr(locking_tools, "terminate_backends_locking_schema", _noop)
    monkeypatch.setattr(locking_tools.asyncio, "sleep", _noop)

    await locking_tools.force_truncate_table(object(), EVIL, "tbl")
    await locking_tools.force_drop_schema(object(), EVIL)

    delete_sql = next(s for s in _CapturingDDLQuery.seen if s.startswith("DELETE FROM"))
    drop_sql = next(s for s in _CapturingDDLQuery.seen if s.startswith("DROP SCHEMA"))
    assert f"DELETE FROM {EVIL_QUOTED}." in delete_sql
    assert f"DROP SCHEMA {EVIL_QUOTED} CASCADE" in drop_sql
    assert f'"{EVIL}"' not in delete_sql
    assert f'"{EVIL}"' not in drop_sql
