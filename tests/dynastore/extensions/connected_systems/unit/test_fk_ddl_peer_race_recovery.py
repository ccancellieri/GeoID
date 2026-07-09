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

"""Unit tests for the Connected Systems FK ``DO $$ ... ADD CONSTRAINT``
DDL blocks (``connected_systems/ddl.py``) and the explicit
``check_constraint_exists`` existence check wired onto them.

Background: these ``DO $$ BEGIN IF NOT EXISTS (...) THEN ALTER TABLE ...
END IF; END $$;`` statements don't start with ``CREATE``, so
``ddl_inference._infer_existence_check`` can't derive an existence check for
them. Without an explicit ``check_query``, ``DDLQuery.existence_check`` stays
``None`` and ``DDLExecutor._try_peer_race_recovery_async`` short-circuits --
a concurrent-DDL duplicate_object (42710) never recovers, even though the
constraint the peer created is exactly the one this DDL was about to add.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.connected_systems.ddl import (
    CONSYS_FK_DATASTREAM_SYSTEM_DDL,
    CONSYS_FK_DEPLOYMENT_SYSTEM_DDL,
    CONSYS_FK_OBSERVATION_DATASTREAM_DDL,
)
from dynastore.modules.db_config import locking_tools
from dynastore.modules.db_config.ddl_inference import _infer_existence_check
from dynastore.modules.db_config.query_executor import DDLQuery


class _PgErr(Exception):
    """Test-only stand-in for asyncpg.PostgresError carrying ``pgcode``."""

    def __init__(self, pgcode: str):
        super().__init__(f"pg error pgcode={pgcode}")
        self.pgcode = pgcode


def _dup_object_exc(pgcode: str = "42710") -> Exception:
    """A duplicate_object (or equivalent) error as SQLAlchemy would wrap it."""
    inner = _PgErr(pgcode)
    exc = Exception(f"duplicate object pgcode={pgcode}")
    setattr(exc, "orig", inner)
    return exc


@pytest.mark.parametrize(
    "fk_ddl",
    [
        CONSYS_FK_DATASTREAM_SYSTEM_DDL,
        CONSYS_FK_OBSERVATION_DATASTREAM_DDL,
        CONSYS_FK_DEPLOYMENT_SYSTEM_DDL,
    ],
)
def test_fk_do_block_is_invisible_to_ddl_inference(fk_ddl: str) -> None:
    """Confirms the structural gap: none of the three FK DO-blocks start
    with CREATE, so auto-inference can't derive an existence check for
    them."""
    assert _infer_existence_check(fk_ddl) is None


@pytest.mark.parametrize(
    "fk_ddl,constraint_name",
    [
        (CONSYS_FK_DATASTREAM_SYSTEM_DDL, "fk_datastream_system"),
        (CONSYS_FK_OBSERVATION_DATASTREAM_DDL, "fk_observation_datastream"),
        (CONSYS_FK_DEPLOYMENT_SYSTEM_DDL, "fk_deployment_system"),
    ],
)
def test_fk_ddlquery_has_explicit_existence_check(
    fk_ddl: str, constraint_name: str
) -> None:
    """The production DDLQuery for each FK block must carry a non-None
    existence check even though inference can't derive one."""

    def _check(conn):
        return locking_tools.check_constraint_exists(conn, constraint_name)

    ddl = DDLQuery(fk_ddl, check_query=_check)

    assert ddl._executor.existence_check is not None


@pytest.mark.asyncio
async def test_fk_ddlquery_peer_race_recovery_fires_on_duplicate_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the explicit check_query wired in, a 42710 duplicate_object on
    one of the FK DDL blocks recovers via the outer-conn re-check -- the
    peer-race recovery a bare (inference-blind) DDLQuery would never reach.
    """
    monkeypatch.setattr(
        locking_tools, "check_constraint_exists", AsyncMock(return_value=True)
    )

    def _check(conn):
        return locking_tools.check_constraint_exists(conn, "fk_datastream_system")

    ddl = DDLQuery(CONSYS_FK_DATASTREAM_SYSTEM_DDL, check_query=_check)
    executor = ddl._executor

    result = await executor._try_peer_race_recovery_async(
        MagicMock(), {}, _dup_object_exc("42710")
    )

    assert result is True


@pytest.mark.asyncio
async def test_fk_ddlquery_peer_race_recovery_does_not_mask_unrelated_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unrelated error (42P01, undefined_table) must not be treated as a
    recoverable peer race, even with the explicit existence check present.
    """
    check_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(locking_tools, "check_constraint_exists", check_mock)

    def _check(conn):
        return locking_tools.check_constraint_exists(conn, "fk_datastream_system")

    ddl = DDLQuery(CONSYS_FK_DATASTREAM_SYSTEM_DDL, check_query=_check)
    executor = ddl._executor

    result = await executor._try_peer_race_recovery_async(
        MagicMock(), {}, _dup_object_exc("42P01")
    )

    assert result is False
    check_mock.assert_not_awaited()
