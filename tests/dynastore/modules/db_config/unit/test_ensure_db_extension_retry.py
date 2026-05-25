"""Retry behavior for foundational DB bootstrap against transient connection drops.

DatastoreModule's lifespan calls ``ensure_init_db`` before traffic is accepted.
Dev compositions reset the DB mid-startup (``db_entrypoint_dev.sh``) which can
surface as ``OperationalError: server closed the connection unexpectedly`` with
``connection_invalidated=True``. The helper ``retry_on_invalidated_connection``
should swallow up to N-1 of those, never the generic ``OperationalError``
(which would mask real bugs) and never any other exception class.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import OperationalError as SAOperationalError

from dynastore.modules.db_config import maintenance_tools


def _make_invalidated_op_error() -> SAOperationalError:
    err = SAOperationalError(
        statement="CREATE EXTENSION",
        params={},
        orig=Exception("server closed the connection unexpectedly"),
    )
    err.connection_invalidated = True  # type: ignore[attr-defined]
    return err


def _make_other_op_error() -> SAOperationalError:
    err = SAOperationalError(
        statement="CREATE EXTENSION",
        params={},
        orig=Exception("syntax error"),
    )
    err.connection_invalidated = False  # type: ignore[attr-defined]
    return err


# --- ensure_db_extension (via the helper) -----------------------------------

@pytest.mark.asyncio
async def test_retries_then_succeeds_on_invalidated_connection():
    execute_mock = AsyncMock(side_effect=[_make_invalidated_op_error(), None])
    with patch.object(
        maintenance_tools.DDLQuery, "execute", execute_mock
    ), patch.object(asyncio, "sleep", AsyncMock()):
        await maintenance_tools.ensure_db_extension(object(), "postgis")
    assert execute_mock.await_count == 2


@pytest.mark.asyncio
async def test_gives_up_after_max_attempts():
    execute_mock = AsyncMock(side_effect=[
        _make_invalidated_op_error(),
        _make_invalidated_op_error(),
        _make_invalidated_op_error(),
    ])
    with patch.object(
        maintenance_tools.DDLQuery, "execute", execute_mock
    ), patch.object(asyncio, "sleep", AsyncMock()):
        with pytest.raises(SAOperationalError):
            await maintenance_tools.ensure_db_extension(object(), "postgis")
    assert execute_mock.await_count == 3


@pytest.mark.asyncio
async def test_does_not_retry_on_non_invalidated_operational_error():
    execute_mock = AsyncMock(side_effect=_make_other_op_error())
    with patch.object(
        maintenance_tools.DDLQuery, "execute", execute_mock
    ), patch.object(asyncio, "sleep", AsyncMock()):
        with pytest.raises(SAOperationalError):
            await maintenance_tools.ensure_db_extension(object(), "postgis")
    assert execute_mock.await_count == 1


@pytest.mark.asyncio
async def test_does_not_retry_on_unrelated_exception():
    execute_mock = AsyncMock(side_effect=RuntimeError("boom"))
    with patch.object(
        maintenance_tools.DDLQuery, "execute", execute_mock
    ), patch.object(asyncio, "sleep", AsyncMock()):
        with pytest.raises(RuntimeError):
            await maintenance_tools.ensure_db_extension(object(), "postgis")
    assert execute_mock.await_count == 1


# --- retry_on_invalidated_connection helper directly ------------------------

@pytest.mark.asyncio
async def test_helper_re_invokes_factory_per_attempt():
    """Each retry must build a fresh awaitable — coroutines are single-shot."""
    calls = []

    async def _step():
        calls.append(len(calls))
        if len(calls) < 2:
            raise _make_invalidated_op_error()
        return "ok"

    with patch.object(asyncio, "sleep", AsyncMock()):
        result = await maintenance_tools.retry_on_invalidated_connection(
            _step, label="test"
        )

    assert result == "ok"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_helper_does_not_retry_on_unrelated_exception():
    calls = 0

    async def _step():
        nonlocal calls
        calls += 1
        raise ValueError("nope")

    with patch.object(asyncio, "sleep", AsyncMock()):
        with pytest.raises(ValueError):
            await maintenance_tools.retry_on_invalidated_connection(
                _step, label="test"
            )

    assert calls == 1


# --- ensure_init_db wraps PlatformConfigService.initialize_storage too -----

@pytest.mark.asyncio
async def test_ensure_init_db_retries_platform_config_initialize_storage():
    """The platform-config bootstrap call goes through raw DDL, not
    ensure_db_extension — it must still be retried so a DB bounce one step
    later does not abort the lifespan."""
    from dynastore.modules.db_config import tools as db_tools

    ext_mock = AsyncMock()
    init_storage_mock = AsyncMock(side_effect=[_make_invalidated_op_error(), None])

    with patch.object(maintenance_tools, "ensure_db_extension", ext_mock), \
         patch("dynastore.modules.db_config.platform_config_service."
               "PlatformConfigService.initialize_storage", init_storage_mock), \
         patch.object(asyncio, "sleep", AsyncMock()):
        await db_tools.ensure_init_db(object())

    assert ext_mock.await_count == 6  # postgis, postgis_topology, btree_gist, btree_gin, pgcrypto, pg_trgm
    assert init_storage_mock.await_count == 2  # one invalidated + one success
