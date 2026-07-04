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

"""Regression tests for GeoID#2900 leg 2 — evicting asyncpg connections
whose protocol state machine was corrupted by a cancelled operation.

Covers:
(a) ``_is_connection_poison_exception`` matcher: real asyncpg exceptions,
    same-named dialect-wrapper classes, ``__cause__``-chained wrapping, and
    the negative cases (bare InterfaceError/OperationalError/
    ConnectionDoesNotExistError, unrelated exceptions) it must NOT match.
(b) ``register_connection_poison_guard`` wired onto a real ``AsyncEngine``'s
    ``handle_error`` dispatch: only poison shapes flip ``is_disconnect``,
    logging the eviction; every other error is left untouched.
"""

from __future__ import annotations

from types import SimpleNamespace

import asyncpg.exceptions as asyncpg_exc
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from dynastore.modules.db_config.connection_poison_guard import (
    _is_connection_poison_exception,
    register_connection_poison_guard,
)


# ---------------------------------------------------------------------------
# (a) Unit: _is_connection_poison_exception
# ---------------------------------------------------------------------------


def test_matches_real_internal_client_error():
    exc = asyncpg_exc.InternalClientError(
        "cannot switch to state 11; another operation (2) is in progress"
    )
    assert _is_connection_poison_exception(exc)


def test_matches_real_protocol_violation_error():
    exc = asyncpg_exc.ProtocolViolationError("unexpected message")
    assert _is_connection_poison_exception(exc)


def test_matches_dialect_wrapped_internal_client_error_by_name_and_message():
    """The SQLAlchemy asyncpg dialect re-raises as its own same-named class
    (AsyncAdapt_asyncpg_dbapi.InternalClientError), not a subclass of the real
    asyncpg exception — must still match by class name + message."""
    wrapped_cls = type("InternalClientError", (Exception,), {})
    exc = wrapped_cls(
        "<class 'asyncpg.exceptions._base.InternalClientError'>: cannot "
        "switch to state 11; another operation (2) is in progress"
    )
    assert _is_connection_poison_exception(exc)


def test_matches_via_cause_chain():
    """A generic wrapper exception chained via `raise ... from orig` is
    still recognized by unwrapping __cause__."""
    orig = asyncpg_exc.InternalClientError("cannot switch to state 12")
    try:
        raise RuntimeError("outer") from orig
    except RuntimeError as outer:
        assert _is_connection_poison_exception(outer)


def test_does_not_match_bare_interface_error():
    """A plain dropped-connection InterfaceError has its own recovery path
    (pool_pre_ping, query_executor retry) and must NOT be evicted here."""
    exc = asyncpg_exc.InterfaceError("connection is closed")
    assert not _is_connection_poison_exception(exc)


def test_does_not_match_connection_does_not_exist_error():
    exc = asyncpg_exc.ConnectionDoesNotExistError(
        "connection was closed in the middle of operation"
    )
    assert not _is_connection_poison_exception(exc)


def test_does_not_match_generic_operational_error():
    from sqlalchemy.exc import OperationalError

    exc = OperationalError("SELECT 1", {}, Exception("server closed the connection"))
    assert not _is_connection_poison_exception(exc)


def test_wrapper_class_name_alone_without_matching_message_is_rejected():
    """Class-name matching without the confirming message fragment must not
    be enough — guards against an unrelated exception reusing the name."""
    wrapped_cls = type("InternalClientError", (Exception,), {})
    exc = wrapped_cls("some unrelated internal error")
    assert not _is_connection_poison_exception(exc)


def test_does_not_match_unrelated_exception():
    assert not _is_connection_poison_exception(ValueError("bad input"))


# ---------------------------------------------------------------------------
# (b) Integration: register_connection_poison_guard on a real AsyncEngine
# ---------------------------------------------------------------------------


def _fire_handle_error(engine, exc: BaseException) -> SimpleNamespace:
    """Simulate SQLAlchemy dispatching its handle_error dialect event."""
    ctx = SimpleNamespace(is_disconnect=False, original_exception=exc)
    engine.sync_engine.dialect.dispatch.handle_error(ctx)
    return ctx


def test_guard_flips_is_disconnect_for_poison_error(caplog):
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
    register_connection_poison_guard(engine, service="test_service")

    exc = asyncpg_exc.InternalClientError(
        "cannot switch to state 11; another operation (2) is in progress"
    )
    with caplog.at_level("WARNING"):
        ctx = _fire_handle_error(engine, exc)

    assert ctx.is_disconnect is True
    assert any(
        "db_connection_poisoned_evicted" in rec.message
        and "service=test_service" in rec.message
        for rec in caplog.records
    )


def test_guard_leaves_non_poison_error_untouched():
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
    register_connection_poison_guard(engine, service="test_service")

    exc = asyncpg_exc.InterfaceError("connection is closed")
    ctx = _fire_handle_error(engine, exc)

    assert ctx.is_disconnect is False


def test_guard_does_not_reprocess_already_flagged_disconnect(caplog):
    """If SQLAlchemy already determined is_disconnect=True (e.g. a genuine
    dead wire), the guard must not re-log or interfere."""
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
    register_connection_poison_guard(engine, service="test_service")

    ctx = SimpleNamespace(
        is_disconnect=True,
        original_exception=asyncpg_exc.InterfaceError("connection is closed"),
    )
    with caplog.at_level("WARNING"):
        engine.sync_engine.dialect.dispatch.handle_error(ctx)

    assert ctx.is_disconnect is True
    assert not any("db_connection_poisoned_evicted" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_engine_dispose_after_guard_registration():
    """Sanity: registering the guard does not break normal engine lifecycle."""
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
    register_connection_poison_guard(engine, service="test_service")
    await engine.dispose()
