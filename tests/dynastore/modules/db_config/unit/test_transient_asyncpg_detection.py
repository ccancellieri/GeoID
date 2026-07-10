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

"""Verify ``_is_transient_asyncpg_error`` recognises the asyncpg client-state
error class names and message fragments that should be surfaced as
``DatabaseConnectionError`` (so callers like ``tasks/dispatcher.py`` can
back off + retry instead of escalating to ERROR).

Tracks the surface for issues #235 (``ConnectionDoesNotExistError``) and
#239 (``InternalClientError`` — "cannot switch to state N").
"""

from dynastore.modules.db_config.query_executor import (
    _is_transient_asyncpg_error,
)


class _FakeAsyncpgError(Exception):
    """Test double — class name is what the detector keys on."""


class TestTransientDetectionByClassName:
    def test_connection_does_not_exist_error_recognised(self):
        exc = type("ConnectionDoesNotExistError", (Exception,), {})("conn closed")
        assert _is_transient_asyncpg_error(exc) is True

    def test_internal_client_error_recognised(self):
        exc = type("InternalClientError", (Exception,), {})("state machine broke")
        assert _is_transient_asyncpg_error(exc) is True

    def test_connection_failure_error_recognised(self):
        exc = type("ConnectionFailureError", (Exception,), {})("network down")
        assert _is_transient_asyncpg_error(exc) is True

    def test_interface_error_recognised(self):
        exc = type("InterfaceError", (Exception,), {})("driver broke")
        assert _is_transient_asyncpg_error(exc) is True

    def test_generic_exception_not_recognised(self):
        assert _is_transient_asyncpg_error(ValueError("oops")) is False

    def test_none_returns_false(self):
        assert _is_transient_asyncpg_error(None) is False


class TestTransientDetectionByMessageFragment:
    """Fallback path — some asyncpg errors are wrapped through SQLAlchemy
    layers that lose the original class identity but preserve the message."""

    def test_cannot_switch_to_state_message(self):
        exc = ValueError("cannot switch to state 12; another operation (2) is in progress")
        assert _is_transient_asyncpg_error(exc) is True

    def test_another_operation_message(self):
        exc = RuntimeError("another operation is in progress")
        assert _is_transient_asyncpg_error(exc) is True

    def test_connection_was_closed_message(self):
        exc = OSError("connection was closed in the middle of operation")
        assert _is_transient_asyncpg_error(exc) is True

    def test_unrelated_message_not_matched(self):
        assert _is_transient_asyncpg_error(ValueError("unrelated failure")) is False


class TestTransientDetectionOfCancelRaceInterfaceError:
    """#3181: asyncpg's own statement-cancel handling losing a race against
    another operation on the same wire raises ``InterfaceError("cannot
    perform operation: another operation is in progress")`` instead of a
    clean pgcode-57014 ``QueryCanceledError``. The cancel-drain path in
    ``managed_transaction`` relies on this predicate to invalidate that
    connection instead of returning it to the pool poisoned."""

    def test_raw_asyncpg_interface_error_recognised(self):
        import asyncpg.exceptions as ae

        exc = ae.InterfaceError(
            "cannot perform operation: another operation is in progress"
        )
        assert _is_transient_asyncpg_error(exc) is True

    def test_sqlalchemy_wrapped_interface_error_recognised(self):
        import asyncpg.exceptions as ae

        class _FakeDBAPIError(Exception):
            """Stand-in for SQLAlchemy's DBAPIError: `.orig` set to the raw
            asyncpg exception, mirroring how the real dialect wraps it."""

            def __init__(self, orig):
                self.orig = orig
                super().__init__(str(orig))

        wrapped = _FakeDBAPIError(
            ae.InterfaceError(
                "cannot perform operation: another operation is in progress"
            )
        )
        assert _is_transient_asyncpg_error(wrapped) is True

    def test_plain_value_error_still_rejected(self):
        assert _is_transient_asyncpg_error(ValueError("unrelated failure")) is False
