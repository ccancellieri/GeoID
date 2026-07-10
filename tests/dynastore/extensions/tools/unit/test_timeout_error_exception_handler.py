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

"""Unit tests for a bare ``TimeoutError`` -> HTTP 503 mapping.

Exercises the handler class directly and the global registry dispatch path
(``handle_exception``) to pin that a starved shared-rebuild wait (e.g.
``tools/cache.py``'s ``get_or_set`` on a cold instance with no stale value)
fails fast with a clean 503 + Retry-After instead of falling through to the
generic 500 fallback.
"""

from __future__ import annotations

import asyncio

from dynastore.extensions.tools.exception_handlers import (
    DatabaseErrorHandler,
    TimeoutErrorExceptionHandler,
    handle_exception,
)


class TestTimeoutErrorExceptionHandler:
    def test_can_handle_bare_timeout_error(self) -> None:
        h = TimeoutErrorExceptionHandler()
        assert h.can_handle(TimeoutError("rebuild wait exceeded budget")) is True

    def test_can_handle_asyncio_timeout_error(self) -> None:
        # asyncio.TimeoutError is the builtin TimeoutError on Python >=3.11.
        h = TimeoutErrorExceptionHandler()
        assert h.can_handle(asyncio.TimeoutError("timed out")) is True

    def test_skips_unrelated_exceptions(self) -> None:
        h = TimeoutErrorExceptionHandler()
        assert h.can_handle(ValueError("nope")) is False
        assert h.can_handle(RuntimeError("nope")) is False
        assert h.can_handle(ConnectionError("nope")) is False

    def test_cancelled_error_is_not_a_timeout_error(self) -> None:
        # asyncio.CancelledError is a BaseException subclass, not an
        # Exception and not a TimeoutError, so it can never reach this
        # handler's can_handle() through the registry's Exception-typed path.
        assert not isinstance(asyncio.CancelledError(), TimeoutError)
        assert not isinstance(asyncio.CancelledError(), Exception)

    def test_returns_503_with_retry_after(self) -> None:
        h = TimeoutErrorExceptionHandler()
        result = h.handle(TimeoutError("shared rebuild wait exceeded budget"))
        assert result is not None
        assert result.status_code == 503
        assert result.headers is not None
        assert result.headers["Retry-After"] == "5"

    def test_registry_dispatch_503(self) -> None:
        result = handle_exception(TimeoutError("shared rebuild wait exceeded budget"))
        assert result.status_code == 503
        assert result.headers["Retry-After"] == "5"

    def test_registry_dispatch_unrelated_exception_stays_500(self) -> None:
        from dynastore.extensions.tools.exception_handlers import _global_registry

        result = _global_registry.handle(
            RuntimeError("boom"), context={}, reraise_unhandled=False
        )
        assert result.status_code == 500

    def test_not_shadowed_by_generic_database_error_handler(self) -> None:
        """A bare TimeoutError is not a DatabaseError; confirm the registry
        still dispatches to the 503 handler rather than the generic one."""
        db_handler = DatabaseErrorHandler()
        assert db_handler.can_handle(TimeoutError("boom")) is False

        result = handle_exception(TimeoutError("shared rebuild wait exceeded budget"))
        assert result.status_code == 503
