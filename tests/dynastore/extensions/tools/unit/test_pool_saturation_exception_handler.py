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

"""Unit tests for ``PoolSaturationError`` -> HTTP 503 mapping (#1894).

Exercises the handler class directly and the global registry dispatch path
(``handle_exception``) to pin that a bounded DB pool-acquire timeout fails
fast with a clean 503 + Retry-After instead of falling through to the
generic ``DatabaseErrorHandler`` (opaque 500).
"""

from __future__ import annotations

from dynastore.extensions.tools.exception_handlers import (
    DatabaseErrorHandler,
    PoolSaturationExceptionHandler,
    handle_exception,
)
from dynastore.modules.db_config.exceptions import PoolSaturationError


class TestPoolSaturationExceptionHandler:
    def test_can_handle_pool_saturation_error(self) -> None:
        h = PoolSaturationExceptionHandler()
        assert h.can_handle(PoolSaturationError("pool saturated")) is True

    def test_skips_unrelated_exceptions(self) -> None:
        h = PoolSaturationExceptionHandler()
        assert h.can_handle(ValueError("nope")) is False
        assert h.can_handle(RuntimeError("nope")) is False

    def test_returns_503_with_retry_after(self) -> None:
        h = PoolSaturationExceptionHandler()
        exc = PoolSaturationError(
            "Database connection pool saturated after waiting 30.0s "
            "for a free connection.",
            retry_after=5,
        )
        result = h.handle(exc)
        assert result is not None
        assert result.status_code == 503
        assert result.headers is not None
        assert result.headers["Retry-After"] == "5"

    def test_retry_after_forwards_configured_value(self) -> None:
        h = PoolSaturationExceptionHandler()
        exc = PoolSaturationError("pool saturated", retry_after=12)
        result = h.handle(exc)
        assert result is not None
        assert result.headers["Retry-After"] == "12"

    def test_registry_dispatch_503(self) -> None:
        result = handle_exception(PoolSaturationError("pool saturated", retry_after=5))
        assert result.status_code == 503
        assert result.headers["Retry-After"] == "5"

    def test_registered_ahead_of_generic_database_error_handler(self) -> None:
        """PoolSaturationError IS-A DatabaseError; the registry must dispatch
        to the more specific 503 handler, not the generic 500 handler."""
        db_handler = DatabaseErrorHandler()
        assert db_handler.can_handle(PoolSaturationError("pool saturated")) is True

        # The registry-level dispatch must still win with 503, proving
        # PoolSaturationExceptionHandler is registered ahead of DatabaseErrorHandler.
        result = handle_exception(PoolSaturationError("pool saturated", retry_after=5))
        assert result.status_code == 503
