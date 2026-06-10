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

"""Unit tests for GCP domain exception → HTTP status mapping.

Covers the three new handlers added by the fastapi-dep-leak removal work
(issue #1969): GcpServiceUnavailableExceptionHandler (503),
GcpFailedDependencyExceptionHandler (424), and GcpInternalErrorHandler (500).
Exercises both the individual handler classes and the global registry dispatch
path (``handle_exception``) to pin handler ordering.
"""

from __future__ import annotations

import pytest

from dynastore.extensions.tools.exception_handlers import (
    GcpFailedDependencyExceptionHandler,
    GcpInternalErrorHandler,
    GcpServiceUnavailableExceptionHandler,
    handle_exception,
)
from dynastore.modules.gcp.errors import (
    GcpFailedDependencyError,
    GcpInternalError,
    GcpServiceUnavailableError,
)


# ---------------------------------------------------------------------------
# GcpServiceUnavailableExceptionHandler
# ---------------------------------------------------------------------------


class TestGcpServiceUnavailableHandler:
    def test_can_handle_service_unavailable(self) -> None:
        h = GcpServiceUnavailableExceptionHandler()
        assert h.can_handle(GcpServiceUnavailableError("down")) is True

    def test_skips_unrelated_exceptions(self) -> None:
        h = GcpServiceUnavailableExceptionHandler()
        assert h.can_handle(ValueError("nope")) is False
        assert h.can_handle(RuntimeError("nope")) is False

    def test_returns_503_with_message(self) -> None:
        h = GcpServiceUnavailableExceptionHandler()
        result = h.handle(GcpServiceUnavailableError("Catalogs service unavailable."))
        assert result is not None
        assert result.status_code == 503
        assert result.detail == "Catalogs service unavailable."

    def test_no_retry_after_header_when_absent(self) -> None:
        h = GcpServiceUnavailableExceptionHandler()
        result = h.handle(GcpServiceUnavailableError("down"))
        assert result is not None
        # headers=None when retry_after is not set
        assert not result.headers or "Retry-After" not in result.headers

    def test_retry_after_header_forwarded(self) -> None:
        h = GcpServiceUnavailableExceptionHandler()
        exc = GcpServiceUnavailableError("still provisioning", retry_after=30)
        result = h.handle(exc)
        assert result is not None
        assert result.status_code == 503
        assert result.headers is not None
        assert result.headers["Retry-After"] == "30"

    def test_registry_dispatch_503(self) -> None:
        result = handle_exception(GcpServiceUnavailableError("Catalogs service unavailable."))
        assert result.status_code == 503
        assert "Catalogs" in result.detail


# ---------------------------------------------------------------------------
# GcpFailedDependencyExceptionHandler
# ---------------------------------------------------------------------------


class TestGcpFailedDependencyHandler:
    def test_can_handle_failed_dependency(self) -> None:
        h = GcpFailedDependencyExceptionHandler()
        assert h.can_handle(GcpFailedDependencyError("provisioning failed")) is True

    def test_skips_unrelated_exceptions(self) -> None:
        h = GcpFailedDependencyExceptionHandler()
        assert h.can_handle(ValueError("nope")) is False
        assert h.can_handle(GcpServiceUnavailableError("nope")) is False

    def test_returns_424_with_message(self) -> None:
        h = GcpFailedDependencyExceptionHandler()
        msg = "Catalog 'my-cat' provisioning failed; storage not available."
        result = h.handle(GcpFailedDependencyError(msg))
        assert result is not None
        assert result.status_code == 424
        assert result.detail == msg

    def test_registry_dispatch_424(self) -> None:
        msg = "Catalog 'my-cat' provisioning failed; storage not available."
        result = handle_exception(GcpFailedDependencyError(msg))
        assert result.status_code == 424
        assert "provisioning failed" in result.detail


# ---------------------------------------------------------------------------
# GcpInternalErrorHandler
# ---------------------------------------------------------------------------


class TestGcpInternalErrorHandler:
    def test_can_handle_internal_error(self) -> None:
        h = GcpInternalErrorHandler()
        assert h.can_handle(GcpInternalError("oops")) is True

    def test_skips_unrelated_exceptions(self) -> None:
        h = GcpInternalErrorHandler()
        assert h.can_handle(ValueError("nope")) is False
        assert h.can_handle(RuntimeError("nope")) is False

    def test_returns_500_with_message(self) -> None:
        h = GcpInternalErrorHandler()
        msg = "Bucket for catalog 'x' was not found despite 'ready' status."
        result = h.handle(GcpInternalError(msg))
        assert result is not None
        assert result.status_code == 500
        assert result.detail == msg

    def test_registry_dispatch_500(self) -> None:
        msg = "GCS did not return a session URI for resumable upload."
        result = handle_exception(GcpInternalError(msg))
        assert result.status_code == 500
        assert "session URI" in result.detail


# ---------------------------------------------------------------------------
# Invariants: unrelated exceptions still propagate correctly
# ---------------------------------------------------------------------------


class TestRegistryInvariantsPreserved:
    """Guard that the new handlers don't disturb existing registry contracts."""

    def test_unknown_exception_still_reraises(self) -> None:
        with pytest.raises(RuntimeError):
            handle_exception(RuntimeError("totally unrelated"))

    def test_value_error_not_found_still_404(self) -> None:
        result = handle_exception(ValueError("collection 'x' not found."))
        assert result.status_code == 404

    def test_value_error_validation_still_422(self) -> None:
        result = handle_exception(ValueError("bad input"))
        assert result.status_code == 422
