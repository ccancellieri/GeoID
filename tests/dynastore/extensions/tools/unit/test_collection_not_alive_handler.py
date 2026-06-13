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

"""``CollectionNotAliveError`` → HTTP mapping for the write gate (#1995, #2066).

missing → 404, tombstoned → 410, provisioning/deleting → 409 + Retry-After,
any other reason (fail-closed lookup errors) → 503.
"""

from __future__ import annotations

import pytest

from dynastore.extensions.tools.exception_handlers import (
    CollectionNotAliveExceptionHandler,
    handle_exception,
)
from dynastore.modules.catalog.collection_service import CollectionNotAliveError


def _exc(reason: str = "missing") -> CollectionNotAliveError:
    return CollectionNotAliveError("cat-1", "col-1", reason)


class TestCanHandle:
    def test_matches_collection_not_alive(self) -> None:
        h = CollectionNotAliveExceptionHandler()
        assert h.can_handle(_exc()) is True

    def test_skips_other_exceptions(self) -> None:
        h = CollectionNotAliveExceptionHandler()
        assert h.can_handle(ValueError("nope")) is False
        assert h.can_handle(RuntimeError("nope")) is False


class TestHandle:
    def test_missing_maps_to_404(self) -> None:
        result = CollectionNotAliveExceptionHandler().handle(_exc("missing"))
        assert result is not None
        assert result.status_code == 404
        body = result.detail
        assert isinstance(body, dict)
        assert body["status"] == 404
        assert body["catalog_id"] == "cat-1"
        assert body["collection_id"] == "col-1"
        assert body["reason"] == "missing"
        assert "title" in body
        assert "type" in body
        assert "detail" in body

    def test_tombstoned_maps_to_410(self) -> None:
        result = CollectionNotAliveExceptionHandler().handle(_exc("tombstoned"))
        assert result is not None
        assert result.status_code == 410
        assert isinstance(result.detail, dict)
        assert result.detail["reason"] == "tombstoned"

    def test_provisioning_maps_to_409_with_retry_after(self) -> None:
        result = CollectionNotAliveExceptionHandler().handle(_exc("provisioning"))
        assert result is not None
        assert result.status_code == 409
        assert isinstance(result.detail, dict)
        assert result.detail["reason"] == "provisioning"
        # Transitional: client should back off and retry the same request.
        assert (result.headers or {}).get("Retry-After") == "5"

    def test_deleting_maps_to_409_with_retry_after(self) -> None:
        result = CollectionNotAliveExceptionHandler().handle(_exc("deleting"))
        assert result is not None
        assert result.status_code == 409
        assert isinstance(result.detail, dict)
        assert result.detail["reason"] == "deleting"
        assert (result.headers or {}).get("Retry-After") == "5"

    def test_terminal_reasons_carry_no_retry_after(self) -> None:
        # missing/tombstoned are terminal — retrying the same request is futile,
        # so no Retry-After is advertised.
        for reason in ("missing", "tombstoned"):
            result = CollectionNotAliveExceptionHandler().handle(_exc(reason))
            assert result is not None
            assert (result.headers or {}).get("Retry-After") is None

    def test_lookup_error_fails_closed_to_503(self) -> None:
        result = CollectionNotAliveExceptionHandler().handle(_exc("lookup-error"))
        assert result is not None
        assert result.status_code == 503
        assert isinstance(result.detail, dict)
        assert result.detail["reason"] == "lookup-error"

    def test_unknown_reason_also_maps_to_503(self) -> None:
        result = CollectionNotAliveExceptionHandler().handle(_exc("weird-state"))
        assert result is not None
        assert result.status_code == 503


class TestRegistryDispatch:
    """Exercise the global registry path so handler ordering is also pinned."""

    def test_handle_exception_routes_missing_to_404(self) -> None:
        result = handle_exception(_exc("missing"))
        assert result.status_code == 404
        assert isinstance(result.detail, dict)
        assert result.detail["collection_id"] == "col-1"

    def test_handle_exception_routes_tombstoned_to_410(self) -> None:
        result = handle_exception(_exc("tombstoned"))
        assert result.status_code == 410

    def test_unknown_exception_still_reraises(self) -> None:
        with pytest.raises(RuntimeError):
            handle_exception(RuntimeError("totally unrelated"))
