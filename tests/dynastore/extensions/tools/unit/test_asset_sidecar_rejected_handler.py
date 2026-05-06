#    Copyright 2025 FAO
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

"""Test the Stage 4.1 ``AssetSidecarRejectedError`` → 409 mapping."""

from __future__ import annotations

import pytest

from dynastore.extensions.tools.exception_handlers import (
    AssetSidecarRejectedExceptionHandler,
    handle_exception,
)
from dynastore.modules.catalog.asset_distributed import AssetSidecarRejectedError


def _exc(
    asset_id: str = "asset-1",
    matcher: str = "filename",
    reason: str = "conflict",
    existing_id: str = "other-asset",
) -> AssetSidecarRejectedError:
    return AssetSidecarRejectedError(
        f"Asset write refused: identity matched via {matcher} "
        f"(existing asset_id={existing_id}); policy=REFUSE_FAIL",
        asset_id=asset_id,
        matcher=matcher,
        reason=reason,
        existing_id=existing_id,
    )


class TestCanHandle:
    def test_matches_asset_sidecar_rejected(self) -> None:
        h = AssetSidecarRejectedExceptionHandler()
        assert h.can_handle(_exc()) is True

    def test_skips_other_exceptions(self) -> None:
        h = AssetSidecarRejectedExceptionHandler()
        assert h.can_handle(ValueError("nope")) is False
        assert h.can_handle(RuntimeError("nope")) is False


class TestHandle:
    def test_returns_409_with_structured_body(self) -> None:
        h = AssetSidecarRejectedExceptionHandler()
        result = h.handle(_exc())

        assert result is not None
        assert result.status_code == 409
        body = result.detail
        assert isinstance(body, dict)
        assert body["status"] == 409
        assert body["asset_id"] == "asset-1"
        assert body["matcher"] == "filename"
        assert body["reason"] == "conflict"
        assert body["existing_id"] == "other-asset"
        assert "title" in body
        assert "type" in body
        assert "detail" in body

    def test_versioning_unsupported_reason_preserved(self) -> None:
        h = AssetSidecarRejectedExceptionHandler()
        result = h.handle(
            _exc(reason="versioning_unsupported", matcher="asset_id")
        )

        assert result is not None
        assert result.status_code == 409
        assert isinstance(result.detail, dict)
        assert result.detail["reason"] == "versioning_unsupported"
        assert result.detail["matcher"] == "asset_id"


class TestRegistryDispatch:
    """Exercise the global registry path so handler ordering is also pinned."""

    def test_handle_exception_routes_to_409(self) -> None:
        result = handle_exception(_exc())
        assert result.status_code == 409
        assert isinstance(result.detail, dict)
        assert result.detail["matcher"] == "filename"

    def test_unknown_exception_still_reraises(self) -> None:
        # Sanity guard: registering the new handler must not have masked
        # the global "no match → re-raise" contract.
        with pytest.raises(RuntimeError):
            handle_exception(RuntimeError("totally unrelated"))
