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

"""Unit tests for GcpCatalogCleanupTask._cleanup_catalog.

Verifies that _cleanup_catalog delegates entirely to
teardown_phantom_gcp_resources (the shared helper factored into
gc_phantom_catalogs.py) and that the return shape contract is preserved:
- ``{"catalog_id": ..., "scope": "catalog", "status": "cleaned"}`` on success
- ``{"catalog_id": ..., "scope": "catalog", "status": "skipped_no_protocols"}``
  when no GCP protocols are registered
- exceptions raised by the helper (e.g. bucket delete failure) propagate
  unchanged so the durable task layer can retry

No real GCP clients; all external calls are mocked at the module boundary.
"""
from __future__ import annotations

import asyncio
from typing import Any, Optional
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("google")  # skip when GCP scope is not installed

from dynastore.tasks.gcp.gcp_catalog_cleanup_task import GcpCatalogCleanupTask


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _task() -> GcpCatalogCleanupTask:
    return GcpCatalogCleanupTask()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# _cleanup_catalog delegates to teardown_phantom_gcp_resources
# ---------------------------------------------------------------------------

class TestCleanupCatalogDelegation:
    """_cleanup_catalog must route through the shared helper."""

    _HELPER = "dynastore.scripts.gc_phantom_catalogs.teardown_phantom_gcp_resources"

    def test_delegates_with_catalog_id_and_bucket_name(self):
        """Helper is called with the exact catalog_id and bucket_name supplied."""
        mock_helper = AsyncMock(return_value={"catalog_id": "c_abc", "status": "cleaned"})

        with patch(self._HELPER, new=mock_helper):
            result = _run(_task()._cleanup_catalog("c_abc", bucket_name="my-bucket"))

        mock_helper.assert_awaited_once_with("c_abc", "my-bucket")
        assert result["catalog_id"] == "c_abc"
        assert result["status"] == "cleaned"

    def test_delegates_without_bucket_name(self):
        """bucket_name=None is passed through so the helper can resolve it."""
        mock_helper = AsyncMock(return_value={"catalog_id": "c_xyz", "status": "cleaned"})

        with patch(self._HELPER, new=mock_helper):
            result = _run(_task()._cleanup_catalog("c_xyz"))

        mock_helper.assert_awaited_once_with("c_xyz", None)
        assert result["status"] == "cleaned"

    def test_return_shape_has_scope_catalog(self):
        """``scope`` key must be present with value ``"catalog"`` in the return dict."""
        mock_helper = AsyncMock(return_value={"catalog_id": "c_abc", "status": "cleaned"})

        with patch(self._HELPER, new=mock_helper):
            result = _run(_task()._cleanup_catalog("c_abc", bucket_name="bkt"))

        assert result.get("scope") == "catalog"

    def test_skipped_no_protocols_is_passed_through_with_scope(self):
        """When the helper reports skipped_no_protocols (no GCP modules loaded),
        the status is preserved and ``scope`` is still added."""
        mock_helper = AsyncMock(
            return_value={"catalog_id": "c_abc", "status": "skipped_no_protocols"}
        )

        with patch(self._HELPER, new=mock_helper):
            result = _run(_task()._cleanup_catalog("c_abc"))

        assert result["status"] == "skipped_no_protocols"
        assert result["scope"] == "catalog"
        assert result["catalog_id"] == "c_abc"

    def test_bucket_delete_failure_propagates(self):
        """If the helper raises (e.g. bucket delete 403), the exception must
        propagate so the durable task layer retries the row."""
        async def _failing_helper(catalog_id: str, bucket_name: Optional[str]) -> Any:
            raise PermissionError("403 bucket access denied")

        with patch(self._HELPER, new=_failing_helper):
            with pytest.raises(PermissionError, match="403"):
                _run(_task()._cleanup_catalog("c_abc", bucket_name="bkt"))

    def test_helper_called_exactly_once(self):
        """Sanity: no accidental double-call of the helper."""
        mock_helper = AsyncMock(return_value={"catalog_id": "c_abc", "status": "cleaned"})

        with patch(self._HELPER, new=mock_helper):
            _run(_task()._cleanup_catalog("c_abc", bucket_name="b"))

        assert mock_helper.await_count == 1
