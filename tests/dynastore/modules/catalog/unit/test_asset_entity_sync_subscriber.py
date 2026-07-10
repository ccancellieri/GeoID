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

"""Unit tests for ``AssetEntitySyncSubscriber`` failure propagation (#2494).

Asset events ride the durable ``tasks.events`` outbox: ``EventDrainTask``
retries a claimed row whenever the listener it dispatches to raises. These
tests pin the current contract:

* INDEX entries carry no per-entry failure policy — any indexer failure
  always raises a single chained exception after every entry has been
  attempted (the FATAL/WARN/IGNORE tolerance-level distinction died with
  the #2494 ``FailurePolicy`` shrink to ``{FATAL, WARN}``, and even those
  two no longer differentiate retry behaviour on the INDEX lane).
* Index-driver resolution failures re-raise (chained) instead of being
  swallowed — resolution failures are transient and worth retrying.
* Malformed events (missing ``catalog_id``/``asset_id``) return without
  raising — a permanent no-op, not a retryable condition.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.catalog.asset_sync import AssetEntitySyncSubscriber
from dynastore.modules.storage.router import ResolvedDriver


def _resolved(
    name: str,
    *,
    index_error: Exception | None = None,
    delete_error: Exception | None = None,
) -> ResolvedDriver:
    """Build a ``ResolvedDriver`` wrapping a fake indexer driver.

    The driver's class is created dynamically so ``driver_ref`` (a computed
    property returning ``type(driver).__name__``) is distinct per fake,
    making failure-summary assertions readable.
    """
    driver_cls = type(name, (), {})
    driver = driver_cls()
    driver.index_asset = AsyncMock(side_effect=index_error)
    driver.delete_asset = AsyncMock(side_effect=delete_error)
    return ResolvedDriver(driver=driver)


GET_DRIVERS = "dynastore.modules.storage.router.get_asset_index_drivers"


class TestOnAssetUpsertMissingIds:
    @pytest.mark.asyncio
    async def test_missing_catalog_id_returns_without_raise(self):
        with patch(GET_DRIVERS, new=AsyncMock()) as mock_resolve:
            await AssetEntitySyncSubscriber.on_asset_upsert(
                catalog_id=None, asset_id="a1", payload={},
            )
        mock_resolve.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_asset_id_returns_without_raise(self):
        with patch(GET_DRIVERS, new=AsyncMock()) as mock_resolve:
            await AssetEntitySyncSubscriber.on_asset_upsert(
                catalog_id="cat-1", asset_id=None, payload={},
            )
        mock_resolve.assert_not_called()


class TestOnAssetUpsertResolutionFailure:
    @pytest.mark.asyncio
    async def test_resolution_failure_raises_chained(self):
        original = RuntimeError("driver registry unavailable")
        with patch(GET_DRIVERS, new=AsyncMock(side_effect=original)):
            with pytest.raises(RuntimeError) as exc_info:
                await AssetEntitySyncSubscriber.on_asset_upsert(
                    catalog_id="cat-1", asset_id="a1", payload={},
                )
        assert exc_info.value.__cause__ is original


class TestOnAssetUpsertAlwaysRaisesOnFailure:
    @pytest.mark.asyncio
    async def test_failure_raises_chained(self):
        original = RuntimeError("es unreachable")
        entry = _resolved("FailingIndexer", index_error=original)
        with patch(GET_DRIVERS, new=AsyncMock(return_value=[entry])):
            with pytest.raises(RuntimeError) as exc_info:
                await AssetEntitySyncSubscriber.on_asset_upsert(
                    catalog_id="cat-1", asset_id="a1", payload={"asset_id": "a1"},
                )
        assert exc_info.value.__cause__ is original
        assert "FailingIndexer" in str(exc_info.value)
        entry.driver.index_asset.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_success_does_not_raise(self):
        entry = _resolved("OkIndexer")
        with patch(GET_DRIVERS, new=AsyncMock(return_value=[entry])):
            await AssetEntitySyncSubscriber.on_asset_upsert(
                catalog_id="cat-1", asset_id="a1", payload={"asset_id": "a1"},
            )
        entry.driver.index_asset.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_mixed_success_and_failure_raises_after_all_attempted(self):
        ok_entry = _resolved("OkIndexer")
        failing_entry = _resolved("FailingIndexer", index_error=RuntimeError("boom"))
        with patch(
            GET_DRIVERS, new=AsyncMock(return_value=[ok_entry, failing_entry]),
        ):
            with pytest.raises(RuntimeError):
                await AssetEntitySyncSubscriber.on_asset_upsert(
                    catalog_id="cat-1", asset_id="a1", payload={"asset_id": "a1"},
                )
        # Every entry attempted before raising — a partial gather must not
        # skip the still-healthy indexer.
        ok_entry.driver.index_asset.assert_awaited_once()
        failing_entry.driver.index_asset.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_indexers_is_noop(self):
        with patch(GET_DRIVERS, new=AsyncMock(return_value=[])):
            await AssetEntitySyncSubscriber.on_asset_upsert(
                catalog_id="cat-1", asset_id="a1", payload={"asset_id": "a1"},
            )


class TestOnAssetDeleteMissingIds:
    @pytest.mark.asyncio
    async def test_missing_catalog_id_returns_without_raise(self):
        with patch(GET_DRIVERS, new=AsyncMock()) as mock_resolve:
            await AssetEntitySyncSubscriber.on_asset_delete(
                catalog_id=None, asset_id="a1", payload={},
            )
        mock_resolve.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_asset_id_returns_without_raise(self):
        with patch(GET_DRIVERS, new=AsyncMock()) as mock_resolve:
            await AssetEntitySyncSubscriber.on_asset_delete(
                catalog_id="cat-1", asset_id=None, payload={},
            )
        mock_resolve.assert_not_called()


class TestOnAssetDeleteResolutionFailure:
    @pytest.mark.asyncio
    async def test_resolution_failure_raises_chained(self):
        original = RuntimeError("driver registry unavailable")
        with patch(GET_DRIVERS, new=AsyncMock(side_effect=original)):
            with pytest.raises(RuntimeError) as exc_info:
                await AssetEntitySyncSubscriber.on_asset_delete(
                    catalog_id="cat-1", asset_id="a1", payload={},
                )
        assert exc_info.value.__cause__ is original


class TestOnAssetDeleteAlwaysRaisesOnFailure:
    @pytest.mark.asyncio
    async def test_failure_raises_chained(self):
        original = RuntimeError("es unreachable")
        entry = _resolved("FailingIndexer", delete_error=original)
        with patch(GET_DRIVERS, new=AsyncMock(return_value=[entry])):
            with pytest.raises(RuntimeError) as exc_info:
                await AssetEntitySyncSubscriber.on_asset_delete(
                    catalog_id="cat-1", asset_id="a1", payload={"asset_id": "a1"},
                )
        assert exc_info.value.__cause__ is original
        entry.driver.delete_asset.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_success_does_not_raise(self):
        entry = _resolved("OkIndexer")
        with patch(GET_DRIVERS, new=AsyncMock(return_value=[entry])):
            await AssetEntitySyncSubscriber.on_asset_delete(
                catalog_id="cat-1", asset_id="a1", payload={"asset_id": "a1"},
            )
        entry.driver.delete_asset.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_asset_id_derived_from_payload_when_missing(self):
        """Pre-existing behaviour — must survive the failure-propagation
        refactor unchanged."""
        entry = _resolved("OkIndexer")
        with patch(GET_DRIVERS, new=AsyncMock(return_value=[entry])) as mock_resolve:
            await AssetEntitySyncSubscriber.on_asset_delete(
                catalog_id="cat-1", asset_id=None, payload={"asset_id": "a1"},
            )
        mock_resolve.assert_awaited_once_with("cat-1", None)
        entry.driver.delete_asset.assert_awaited_once_with("cat-1", "a1")
