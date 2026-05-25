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

"""Unit test for ``AssetService.update_asset`` timestamp bump.

No DB: the write driver + ``get_asset`` + secondary fan-out are stubbed so the
doc-building logic is exercised in isolation. Asserts the ``updated_doc`` handed
to ``index_asset`` carries a FRESH ``updated_at`` rather than echoing the stale
value from the current row (the bug: async gdal-processing writes that add
``metadata.gdalinfo`` never bumped the modification timestamp).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.asset_service import (
    Asset,
    AssetService,
    AssetUpdate,
)
from dynastore.modules.catalog.asset_service import AssetKind, AssetStatus


def _make_service() -> AssetService:
    return AssetService(engine=MagicMock(name="engine"))


def _stale_asset(stale_at: datetime) -> Asset:
    return Asset(
        asset_id="a1",
        kind=AssetKind.PHYSICAL,
        status=AssetStatus.ACTIVE,
        catalog_id="cat1",
        collection_id="coll1",
        filename="f.tif",
        metadata={"k": "v"},
        created_at=stale_at,
        updated_at=stale_at,
    )


@pytest.mark.asyncio
async def test_update_asset_bumps_updated_at():
    service = _make_service()
    stale = datetime(2020, 1, 1, tzinfo=timezone.utc)
    current = _stale_asset(stale)

    write_driver = MagicMock(name="write_driver")
    write_driver.index_asset = AsyncMock(return_value=None)
    # Canonical re-read echoes whatever was written.
    write_driver.get_asset = AsyncMock(return_value=None)

    p_get = patch.object(service, "get_asset", AsyncMock(return_value=current))
    p_fanout = patch.object(
        service, "_fan_out_asset_writes", AsyncMock(return_value=None)
    )
    # update_asset imports get_asset_driver locally from the router module.
    p_driver = patch(
        "dynastore.modules.storage.router.get_asset_driver",
        AsyncMock(return_value=write_driver),
    )

    before = datetime.now(timezone.utc) - timedelta(seconds=1)
    with p_get, p_fanout, p_driver:
        await service.update_asset(
            catalog_id="cat1",
            asset_id="a1",
            update=AssetUpdate(metadata={"gdalinfo": {"bands": 3}}),
            collection_id="coll1",
        )

    write_driver.index_asset.assert_awaited_once()
    _, written_doc = write_driver.index_asset.await_args.args[:2]
    new_updated_at = written_doc["updated_at"]
    assert new_updated_at != current.updated_at
    assert new_updated_at > current.updated_at
    assert new_updated_at >= before
