"""Unit tests for ``GdalInfoTask.run`` locator-driven contract.

The task no longer trusts an ``asset_uri`` passed in ``inputs``; it resolves
the asset from its locators (``asset_id`` + ``catalog_id`` [+ ``collection_id``])
and reads the URI from the asset row, which is the source of truth. This lets
the same process be invoked through the standard OGC Processes API with only
identifiers — no caller-supplied URI.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

# The task module hard-imports ``osgeo`` (strict gate at the task tier), so it
# is only importable where the GDAL runtime is present.
pytest.importorskip("osgeo")

from dynastore.modules.catalog.asset_service import AssetTypeEnum  # noqa: E402
from dynastore.tasks.gdal.gdalinfo_task import GdalInfoTask  # noqa: E402


def test_run_requires_asset_id_and_catalog_id():
    task = GdalInfoTask(app_state=object())
    payload = SimpleNamespace(inputs={}, asset=None)
    with pytest.raises(ValueError, match="asset_id"):
        asyncio.run(task.run(payload))


def test_run_ignores_inputs_asset_uri_and_uses_asset_row():
    # Even when a (stale) asset_uri is supplied in inputs, the task reads the
    # URI from the resolved asset. With the asset row carrying no URI it must
    # fail with the row-derived error, proving inputs.asset_uri is not used.
    task = GdalInfoTask(app_state=object())
    asset = SimpleNamespace(
        asset_id="a1",
        catalog_id="c1",
        collection_id=None,
        uri=None,
        asset_type=AssetTypeEnum.RASTER,
        metadata={},
    )
    payload = SimpleNamespace(
        inputs={
            "asset_id": "a1",
            "catalog_id": "c1",
            "asset_uri": "gs://stale/should-be-ignored.tif",
        },
        asset=asset,
    )
    with pytest.raises(ValueError, match="no URI"):
        asyncio.run(task.run(payload))
