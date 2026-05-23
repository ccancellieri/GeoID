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

"""Unit tests for the shared OGC helpers hoisted into ``OGCServiceMixin``.

These cover the three clusters consolidated out of the Coverages, EDR, and
DGGS services:

* ``ogc_asset_href`` — asset-href resolution (module-level function)
* ``OGCServiceMixin._get_first_item`` — first-item-as-dict, None-on-empty
* ``OGCServiceMixin._get_plugin_config`` — config waterfall, default-on-error

All collaborators are mocked; no database is touched.
"""

from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from dynastore.extensions.ogc_base import OGCServiceMixin, ogc_asset_href


# ---------------------------------------------------------------------------
# ogc_asset_href
# ---------------------------------------------------------------------------


def test_asset_href_prefers_data_key():
    item = {
        "assets": {
            "data": {"href": "gs://bucket/data.tif"},
            "coverage": {"href": "gs://bucket/cov.tif"},
        }
    }
    assert ogc_asset_href(item) == "gs://bucket/data.tif"


def test_asset_href_prefers_coverage_key_when_no_data():
    item = {"assets": {"coverage": {"href": "gs://bucket/cov.tif"}}}
    assert ogc_asset_href(item) == "gs://bucket/cov.tif"


def test_asset_href_falls_back_to_any_with_href():
    item = {"assets": {"thumbnail": {"href": "https://host/thumb.png"}}}
    assert ogc_asset_href(item) == "https://host/thumb.png"


def test_asset_href_raises_404_with_default_detail():
    with pytest.raises(HTTPException) as exc:
        ogc_asset_href({"assets": {}})
    assert exc.value.status_code == 404
    assert exc.value.detail == "No asset href on item."


def test_asset_href_raises_404_with_custom_detail():
    with pytest.raises(HTTPException) as exc:
        ogc_asset_href({}, error_detail="No asset href on coverage item.")
    assert exc.value.status_code == 404
    assert exc.value.detail == "No asset href on coverage item."


# ---------------------------------------------------------------------------
# OGCServiceMixin._get_first_item
# ---------------------------------------------------------------------------


class _Svc(OGCServiceMixin):
    """Bare concrete subclass for exercising the mixin in isolation."""


@pytest.mark.asyncio
async def test_get_first_item_returns_dict_from_pydantic_model():
    class _Feature:
        def model_dump(self, **kwargs):
            # Echo the kwargs so we can assert on them too.
            return {"id": "it1", "_kwargs": kwargs}

    svc = _Svc()
    catalogs = AsyncMock()
    catalogs.search_items = AsyncMock(return_value=[_Feature()])
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    result = await svc._get_first_item("cat", "col")

    assert result["id"] == "it1"
    # Same model_dump kwargs the original per-service helpers used.
    assert result["_kwargs"] == {"by_alias": True, "exclude_none": True}


@pytest.mark.asyncio
async def test_get_first_item_coerces_plain_dict_feature():
    svc = _Svc()
    catalogs = AsyncMock()
    catalogs.search_items = AsyncMock(return_value=[{"id": "raw"}])
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    result = await svc._get_first_item("cat", "col")
    assert result == {"id": "raw"}


@pytest.mark.asyncio
async def test_get_first_item_none_on_empty_collection():
    svc = _Svc()
    catalogs = AsyncMock()
    catalogs.search_items = AsyncMock(return_value=[])
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    assert await svc._get_first_item("cat", "col") is None


@pytest.mark.asyncio
async def test_get_first_item_none_on_search_error():
    svc = _Svc()
    catalogs = AsyncMock()
    catalogs.search_items = AsyncMock(side_effect=RuntimeError("boom"))
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    assert await svc._get_first_item("cat", "col") is None


# ---------------------------------------------------------------------------
# OGCServiceMixin._get_plugin_config
# ---------------------------------------------------------------------------


class _Cfg:
    def __init__(self):
        self.flavour = "default"


@pytest.mark.asyncio
async def test_get_plugin_config_returns_resolved_config():
    resolved = _Cfg()
    resolved.flavour = "resolved"

    svc = _Svc()
    configs = AsyncMock()
    configs.get_config = AsyncMock(return_value=resolved)
    svc._get_configs_service = AsyncMock(return_value=configs)

    out = await svc._get_plugin_config(_Cfg, "cat", "col")

    assert out is resolved
    configs.get_config.assert_awaited_once_with(_Cfg, "cat", "col")


@pytest.mark.asyncio
async def test_get_plugin_config_defaults_on_error():
    svc = _Svc()
    configs = AsyncMock()
    configs.get_config = AsyncMock(side_effect=RuntimeError("configs down"))
    svc._get_configs_service = AsyncMock(return_value=configs)

    out = await svc._get_plugin_config(_Cfg)

    # Falls back to a default-constructed instance of the requested class.
    assert isinstance(out, _Cfg)
    assert out.flavour == "default"


@pytest.mark.asyncio
async def test_get_plugin_config_defaults_when_service_unavailable():
    svc = _Svc()
    svc._get_configs_service = AsyncMock(side_effect=HTTPException(status_code=500))

    out = await svc._get_plugin_config(_Cfg, "cat")
    assert isinstance(out, _Cfg)
