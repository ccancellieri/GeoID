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


# ---------------------------------------------------------------------------
# OGCServiceMixin._get_storage_service
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_storage_service_resolves_and_caches(monkeypatch):
    """Resolves StorageProtocol once and caches the result for reuse."""
    import dynastore.extensions.ogc_base as ogc_base
    from dynastore.models.protocols import StorageProtocol

    sentinel = object()
    requested = []

    def _fake_get_protocol(proto):
        requested.append(proto)
        return sentinel

    monkeypatch.setattr(ogc_base, "get_protocol", _fake_get_protocol)

    svc = _Svc()
    first = await svc._get_storage_service()
    second = await svc._get_storage_service()

    assert first is sentinel
    assert second is sentinel
    # Resolved exactly once (cached) and asked for the storage protocol.
    assert requested == [StorageProtocol]


@pytest.mark.asyncio
async def test_get_storage_service_returns_none_when_unavailable(monkeypatch):
    """Storage is optional — an unavailable service yields None, not an error."""
    import dynastore.extensions.ogc_base as ogc_base

    monkeypatch.setattr(ogc_base, "get_protocol", lambda proto: None)

    svc = _Svc()
    assert await svc._get_storage_service() is None


# ---------------------------------------------------------------------------
# OGCServiceMixin._ogc_list_catalogs / _ogc_list_collections
#
# Shared by Coverages, EDR, Records, and Moving Features. Two behaviours
# pinned here:
# * the internal ``id`` is never exposed — ``external_id`` wins when set.
# * ``lang='*'`` resolves via resolve_localized_field (clean, filtered dict),
#   not resolve_localized (raw model -> null-padded dict once serialized).
# ---------------------------------------------------------------------------


class _Catalog:
    def __init__(self, id, external_id=None, title=None):
        self.id = id
        self.external_id = external_id
        self.title = title


class _Collection:
    def __init__(self, id, external_id=None, title=None, description=None):
        self.id = id
        self.external_id = external_id
        self.title = title
        self.description = description


@pytest.mark.asyncio
async def test_ogc_list_catalogs_normalizes_id_to_external_id():
    svc = _Svc()
    catalogs = AsyncMock()
    catalogs.list_catalogs = AsyncMock(
        return_value=[_Catalog(id="internal-1", external_id="public-cat")]
    )
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    result = await svc._ogc_list_catalogs(limit=10)

    assert result["catalogs"] == [{"id": "public-cat", "title": None}]


@pytest.mark.asyncio
async def test_ogc_list_catalogs_falls_back_to_internal_id_when_no_external_id():
    svc = _Svc()
    catalogs = AsyncMock()
    catalogs.list_catalogs = AsyncMock(return_value=[_Catalog(id="only-internal")])
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    result = await svc._ogc_list_catalogs(limit=10)

    assert result["catalogs"][0]["id"] == "only-internal"


@pytest.mark.asyncio
async def test_ogc_list_catalogs_language_none_passes_title_through_unchanged():
    from dynastore.models.localization import LocalizedText

    svc = _Svc()
    title = LocalizedText(en="Catalog", fr="Catalogue")
    catalogs = AsyncMock()
    catalogs.list_catalogs = AsyncMock(
        return_value=[_Catalog(id="c1", external_id="c1", title=title)]
    )
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    result = await svc._ogc_list_catalogs(limit=10)

    assert result["catalogs"][0]["title"] is title


@pytest.mark.asyncio
async def test_ogc_list_catalogs_resolves_single_language():
    from dynastore.models.localization import LocalizedText

    svc = _Svc()
    title = LocalizedText(en="Catalog", fr="Catalogue")
    catalogs = AsyncMock()
    catalogs.list_catalogs = AsyncMock(
        return_value=[_Catalog(id="c1", external_id="c1", title=title)]
    )
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    result = await svc._ogc_list_catalogs(limit=10, language="fr")

    assert result["catalogs"][0]["title"] == "Catalogue"


@pytest.mark.asyncio
async def test_ogc_list_catalogs_wildcard_returns_clean_dict_no_null_padding():
    """lang='*' must not leak unset languages as explicit ``None`` keys.

    Regression test for the bug fixed by swapping ``resolve_localized`` for
    ``resolve_localized_field``: the former returned the raw LocalizedText
    model unchanged for ``lang='*'``, which serializes with every unset
    language field as an explicit null once the response is dumped.
    """
    from dynastore.models.localization import LocalizedText

    svc = _Svc()
    title = LocalizedText(en="Catalog")  # fr/es/... left unset
    catalogs = AsyncMock()
    catalogs.list_catalogs = AsyncMock(
        return_value=[_Catalog(id="c1", external_id="c1", title=title)]
    )
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    result = await svc._ogc_list_catalogs(limit=10, language="*")

    resolved_title = result["catalogs"][0]["title"]
    assert resolved_title == {"en": "Catalog"}
    assert "fr" not in resolved_title


@pytest.mark.asyncio
async def test_ogc_list_collections_normalizes_id_and_omits_none_fields():
    svc = _Svc()
    catalogs = AsyncMock()
    catalogs.list_collections = AsyncMock(
        return_value=[_Collection(id="internal-1", external_id="public-coll")]
    )
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    result = await svc._ogc_list_collections("cat", limit=10, language="en")

    assert result["collections"] == [{"id": "public-coll"}]


@pytest.mark.asyncio
async def test_ogc_list_collections_wildcard_returns_clean_dicts_no_null_padding():
    from dynastore.models.localization import LocalizedText

    svc = _Svc()
    coll = _Collection(
        id="c1",
        external_id="c1",
        title=LocalizedText(en="Coll"),
        description=LocalizedText(en="Desc", fr="Descr"),
    )
    catalogs = AsyncMock()
    catalogs.list_collections = AsyncMock(return_value=[coll])
    svc._get_catalogs_service = AsyncMock(return_value=catalogs)

    result = await svc._ogc_list_collections("cat", limit=10, language="*")

    entry = result["collections"][0]
    assert entry["title"] == {"en": "Coll"}
    assert entry["description"] == {"en": "Desc", "fr": "Descr"}
