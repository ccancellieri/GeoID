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

"""Aligned-path coverage for the WFS extension (catalog/collection convention).

`/wfs/catalogs/{catalog_id}` is the OGC-aligned catalog-scoped entry point;
`/wfs/{catalog_id}` is kept as a deprecated alias for existing WFS clients
(QGIS/ArcGIS connections are typically saved by URL).
"""

import pytest

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "stac", "collection_postgresql", "catalog_postgresql"
    ),
    pytest.mark.enable_extensions("features", "wfs", "assets", "stac"),
]


async def test_get_capabilities_aligned_path(
    in_process_client_module, setup_collection, setup_catalog
):
    catalog_id = setup_catalog
    params = {"service": "WFS", "request": "GetCapabilities"}

    aligned = await in_process_client_module.get(
        f"/wfs/catalogs/{catalog_id}", params=params
    )
    legacy = await in_process_client_module.get(f"/wfs/{catalog_id}", params=params)

    assert aligned.status_code == 200
    assert legacy.status_code == 200
    assert aligned.text == legacy.text


async def test_get_feature_aligned_path(
    in_process_client_module, setup_collection, setup_catalog
):
    catalog_id = setup_catalog
    collection_id = setup_collection
    params = {
        "service": "WFS",
        "request": "GetFeature",
        "typenames": f"{catalog_id}:{collection_id}",
        "outputformat": "application/json",
    }

    r = await in_process_client_module.get(
        f"/wfs/catalogs/{catalog_id}", params=params
    )
    assert r.status_code == 200
    assert r.json()["features"] == []
