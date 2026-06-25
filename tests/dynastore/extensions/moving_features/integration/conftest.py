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

import pytest


@pytest.fixture
async def setup_catalog(sysadmin_in_process_client, catalog_data, catalog_id):
    """Fixture to ensure a catalog exists using the STAC API."""
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    if r.status_code == 409:
        pass
    else:
        assert r.status_code == 201, f"Failed to create setup catalog: {r.text}"
    
    yield catalog_id


@pytest.fixture
async def setup_collection(
    sysadmin_in_process_client, setup_catalog, collection_data, collection_id
):
    """Fixture to ensure a collection exists using the STAC API."""
    catalog_id = setup_catalog
    
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=collection_data
    )
    if r.status_code == 409:
        pass
    else:
        assert r.status_code == 201, f"Failed to create setup collection: {r.text}"
    
    yield collection_id
