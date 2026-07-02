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
import os
from dynastore.tools.identifiers import generate_id_hex

from tests.dynastore.extensions.conftest import build_setup_fixtures

@pytest.fixture
def dynastore_extensions():
    # Both features and web extensions are needed for test_search_api.py
    return ["features", "web"]

@pytest.fixture
def catalog_id():
    return f"cat_{generate_id_hex()}"

@pytest.fixture
def collection_id():
    return f"coll_{generate_id_hex()}"

@pytest.fixture
def catalog_data(catalog_id):
    return {
        "id": catalog_id,
        "title": "Test Catalog",
        "description": "Test Catalog for Query Transform",
    }

@pytest.fixture
def collection_data(collection_id):
    return {
        "id": collection_id,
        "description": "Test Collection",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
    }

# Delete-before-create, 201-or-409 accepted, delete on teardown for both
# catalog and collection.
setup_catalog, setup_collection = build_setup_fixtures(
    "/features",
    delete_before=True,
    assert_mode="loose",
    catalog_teardown=True,
    collection_teardown=True,
)

@pytest.fixture
async def setup_catalog_with_collection(setup_catalog, setup_collection):
    yield setup_catalog, setup_collection
