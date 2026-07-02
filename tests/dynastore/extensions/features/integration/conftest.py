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
from dynastore.tools.identifiers import generate_geoid, generate_id_hex

from tests.dynastore.extensions.conftest import build_setup_fixtures

# --- Test Data Fixtures ---


@pytest.fixture
def catalog_id():
    """Generate a unique catalog ID for testing."""
    return f"cat_{generate_id_hex()}"


@pytest.fixture
def collection_id():
    """Generate a unique collection ID for testing."""
    return f"coll_{generate_id_hex()}"


@pytest.fixture
def item_id():
    """Generate a unique item ID for testing."""
    return generate_geoid()


@pytest.fixture
def catalog_data(catalog_id):
    """Fixture providing test data for catalog creation."""
    return {
        "id": catalog_id,
        "title": "Test Catalog",
        "description": "A test catalog for OGC API Features",
    }


@pytest.fixture
def collection_data(collection_id):
    """Fixture providing test data for collection creation."""
    return {
        "id": collection_id,
        "description": "Test Collection",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
    }


@pytest.fixture
def item_raw_data(item_id):
    """Fixture providing test GeoJSON feature data."""
    return {
        "type": "Feature",
        "id": item_id,
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [0, 0, 0, 0],
        "properties": {"name": "Test Item"},
    }


@pytest.fixture
def config_catalog_data():
    """Fixture for collection plugin config."""
    return {"geometry_storage": {"target_srid": 4326, "geometry_column": "geom"}}


# --- Setup/Cleanup Fixtures (Features Extension Level) ---
#
# Delete-before-create (hard delete) with no response assertion and no
# teardown delete (cleanup is handled by the session-level DB reset).

setup_catalog, setup_collection = build_setup_fixtures("/features", delete_before=True)
