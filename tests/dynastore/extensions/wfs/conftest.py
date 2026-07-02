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

import asyncio

import pytest
from dynastore.tools.identifiers import generate_id_hex

from tests.dynastore.extensions.conftest import build_setup_fixtures


@pytest.fixture
def catalog_id(data_id):
    # Use unique ID per test to avoid partition overlapping issues during rapid create/delete
    return f"cat_{generate_id_hex()}"


@pytest.fixture
def collection_id():
    return "region"


@pytest.fixture
def catalog_data(catalog_id):
    return {
        "id": catalog_id,
        "title": f"Title for {catalog_id}",
        "description": "Test Catalog for WFS",
    }


@pytest.fixture
def collection_data(collection_id):
    return {
        "id": collection_id,
        "title": f"Title for {collection_id}",
        "description": "Test Collection for WFS",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [],
    }


@pytest.fixture
def config_data():
    # Phase 1.6: ``collection_type`` was hoisted off the PG driver config
    # into a standalone ``CollectionInfo`` PluginConfig at collection scope.
    # The PG driver config now ONLY carries driver-local concerns
    # (sidecars, partitioning, etc.).
    return {
        "sidecars": [
            {
                "sidecar_type": "geometries",
                "target_srid": 4326,
            },
            {
                "sidecar_type": "attributes",
            },
        ],
    }


# Module-scoped client, strict 201-only create assertion, catalog delete on
# teardown (collection cleanup cascades from the catalog delete).
setup_catalog, _setup_collection_base = build_setup_fixtures(
    "/features",
    client_fixture="in_process_client_module",
    assert_mode="strict",
    catalog_teardown=True,
)


@pytest.fixture
async def setup_collection(
    in_process_client_module, setup_catalog, _setup_collection_base, config_data
):
    catalog_id = setup_catalog
    collection_id = _setup_collection_base

    # The config storage for a freshly-created collection can briefly return
    # 409 while provisioning settles (the "config PUT right after create"
    # race). It is harmless and self-clears, but surfaces under xdist parallel
    # load and flakes this fixture. The 409 detail itself names this and tells
    # operators to poll ``provisioning_status`` until ready.
    #
    # On a healthy local stack the first PUT succeeds in <1 s; under CI xdist
    # the catalog has been observed taking 60+ s to leave the provisioning
    # state. The previous 10-attempt × 0.2 s exponential loop (~11 s budget)
    # exhausted before the catalog reached ``ready``. Budget here is ~120 s
    # total with capped backoff, only paid on the flaky path. See #1361 + the
    # 2026-05-26 CI dispatch run 26434105114 unit-tests failure for the data
    # points behind these numbers.
    config_url = (
        f"/configs/catalogs/{catalog_id}/collections/{collection_id}"
        f"/plugins/collection_plugin_config"
    )
    for attempt in range(120):  # up to ~120 s, only when 409 keeps recurring
        r = await in_process_client_module.put(config_url, json=config_data)
        if r.status_code != 409:
            break
        await asyncio.sleep(min(0.2 * (attempt + 1), 2.0))
    assert r.status_code in [200, 204], (
        f"config PUT failed after retries: {r.status_code} {r.text}"
    )

    yield collection_id
    # Cleanup is handled by setup_catalog (delete catalog deletes collections)
