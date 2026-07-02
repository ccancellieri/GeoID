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

"""Real-PostgreSQL integration test for the region_mapping claims registry
(dynastore#443).

Reproduces a read-path failure observed live on dev: applying the
``region_mappings_registry`` + ``region_mapping`` presets writes claim rows
successfully (a cross-mapping duplicate claim correctly hits the
``claim_ci`` UNIQUE sidecar and returns 409, proving both the row and the
constraint exist), but every read of those rows fails —
``search_items``/records listing returns zero rows, and a by-id read raises.

This mirrors the exact provisioning + write shape the two presets use
(``build_registry_items_schema()``, ``build_registry_routing_configs()``,
``Hint.DEFER`` + ``lang="*"``, and the GeoJSON Feature record shape
``region_mapping.apply()`` upserts) against a real PostgreSQL instance — the
mocked unit tests never exercise the actual PG driver SQL, which is why
they stayed green while the registry was broken live.

Uses freshly generated catalog/collection ids (not the reserved
``_region_mappings_`` singleton) purely for DB isolation between test runs;
every other aspect of the provisioning and write is identical to what the
presets do.

``test_registry_search_items_finds_written_claim`` and
``test_registry_get_item_by_external_id_returns_the_claim`` are marked
``xfail(strict=False)`` — they reproduce the #443 read-path bug itself,
which is a separate, still-open root cause from the provisioning race fixed
alongside them here (#2747: ``ensure_registry_provisioned`` no longer races
``create_collection`` against the async ``catalog_provision`` task). Once
#443 is fixed these should start passing; ``strict=False`` means that flip
won't fail CI, it'll just surface as an XPASS to prompt removing the marker.
"""
from __future__ import annotations

import pytest

from dynastore.extensions.region_mapping.registry_data import (
    build_registry_items_schema,
    build_registry_routing_configs,
    item_id_for,
)
from dynastore.extensions.tools.catalog_readiness import wait_for_catalog_ready
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.models.query_builder import FilterCondition, QueryRequest
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
)
from dynastore.tools.discovery import get_protocol
from dynastore.tools.identifiers import generate_id_hex

# "tasks" is required even with Hint.DEFER — catalog creation always
# enqueues a background catalog_provision task for the non-deferrable
# catalog_core step (see wait_for_catalog_ready's docstring in
# dynastore.extensions.tools.catalog_readiness).
pytestmark = pytest.mark.enable_modules(
    "db_config", "db", "catalog", "stats", "iam", "stac",
    "collection_postgresql", "catalog_postgresql", "tasks",
)


async def _provision_registry_like_catalog(catalog_id: str, collection_id: str) -> None:
    """Provision a catalog/collection exactly as
    ``region_mappings_registry.ensure_registry_provisioned`` does."""
    catalogs = get_protocol(CatalogsProtocol)
    config = get_protocol(ConfigsProtocol)
    assert catalogs is not None
    assert config is not None

    await catalogs.create_catalog(
        {"id": catalog_id, "title": {"en": "Region Mapping Claims Registry"}},
        lang="*",
        hints=frozenset({Hint.DEFER}),
    )
    # Mirrors the #2747 fix in ensure_registry_provisioned: a Hint.DEFER
    # create only enqueues the async catalog_provision task for the
    # non-deferrable catalog_core step (tenant PG schema) — create_catalog
    # returns before that task runs, so create_collection below would
    # otherwise race a schema that doesn't exist yet.
    await wait_for_catalog_ready(catalog_id, catalogs_svc=catalogs, caller="test")
    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "title": {"en": "Region Mapping Claims"},
            "layer_config": {"collection_type": "RECORDS"},
            "schema": build_registry_items_schema(),
        },
        lang="*",
    )

    catalog_routing, collection_routing, items_routing = build_registry_routing_configs()
    await config.set_config(
        CatalogRoutingConfig, catalog_routing,
        catalog_id=catalog_id, check_immutability=False,
    )
    await config.set_config(
        CollectionRoutingConfig, collection_routing,
        catalog_id=catalog_id, collection_id=collection_id, check_immutability=False,
    )
    await config.set_config(
        ItemsRoutingConfig, items_routing,
        catalog_id=catalog_id, collection_id=collection_id, check_immutability=False,
    )


def _claim_record(mapping_id: str, claim_ci: str) -> dict:
    """One claim record exactly as ``region_mapping.apply()`` upserts it —
    GeoJSON Feature, ``geometry: None`` (RECORDS collections carry no
    geometry), nested ``properties``."""
    return {
        "type": "Feature",
        "id": item_id_for(mapping_id, claim_ci),
        "geometry": None,
        "properties": {
            "claim": "country",
            "claim_ci": claim_ci,
            "mapping_id": mapping_id,
            "role": "primary",
            "src_catalog": "fao",
            "src_collection": "countries",
            "region_prop": "adm0_code",
            "alias": "country",
            "title": "Countries",
        },
    }


@pytest.mark.asyncio
async def test_region_mappings_registry_preset_first_apply_succeeds_without_retry(
    app_lifespan,
) -> None:
    """The live #2747 symptom: the very first apply of the
    ``region_mappings_registry`` preset against a fresh deployment must not
    500 — ``create_catalog(..., Hint.DEFER)`` only enqueues the async
    ``catalog_provision`` task for the tenant PG schema, and previously
    ``create_collection`` ran immediately after, racing that task.

    Runs first in this file (ahead of the two #443 xfail tests below): each
    test gets its own fresh ``app_lifespan``/dispatcher, but a background
    task from a prior test's imperfect teardown can still drag on the
    process's event loop for a few seconds, and this test's assertion is
    time-budgeted (bounded poll in ``wait_for_catalog_ready``) — running it
    first avoids that unrelated cross-test timing interference."""
    from dynastore.extensions.region_mapping.presets.region_mappings_registry import (
        REGION_MAPPINGS_REGISTRY_PRESET,
    )
    from dynastore.extensions.region_mapping.registry_data import (
        MAPPINGS_COLLECTION_ID,
        REGISTRY_CATALOG_ID,
    )
    from dynastore.modules.storage.presets.preset import PresetContext

    catalogs = get_protocol(CatalogsProtocol)
    config = get_protocol(ConfigsProtocol)
    assert catalogs is not None
    assert config is not None

    ctx = PresetContext(
        db=None, iam=None, policy=None, config=config, tasks=None, cron=None,
        libs=None, principal=None, scope="platform", catalogs=catalogs,
    )

    # Must not raise — the live symptom was a 500 on this very first call.
    await REGION_MAPPINGS_REGISTRY_PRESET.apply(
        REGION_MAPPINGS_REGISTRY_PRESET.params_model(), "platform", ctx,
    )

    collection = await catalogs.get_collection(REGISTRY_CATALOG_ID, MAPPINGS_COLLECTION_ID)
    assert collection is not None


@pytest.mark.xfail(reason="dynastore#443 region-mapping read path (search returns 0 rows, by-id read 500), tracked separately", strict=False)
@pytest.mark.asyncio
async def test_registry_search_items_finds_written_claim(app_lifespan) -> None:
    """search_items with a mapping_id/role filter must return the row the
    write path just wrote — live symptom: 0 rows for data PG demonstrably
    contains."""
    catalog_id = f"cat_{generate_id_hex()}"
    collection_id = f"coll_{generate_id_hex()}"
    mapping_id = f"{catalog_id}_{collection_id}"
    claim_ci = "country"

    await _provision_registry_like_catalog(catalog_id, collection_id)

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    await catalogs.upsert(catalog_id, collection_id, _claim_record(mapping_id, claim_ci))

    features = await catalogs.search_items(
        catalog_id,
        collection_id,
        QueryRequest(
            filters=[
                FilterCondition(field="mapping_id", operator="eq", value=mapping_id),
                FilterCondition(field="role", operator="eq", value="primary"),
            ],
        ),
    )

    assert len(features) == 1, (
        f"expected 1 row for mapping_id={mapping_id!r}, search_items "
        f"returned {len(features)}"
    )
    assert features[0].properties["claim_ci"] == claim_ci


@pytest.mark.xfail(reason="dynastore#443 region-mapping read path (search returns 0 rows, by-id read 500), tracked separately", strict=False)
@pytest.mark.asyncio
async def test_registry_get_item_by_external_id_returns_the_claim(app_lifespan) -> None:
    """A by-id read of a just-written claim must succeed — live symptom:
    HTTP 500 / an unhandled exception on both the records and features
    families."""
    catalog_id = f"cat_{generate_id_hex()}"
    collection_id = f"coll_{generate_id_hex()}"
    mapping_id = f"{catalog_id}_{collection_id}"
    claim_ci = "country"
    item_id = item_id_for(mapping_id, claim_ci)

    await _provision_registry_like_catalog(catalog_id, collection_id)

    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    await catalogs.upsert(catalog_id, collection_id, _claim_record(mapping_id, claim_ci))

    fetched = await catalogs.get_item(catalog_id, collection_id, item_id)

    assert fetched is not None, f"item {item_id!r} not found after upsert"
    assert fetched.properties["claim_ci"] == claim_ci
