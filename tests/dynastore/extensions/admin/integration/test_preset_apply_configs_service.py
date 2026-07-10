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

"""Real ``ConfigsService`` integration coverage for preset apply (#969).

The unit suite (``unit/test_admin_presets_endpoint.py``) mocks
``ConfigsProtocol.set_config``, so it proves the apply loop *calls*
``set_config`` with the right classes/scope but never exercises the
validate → persist → cascade-validator chain end to end. This module
fills that gap against a live ``ConfigsService`` + Postgres.

Two paths are pinned:

1. **Green path** — apply the ``public_catalog`` preset to a clean
   catalog and assert the three routing tiers were persisted at catalog
   scope through the real ``set_config`` lifecycle (read back the
   tier-local rows, not a waterfall-resolved view). This proves apply
   goes *through* ``set_config`` rather than around it.

2. **Reachable reject path** — the issue's original red-path recipe
   ("apply a preset that mixes catalog-private + items-public so the
   cascade rejects") is *unreachable* via preset apply: every shipped
   preset is internally consistent, and apply does not re-validate
   already-persisted child collection configs. The composition guard
   (``_assert_public_collection_has_public_parent``) is instead reached
   by writing a *public* collection routing config under a *private*
   (PG-only) parent catalog. That write must fail closed with HTTP 400
   and the guard's specific detail message — not 500, not a silent pass.

Both presets pin Elasticsearch driver refs on the public tiers, so the
ES drivers must be registered for the routing validators to accept them
(an unregistered WRITE driver is rejected outright by
``_validate_collection_routing_config``). The ``elasticsearch`` module
supplies those drivers plus the live client; the tests therefore run
only when a real Elasticsearch is available and are skipped otherwise —
the same opt-in contract the rest of the ES integration suite uses.
"""

from __future__ import annotations

import logging

import pytest
import pytest_asyncio
from httpx import AsyncClient

from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
)
from dynastore.tools.discovery import get_protocol
from tests.dynastore.test_utils import generate_test_id

logger = logging.getLogger(__name__)

# These tests need the Elasticsearch *drivers* registered so the routing
# validators accept the public ES driver refs the presets pin (the publicness
# checks and the composition guard key off those refs). The ``elasticsearch``
# module registers the catalog/collection/items ES drivers *and* the live
# client. Once the catalog ES driver is a registered WRITE-capable store,
# catalog creation fans out to it synchronously, so a real Elasticsearch
# instance is required — gated behind ``@pytest.mark.elasticsearch`` /
# ``DYNASTORE_RUN_ELASTICSEARCH_TESTS=true`` exactly like the other ES
# integration tests. ``enable_modules`` replaces the default list entirely,
# so the baseline modules the admin/configs/features surfaces depend on are
# repeated here alongside ``elasticsearch``.
pytestmark = [
    pytest.mark.enable_modules(
        "db_config",
        "db",
        "catalog",
        "stats",
        "iam",
        "stac",
        "collection_postgresql",
        "catalog_postgresql",
        "elasticsearch",
    ),
    pytest.mark.enable_extensions("features"),
    pytest.mark.elasticsearch,
]

# Driver refs the publicness checks key off (mirrors the private module
# constants in ``routing_config.py``; duplicated here so the assertions
# are self-documenting rather than importing private names).
PUBLIC_CATALOG_ES_DRIVER = "catalog_elasticsearch_driver"
PUBLIC_COLLECTION_ES_DRIVER = "collection_elasticsearch_driver"
CATALOG_PG_DRIVER = "catalog_postgresql_driver"


@pytest_asyncio.fixture
async def fresh_catalog(sysadmin_in_process_client: AsyncClient):
    """Create one empty, immediately-ready catalog and tear it down.

    No GCP module in the default stack, so the catalog provisions
    synchronously and is ``ready`` the moment POST returns — config
    writes against it pass ``require_catalog_ready`` without polling.
    """
    catalog_id = f"cat_{generate_test_id()}"
    resp = await sysadmin_in_process_client.post(
        "/features/catalogs",
        json={
            "id": catalog_id,
            "title": catalog_id,
            "description": "Preset-apply integration test catalog (#969)",
        },
    )
    assert resp.status_code in (201, 409), (
        f"Failed to create catalog {catalog_id}: {resp.status_code} {resp.text}"
    )

    yield catalog_id

    try:
        await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}")
    except Exception as exc:  # pragma: no cover — best-effort cleanup
        logger.warning("Cleanup of catalog %s failed: %s", catalog_id, exc)


def _write_drivers(routing_row: dict | None, operation: str) -> set[str]:
    """Pull the set of ``driver_ref`` strings pinned under ``operations[op]``
    from a tier-local routing-config row (the dict ``get_persisted_config``
    returns), tolerating absent keys."""
    if not routing_row:
        return set()
    operations = routing_row.get("operations") or {}
    return {entry["driver_ref"] for entry in operations.get(operation, [])}


@pytest.mark.asyncio
class TestPresetApplyAgainstRealConfigsService:
    """Preset apply through a live ``ConfigsService`` (#969)."""

    async def test_apply_public_catalog_preset_persists_public_routing_tiers(
        self,
        sysadmin_in_process_client: AsyncClient,
        fresh_catalog: str,
    ):
        """POST .../presets/public_catalog → 200, and the catalog +
        collection routing tiers are persisted at catalog scope with the
        public ES drivers pinned in ``operations[INDEX]`` (the #2494 lane
        model — materialization/derived-search targets live in INDEX, not
        WRITE).

        Reading back via ``get_persisted_config`` (tier-local, no
        waterfall) proves the rows were actually written at the catalog
        scope by the real ``set_config`` lifecycle — not merely resolved
        from a platform/code default.
        """
        catalog_id = fresh_catalog

        resp = await sysadmin_in_process_client.post(
            f"/configs/catalogs/{catalog_id}/presets/public_catalog"
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["preset"] == "public_catalog"
        assert body["catalog_id"] == catalog_id
        assert body["applied"] == [
            "catalog_routing",
            "collection_template",
            "items_template",
        ]

        configs = get_protocol(ConfigsProtocol)
        assert configs is not None, "ConfigsProtocol not registered"

        catalog_row = await configs.get_persisted_config(
            CatalogRoutingConfig, catalog_id=catalog_id
        )
        assert catalog_row is not None, (
            "public_catalog apply did not persist a CatalogRoutingConfig at "
            "catalog scope — apply bypassed set_config or wrote the wrong tier."
        )
        catalog_write_drivers = _write_drivers(catalog_row, "WRITE")
        assert CATALOG_PG_DRIVER in catalog_write_drivers, (
            "catalog routing WRITE must pin the PG primary store."
        )
        catalog_index_drivers = _write_drivers(catalog_row, "INDEX")
        assert PUBLIC_CATALOG_ES_DRIVER in catalog_index_drivers, (
            f"catalog routing INDEX drivers {sorted(catalog_index_drivers)} "
            f"must pin the public catalog ES driver — the preset is supposed "
            f"to make the catalog globally navigable."
        )

        collection_row = await configs.get_persisted_config(
            CollectionRoutingConfig, catalog_id=catalog_id
        )
        assert collection_row is not None, (
            "public_catalog apply did not persist a CollectionRoutingConfig "
            "template at catalog scope."
        )
        collection_index_drivers = _write_drivers(collection_row, "INDEX")
        assert PUBLIC_COLLECTION_ES_DRIVER in collection_index_drivers, (
            f"collection template INDEX drivers "
            f"{sorted(collection_index_drivers)} must pin the public "
            f"collection ES driver."
        )

    async def test_public_collection_under_private_catalog_is_rejected(
        self,
        sysadmin_in_process_client: AsyncClient,
        fresh_catalog: str,
    ):
        """Apply ``private_catalog`` (PG-only catalog routing), then PUT a
        *public* collection routing config under it → HTTP 400 with the
        composition-guard detail.

        This is the only path that actually reaches
        ``_assert_public_collection_has_public_parent``: preset apply
        cannot surface it (presets are internally consistent and apply
        does not re-validate children), so #969's privacy invariant is
        pinned here via the collection-config PUT instead.
        """
        catalog_id = fresh_catalog

        # 1. Make the catalog private (PG-only catalog routing — no public
        #    catalog ES envelope). Goes through the real set_config path.
        apply = await sysadmin_in_process_client.post(
            f"/configs/catalogs/{catalog_id}/presets/private_catalog"
        )
        assert apply.status_code == 200, apply.text

        # Sanity: the parent catalog routing is NOT public after apply.
        configs = get_protocol(ConfigsProtocol)
        assert configs is not None
        catalog_row = await configs.get_persisted_config(
            CatalogRoutingConfig, catalog_id=catalog_id
        )
        assert PUBLIC_CATALOG_ES_DRIVER not in _write_drivers(catalog_row, "INDEX"), (
            "private_catalog preset must leave the catalog non-public for the "
            "guard precondition to hold."
        )

        # 2. Attempt to write a PUBLIC collection routing config (pins the
        #    public collection ES driver in INDEX — the #2494 lane-model
        #    membership key) under the private parent.
        public_collection_routing = {
            "operations": {
                "WRITE": [
                    {
                        "driver_ref": "collection_postgresql_driver",
                        "hints": [],
                        "on_failure": "fatal",
                    },
                ],
                "READ": [
                    {
                        "driver_ref": "collection_postgresql_driver",
                        "hints": [],
                        "on_failure": "fatal",
                    }
                ],
                "INDEX": [
                    {
                        "driver_ref": PUBLIC_COLLECTION_ES_DRIVER,
                        "hints": [],
                        "source": "auto",
                    },
                ],
            }
        }
        collection_id = f"col_{generate_test_id()}"
        resp = await sysadmin_in_process_client.put(
            f"/configs/catalogs/{catalog_id}/collections/{collection_id}"
            f"/plugins/collection_routing_config",
            params={"create_if_missing": "true"},
            json=public_collection_routing,
        )

        assert resp.status_code == 400, (
            f"Expected the composition guard to reject a public collection "
            f"under a private catalog with HTTP 400, got {resp.status_code}: "
            f"{resp.text}"
        )
        detail = resp.json().get("detail", "")
        detail_text = detail if isinstance(detail, str) else str(detail)
        assert "Composition guard" in detail_text, (
            f"400 detail must carry the composition-guard message, got: "
            f"{detail_text!r}"
        )
        assert "requires its parent catalog to be public" in detail_text, (
            f"400 detail must explain the public-parent requirement, got: "
            f"{detail_text!r}"
        )
