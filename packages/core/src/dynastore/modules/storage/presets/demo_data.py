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

"""Demo-data preset — canonical data-contributor example (dynastore#307).

Seeds a ``demo_catalog`` / ``demo_collection`` with a 2×3 grid of map-tile
polygons covering the Italian peninsula, as a reversible platform preset.
This is the recommended way to deliver built-in seed data: declare a
``DataContributor``, wrap it in a ``MultiContributorPreset``, and register it.

This preset replaces the manual demo-data CLI that previously lived under
``tools/demo`` (deleted in favour of the preset API).  Running ``apply`` is
idempotent — the catalog and collection are created only when absent, and
items are upserted.  Running ``revoke`` removes the items and, because
``manage_catalog`` / ``manage_collection`` are both ``True``, deletes the
catalog and collection only when this preset created them and the collection
is empty.

The seed content mirrors the legacy ``POST /admin/demo/populate`` web endpoint
so that retiring those ``/admin/demo/*`` routes in favour of applying this
preset (``POST/DELETE /configs/presets/demo_data``) causes no visible change to
the demo data — that route removal is tracked as a follow-up.
"""
from __future__ import annotations

from typing import Iterable

from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
)

from .multi_contributor import MultiContributorPreset
from .preset import DataSeed

# ---------------------------------------------------------------------------
# Catalog / collection / item payloads for the demo seed. The catalog,
# collection and the 2×3 Italy tile grid mirror the legacy
# ``/admin/demo/populate`` endpoint so the two stay a single source of truth.
# ---------------------------------------------------------------------------

_CATALOG_DATA = {
    "id": "demo_catalog",
    "title": {"en": "Demo Catalog", "it": "Catalogo Demo"},
    "description": {
        "en": "Demo catalog for testing purposes.",
        "it": "Catalogo demo per scopi di test.",
    },
    "keywords": ["demo", "dynastore", "geospatial"],
    "license": "CC-BY-4.0",
}

_COLLECTION_DATA = {
    "id": "demo_collection",
    "title": {"en": "Italy Tile Grid"},
    "description": {"en": "A 2×3 grid of map-tile polygons covering the Italian peninsula."},
    "type": "Feature",
}


def _tile_polygon(col: int, row: int) -> dict:
    """One map-tile polygon over Italy.

    lon: 6.6 – 18.5 (col width ≈ 5.95°); lat: 37.9 – 47.1 (row height ≈ 3.07°).
    """
    lon0 = 6.6 + col * 5.95
    lon1 = lon0 + 5.95
    lat0 = 37.9 + row * 3.07
    lat1 = lat0 + 3.07
    return {
        "type": "Polygon",
        "coordinates": [[
            [lon0, lat0], [lon1, lat0],
            [lon1, lat1], [lon0, lat1],
            [lon0, lat0],   # close ring
        ]],
    }


_ROW_LABELS = ["south", "centre", "north"]
_COL_LABELS = ["west", "east"]

# 2 columns × 3 rows = 6 tiles, in (row, col) order.
_DEMO_ITEMS = tuple(
    {
        "id": f"tile_{_COL_LABELS[c]}_{_ROW_LABELS[r]}",
        "type": "Feature",
        "geometry": _tile_polygon(c, r),
        "properties": {
            "name": f"Italy – {_ROW_LABELS[r].capitalize()} {_COL_LABELS[c].capitalize()}",
            "description": f"Map tile column {c} row {r} over Italy",
            "col": c, "row": r,
        },
    }
    for r in range(3) for c in range(2)
)


# A public sample object standing in for "the original bucket": the demo
# registers it as a VIRTUAL (href) asset, so the bucket-free demo_catalog
# exposes an asset without uploading bytes or provisioning a bucket — the asset
# is served directly from its original bucket.
_DEMO_VIRTUAL_ASSET_HREF = (
    "gs://gcp-public-data-landsat/LC08/01/044/034/"
    "LC08_L1GT_044034_20130330_20170310_01_T2/"
    "LC08_L1GT_044034_20130330_20170310_01_T2_B4.TIF"
)


# ---------------------------------------------------------------------------
# Data contributor
# ---------------------------------------------------------------------------

def _demo_items_routing() -> ItemsRoutingConfig:
    """PG-primary + Elasticsearch-INDEX routing for the demo collection.

    Without this, demo_collection inherits the ambient (often PG-only) routing
    and its items never reach Elasticsearch, so the STAC view returns
    ``numberMatched: 0`` (#2241). The ES INDEX entry is ``source="auto"`` so
    the indexer is self-registered rather than suppressed as operator-managed;
    INDEX is async by lane definition, so it stays durable without blocking
    the PG primary.
    """
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(driver_ref="items_postgresql_driver"),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


class _DemoDataContributor:
    """Yields the single demo seed that ``MultiContributorPreset`` will apply."""

    def get_data(self) -> Iterable[DataSeed]:
        yield DataSeed(
            catalog_id="demo_catalog",
            collection_id="demo_collection",
            catalog_data=_CATALOG_DATA,
            collection_data=_COLLECTION_DATA,
            items=_DEMO_ITEMS,
            manage_catalog=True,
            manage_collection=True,
            items_routing=_demo_items_routing(),
            # feature-only demo — no GCS bucket needed (born bucket-free)
            defer_provisioning=True,
            virtual_assets=(
                {
                    "asset_id": "demo_virtual_cog",
                    "href": _DEMO_VIRTUAL_ASSET_HREF,
                    "metadata": {
                        "title": "Demo COG (virtual asset from the original bucket)",
                        "roles": ["data"],
                    },
                },
            ),
        )


# ---------------------------------------------------------------------------
# Preset instance — registered in presets/__init__.py
# ---------------------------------------------------------------------------

DEMO_DATA_PRESET = MultiContributorPreset(
    name="demo_data",
    description=(
        "Seed a demo catalog (demo_catalog/demo_collection) with a 2×3 grid of "
        "map-tile polygons over Italy and a virtual (href-based, bucket-free) "
        "COG asset — the data-contributor demonstration preset (mirrors the "
        "legacy /admin/demo/populate content)."
    ),
    keywords=("demo", "data", "platform", "catalog", "seed"),
    contributors_factory=lambda: [_DemoDataContributor()],
)
