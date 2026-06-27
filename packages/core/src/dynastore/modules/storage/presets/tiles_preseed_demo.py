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

"""``tiles_preseed_demo`` preset — one-click tile preseed demo (PLATFORM tier).

Applies at platform scope (``POST /configs/presets/tiles_preseed_demo``):

  1. Creates the ``demo_tiles_catalog`` catalog and
     ``demo_tiles_collection`` collection (skip-if-exists), seeded with a
     3×2 grid of bounding-box polygons covering the Italian peninsula (the
     same geometry family as ``demo_data``). The collection uses
     PG-primary + Elasticsearch-secondary routing so items are searchable.

  2. Submits an async ``tiles_preseed`` job to generate PMTiles archives for
     the ``demo_tiles_collection`` at ``WebMercatorQuad`` zoom levels.

``TilesPreseedConfig.preseed_enabled`` defaults to ``True`` in the config
model. The config waterfall (collection → catalog → platform → code default)
returns a default ``TilesPreseedConfig()`` when no explicit row is stored,
so the task will proceed without any additional config write.

Revoke: removes the seeded items, the collection, and the catalog when each
is found to be empty. The ``tiles_preseed`` job result (stored tiles) is not
undone — tiles must be cleared independently via the tile-storage layer.
"""
from __future__ import annotations

from typing import Iterable

from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
)

from .multi_contributor import MultiContributorPreset
from .preset import DataSeed, TaskSeed

_CATALOG_ID = "demo_tiles_catalog"
_COLLECTION_ID = "demo_tiles_collection"

_CATALOG_DATA = {
    "id": _CATALOG_ID,
    "title": {"en": "Tile Preseed Demo"},
    "description": {
        "en": (
            "Demo catalog populated by the tiles_preseed_demo preset. "
            "Contains a small set of polygon features used to demonstrate "
            "the tile preseed pipeline."
        ),
    },
    "keywords": ["demo", "tiles", "preseed", "platform"],
    "license": "CC-BY-4.0",
}

_COLLECTION_DATA = {
    "id": _COLLECTION_ID,
    "title": {"en": "Italy Tile Grid (demo)"},
    "description": {
        "en": (
            "A 3×2 grid of map-tile polygons covering the Italian peninsula, "
            "used as input for the tiles_preseed_demo preset."
        ),
    },
}


def _tile_polygon(col: int, row: int) -> dict:
    """One map-tile polygon over Italy.

    lon: 6.6–18.5 (col width ≈ 5.95°); lat: 37.9–47.1 (row height ≈ 3.07°).
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
            [lon0, lat0],
        ]],
    }


_ROW_LABELS = ["south", "centre", "north"]
_COL_LABELS = ["west", "east"]

_DEMO_ITEMS = tuple(
    {
        "id": f"tile_{_COL_LABELS[c]}_{_ROW_LABELS[r]}",
        "type": "Feature",
        "geometry": _tile_polygon(c, r),
        "properties": {
            "name": f"Italy – {_ROW_LABELS[r].capitalize()} {_COL_LABELS[c].capitalize()}",
            "col": c,
            "row": r,
        },
    }
    for r in range(3) for c in range(2)
)


def _tiles_items_routing() -> ItemsRoutingConfig:
    """PG-primary + Elasticsearch-secondary routing.

    Mirrors demo_data routing so seeded items are searchable via STAC
    search after the ES secondary write completes.
    """
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    secondary_index=True,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(driver_ref="items_postgresql_driver"),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


class _TilesPreseedDemoContributor:
    """Data + task contributor for the tiles_preseed_demo preset."""

    def get_data(self) -> Iterable[DataSeed]:
        yield DataSeed(
            catalog_id=_CATALOG_ID,
            collection_id=_COLLECTION_ID,
            catalog_data=_CATALOG_DATA,
            collection_data=_COLLECTION_DATA,
            items=_DEMO_ITEMS,
            manage_catalog=True,
            manage_collection=True,
            items_routing=_tiles_items_routing(),
        )

    def get_tasks(self) -> Iterable[TaskSeed]:
        # TilePreseedRequest requires catalog_id; collection_id scopes the
        # preseed to the single demo collection.  ``preseed_enabled`` defaults
        # to True in TilesPreseedConfig so no extra config write is needed.
        yield TaskSeed(
            process_id="tiles_preseed",
            inputs={
                "catalog_id": _CATALOG_ID,
                "collection_id": _COLLECTION_ID,
                "output_format": "pmtiles",
                "operation": "seed",
            },
            async_mode=True,
            dedup_key="preset:tiles_preseed_demo:tiles_preseed",
        )


TILES_PRESEED_DEMO_PRESET = MultiContributorPreset(
    name="tiles_preseed_demo",
    description=(
        "Create the demo_tiles_catalog/demo_tiles_collection seeded with a 3×2 "
        "grid of Italy tile polygons, then submit an async tiles_preseed job to "
        "generate PMTiles archives for the collection at WebMercatorQuad zoom "
        "levels. Requires tile storage (GCS bucket or local backend) to be "
        "configured on the deployment. Designed for CI / demo use."
    ),
    keywords=("demo", "data", "platform", "tiles", "preseed"),
    contributors_factory=lambda: [_TilesPreseedDemoContributor()],
)
