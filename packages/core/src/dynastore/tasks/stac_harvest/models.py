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

"""Input model for the stac_harvest OGC Process."""

from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from dynastore.modules.storage.presets.routing import RoutingDrivers

# Legacy ``storage_backend`` literal → ``drivers`` combination.  Older callers
# POST ``storage_backend`` (es / es_pg / pg); map it to the routing ``drivers``
# enum so those requests keep working.
_LEGACY_BACKEND_TO_DRIVERS = {
    "es": RoutingDrivers.ES,
    "es_pg": RoutingDrivers.PG_ES,
    "pg": RoutingDrivers.PG,
}


class StacHarvestCursor(BaseModel):
    """Resume position within a harvest walk, stamped by the harvest loop (#3034).

    ``collection_id`` is the *source* collection id currently in progress
    (``None`` before the first collection starts, or between two collections
    in a full-catalog harvest). ``items_href`` is the STAC ``rel=next`` items
    page URL to resume from within that collection (``None`` means start it
    from the beginning). ``done`` marks that collection's item walk as fully
    drained, so a resumed catalog walk skips it and moves to the next one.

    Populated automatically after each items-page batch write commits — never
    set this on submit.
    """

    collection_id: Optional[str] = None
    items_href: Optional[str] = None
    done: bool = False


class StacHarvestRequest(BaseModel):
    """Inputs for the ``stac_harvest`` OGC Process.

    Harvests a remote STAC source into a local dynastore catalog.  The source
    ``catalog_url`` may point at a STAC **catalog** (exposes ``/collections``)
    or directly at a single STAC **collection** (a document whose ``type`` is
    ``Collection``): the task auto-detects which and harvests accordingly.
    Collections and items are upserted idempotently, keyed on the STAC ``id``.
    """

    catalog_url: str = Field(
        ...,
        description=(
            "Source STAC URL.  Either a catalog base that exposes /collections "
            "and /collections/{id}/items, or a single collection document "
            "(type=Collection) to harvest just that collection."
        ),
    )
    target_catalog: str = Field(
        ...,
        description="ID of the local dynastore catalog to write into.",
    )
    target_collection: Optional[str] = Field(
        default=None,
        description=(
            "Destination collection id for a single-collection harvest.  When "
            "the source URL points at one collection and this is set, items "
            "land in this collection and routing is pinned at collection scope; "
            "when unset, the source collection's id is used.  Ignored when the "
            "source is a full catalog."
        ),
    )
    max_collections: int = Field(
        default=0,
        ge=0,
        description="Maximum number of source collections to harvest (0 = all).",
    )
    max_items: int = Field(
        default=0,
        ge=0,
        description="Maximum number of items per collection to harvest (0 = all).",
    )
    with_assets: bool = Field(
        default=True,
        description=(
            "When True, register each item asset href as a virtual asset "
            "(dynastore stores only the href, never the bytes)."
        ),
    )
    drivers: RoutingDrivers = Field(
        default=RoutingDrivers.ES,
        description=(
            "Storage routing for this harvest (applied via the ``routing`` "
            "preset before any write).  ``es`` routes items directly to public "
            "Elasticsearch so they are immediately searchable; ``pg_es`` writes "
            "PG primary + async ES secondary; ``pg`` uses PG only; ``pg_pes`` "
            "writes PG primary + private ES secondary.  Legacy ``storage_backend`` "
            "(es / es_pg / pg) is still accepted and mapped to this field."
        ),
    )
    resume: Optional[StacHarvestCursor] = Field(
        default=None,
        description=(
            "Resume cursor stamped by the harvest loop after each completed "
            "items page (#3034) — do not set on submit. A retry of this task "
            "after a timeout/kill resumes the walk from here instead of "
            "restarting the whole source catalog from the beginning."
        ),
    )

    @field_validator("catalog_url")
    @classmethod
    def _validate_catalog_url(cls, v: str) -> str:
        if not v.startswith("https://") and not v.startswith("http://"):
            raise ValueError("catalog_url must start with http:// or https://")
        return v.rstrip("/")

    @model_validator(mode="before")
    @classmethod
    def _map_legacy_storage_backend(cls, data):
        """Map a legacy ``storage_backend`` input onto ``drivers``.

        Only applied when ``drivers`` was not given explicitly, so a caller can
        migrate at its own pace; the legacy key is otherwise ignored.
        """
        if isinstance(data, dict) and "drivers" not in data:
            legacy = data.get("storage_backend")
            mapped = _LEGACY_BACKEND_TO_DRIVERS.get(legacy) if legacy else None
            if mapped is not None:
                data = {**data, "drivers": mapped}
        return data
