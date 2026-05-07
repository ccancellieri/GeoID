#    Copyright 2025 FAO
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

"""
StorageLocation — typed projection of where a driver stores its data.

Each driver implements ``location(catalog_id, collection_id) -> StorageLocation``
to expose its physical coordinates without callers having to dispatch on driver
class or know driver-specific config fields.

This replaces the old ``StorageLocationResolver`` Protocol (which returned ``Any``)
and the pattern of ``isinstance(driver, ItemsPostgresqlDriver)`` + reading
``physical_schema``/``physical_table`` directly.

Callers that need "where does this live" use ``driver.location(...)`` and read
``canonical_uri`` or ``identifiers[key]`` without coupling to backend types.
"""

from typing import Dict

from pydantic import BaseModel, ConfigDict, Field


class StorageLocation(BaseModel):
    """Physical storage coordinates for a catalog/collection in a specific driver.

    Produced by ``CollectionItemsStore.location()`` and
    ``AssetStore.location()``.

    Attributes:
        backend:        Driver type identifier (e.g. ``"postgresql"``,
                        ``"elasticsearch"``, ``"iceberg"``, ``"duckdb"``,
                        ``"elasticsearch_assets"``).
        canonical_uri:  Stable, human-readable URI describing the physical
                        location.  Format is backend-specific:

                        - ``postgresql://{schema}.{table}``
                        - ``iceberg://{catalog}/{namespace}/{table}``
                        - ``duckdb:///{path}?format={format}``
                        - ``es://{index_pattern}``

        identifiers:    Backend-specific key → value map for programmatic
                        access.  Keys vary by backend:

                        - PostgreSQL: ``{"schema": ..., "table": ...}``
                        - Iceberg:    ``{"catalog": ..., "namespace": ..., "table": ...}``
                        - DuckDB:     ``{"path": ..., "format": ...}``
                        - ES:         ``{"index_pattern": ..., "prefix": ...}``

        display_label:  Short human-readable label for UI / log messages.
    """

    model_config = ConfigDict(frozen=True)

    backend: str = Field(description="Driver type identifier.")
    canonical_uri: str = Field(description="Stable URI describing the physical location.")
    identifiers: Dict[str, str] = Field(
        default_factory=dict,
        description="Backend-specific key→value coordinates.",
    )
    display_label: str = Field(description="Short label for UI / logs.")
