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

"""Unified input model for the public ``elasticsearch_indexer`` OGC Process.

A single shape that can drive either a catalog-wide reindex (when only
``catalog_id`` is set) or a single-collection reindex (when ``collection_id``
is also set). The dispatcher class :class:`.indexer_task.ElasticsearchIndexerTask`
adapts this to the legacy ``BulkCatalogReindexInputs`` / ``BulkCollectionReindexInputs``
expected by the underlying implementations.
"""
from typing import Optional

from pydantic import BaseModel, Field


class ElasticsearchIndexerRequest(BaseModel):
    catalog_id: str = Field(
        ..., description="Catalog to reindex into the per-tenant items index."
    )
    collection_id: Optional[str] = Field(
        None,
        description=(
            "If set, only reindex this collection. Otherwise reindex every "
            "collection of the catalog that routes through the regular ES driver."
        ),
    )
    driver: Optional[str] = Field(
        None,
        description=(
            "Restrict the reindex to a single secondary driver (e.g. only the "
            "regular ES driver). Omit to reindex through every active driver."
        ),
    )
    page_size: Optional[int] = Field(
        None,
        gt=0,
        description=(
            "Rows fetched from canonical storage per read page. An explicit "
            "value is honored verbatim; use a small page for geometry-heavy "
            "collections. Omit to defer to the ES writer's preferred chunk "
            "size (falling back to the module default)."
        ),
    )
