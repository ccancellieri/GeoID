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

from typing import ClassVar, List, Optional, Tuple
from pydantic import Field
from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin

class FeaturesPluginConfig(ExposableConfigMixin, PluginConfig):
    """
    Runtime configuration for the OGC Features extension.
    Controls caching and visibility.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "extensions", "features")

    # Caching
    cache_on_demand: Mutable[bool] = Field(
        False,
        description="If True, generated Features responses are saved to the bucket storage."
    )
    
    storage_priority: Mutable[List[str]] = Field(
        default=["bucket"],
        description="Priority list of storage providers to use for saving cached responses."
    )

    # Pagination policy (OGC API - Features Part 1 Core, /req/core/fc-limit-*)
    default_limit: Mutable[int] = Field(
        default=10,
        ge=1,
        description="Page size used for GET /items when ``limit`` is omitted.",
    )
    max_limit: Mutable[int] = Field(
        default=1000,
        ge=1,
        description=(
            "Maximum page size for GET /items. A requested ``limit`` above "
            "this value is clamped, never rejected (fc-limit-response-1)."
        ),
    )

    # Response byte budget (#2681): bounds page-assembly memory regardless
    # of how many/how large the matched geometries are. A `limit` ceiling
    # alone cannot do this — a handful of large multipolygons can already
    # exceed process memory well under `max_limit`.
    max_response_bytes: Mutable[Optional[int]] = Field(
        default=10_000_000,
        ge=1,
        examples=[10_000_000, 50_000_000],
        description=(
            "Byte budget for a single GET /items GeoJSON response. Once the "
            "serialized ``features`` bytes cross this size the page is cut "
            "short and a ``next`` link is returned that resumes exactly "
            "where the page left off; ``numberReturned`` reflects what was "
            "actually served, ``numberMatched`` is unaffected. ``null`` "
            "disables the budget (unbounded page, previous behaviour)."
        ),
    )
