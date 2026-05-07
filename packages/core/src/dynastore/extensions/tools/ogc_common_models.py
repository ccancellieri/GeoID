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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Canonical OGC API Common response models.

These are the single source of truth for models shared across all OGC-protocol
extensions (Features, STAC, Records, Maps, Coverages, EDR, etc.).  Extension-specific
models should import from here rather than defining their own copies.
"""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from dynastore.models.shared_models import Link


class Conformance(BaseModel):
    """OGC API conformance response (OGC API Common clause 7.4)."""

    conformsTo: List[str]


class LandingPage(BaseModel):
    """OGC API landing page response (OGC API Common clause 7.2).

    Subclass this if you need extension-specific ``json_schema_extra``
    (e.g. Features adds OpenAPI examples).
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "title": "DynaStore OGC API",
                    "description": "Access to geospatial data via OGC API",
                    "links": [
                        {"rel": "self", "href": "https://example.com/", "type": "application/json"},
                        {"rel": "conformance", "href": "https://example.com/conformance", "type": "application/json"},
                        {"rel": "service-doc", "href": "https://example.com/api", "type": "application/json"},
                    ],
                }
            ]
        },
    )

    title: Optional[str] = None
    description: Optional[str] = None
    links: List[Link] = Field(default_factory=list)
