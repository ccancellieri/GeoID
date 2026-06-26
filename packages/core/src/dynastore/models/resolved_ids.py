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

"""
Resolved identifier models for the ID resolution boundary.

These models represent the split between immutable internal IDs and mutable
external IDs (public labels). They are storage-agnostic and used at protocol
boundaries to ensure consistency across the system.

Terminology (2026-06-26 Decision):
- `id`: Internal identifier (PK) - **Immutable**
- `external_id`: External label (user-facing) - **Mutable** (rename)

See Issue #2430 for background.
"""

from pydantic import BaseModel


class ResolvedCatalogIds(BaseModel):
    """Resolved catalog identifiers - storage-agnostic.

    Represents the split between the immutable internal catalog ID and the
    mutable external label (public-facing name).
    """

    id: str
    external_id: str


class ResolvedCollectionIds(BaseModel):
    """Resolved collection identifiers - storage-agnostic.

    Represents the split between the immutable internal collection ID and the
    mutable external label (public-facing name), along with parent catalog context.
    """

    id: str
    external_id: str
    catalog_id: str
