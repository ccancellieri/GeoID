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

"""JoinsLinkContributor — advertises OGC API - Joins endpoints in collection responses.

Emits two ``resource_root`` links per collection so OGC Features, STAC, and
other consumers surface the /join surface without callers needing to know
the join extension's prefix.

Links are collection-scoped (skipped for individual item refs).
"""

from __future__ import annotations

from functools import partial

from dynastore.models.protocols.link_contrib import make_resource_root_contributor

#: Contributes:
#: - ``rel="join"`` (GET)  — describe endpoint (capabilities + supported drivers)
#: - ``rel="join"`` (POST) — execute endpoint
JoinsLinkContributor = partial(
    make_resource_root_contributor,
    rel="join",
    path_template="{base}/join/catalogs/{catalog_id}/collections/{collection_id}/join",
    methods=(
        ("GET", "OGC API - Joins: describe"),
        ("POST", "OGC API - Joins: execute"),
    ),
    priority=180,  # matches JoinsService.priority
)
