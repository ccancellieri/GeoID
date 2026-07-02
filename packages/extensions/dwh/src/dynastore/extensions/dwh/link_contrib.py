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

"""DwhLinkContributor — advertises legacy /dwh join endpoint in collection responses.

Emits a ``resource_root`` link per collection so consumers can discover the
DWH join surface alongside the OGC API - Joins surface. Remove once clients
have migrated to /join and DwhService is retired.
"""

from __future__ import annotations

from functools import partial

from dynastore.models.protocols.link_contrib import make_resource_root_contributor

#: Contributes ``rel="dwh-join"`` (POST) at ``resource_root`` — the
#: per-catalog join endpoint that accepts a DWHJoinRequestBase body.
DwhLinkContributor = partial(
    make_resource_root_contributor,
    rel="dwh-join",
    path_template="{base}/dwh/catalogs/{catalog_id}/join",
    methods=(
        ("POST", "Data Warehouse join (legacy — use /join instead)"),
    ),
    priority=100,  # matches DwhService.priority
)
