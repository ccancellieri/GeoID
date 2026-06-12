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

"""Sidecar physical-table naming — single source of truth.

A leaf module (no intra-subpackage imports) so both the sidecar DDL code
and out-of-subpackage callers (e.g. the volumes tiler resolving the
geometries sidecar) can route through one helper without a circular
import. The ``<physical_table>_<sidecar_id>`` convention lives here and
nowhere else.
"""

from __future__ import annotations


def sidecar_table_name(physical_table: str, sidecar_id: str) -> str:
    """Compute a sidecar's physical PG table name from the hub table.

    Single source of truth for the ``<physical_table>_<sidecar_id>``
    convention. Every sidecar's DDL/query code and any external caller
    must route through this helper rather than re-spelling the f-string,
    so the naming rule changes in exactly one place.
    """
    return f"{physical_table}_{sidecar_id}"
