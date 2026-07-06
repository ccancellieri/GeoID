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

"""Unit tests for the shared ``get_join_clause`` default on ``SidecarProtocol``
(issue #2700 Part 3).

The item_metadata / access_envelope / geometries sidecars now rely on the
base implementation for their plain ``hub.geoid = sidecar.geoid`` join; the
attributes sidecar keeps its own override (validity-range predicate) and is
covered separately by ``test_attrs_ext_id_sort_index.py``.
"""

from __future__ import annotations

from dynastore.modules.storage.drivers.pg_sidecars.access_envelope import (
    AccessEnvelopeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.access_envelope_config import (
    AccessEnvelopeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries import GeometriesSidecar
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.item_metadata import (
    ItemMetadataSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.item_metadata_config import (
    ItemMetadataSidecarConfig,
)


def test_geometries_uses_base_default_join_clause():
    sidecar = GeometriesSidecar(GeometriesSidecarConfig())
    clause = sidecar.get_join_clause(schema="s", hub_table="t")
    assert clause == 'LEFT JOIN "s"."t_geometries" sc_geometries ON h.geoid = sc_geometries.geoid'


def test_item_metadata_uses_base_default_join_clause():
    sidecar = ItemMetadataSidecar(ItemMetadataSidecarConfig())
    clause = sidecar.get_join_clause(schema="s", hub_table="t")
    assert clause == (
        'LEFT JOIN "s"."t_item_metadata" sc_item_metadata ON h.geoid = sc_item_metadata.geoid'
    )


def test_access_envelope_overrides_default_alias_only():
    """The access_envelope sidecar keeps its established short 'ae' alias
    (hardcoded elsewhere into its own WHERE-clause builder) while otherwise
    using the shared base join-clause shape."""
    sidecar = AccessEnvelopeSidecar(
        config=AccessEnvelopeSidecarConfig(column_name="access_envelope")
    )
    clause = sidecar.get_join_clause(schema="s", hub_table="t")
    assert clause == 'LEFT JOIN "s"."t_access_envelope" ae ON h.geoid = ae.geoid'


def test_explicit_sidecar_alias_overrides_the_default_for_all_three():
    for sidecar in (
        GeometriesSidecar(GeometriesSidecarConfig()),
        ItemMetadataSidecar(ItemMetadataSidecarConfig()),
        AccessEnvelopeSidecar(config=AccessEnvelopeSidecarConfig(column_name="access_envelope")),
    ):
        clause = sidecar.get_join_clause(schema="s", hub_table="t", sidecar_alias="custom")
        assert " custom ON " in clause


def test_join_type_and_extra_condition_are_honored():
    sidecar = GeometriesSidecar(GeometriesSidecarConfig())
    clause = sidecar.get_join_clause(
        schema="s", hub_table="t", join_type="INNER",
        extra_condition="AND sc_geometries.geom IS NOT NULL",
    )
    assert clause == (
        'INNER JOIN "s"."t_geometries" sc_geometries ON h.geoid = sc_geometries.geoid '
        "AND sc_geometries.geom IS NOT NULL"
    )
