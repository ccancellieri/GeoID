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

"""Producer-level properties contract (#1826, follow-up to #1818/#1827).

The PG read path must place user-attribute columns ONLY in
``feature.properties`` — never *also* echo them up as top-level GeoJSON
foreign members via ``model_extra``. Before this fix the
``map_row_to_feature`` context bridge duplicated every attribute column into
``__pydantic_extra__``, which:

  * emitted non-spec GeoJSON on the OGC Features ``/items`` wire
    (``{"type":"Feature","CODE":"IT","properties":{"CODE":"IT"}}``), and
  * forced every join/export consumer to reconcile ``model_extra`` against
    ``properties`` (the stream-boundary band-aid ``normalize_feature_attributes``
    added in #1818).

These tests pin the contract at the producer so the normalization downstream is
a genuine no-op safety net.
"""

from unittest.mock import MagicMock

from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FeaturePipelineContext,
)
from dynastore.modules.tools.item_stream import resolve_join_value


def _svc() -> ItemService:
    return ItemService(engine=MagicMock())


def test_user_attributes_live_only_in_properties() -> None:
    """A JSONB-mode read places attribute columns in ``properties`` and NOT in
    ``model_extra`` — no top-level duplication on the wire."""
    svc = _svc()
    col_config = ItemsPostgresqlDriverConfig()
    row = {"geoid": "g-1", "external_id": "ext-1", "CODE": "IT", "NAME": "Italy"}

    feat = svc.map_row_to_feature(row, col_config)

    assert feat.properties == {"CODE": "IT", "NAME": "Italy"}
    # The producer contract: attributes must NOT be echoed up as foreign members.
    assert (feat.model_extra or {}) == {}


def test_wire_dump_has_no_foreign_member_duplicates() -> None:
    """``model_dump`` (the OGC Features serialization) exposes only structural
    GeoJSON keys — attribute columns appear once, under ``properties``."""
    svc = _svc()
    col_config = ItemsPostgresqlDriverConfig()
    row = {"geoid": "g-1", "external_id": "ext-1", "CODE": "IT", "NAME": "Italy"}

    dumped = svc.map_row_to_feature(row, col_config).model_dump(exclude_none=True)

    assert set(dumped) == {"id", "type", "properties"}
    assert "CODE" not in dumped and "NAME" not in dumped
    assert dumped["properties"] == {"CODE": "IT", "NAME": "Italy"}


def test_explicitly_requested_system_field_still_surfaces() -> None:
    """#1827 must keep working: a SYSTEM field named explicitly in ``select``
    is exposed in the ``system`` foreign-member section so a dwh join with
    ``join_source="system"`` can use it as a key. The #1826 props-dedup must
    not collapse that section."""
    svc = _svc()
    col_config = ItemsPostgresqlDriverConfig()
    row = {"geoid": "g-1", "external_id": "ext-1", "CODE": "IT"}

    ctx = FeaturePipelineContext(lang="en", requested_fields={"external_id"})
    feat = svc.map_row_to_feature(row, col_config, context=ctx)

    # The join key resolves from the system section (#1827 contract).
    assert resolve_join_value(feat, "external_id", "system") == "ext-1"
    assert (feat.model_extra or {}).get("system", {}).get("external_id") == "ext-1"

    # The user attribute is reachable as a property and is NOT echoed as a
    # bare top-level foreign member (the #1826 contract holds alongside #1827).
    assert feat.properties.get("CODE") == "IT"
    assert "CODE" not in (feat.model_extra or {})
    assert "CODE" not in feat.model_dump(exclude_none=True)


def test_jsonb_blob_column_not_echoed_as_foreign_member() -> None:
    """JSONB-blob mode: a row carrying the raw ``attributes`` blob column must
    promote the blob *content* into ``properties`` and NOT re-emit the blob as a
    top-level ``attributes`` foreign member.

    This reproduces the ``sri_lanka_pia`` /items regression where the OGC
    Features wire showed ``{"properties":{...}, "attributes":{...}}`` — the raw
    JSONB storage column leaking beside ``properties``.
    """
    svc = _svc()
    col_config = ItemsPostgresqlDriverConfig()
    row = {
        "geoid": "g-1",
        "external_id": "LKA_1944_984",
        "attributes": {
            "FID": 984,
            "OBJECTID": 1944,
            "IRMA_CODE": "LKA_1944_984",
        },
    }

    feat = svc.map_row_to_feature(row, col_config)

    # Blob content is promoted into properties ...
    assert feat.properties.get("FID") == 984
    assert feat.properties.get("OBJECTID") == 1944
    assert feat.properties.get("IRMA_CODE") == "LKA_1944_984"
    # ... and the raw blob column never surfaces on the Feature root.
    assert "attributes" not in (feat.model_extra or {})
    dumped = feat.model_dump(exclude_none=True)
    assert "attributes" not in dumped
    assert set(dumped) <= {"id", "type", "properties", "geometry", "bbox"}
