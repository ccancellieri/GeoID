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

"""Unit tests for the ``ItemsSchema`` config-save validate handlers (#1489).

Covers two #1489 follow-ups on top of #1488/#1491:

* ``_validate_items_schema_reserved_names`` now also rejects a reserved root
  name introduced via ``FieldDefinition.alias`` — not just the declared ``name``
  key (item 3).
* ``_validate_items_schema_storage_realizable`` now enforces the silent-drop
  invariant from the FIRST save, before any PG driver config exists, by
  simulating the bridge against a default AUTOMATIC sidecar (item 2). The
  simulation is config-only (no DDL), honouring ``feedback_never_migrate_db``.

These handlers run pre-persist inside the config-save transaction; a raised
``ValueError`` becomes an HTTP 400 and rolls back the upsert.
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.storage.driver_config import (
    ItemsSchema,
    _validate_items_schema_reserved_names,
    _validate_items_schema_storage_realizable,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
    PostgresType,
)


def _schema(**fields: FieldDefinition) -> ItemsSchema:
    return ItemsSchema(fields=dict(fields))


def _fd(name: str, *, data_type: str = "string", alias: str | None = None,
        required: bool = False, container: str = "properties") -> FieldDefinition:
    return FieldDefinition(
        name=name, data_type=data_type, alias=alias, required=required,
        container=container,
    )


# ---------------------------------------------------------------------------
# _validate_items_schema_reserved_names — name AND alias collisions (item 3)
# ---------------------------------------------------------------------------


async def test_reserved_name_collision_on_key_rejected():
    schema = _schema(geometry=_fd("geometry", data_type="string"))
    with pytest.raises(ValueError, match=r"reserved root name"):
        await _validate_items_schema_reserved_names(schema, "cat", "col", None)


async def test_reserved_name_collision_on_alias_rejected():
    # The declared key is harmless, but the alias shadows a system root field.
    schema = _schema(my_geom=_fd("my_geom", alias="geometry"))
    with pytest.raises(ValueError) as exc:
        await _validate_items_schema_reserved_names(schema, "cat", "col", None)
    msg = str(exc.value)
    assert "alias=" in msg and "geometry" in msg


async def test_reserved_name_both_key_and_alias_reported_together():
    schema = _schema(
        bbox=_fd("bbox"),                       # key collision
        my_geom=_fd("my_geom", alias="geometry"),  # alias collision
    )
    with pytest.raises(ValueError) as exc:
        await _validate_items_schema_reserved_names(schema, "cat", "col", None)
    msg = str(exc.value)
    assert "bbox" in msg
    assert "geometry" in msg


async def test_clean_schema_passes_reserved_names():
    schema = _schema(
        code=_fd("code"),
        adm0_name=_fd("adm0_name", alias="country"),
    )
    # Must not raise.
    await _validate_items_schema_reserved_names(schema, "cat", "col", None)


async def test_reserved_names_noop_on_non_items_schema():
    await _validate_items_schema_reserved_names(object(), "cat", "col", None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# _validate_items_schema_reserved_names — container-gated relaxation (#2642)
# ---------------------------------------------------------------------------


async def test_non_structural_reserved_name_with_properties_container_passes():
    # ``external_id`` is a reserved root field of the private tenant feature
    # mapping, but it is NOT a true GeoJSON/STAC structural member. Declared
    # with ``container="properties"`` it resolves to the distinct
    # ``properties.external_id`` path, so it no longer collides with the
    # doc root and must be allowed (#2642).
    schema = _schema(external_id=_fd("external_id", container="properties"))
    # Must not raise.
    await _validate_items_schema_reserved_names(schema, "cat", "col", None)


async def test_true_structural_reserved_name_still_rejected_regardless_of_container():
    # ``collection`` is a true GeoJSON/STAC structural member — it stays
    # blocked even with ``container="properties"`` since the private write
    # projection would otherwise need to quarantine it into
    # ``properties.extras`` rather than storing it as declared (#2642).
    schema = _schema(collection=_fd("collection", container="properties"))
    with pytest.raises(ValueError, match=r"reserved root name"):
        await _validate_items_schema_reserved_names(schema, "cat", "col", None)


async def test_nested_container_names_still_rejected_with_properties_container():
    # ``properties`` / ``extras`` stay blocked even under
    # ``container="properties"`` — they'd collide with the quarantine
    # bucket used by ``project_private_doc`` (#2642).
    schema = _schema(
        properties=_fd("properties", container="properties"),
        extras=_fd("extras", container="properties"),
    )
    with pytest.raises(ValueError, match=r"reserved root name"):
        await _validate_items_schema_reserved_names(schema, "cat", "col", None)


async def test_non_structural_reserved_name_blocked_outside_properties_container():
    # Without the ``container="properties"`` exemption, ``external_id``
    # still collides with the doc root (e.g. declared under ``"system"``),
    # so the pre-#2642 protection holds.
    schema = _schema(external_id=_fd("external_id", container="system"))
    with pytest.raises(ValueError, match=r"reserved root name"):
        await _validate_items_schema_reserved_names(schema, "cat", "col", None)


# ---------------------------------------------------------------------------
# _validate_items_schema_storage_realizable — first-save coverage (item 2)
# ---------------------------------------------------------------------------


async def test_storage_realizable_first_save_accepts_columnar_schema():
    # No ConfigsProtocol registered in a pure unit test → get_protocol() is None
    # → the handler falls back to a default AUTOMATIC sidecar instead of skipping.
    # A required field forces COLUMNAR resolution; the bridge promotes every
    # non-geometry field, so nothing is unreachable and the save is accepted.
    schema = _schema(
        code=_fd("code", required=True),   # forces COLUMNAR resolution
        name=_fd("name"),                  # plain field — must be promoted too
        geom=_fd("geom", data_type="geometry"),
    )
    await _validate_items_schema_storage_realizable(schema, "cat", "col", None)


async def test_storage_realizable_first_save_accepts_jsonb_schema():
    # Plain AUTO-only schema resolves JSONB (no columns) → blob catches every
    # field → accepted without inspecting promotions.
    schema = _schema(name=_fd("name"), note=_fd("note"))
    await _validate_items_schema_storage_realizable(schema, "cat", "col", None)


async def test_storage_realizable_rejects_bridge_regression(monkeypatch):
    # Simulate a future regression in the bridge: COLUMNAR resolution but a
    # non-geometry field left unpromoted (the #1488 silent-drop class). The
    # backstop must reject the save with an actionable error naming the field.
    schema = _schema(a=_fd("a"), b=_fd("b"))

    def _broken_bridge(_schema, sidecar):  # noqa: ANN001
        # attribute_schema present → AUTOMATIC resolves COLUMNAR, but 'b' is
        # missing from the promotions.
        return FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.AUTOMATIC,
            attribute_schema=[AttributeSchemaEntry(name="a", type=PostgresType.TEXT)],
        )

    monkeypatch.setattr(
        "dynastore.modules.storage.field_constraints.bridge_schema_to_attribute_sidecar",
        _broken_bridge,
    )
    with pytest.raises(ValueError) as exc:
        await _validate_items_schema_storage_realizable(schema, "cat", "col", None)
    assert "'b'" in str(exc.value) or "b" in str(exc.value)
    assert "silently dropped" in str(exc.value)


async def test_storage_realizable_noop_without_catalog_collection():
    schema = _schema(code=_fd("code", required=True))
    # No catalog/collection → nothing to validate against.
    await _validate_items_schema_storage_realizable(schema, None, None, None)


async def test_storage_realizable_noop_on_non_items_schema():
    await _validate_items_schema_storage_realizable(object(), "cat", "col", None)  # type: ignore[arg-type]
