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

"""Pin the Phase 1.6 ``CollectionInfo`` PluginConfig hoist.

Before Phase 1.6, ``collection_type`` was a field on
``ItemsPostgresqlDriverConfig`` — the kind of a collection (VECTOR /
RASTER / RECORDS) was buried inside ONE storage backend's config and
the other drivers (Iceberg, DuckDB, etc.) had to either re-invent or
ignore it.

After Phase 1.6, ``CollectionInfo`` is its own ``PluginConfig`` at
collection scope, addressable via
``/configs/catalogs/{c}/collections/{c}/plugins/collection_type``.
The PG driver (and every other capable driver) reads it via
``configs.get_config(CollectionInfo, catalog_id, collection_id)`` and
passes the ``kind.value`` to the sidecar resolver.

These tests pin:
- The class shape and registration.
- The PG driver config no longer carries the field.
- The sidecar resolver still drops geometry sidecars for RECORDS
  (logic relocated from a model_validator).
"""

from __future__ import annotations

import pytest

from dynastore.modules.catalog.catalog_config import (
    CollectionInfo,
    CollectionKind,
)
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars import _effective_sidecars


def test_collection_type_is_a_plugin_config():
    from dynastore.models.plugin_config import PluginConfig

    assert issubclass(CollectionInfo, PluginConfig)


def test_collection_type_address_and_freeze_at():
    assert CollectionInfo._address == ("platform", "catalog", "collection", "info")
    assert CollectionInfo._freeze_at == "collection"


def test_collection_type_class_key_is_snake_case():
    assert CollectionInfo.class_key() == "collection_info"


def test_collection_type_default_is_vector():
    ct = CollectionInfo()
    assert ct.kind is CollectionKind.VECTOR


def test_collection_type_accepts_records_and_raster():
    assert CollectionInfo(kind=CollectionKind.RECORDS).kind is CollectionKind.RECORDS
    assert CollectionInfo(kind=CollectionKind.RASTER).kind is CollectionKind.RASTER


def test_collection_type_in_registry():
    """The hoisted class must appear in ``list_registered_configs`` so
    the ``/configs/registry`` endpoint surfaces it.
    """
    from dynastore.models.plugin_config import list_registered_configs

    configs = list_registered_configs()
    assert "collection_info" in configs
    assert configs["collection_info"] is CollectionInfo


def test_pg_driver_config_no_longer_carries_collection_type():
    """The Phase 1.6 hoist removes ``collection_type`` from the PG driver
    config's ``model_fields``.  ``ItemsPostgresqlDriverConfig`` declares
    ``model_config = ConfigDict(extra="allow")`` to keep the door open for
    unknown driver-local plumbing, so a payload that still carries
    ``collection_type`` won't error out — but the field is no longer part
    of the model schema, so:
    - JSON Schema / OpenAPI no longer advertises it.
    - The PG driver's runtime path ignores it (the resolver receives
      ``collection_type`` from its async caller's ``CollectionInfo``
      fetch, not from this driver config).
    """
    assert "collection_type" not in ItemsPostgresqlDriverConfig.model_fields


def test_resolver_drops_geometry_for_records():
    """The ``strip_geometry_for_records`` logic was relocated from a
    model_validator on the PG driver config to the sidecar resolver.
    Pin the new home of the behaviour.
    """
    cfg = ItemsPostgresqlDriverConfig()
    resolved = _effective_sidecars(
        cfg, catalog_id="cat", collection_id="col",
        collection_type="RECORDS",
    )
    types = [s.sidecar_type for s in resolved]
    assert "geometries" not in types
    assert "attributes" in types


def test_resolver_keeps_geometry_for_vector_default():
    cfg = ItemsPostgresqlDriverConfig()
    resolved = _effective_sidecars(
        cfg, catalog_id="cat", collection_id="col",
    )
    types = [s.sidecar_type for s in resolved]
    assert "geometries" in types
    assert "attributes" in types


def test_resolver_drops_explicit_geometry_for_records():
    """Caller-explicit geometry sidecar is also dropped at RECORDS scope."""
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )

    cfg = ItemsPostgresqlDriverConfig(
        sidecars=[GeometriesSidecarConfig(sidecar_type="geometries")],
    )
    resolved = _effective_sidecars(
        cfg, catalog_id="cat", collection_id="col",
        collection_type="RECORDS",
    )
    types = [s.sidecar_type for s in resolved]
    assert "geometries" not in types


@pytest.mark.parametrize("kind_str,expected_has_geometries", [
    ("VECTOR",  True),
    ("RECORDS", False),
])
def test_resolver_collection_type_round_trip(kind_str: str, expected_has_geometries: bool):
    """End-to-end: ``CollectionInfo.kind.value`` is what the resolver
    expects, so the wire shape round-trips without a string conversion.
    """
    ct = CollectionInfo(kind=CollectionKind(kind_str))
    cfg = ItemsPostgresqlDriverConfig()
    resolved = _effective_sidecars(
        cfg, catalog_id="cat", collection_id="col",
        collection_type=ct.kind.value,
    )
    types = [s.sidecar_type for s in resolved]
    assert ("geometries" in types) is expected_has_geometries


# ---------------------------------------------------------------------------
# RFC #2550: ``allow_geometry`` capability override (Issue #2645)
# ---------------------------------------------------------------------------


def test_collection_info_allow_geometry_default_is_none():
    assert CollectionInfo().allow_geometry is None


def test_records_allow_geometry_true_injects_geometry_by_default():
    """A default-body RECORDS collection with ``allow_geometry=True`` gets
    the geometries sidecar injected, same as VECTOR.
    """
    cfg = ItemsPostgresqlDriverConfig()
    resolved = _effective_sidecars(
        cfg, catalog_id="cat", collection_id="col",
        collection_type="RECORDS",
        context={"allow_geometry": True},
    )
    types = [s.sidecar_type for s in resolved]
    assert "geometries" in types
    assert "attributes" in types


def test_records_allow_geometry_true_honours_explicit_geometry_sidecar():
    """A RECORDS collection with an explicit geometries sidecar AND
    ``allow_geometry=True`` keeps the explicit config instead of stripping
    it (the RECORDS-strip in ``_effective_sidecars`` step 1 only applies
    when ``allow_geometry`` is not ``True``).
    """
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )

    cfg = ItemsPostgresqlDriverConfig(
        sidecars=[GeometriesSidecarConfig(sidecar_type="geometries")],
    )
    resolved = _effective_sidecars(
        cfg, catalog_id="cat", collection_id="col",
        collection_type="RECORDS",
        context={"allow_geometry": True},
    )
    types = [s.sidecar_type for s in resolved]
    assert "geometries" in types


def test_vector_allow_geometry_false_drops_default_geometry():
    """A default-body VECTOR collection with ``allow_geometry=False``
    never gets a geometries sidecar injected, even though ``kind`` alone
    would default to VECTOR → geometry.
    """
    cfg = ItemsPostgresqlDriverConfig()
    resolved = _effective_sidecars(
        cfg, catalog_id="cat", collection_id="col",
        collection_type="VECTOR",
        context={"allow_geometry": False},
    )
    types = [s.sidecar_type for s in resolved]
    assert "geometries" not in types
    assert "attributes" in types


@pytest.mark.parametrize("collection_type", ["VECTOR", "RASTER"])
def test_allow_geometry_false_strips_materialized_geometry_for_any_kind(collection_type):
    """``allow_geometry=False`` forces geometry off even for a non-RECORDS
    collection whose *persisted* ``sidecars`` already carries an explicit
    geometry sidecar (the materialised state after ``ensure_storage``).

    Regression for the RFC #2550 "off regardless of kind" contract: the
    explicit-strip previously fired only for RECORDS, so ``False`` was a
    silent no-op on an already-materialised VECTOR/RASTER collection.
    """
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )

    cfg = ItemsPostgresqlDriverConfig(
        sidecars=[GeometriesSidecarConfig(sidecar_type="geometries")],
    )
    resolved = _effective_sidecars(
        cfg, catalog_id="cat", collection_id="col",
        collection_type=collection_type,
        context={"allow_geometry": False},
    )
    assert "geometries" not in [s.sidecar_type for s in resolved]


@pytest.mark.parametrize("collection_type", ["VECTOR", "RECORDS"])
def test_allow_geometry_none_is_byte_identical_to_kind_only_resolution(collection_type):
    """Hard constraint: ``allow_geometry=None`` (unset/default) must resolve
    identically to omitting the ``allow_geometry`` key from context
    entirely — the pre-existing kind-only behaviour.
    """
    cfg = ItemsPostgresqlDriverConfig()
    resolved_without_key = _effective_sidecars(
        cfg, catalog_id="cat", collection_id="col",
        collection_type=collection_type,
    )
    resolved_with_none = _effective_sidecars(
        cfg, catalog_id="cat", collection_id="col",
        collection_type=collection_type,
        context={"allow_geometry": None},
    )
    types_without = sorted(s.sidecar_type for s in resolved_without_key)
    types_with = sorted(s.sidecar_type for s in resolved_with_none)
    assert types_without == types_with
