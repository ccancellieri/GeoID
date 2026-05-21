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
    from dynastore.modules.db_config.plugin_config import PluginConfig

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
    from dynastore.modules.db_config.plugin_config import list_registered_configs

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
