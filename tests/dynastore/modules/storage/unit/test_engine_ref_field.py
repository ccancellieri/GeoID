"""Cycle F.2 — pin the ``engine_ref`` field + ``required_engine_class``
discriminator on driver configs.

Each concrete driver config carries:

- ``required_engine_class: ClassVar[str]`` — the platform engine kind
  this driver class consumes (matches the engine's ``engine_class``
  ClassVar; F.1 lists the four current engine kinds).
- ``engine_ref: Optional[str]`` field — defaults to
  ``required_engine_class`` (single-instance-per-kind in F.1; F.4
  enables operator-chosen ref names).
- A model-validator that defaults+validates ``engine_ref``.

These tests pin the contract for every concrete config in the F.1
compatibility table.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from dynastore.modules.elasticsearch.catalog_es_driver import (
    CatalogElasticsearchDriverConfig,
)
from dynastore.modules.elasticsearch.collection_es_driver import (
    CollectionElasticsearchDriverConfig,
)
from dynastore.modules.stac.drivers.postgresql import (
    CatalogStacPostgresqlDriverConfig,
    CollectionStacPostgresqlDriverConfig,
)
from dynastore.modules.storage.driver_config import (
    AssetElasticsearchDriverConfig,
    AssetPostgresqlDriverConfig,
    ItemsDuckdbDriverConfig,
    ItemsElasticsearchDriverConfig,
    ItemsElasticsearchPrivateDriverConfig,
    ItemsIcebergDriverConfig,
    ItemsPostgresqlDriverConfig,
)
from dynastore.modules.storage.drivers.catalog_postgresql import (
    CatalogPostgresqlDriverConfig,
)
from dynastore.modules.storage.drivers.collection_postgresql import (
    CollectionPostgresqlDriverConfig,
)
from dynastore.modules.storage.drivers.core_postgresql import (
    CatalogCorePostgresqlDriverConfig,
    CollectionCorePostgresqlDriverConfig,
)


# F.1 compatibility table — verbatim from binary-leaping-lightning.md.
# Plus the 4 PG sub-driver configs swept in by F.2-fixup (composition
# wrapper inner drivers — they surface in the configs API tree as their
# own PluginConfig classes, so they need the same engine binding).
ENGINE_COMPATIBILITY = [
    (ItemsPostgresqlDriverConfig, "postgresql_engine"),
    (CatalogPostgresqlDriverConfig, "postgresql_engine"),
    (CollectionPostgresqlDriverConfig, "postgresql_engine"),
    (AssetPostgresqlDriverConfig, "postgresql_engine"),
    (ItemsElasticsearchDriverConfig, "elasticsearch_engine"),
    (ItemsElasticsearchPrivateDriverConfig, "elasticsearch_engine"),
    (CatalogElasticsearchDriverConfig, "elasticsearch_engine"),
    (CollectionElasticsearchDriverConfig, "elasticsearch_engine"),
    (AssetElasticsearchDriverConfig, "elasticsearch_engine"),
    (ItemsDuckdbDriverConfig, "duckdb_engine"),
    (ItemsIcebergDriverConfig, "iceberg_engine"),
    # PG sub-drivers (F.2-fixup): inner composition drivers feeding the
    # PG-tier wrapper.  Same engine kind as their wrapper.
    (CollectionStacPostgresqlDriverConfig, "postgresql_engine"),
    (CatalogStacPostgresqlDriverConfig, "postgresql_engine"),
    (CollectionCorePostgresqlDriverConfig, "postgresql_engine"),
    (CatalogCorePostgresqlDriverConfig, "postgresql_engine"),
]


# ---------------------------------------------------------------------------
# required_engine_class declarations match the plan's compatibility table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls,expected_engine_class", ENGINE_COMPATIBILITY)
def test_concrete_driver_config_declares_required_engine_class(
    cls, expected_engine_class,
):
    """Every concrete driver config in the F.1 compatibility table must
    declare ``required_engine_class`` matching the plan."""
    assert cls.required_engine_class == expected_engine_class, (
        f"{cls.__name__} declares required_engine_class="
        f"{cls.required_engine_class!r}, expected "
        f"{expected_engine_class!r} per the F.1 compatibility table."
    )


# ---------------------------------------------------------------------------
# engine_ref defaults to required_engine_class
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls,expected_engine_class", ENGINE_COMPATIBILITY)
def test_engine_ref_defaults_to_required_engine_class(
    cls, expected_engine_class,
):
    """Default-constructed driver config has ``engine_ref`` defaulted to
    ``required_engine_class`` (the F.1 single-instance contract)."""
    cfg = cls()
    assert cfg.engine_ref == expected_engine_class, (
        f"{cls.__name__}() default engine_ref={cfg.engine_ref!r}, "
        f"expected {expected_engine_class!r} (the engine's class_key)."
    )


@pytest.mark.parametrize("cls,expected_engine_class", ENGINE_COMPATIBILITY)
def test_engine_ref_explicit_match_accepted(cls, expected_engine_class):
    """Explicitly setting ``engine_ref`` to the matching engine_class
    is accepted (operator-explicit equivalent of the default)."""
    cfg = cls(engine_ref=expected_engine_class)
    assert cfg.engine_ref == expected_engine_class


# ---------------------------------------------------------------------------
# engine_ref incompatibility rejected
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls,expected_engine_class", ENGINE_COMPATIBILITY)
def test_engine_ref_incompatible_value_rejected(cls, expected_engine_class):
    """An ``engine_ref`` that doesn't match ``required_engine_class``
    is rejected with a clear error.  In F.2 (single-instance-per-kind)
    the static check is identity; F.4 will rewrite it to a registry
    lookup."""
    bogus_ref = "nonsense_engine"
    if expected_engine_class == bogus_ref:
        bogus_ref = "another_engine"  # extremely defensive
    with pytest.raises(
        ValidationError,
        match=r"incompatible with required_engine_class",
    ):
        cls(engine_ref=bogus_ref)


# ---------------------------------------------------------------------------
# Cross-class ref rejection — pin one explicitly so a regression that
# accidentally accepts every ref doesn't slip through.
# ---------------------------------------------------------------------------


def test_pg_driver_rejects_es_engine_ref():
    """Sanity: a PG driver config rejects an ES engine_ref even if the
    ES engine is a real registered engine_class."""
    with pytest.raises(
        ValidationError,
        match=r"incompatible with required_engine_class='postgresql_engine'",
    ):
        ItemsPostgresqlDriverConfig(engine_ref="elasticsearch_engine")


def test_es_driver_rejects_duckdb_engine_ref():
    """Sanity: an ES driver config rejects a DuckDB engine_ref."""
    with pytest.raises(
        ValidationError,
        match=r"incompatible with required_engine_class='elasticsearch_engine'",
    ):
        ItemsElasticsearchDriverConfig(engine_ref="duckdb_engine")


# ---------------------------------------------------------------------------
# None / empty-string ⇒ defaulted by validator
# ---------------------------------------------------------------------------


def test_engine_ref_none_is_defaulted():
    cfg = ItemsPostgresqlDriverConfig(engine_ref=None)
    assert cfg.engine_ref == "postgresql_engine"


def test_engine_ref_empty_string_is_defaulted():
    cfg = ItemsPostgresqlDriverConfig(engine_ref="")
    assert cfg.engine_ref == "postgresql_engine"
