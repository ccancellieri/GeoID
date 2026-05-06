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
def test_engine_ref_unknown_string_accepted(cls, expected_engine_class):
    """Cycle F.4c.3 widens the validator: an ``engine_ref`` that is NOT
    in the engine registry (operator-chosen multi-instance name like
    ``pg_main``) is accepted at validate-time after a snake_case format
    check.  Runtime / PATCH-handler enforces existence at platform.engines.{ref}.

    Pre-F.4c.3 this exact case raised ``ValueError(... incompatible with
    required_engine_class)`` even when the ref simply hadn't been
    declared yet — that broke the multi-instance use case.
    """
    operator_ref = "pg_main"  # snake_case, not in registry
    if expected_engine_class == operator_ref:
        operator_ref = "main_instance"  # extremely defensive
    cfg = cls(engine_ref=operator_ref)
    assert cfg.engine_ref == operator_ref


@pytest.mark.parametrize("cls,expected_engine_class", ENGINE_COMPATIBILITY)
def test_engine_ref_invalid_format_rejected(cls, expected_engine_class):
    """Operator-chosen refs that don't match the snake_case format are
    rejected at validate-time so PATCH bodies surface the typo clearly
    rather than leaking through to a 503-on-runtime."""
    with pytest.raises(
        ValidationError,
        match=r"snake_case format",
    ):
        cls(engine_ref="BadRef-with-Caps!")


# ---------------------------------------------------------------------------
# Cross-class ref rejection — pin one explicitly so a regression that
# accidentally accepts every ref doesn't slip through.
# ---------------------------------------------------------------------------


def test_pg_driver_rejects_es_engine_ref():
    """A PG driver config rejects ``engine_ref="elasticsearch_engine"``
    because the registry resolves the ref to the ES engine_class."""
    with pytest.raises(
        ValidationError,
        match=r"engine_class='elasticsearch_engine', incompatible with "
              r"required_engine_class='postgresql_engine'",
    ):
        ItemsPostgresqlDriverConfig(engine_ref="elasticsearch_engine")


def test_es_driver_rejects_duckdb_engine_ref():
    """An ES driver config rejects ``engine_ref="duckdb_engine"`` because
    the registry resolves the ref to the DuckDB engine_class."""
    with pytest.raises(
        ValidationError,
        match=r"engine_class='duckdb_engine', incompatible with "
              r"required_engine_class='elasticsearch_engine'",
    ):
        ItemsElasticsearchDriverConfig(engine_ref="duckdb_engine")


def test_pg_driver_rejects_pg_engine_config_class_key_for_other_kind():
    """Registry's path-1 lookup also catches engine class_key-shaped refs
    pointing at the wrong kind."""
    with pytest.raises(
        ValidationError,
        match=r"engine_class='elasticsearch_engine', incompatible",
    ):
        ItemsPostgresqlDriverConfig(engine_ref="elasticsearch_engine_config")


# ---------------------------------------------------------------------------
# None / empty-string ⇒ defaulted by validator
# ---------------------------------------------------------------------------


def test_engine_ref_none_is_defaulted():
    cfg = ItemsPostgresqlDriverConfig(engine_ref=None)
    assert cfg.engine_ref == "postgresql_engine"


def test_engine_ref_empty_string_is_defaulted():
    cfg = ItemsPostgresqlDriverConfig(engine_ref="")
    assert cfg.engine_ref == "postgresql_engine"
