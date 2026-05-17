"""Cycle F.1 — pin the engines layer at ``platform.engines.*``.

Engines are platform-only connection-and-pool resources, sysadmin-locked
by default.  Drivers reference them via ``engine_ref`` (Cycle F.2).

These tests pin:
- Each concrete engine class registers in the TypedModelRegistry
  (``list_registered_configs``) under its ``engine_class`` snake_case key.
- Each engine declares the right ``_address``, ``_visibility``, and
  ``engine_class`` discriminator.
- ``EngineLifecycleConfig`` defaults are global / non-evicting.
- ``policy="ttl_lru"`` requires ``ttl_seconds`` (model-validator).
- ``Secret``-typed connection fields round-trip through Pydantic
  validate / dump cycles (mask in default mode).
- The abstract ``EngineConfig`` base does NOT register — only its
  concrete subclasses.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from dynastore.modules.db_config.engine_config import (
    DuckdbEngineConfig,
    ElasticsearchEngineConfig,
    EngineConfig,
    EngineLifecycleConfig,
    IcebergEngineConfig,
    PostgresqlEngineConfig,
)
from dynastore.modules.db_config.plugin_config import list_registered_configs
from dynastore.tools.secrets import Secret


# ---------------------------------------------------------------------------
# EngineLifecycleConfig
# ---------------------------------------------------------------------------


def test_lifecycle_default_is_global_immutable():
    """Default policy is ``global`` (lazy singleton, never evicted) +
    immutable (cannot be reconfigured at runtime PATCH path)."""
    cfg = EngineLifecycleConfig()
    assert cfg.policy == "global"
    assert cfg.immutable is True
    assert cfg.ttl_seconds is None
    assert cfg.max_parallel is None


def test_lifecycle_ttl_lru_requires_ttl_seconds():
    """``policy='ttl_lru'`` without ``ttl_seconds`` is a config error —
    operators must pick a value matching the engine's idle-cost
    profile (no implicit default)."""
    with pytest.raises(ValidationError, match=r"ttl_lru.*ttl_seconds"):
        EngineLifecycleConfig(policy="ttl_lru")


def test_lifecycle_ttl_lru_accepts_ttl_seconds():
    cfg = EngineLifecycleConfig(policy="ttl_lru", ttl_seconds=600)
    assert cfg.policy == "ttl_lru"
    assert cfg.ttl_seconds == 600


def test_lifecycle_ttl_seconds_must_be_positive():
    with pytest.raises(ValidationError):
        EngineLifecycleConfig(policy="ttl_lru", ttl_seconds=0)


def test_lifecycle_max_parallel_must_be_positive():
    with pytest.raises(ValidationError):
        EngineLifecycleConfig(max_parallel=0)


# ---------------------------------------------------------------------------
# Abstract EngineConfig base
# ---------------------------------------------------------------------------


def test_engine_config_is_abstract_base():
    """``EngineConfig`` itself is abstract — concrete subclasses (one
    per engine kind) carry the discriminator + connection fields."""
    assert EngineConfig.is_abstract_base is True


def test_engine_config_marked_abstract_in_registry():
    """``EngineConfig`` is registered (the registry surfaces every
    PluginConfig subclass) but flagged ``is_abstract_base=True`` so
    composer filters / publishers can hide it.  Same pattern as
    ``DriverPluginConfig`` / ``CollectionDriverConfig``."""
    configs = list_registered_configs()
    assert "engine_config" in configs
    assert configs["engine_config"] is EngineConfig
    assert EngineConfig.__dict__.get("is_abstract_base", False) is True


def test_concrete_engine_subclass_must_override_engine_class():
    """A concrete ``EngineConfig`` subclass that forgets to override
    ``engine_class`` must fail at class-creation time.  Without the
    guard, the engine would silently inherit the empty-string default
    and be invisible to driver-side ``required_engine_class`` matching
    (F.2)."""
    from typing import ClassVar, Tuple

    with pytest.raises(TypeError, match=r"does not declare ``engine_class``"):
        class _BadEngineConfig(EngineConfig):  # noqa: F841
            # Forgot to set engine_class — should fail at __init_subclass__.
            _address: ClassVar[Tuple[str, ...]] = ("platform", "engines")


# ---------------------------------------------------------------------------
# Concrete engine classes — discriminators + addresses + visibility
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cls,expected_engine_class,expected_class_key",
    [
        (PostgresqlEngineConfig, "postgresql_engine", "postgresql_engine_config"),
        (ElasticsearchEngineConfig, "elasticsearch_engine", "elasticsearch_engine_config"),
        (DuckdbEngineConfig, "duckdb_engine", "duckdb_engine_config"),
        (IcebergEngineConfig, "iceberg_engine", "iceberg_engine_config"),
    ],
)
def test_engine_classes_declare_engine_class_discriminator(
    cls, expected_engine_class, expected_class_key,
):
    """``engine_class`` ClassVar pins the engine kind for runtime
    dispatch.  ``class_key()`` is the snake_case wire identity (used
    as the registry / DB row key)."""
    assert cls.engine_class == expected_engine_class
    assert cls.class_key() == expected_class_key


@pytest.mark.parametrize(
    "cls",
    [PostgresqlEngineConfig, ElasticsearchEngineConfig, DuckdbEngineConfig, IcebergEngineConfig],
)
def test_engine_classes_share_platform_engines_address(cls):
    """All four engines live at ``("platform", "engines")``; the
    composer separates them by ``class_key()``."""
    assert cls._address == ("platform", "engines")
    assert cls._visibility == "platform"


@pytest.mark.parametrize(
    "cls",
    [PostgresqlEngineConfig, ElasticsearchEngineConfig, DuckdbEngineConfig, IcebergEngineConfig],
)
def test_engine_classes_register_in_typed_model_registry(cls):
    """Each concrete engine class must appear in
    ``list_registered_configs()`` so ``/configs/registry`` and the
    configs API tree surface it."""
    configs = list_registered_configs()
    key = cls.class_key()
    assert key in configs, (
        f"{cls.__name__} did not register — module-load side-effect import "
        f"in db_config_service.py likely missing or broken."
    )
    assert configs[key] is cls


# ---------------------------------------------------------------------------
# Concrete engine classes — defaults + lifecycle inheritance
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cls",
    [PostgresqlEngineConfig, ElasticsearchEngineConfig, DuckdbEngineConfig, IcebergEngineConfig],
)
def test_engine_classes_default_to_enabled_and_global_lifecycle(cls):
    """Every engine ships with ``enabled=True`` + default
    ``EngineLifecycleConfig`` (policy=global)."""
    cfg = cls()
    assert cfg.enabled is True
    assert isinstance(cfg.lifecycle, EngineLifecycleConfig)
    assert cfg.lifecycle.policy == "global"


def test_postgresql_engine_pool_size_bounds():
    PostgresqlEngineConfig(pool_size=200)  # max
    with pytest.raises(ValidationError):
        PostgresqlEngineConfig(pool_size=0)
    with pytest.raises(ValidationError):
        PostgresqlEngineConfig(pool_size=201)


def test_elasticsearch_engine_request_timeout_default():
    cfg = ElasticsearchEngineConfig()
    assert cfg.request_timeout_sec == 30


def test_duckdb_engine_pool_size_threads_bounds():
    DuckdbEngineConfig(pool_size=64, threads=64)
    with pytest.raises(ValidationError):
        DuckdbEngineConfig(pool_size=0)
    with pytest.raises(ValidationError):
        DuckdbEngineConfig(threads=65)


def test_iceberg_engine_catalog_properties_default_empty():
    cfg = IcebergEngineConfig()
    assert cfg.catalog_properties == {}


# ---------------------------------------------------------------------------
# Secret round-trip
# ---------------------------------------------------------------------------


def test_postgresql_engine_accepts_plaintext_connection_url():
    """Operator PUTs a plaintext URL; Pydantic wraps it into a Secret."""
    cfg = PostgresqlEngineConfig(connection_url="postgresql://u:p@h/db")
    assert isinstance(cfg.connection_url, Secret)
    assert cfg.connection_url.reveal() == "postgresql://u:p@h/db"


def test_elasticsearch_engine_secret_masks_in_default_dump():
    """``model_dump()`` without context masks Secret fields — ensures
    the admin UI / logs never expose plaintext."""
    cfg = ElasticsearchEngineConfig(
        cluster_url="https://es.example.com",
        api_key="super-secret-key",
    )
    dumped = cfg.model_dump()
    assert dumped["cluster_url"] == "***"
    assert dumped["api_key"] == "***"


def test_iceberg_engine_catalog_properties_masks_secret_values():
    """Each value in the free-form dict is a Secret — masked in
    response dumps, encrypted at rest."""
    cfg = IcebergEngineConfig(
        catalog_properties={"aws_access_key": "AKIA..."},
    )
    dumped = cfg.model_dump()
    assert dumped["catalog_properties"]["aws_access_key"] == "***"
