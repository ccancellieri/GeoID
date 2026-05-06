"""Cycle F.4a — pin the engine registry resolver.

The engine registry maps ``engine_ref`` strings to the engine_class
discriminator so the F.2 driver-config validator (and F.4c's ref-keyed
storage layer) can validate ``engine_ref`` values against the four
F.1 engine kinds.

These tests pin:
- ``list_registered_engines()`` returns every concrete ``EngineConfig``
  subclass keyed by ``class_key()``; the abstract base is filtered out.
- ``resolve_engine_class()`` resolves both the ``class_key`` and the
  ``engine_class`` discriminator forms.
- Unknown / empty refs return ``None`` (the negative case callers
  must handle as a config error).
"""
from __future__ import annotations

import pytest

from dynastore.modules.db_config.engine_config import (
    DuckdbEngineConfig,
    ElasticsearchEngineConfig,
    EngineConfig,
    IcebergEngineConfig,
    PostgresqlEngineConfig,
)
from dynastore.modules.db_config.engine_registry import (
    list_registered_engines,
    resolve_engine_class,
)


# ---------------------------------------------------------------------------
# list_registered_engines()
# ---------------------------------------------------------------------------


def test_list_registered_engines_includes_all_four_concrete_classes():
    """The four F.1 engine kinds (PG / ES / DuckDB / Iceberg) all appear."""
    engines = list_registered_engines()
    expected = {
        "postgresql_engine_config": PostgresqlEngineConfig,
        "elasticsearch_engine_config": ElasticsearchEngineConfig,
        "duckdb_engine_config": DuckdbEngineConfig,
        "iceberg_engine_config": IcebergEngineConfig,
    }
    for key, cls in expected.items():
        assert key in engines, (
            f"{cls.__name__} missing from list_registered_engines() — "
            f"the F.1 side-effect import in db_config_service.py may be broken."
        )
        assert engines[key] is cls


def test_list_registered_engines_filters_abstract_base():
    """``EngineConfig`` is abstract — it must not appear in the registry view."""
    engines = list_registered_engines()
    assert "engine_config" not in engines
    assert EngineConfig not in engines.values()


def test_list_registered_engines_keys_are_class_key_form():
    """Keys are snake_case of the class name (the wire identity)."""
    engines = list_registered_engines()
    for key, cls in engines.items():
        assert key == cls.class_key(), (
            f"{cls.__name__} registered under {key!r} but class_key()="
            f"{cls.class_key()!r}"
        )


# ---------------------------------------------------------------------------
# resolve_engine_class() — direct class_key lookup
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ref,expected_engine_class",
    [
        ("postgresql_engine_config", "postgresql_engine"),
        ("elasticsearch_engine_config", "elasticsearch_engine"),
        ("duckdb_engine_config", "duckdb_engine"),
        ("iceberg_engine_config", "iceberg_engine"),
    ],
)
def test_resolve_engine_class_by_class_key(ref, expected_engine_class):
    """Direct class_key lookup resolves to the engine's discriminator."""
    assert resolve_engine_class(ref) == expected_engine_class


# ---------------------------------------------------------------------------
# resolve_engine_class() — engine_class discriminator lookup (fallback)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "engine_class",
    ["postgresql_engine", "elasticsearch_engine", "duckdb_engine", "iceberg_engine"],
)
def test_resolve_engine_class_by_discriminator(engine_class):
    """Fallback lookup against ``cls.engine_class`` covers the form
    F.2's driver-config validator uses for ``required_engine_class``
    matching."""
    assert resolve_engine_class(engine_class) == engine_class


# ---------------------------------------------------------------------------
# resolve_engine_class() — negative cases
# ---------------------------------------------------------------------------


def test_resolve_engine_class_unknown_ref_returns_none():
    """Unknown ref must surface as ``None`` so callers can raise a 422 /
    config error rather than silently default to a registered engine."""
    assert resolve_engine_class("nonsense_engine") is None
    assert resolve_engine_class("pg_main") is None  # F.4c will register operator-chosen refs


def test_resolve_engine_class_empty_ref_returns_none():
    """Empty / falsy refs are treated as unknown."""
    assert resolve_engine_class("") is None
