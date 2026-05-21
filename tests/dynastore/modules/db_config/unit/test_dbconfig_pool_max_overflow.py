"""Regression test: ``DBConfig.pool_max_overflow`` must never go negative.

Background (dynastore #320): both engine factories computed SQLAlchemy's
``max_overflow`` inline as ``pool_max_size - pool_min_size``. SQLAlchemy
treats a *negative* ``max_overflow`` as **unbounded** overflow. So a
misconfigured environment where ``DB_POOL_MIN_SIZE`` exceeds
``DB_POOL_MAX_SIZE`` (e.g. the incident's own recommended mitigation of
bumping the min while the max stayed small) silently flipped the pool from
"too small" to "unlimited" — an operator typo became a connection/memory
exhaustion outage.

Fix: a single clamped ``pool_max_overflow`` property that floors the gap at
0 and warns when ``max < min``, used by both engine factories.
"""
from __future__ import annotations

from dynastore.modules.db_config.db_config import DBConfig


def _cfg(min_size: int, max_size: int) -> DBConfig:
    cfg = DBConfig()
    # Instance attributes shadow the env-driven class attributes.
    cfg.pool_min_size = min_size
    cfg.pool_max_size = max_size
    return cfg


def test_pool_max_overflow_normal_gap():
    assert _cfg(min_size=5, max_size=100).pool_max_overflow == 95


def test_pool_max_overflow_equal_is_zero():
    assert _cfg(min_size=10, max_size=10).pool_max_overflow == 0


def test_pool_max_overflow_clamped_to_zero_when_max_below_min():
    # The footgun: max < min must NOT produce a negative value (SQLAlchemy
    # would read negative as "unlimited overflow").
    assert _cfg(min_size=15, max_size=5).pool_max_overflow == 0


def test_pool_max_overflow_warns_on_misconfiguration(caplog):
    import logging

    with caplog.at_level(logging.WARNING):
        _cfg(min_size=15, max_size=5).pool_max_overflow
    assert any(
        "DB_POOL_MAX_SIZE" in rec.message and "DB_POOL_MIN_SIZE" in rec.message
        for rec in caplog.records
    ), "expected a WARNING naming both env vars when max < min"


def test_engine_factories_use_clamped_property_not_inline_subtraction():
    """Source-level pin: neither engine factory may compute max_overflow as a
    raw subtraction again — that bypasses the negative clamp."""
    import pathlib

    here = pathlib.Path(__file__).resolve()
    repo_root = here.parents[5]
    for rel in (
        "packages/core/src/dynastore/modules/db/db_service.py",
        "packages/core/src/dynastore/modules/datastore/datastore_service.py",
    ):
        text = (repo_root / rel).read_text(encoding="utf-8")
        assert "pool_max_size - " not in text and "pool_max_size-" not in text, (
            f"{rel} still subtracts inline; use db_config.pool_max_overflow "
            "so the negative-overflow clamp (#320) cannot be bypassed."
        )
