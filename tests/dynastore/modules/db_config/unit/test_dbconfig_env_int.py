"""Regression test: DBConfig numeric env reads must tolerate mis-templated vars.

Background (#1581): ``DBConfig`` read every pool/timeout tunable as a bare
``int(os.getenv("DB_POOL_RECYCLE", "1800"))``. The default only applies when
the var is *unset*; a deploy that templates the var through ``iac.yml`` but
leaves it undefined passes the literal ``${DB_POOL_RECYCLE}`` into the
container, so ``int()`` raises ``ValueError`` at import → the gunicorn worker
dies → the Cloud Run startup probe fails the whole revision rollout. Observed
live deploying a new catalog service (un-fao/fao-aip-catalog#194):
``ValueError: invalid literal for int() with base 10: '${DB_POOL_RECYCLE}'``.

Fix: a tolerant ``_env_int`` helper that treats empty / non-numeric values
(including unsubstituted ``${...}`` placeholders) as the default with a WARN,
applied to all nine numeric env reads.
"""
from __future__ import annotations

import logging

from dynastore.modules.db_config.db_config import _env_int


def test_valid_int_is_parsed(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "42")
    assert _env_int("DB_TEST_INT", 1800) == 42


def test_unset_falls_back_to_default(monkeypatch):
    monkeypatch.delenv("DB_TEST_INT", raising=False)
    assert _env_int("DB_TEST_INT", 1800) == 1800


def test_empty_string_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "")
    assert _env_int("DB_TEST_INT", 1800) == 1800


def test_whitespace_only_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "   ")
    assert _env_int("DB_TEST_INT", 1800) == 1800


def test_surrounding_whitespace_is_stripped(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "  60 ")
    assert _env_int("DB_TEST_INT", 30) == 60


def test_unsubstituted_placeholder_does_not_crash(monkeypatch):
    """The exact #1581 repro: an unsubstituted ${...} must yield the default,
    not raise ValueError."""
    monkeypatch.setenv("DB_POOL_RECYCLE", "${DB_POOL_RECYCLE}")
    assert _env_int("DB_POOL_RECYCLE", 1800) == 1800


def test_nonnumeric_junk_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "not-a-number")
    assert _env_int("DB_TEST_INT", 5) == 5


def test_invalid_value_warns_naming_the_var(monkeypatch, caplog):
    monkeypatch.setenv("DB_POOL_RECYCLE", "${DB_POOL_RECYCLE}")
    with caplog.at_level(logging.WARNING):
        _env_int("DB_POOL_RECYCLE", 1800)
    assert any(
        "DB_POOL_RECYCLE" in rec.message and "1800" in rec.message
        for rec in caplog.records
    ), "expected a WARNING naming the env var and the default it fell back to"


def test_unset_and_empty_do_not_warn(monkeypatch, caplog):
    """Absent/empty is normal (the default is intended) — only genuinely
    invalid values are noisy."""
    monkeypatch.delenv("DB_TEST_INT", raising=False)
    with caplog.at_level(logging.WARNING):
        _env_int("DB_TEST_INT", 10)
    monkeypatch.setenv("DB_TEST_INT", "")
    with caplog.at_level(logging.WARNING):
        _env_int("DB_TEST_INT", 10)
    assert not caplog.records, "absent/empty env must not emit a WARNING"


def test_no_bare_int_getenv_remains_in_db_config():
    """Source-level pin: the crash-prone ``int(os.getenv(...))`` pattern must
    not reappear in db_config.py — every numeric env read goes through
    ``_env_int`` so a mis-templated value can never crash startup (#1581)."""
    import pathlib

    here = pathlib.Path(__file__).resolve()
    repo_root = here.parents[5]
    text = (
        repo_root
        / "packages/core/src/dynastore/modules/db_config/db_config.py"
    ).read_text(encoding="utf-8")
    assert "int(os.getenv(" not in text, (
        "db_config.py reintroduced a bare int(os.getenv(...)); use _env_int "
        "so an unsubstituted ${...} / empty env cannot crash startup (#1581)."
    )
