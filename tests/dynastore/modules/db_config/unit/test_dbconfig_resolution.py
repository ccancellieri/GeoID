"""DBConfig tunable resolution: valid env var → db_config.json → code default.

Background (#1581): ``DBConfig`` read every pool/timeout tunable as a bare
``int(os.getenv("DB_POOL_RECYCLE", "1800"))``. The default only applies when
the var is *unset*; a deploy that templates the var through ``iac.yml`` but
leaves it undefined passes the literal ``${DB_POOL_RECYCLE}`` into the
container, so ``int()`` raises ``ValueError`` at import → the gunicorn worker
dies → the Cloud Run startup probe fails the whole revision rollout. Observed
live deploying a new catalog service (un-fao/fao-aip-catalog#194):
``ValueError: invalid literal for int() with base 10: '${DB_POOL_RECYCLE}'``.

Durable fix (maintainer direction — "use the config, not env vars"): a
file-backed ``db_config.json`` layer (``instance.load_db_config``) supplies
tunables as JSON *values*, which are never shell-substituted and so can never
arrive as a literal ``${...}`` placeholder. ``_cfg_int`` / ``_cfg_str`` resolve
each tunable in the order valid-env → file → default, tolerating an
unsubstituted/empty/non-numeric source at every layer.
"""
from __future__ import annotations

import json
import logging

from dynastore.modules.db_config import instance
from dynastore.modules.db_config.db_config import _cfg_int, _cfg_str


# --------------------------------------------------------------------------- #
# _cfg_int — env layer                                                        #
# --------------------------------------------------------------------------- #
def test_env_valid_int_is_parsed(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "42")
    assert _cfg_int("DB_TEST_INT", 1800, file_values={}) == 42


def test_env_unset_falls_back_to_default(monkeypatch):
    monkeypatch.delenv("DB_TEST_INT", raising=False)
    assert _cfg_int("DB_TEST_INT", 1800, file_values={}) == 1800


def test_env_empty_string_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "")
    assert _cfg_int("DB_TEST_INT", 1800, file_values={}) == 1800


def test_env_whitespace_only_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "   ")
    assert _cfg_int("DB_TEST_INT", 1800, file_values={}) == 1800


def test_env_surrounding_whitespace_is_stripped(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "  60 ")
    assert _cfg_int("DB_TEST_INT", 30, file_values={}) == 60


def test_env_unsubstituted_placeholder_does_not_crash(monkeypatch):
    """The exact #1581 repro: an unsubstituted ${...} must yield the default,
    not raise ValueError."""
    monkeypatch.setenv("DB_POOL_RECYCLE", "${DB_POOL_RECYCLE}")
    assert _cfg_int("DB_POOL_RECYCLE", 1800, file_values={}) == 1800


def test_env_nonnumeric_junk_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "not-a-number")
    assert _cfg_int("DB_TEST_INT", 5, file_values={}) == 5


def test_invalid_env_warns_naming_the_var_and_default(monkeypatch, caplog):
    monkeypatch.setenv("DB_POOL_RECYCLE", "${DB_POOL_RECYCLE}")
    with caplog.at_level(logging.WARNING):
        _cfg_int("DB_POOL_RECYCLE", 1800, file_values={})
    assert any(
        "DB_POOL_RECYCLE" in rec.message and "1800" in rec.message
        for rec in caplog.records
    ), "expected a WARNING naming the env var and the default it fell back to"


def test_unset_and_empty_do_not_warn(monkeypatch, caplog):
    """Absent/empty is normal (the default is intended) — only genuinely
    invalid values are noisy."""
    monkeypatch.delenv("DB_TEST_INT", raising=False)
    with caplog.at_level(logging.WARNING):
        _cfg_int("DB_TEST_INT", 10, file_values={})
    monkeypatch.setenv("DB_TEST_INT", "")
    with caplog.at_level(logging.WARNING):
        _cfg_int("DB_TEST_INT", 10, file_values={})
    assert not caplog.records, "absent/empty env must not emit a WARNING"


# --------------------------------------------------------------------------- #
# _cfg_int — file layer + precedence                                          #
# --------------------------------------------------------------------------- #
def test_file_value_used_when_env_unset(monkeypatch):
    monkeypatch.delenv("DB_TEST_INT", raising=False)
    assert _cfg_int("DB_TEST_INT", 5, file_values={"DB_TEST_INT": "42"}) == 42


def test_file_native_int_value_is_accepted(monkeypatch):
    monkeypatch.delenv("DB_TEST_INT", raising=False)
    assert _cfg_int("DB_TEST_INT", 5, file_values={"DB_TEST_INT": 42}) == 42


def test_valid_env_wins_over_file(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "10")
    assert _cfg_int("DB_TEST_INT", 5, file_values={"DB_TEST_INT": "42"}) == 10


def test_invalid_env_falls_through_to_file(monkeypatch, caplog):
    """A leaked ${...} env must not shadow a valid file value."""
    monkeypatch.setenv("DB_TEST_INT", "${DB_TEST_INT}")
    with caplog.at_level(logging.WARNING):
        result = _cfg_int("DB_TEST_INT", 5, file_values={"DB_TEST_INT": "42"})
    assert result == 42
    assert any("env" in rec.message for rec in caplog.records), (
        "expected a WARNING that the invalid env source was ignored"
    )


def test_invalid_file_falls_back_to_default(monkeypatch, caplog):
    monkeypatch.delenv("DB_TEST_INT", raising=False)
    with caplog.at_level(logging.WARNING):
        result = _cfg_int("DB_TEST_INT", 7, file_values={"DB_TEST_INT": "nope"})
    assert result == 7
    assert any("db_config.json" in rec.message for rec in caplog.records)


def test_both_sources_invalid_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("DB_TEST_INT", "${X}")
    assert _cfg_int("DB_TEST_INT", 9, file_values={"DB_TEST_INT": "${Y}"}) == 9


# --------------------------------------------------------------------------- #
# _cfg_str                                                                    #
# --------------------------------------------------------------------------- #
def test_str_env_valid(monkeypatch):
    monkeypatch.setenv("DB_TEST_STR", "5s")
    assert _cfg_str("DB_TEST_STR", "30s", file_values={}) == "5s"


def test_str_env_unset_default(monkeypatch):
    monkeypatch.delenv("DB_TEST_STR", raising=False)
    assert _cfg_str("DB_TEST_STR", "30s", file_values={}) == "30s"


def test_str_env_empty_default(monkeypatch):
    monkeypatch.setenv("DB_TEST_STR", "  ")
    assert _cfg_str("DB_TEST_STR", "30s", file_values={}) == "30s"


def test_str_env_placeholder_falls_to_default(monkeypatch, caplog):
    """A DSN/timeout left as ${...} is a valid string but a broken config —
    it must be skipped, not used (closes the gap #1581's int-only guard left)."""
    monkeypatch.setenv("DATABASE_URL", "${DATABASE_URL}")
    with caplog.at_level(logging.WARNING):
        result = _cfg_str("DATABASE_URL", "postgresql://d/x", file_values={})
    assert result == "postgresql://d/x"
    assert any("DATABASE_URL" in rec.message for rec in caplog.records)


def test_str_file_used_when_env_unset(monkeypatch):
    monkeypatch.delenv("DB_TEST_STR", raising=False)
    assert _cfg_str("DB_TEST_STR", "30s", file_values={"DB_TEST_STR": "5s"}) == "5s"


def test_str_valid_env_wins_over_file(monkeypatch):
    monkeypatch.setenv("DB_TEST_STR", "1s")
    assert _cfg_str("DB_TEST_STR", "30s", file_values={"DB_TEST_STR": "5s"}) == "1s"


def test_str_placeholder_env_falls_through_to_file(monkeypatch):
    monkeypatch.setenv("DB_TEST_STR", "${DB_TEST_STR}")
    assert _cfg_str("DB_TEST_STR", "30s", file_values={"DB_TEST_STR": "5s"}) == "5s"


def test_str_unset_and_empty_do_not_warn(monkeypatch, caplog):
    monkeypatch.delenv("DB_TEST_STR", raising=False)
    with caplog.at_level(logging.WARNING):
        _cfg_str("DB_TEST_STR", "30s", file_values={})
    monkeypatch.setenv("DB_TEST_STR", "")
    with caplog.at_level(logging.WARNING):
        _cfg_str("DB_TEST_STR", "30s", file_values={})
    assert not caplog.records


# --------------------------------------------------------------------------- #
# instance.load_db_config                                                     #
# --------------------------------------------------------------------------- #
def test_load_db_config_missing_file_is_silent(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(instance, "DB_CONFIG_FILE", tmp_path / "db_config.json")
    with caplog.at_level(logging.WARNING):
        assert instance.load_db_config() == {}
    assert not caplog.records, "an absent db_config.json is normal — no WARN"


def test_load_db_config_valid_json(monkeypatch, tmp_path):
    f = tmp_path / "db_config.json"
    f.write_text(json.dumps({"DB_POOL_RECYCLE": 900, "DATABASE_URL": "postgresql://x"}))
    monkeypatch.setattr(instance, "DB_CONFIG_FILE", f)
    loaded = instance.load_db_config()
    assert loaded["DB_POOL_RECYCLE"] == 900
    assert loaded["DATABASE_URL"] == "postgresql://x"


def test_load_db_config_malformed_json_warns_and_empty(monkeypatch, tmp_path, caplog):
    f = tmp_path / "db_config.json"
    f.write_text("{not valid json")
    monkeypatch.setattr(instance, "DB_CONFIG_FILE", f)
    with caplog.at_level(logging.WARNING):
        assert instance.load_db_config() == {}
    assert any("unreadable" in rec.message for rec in caplog.records)


def test_load_db_config_non_object_warns_and_empty(monkeypatch, tmp_path, caplog):
    f = tmp_path / "db_config.json"
    f.write_text(json.dumps([1, 2, 3]))
    monkeypatch.setattr(instance, "DB_CONFIG_FILE", f)
    with caplog.at_level(logging.WARNING):
        assert instance.load_db_config() == {}
    assert any("not a JSON object" in rec.message for rec in caplog.records)


# --------------------------------------------------------------------------- #
# Source-level pins                                                           #
# --------------------------------------------------------------------------- #
def _db_config_source() -> str:
    import pathlib

    here = pathlib.Path(__file__).resolve()
    repo_root = here.parents[5]
    return (
        repo_root / "packages/core/src/dynastore/modules/db_config/db_config.py"
    ).read_text(encoding="utf-8")


def test_no_bare_int_getenv_remains_in_db_config():
    """The crash-prone ``int(os.getenv(...))`` pattern must not reappear — every
    numeric tunable goes through ``_cfg_int`` so a mis-templated value can never
    crash startup (#1581)."""
    text = _db_config_source()
    assert "int(os.getenv(" not in text, (
        "db_config.py reintroduced a bare int(os.getenv(...)); use _cfg_int so "
        "an unsubstituted ${...} / empty env cannot crash startup (#1581)."
    )


def test_string_tunables_route_through_cfg_str():
    """The free-form string reads must go through ``_cfg_str`` (not bare
    ``os.getenv``) so an unsubstituted ${...} can't silently mis-configure the
    connection."""
    text = _db_config_source()
    for name in ("DATABASE_URL", "DB_LOCK_TIMEOUT", "DB_IDLE_IN_TRANSACTION_TIMEOUT"):
        assert f'_cfg_str("{name}"' in text, f"{name} must resolve via _cfg_str"
        assert f'os.getenv("{name}"' not in text, (
            f"{name} still read via bare os.getenv — route it through _cfg_str"
        )
