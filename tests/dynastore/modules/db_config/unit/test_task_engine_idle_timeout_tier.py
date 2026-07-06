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

"""#2837 regression fix: task-side engines get their own idle-in-transaction
budget instead of inheriting the 10s serving-tier default.

Background: #2837 gave ad-hoc, NullPool-backed task/job engines
(``task_engine_connect_args``) the same lock-safety ``server_settings`` as
the shared serving engine, including ``idle_in_transaction_session_timeout``.
Those task engines routinely interleave a PG transaction with slow
secondary-store I/O (ES bulk writes during ``elasticsearch_indexer`` /
``storage_drain``), so the 10s serving budget killed them mid-write with
``asyncpg.exceptions.ConnectionDoesNotExistError``. This pins the fix: task
engines resolve their idle budget from
``DBConfig.task_idle_in_transaction_session_timeout`` (default 300s), while
the shared serving engine keeps its own ``idle_in_transaction_session_timeout``
(default 10s) untouched.
"""
from __future__ import annotations

from dynastore.modules.db_config.db_config import DBConfig, _cfg_str
from dynastore.modules.db_config.db_timeout_config import task_engine_connect_args


# --------------------------------------------------------------------------- #
# DBConfig field                                                              #
# --------------------------------------------------------------------------- #
def test_default_task_idle_timeout_is_300s():
    cfg = DBConfig()
    assert cfg.task_idle_in_transaction_session_timeout == "300s"


def test_default_serving_idle_timeout_is_unchanged_10s():
    """The shared serving-tier default must stay at 10s (#2832) — only the
    task tier changes here."""
    cfg = DBConfig()
    assert cfg.idle_in_transaction_session_timeout == "10s"


def test_env_sets_task_idle_timeout(monkeypatch):
    monkeypatch.setenv("DB_TASK_IDLE_IN_TRANSACTION_TIMEOUT", "120s")
    result = _cfg_str(
        "DB_TASK_IDLE_IN_TRANSACTION_TIMEOUT", "300s", file_values={}
    )
    assert result == "120s"


def test_task_and_serving_idle_timeout_are_independent_env_vars(monkeypatch):
    monkeypatch.setenv("DB_IDLE_IN_TRANSACTION_TIMEOUT", "5s")
    monkeypatch.delenv("DB_TASK_IDLE_IN_TRANSACTION_TIMEOUT", raising=False)
    serving = _cfg_str("DB_IDLE_IN_TRANSACTION_TIMEOUT", "10s", file_values={})
    task = _cfg_str(
        "DB_TASK_IDLE_IN_TRANSACTION_TIMEOUT", "300s", file_values={}
    )
    assert serving == "5s"
    assert task == "300s"


# --------------------------------------------------------------------------- #
# task_engine_connect_args uses the task-tier value                          #
# --------------------------------------------------------------------------- #
class _FakeDBConfig:
    lock_timeout = "5s"
    statement_timeout = "0"
    idle_in_transaction_session_timeout = "10s"
    task_idle_in_transaction_session_timeout = "300s"
    # Task engines now carry the shared engine's TCP keepalives too (#3057),
    # built via the mode-aware build_connection_server_settings() helper.
    tcp_keepalives_idle = 300
    tcp_keepalives_interval = 30
    tcp_keepalives_count = 5
    # Default connection mode — full server_settings ride the startup packet
    # (a transaction pooler would send application_name only, #3081).
    db_pooling_mode = "direct"


def test_task_engine_connect_args_uses_task_tier_idle_value():
    connect_args = task_engine_connect_args(_FakeDBConfig)
    settings = connect_args["server_settings"]
    assert settings["idle_in_transaction_session_timeout"] == "300s"
    assert settings["lock_timeout"] == "5s"


def test_task_engine_connect_args_does_not_leak_serving_idle_value():
    fake = _FakeDBConfig()
    fake.idle_in_transaction_session_timeout = "10s"
    fake.task_idle_in_transaction_session_timeout = "77s"
    connect_args = task_engine_connect_args(fake)
    assert connect_args["server_settings"]["idle_in_transaction_session_timeout"] == "77s"


def test_task_engine_connect_args_with_real_dbconfig_class():
    """Sanity check against the real DBConfig class (as production call sites
    invoke it: ``task_engine_connect_args(DBConfig)``)."""
    connect_args = task_engine_connect_args(DBConfig)
    settings = connect_args["server_settings"]
    assert settings["idle_in_transaction_session_timeout"] == (
        DBConfig.task_idle_in_transaction_session_timeout
    )
    assert settings["idle_in_transaction_session_timeout"] != (
        DBConfig.idle_in_transaction_session_timeout
    )


# --------------------------------------------------------------------------- #
# Serving path (db_service.py) is untouched                                   #
# --------------------------------------------------------------------------- #
def _db_service_source() -> str:
    import pathlib
    here = pathlib.Path(__file__).resolve()
    repo_root = here.parents[5]
    return (
        repo_root / "packages/core/src/dynastore/modules/db/db_service.py"
    ).read_text(encoding="utf-8")


def test_db_service_still_uses_shared_idle_timeout_not_task_tier():
    """The shared engine must keep resolving idle_in_transaction_session_timeout
    from resolve_timeout_settings()/DBConfig.idle_in_transaction_session_timeout —
    it must NOT be switched onto the task-tier field."""
    source = _db_service_source()
    assert "task_idle_in_transaction_session_timeout" not in source, (
        "db_service.py (the shared serving engine) must not reference the "
        "task-tier idle timeout — only ad-hoc task engines use it."
    )
    # Post-#3081 the shared engine builds its server_settings via the
    # pooler-aware helper (which still merges lock_safety_server_settings).
    assert "build_connection_server_settings(" in source
