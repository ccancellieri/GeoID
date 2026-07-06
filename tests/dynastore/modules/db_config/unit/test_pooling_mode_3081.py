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

"""#3081 — AlloyDB Managed Connection Pooling (PgBouncer, :6432) support.

A transaction-mode pooler forwards only an allowlist of startup parameters and
aborts the connection on the first unrecognized one. The lock-safety GUCs +
TCP keepalives the engines otherwise pass via asyncpg ``server_settings`` must
therefore NOT ride the startup packet in ``transaction_pooler`` mode; the
timeouts are re-applied per transaction via ``SET LOCAL`` (as a single
``set_config(..., is_local => true)`` round-trip) instead. ``direct`` mode
(the default, on-prem / raw AlloyDB :5432) is unchanged.
"""
from __future__ import annotations

from sqlalchemy import create_engine, text

from dynastore.modules.db_config import db_timeout_config as t


class _Cfg:
    tcp_keepalives_idle = 300
    tcp_keepalives_interval = 30
    tcp_keepalives_count = 5
    lock_timeout = "5s"
    statement_timeout = "0"
    idle_in_transaction_session_timeout = "10s"
    task_idle_in_transaction_session_timeout = "300s"
    db_pooling_mode = "direct"


def _direct():
    class C(_Cfg):
        db_pooling_mode = "direct"

    return C


def _pooler():
    class C(_Cfg):
        db_pooling_mode = "transaction_pooler"

    return C


# --------------------------------------------------------------------------- #
# is_transaction_pooler                                                       #
# --------------------------------------------------------------------------- #
def test_is_transaction_pooler_direct_is_false():
    assert t.is_transaction_pooler(_direct()) is False


def test_is_transaction_pooler_pooler_is_true():
    assert t.is_transaction_pooler(_pooler()) is True


def test_is_transaction_pooler_missing_attr_defaults_direct():
    class Bare:
        pass

    assert t.is_transaction_pooler(Bare) is False


# --------------------------------------------------------------------------- #
# build_connection_server_settings                                            #
# --------------------------------------------------------------------------- #
def test_direct_mode_carries_full_startup_settings():
    ss = t.build_connection_server_settings(
        _direct(),
        application_name="svc",
        lock_timeout="5s",
        idle_in_transaction_session_timeout="10s",
        statement_timeout="55s",
    )
    assert ss["application_name"] == "svc"
    assert ss["tcp_keepalives_idle"] == "300"
    assert ss["tcp_keepalives_interval"] == "30"
    assert ss["tcp_keepalives_count"] == "5"
    assert ss["lock_timeout"] == "5s"
    assert ss["idle_in_transaction_session_timeout"] == "10s"
    assert ss["statement_timeout"] == "55s"


def test_pooler_mode_sends_application_name_only():
    ss = t.build_connection_server_settings(
        _pooler(),
        application_name="svc",
        lock_timeout="5s",
        idle_in_transaction_session_timeout="10s",
        statement_timeout="55s",
    )
    assert ss == {"application_name": "svc"}, (
        "a transaction pooler rejects every non-allowlisted startup parameter, "
        "so only application_name may ride the startup packet"
    )


def test_direct_mode_omits_statement_timeout_when_not_given():
    ss = t.build_connection_server_settings(
        _direct(),
        application_name="svc",
        lock_timeout="5s",
        idle_in_transaction_session_timeout="300s",
    )
    assert "statement_timeout" not in ss  # task engines never bound it


def test_tcp_keepalive_server_settings_empty_behind_pooler():
    assert t.tcp_keepalive_server_settings(_pooler()) == {}
    assert t.tcp_keepalive_server_settings(_direct())["tcp_keepalives_idle"] == "300"


# --------------------------------------------------------------------------- #
# pooler_timeout_set_local_sql                                                #
# --------------------------------------------------------------------------- #
def test_set_local_sql_none_in_direct_mode():
    assert (
        t.pooler_timeout_set_local_sql(
            _direct(),
            lock_timeout="5s",
            idle_in_transaction_session_timeout="10s",
            statement_timeout="55s",
        )
        is None
    )


def test_set_local_sql_uses_single_set_config_select():
    sql = t.pooler_timeout_set_local_sql(
        _pooler(),
        lock_timeout="5s",
        idle_in_transaction_session_timeout="10s",
        statement_timeout="55s",
    )
    # A single SELECT (one round-trip, survives asyncpg's extended protocol),
    # using the function form of SET LOCAL (is_local => true).
    assert sql.startswith("SELECT set_config(")
    assert sql.count("SELECT") == 1
    assert "set_config('lock_timeout', '5s', true)" in sql
    assert "set_config('idle_in_transaction_session_timeout', '10s', true)" in sql
    assert "set_config('statement_timeout', '55s', true)" in sql


def test_set_local_sql_omits_disabled_statement_timeout():
    sql = t.pooler_timeout_set_local_sql(
        _pooler(),
        lock_timeout="5s",
        idle_in_transaction_session_timeout="10s",
        statement_timeout="0",
    )
    assert "statement_timeout" not in sql
    assert "lock_timeout" in sql and "idle_in_transaction_session_timeout" in sql


# --------------------------------------------------------------------------- #
# register_pooler_timeout_guard (begin-event wiring)                          #
# --------------------------------------------------------------------------- #
def test_guard_runs_sql_on_top_level_transaction():
    # PRAGMA user_version is an observable side effect that proves the guard
    # SQL executed inside the begin event, before the transaction body runs.
    eng = create_engine("sqlite://")
    t.register_pooler_timeout_guard(eng, "PRAGMA user_version = 42")
    with eng.connect() as c:
        with c.begin():
            observed = c.exec_driver_sql("PRAGMA user_version").scalar()
    assert observed == 42


def test_guard_is_noop_when_sql_is_none():
    eng = create_engine("sqlite://")
    t.register_pooler_timeout_guard(eng, None)  # direct mode → no listener
    with eng.connect() as c:
        with c.begin():
            observed = c.exec_driver_sql("PRAGMA user_version").scalar()
    assert observed == 0  # untouched — the guard registered nothing


def test_guard_fires_on_begin_not_on_savepoint():
    from sqlalchemy import event

    eng = create_engine("sqlite://")
    t.register_pooler_timeout_guard(eng, "SELECT 1")
    fired = {"begin": 0, "savepoint": 0}

    @event.listens_for(eng, "begin")
    def _b(conn):  # noqa: ANN001
        fired["begin"] += 1

    @event.listens_for(eng, "savepoint")
    def _s(conn, name):  # noqa: ANN001
        fired["savepoint"] += 1

    with eng.connect() as c:
        with c.begin():
            with c.begin_nested():
                c.execute(text("SELECT 1"))
    # The timeouts are set once on the outermost transaction, never re-applied
    # on a SAVEPOINT (which would only scope them to the savepoint anyway).
    assert fired["begin"] == 1
    assert fired["savepoint"] == 1


# --------------------------------------------------------------------------- #
# task_engine_connect_args is mode-aware                                      #
# --------------------------------------------------------------------------- #
def test_task_connect_args_direct_carries_keepalives_and_task_idle():
    ss = t.task_engine_connect_args(_direct())["server_settings"]
    assert ss["tcp_keepalives_idle"] == "300"  # #3057 — task keepalives
    assert ss["idle_in_transaction_session_timeout"] == "300s"  # task tier
    assert "statement_timeout" not in ss


def test_task_connect_args_pooler_is_application_name_only():
    ss = t.task_engine_connect_args(_pooler())["server_settings"]
    assert list(ss.keys()) == ["application_name"]
