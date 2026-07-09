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
    pool_recycle = 111
    database_url = "postgresql://u:p@host:5432/db"


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


def test_task_connect_args_disable_asyncpg_statement_caches_in_all_modes():
    for cfg in (_direct(), _pooler()):
        connect_args = t.task_engine_connect_args(cfg)
        assert connect_args["prepared_statement_cache_size"] == 0
        assert connect_args["statement_cache_size"] == 0
        assert callable(connect_args["prepared_statement_name_func"])


# --------------------------------------------------------------------------- #
# LISTEN engine for transaction-pooler deployments                            #
# --------------------------------------------------------------------------- #
def test_create_listen_engine_returns_none_without_direct_url():
    assert t.create_listen_engine(_pooler()) is None


def test_create_listen_engine_uses_direct_url_and_pooler_safe_args(monkeypatch):
    import sqlalchemy.ext.asyncio as sa_async
    import dynastore.modules.db.db_service as db_service

    class C(_Cfg):
        db_pooling_mode = "transaction_pooler"
        listen_database_url = "postgresql://u:p@direct-host:5432/db"
        connect_timeout = 7

    sentinel = object()
    captured: dict = {}

    def _fake_create_async_engine(url, **kwargs):  # noqa: ANN001
        captured["url"] = str(url)
        captured["kwargs"] = kwargs
        return sentinel

    armed: list = []

    monkeypatch.setattr(sa_async, "create_async_engine", _fake_create_async_engine)
    monkeypatch.setattr(
        db_service,
        "_arm_client_socket_keepalive",
        lambda engine, cfg: armed.append((engine, cfg)),
    )

    assert t.create_listen_engine(C) is sentinel
    assert armed == [(sentinel, C)]
    assert captured["url"] == "postgresql+asyncpg://u:p@direct-host:5432/db"
    assert captured["kwargs"]["poolclass"].__name__ == "NullPool"

    connect_args = captured["kwargs"]["connect_args"]
    assert connect_args["timeout"] == 7
    assert connect_args["prepared_statement_cache_size"] == 0
    assert connect_args["statement_cache_size"] == 0
    assert callable(connect_args["prepared_statement_name_func"])
    assert list(connect_args["server_settings"].keys()) == ["application_name"]


# --------------------------------------------------------------------------- #
# create_task_engine bounded pool (#3145)                                     #
# --------------------------------------------------------------------------- #
def test_create_task_engine_uses_bounded_pool_not_nullpool(monkeypatch):
    """A drain run reuses one warm connection across its several
    ``managed_transaction`` blocks instead of reconnecting through the
    transaction pooler each time -- while staying small enough it never
    competes with the shared serving pool for capacity."""
    import sqlalchemy.ext.asyncio as sa_async
    import dynastore.modules.db.db_service as db_service

    sentinel = object()
    captured: dict = {}

    def _fake_create_async_engine(url, **kwargs):  # noqa: ANN001
        captured["url"] = str(url)
        captured["kwargs"] = kwargs
        return sentinel

    armed: list = []

    monkeypatch.setattr(sa_async, "create_async_engine", _fake_create_async_engine)
    monkeypatch.setattr(
        db_service,
        "_arm_client_socket_keepalive",
        lambda engine, cfg: armed.append((engine, cfg)),
    )

    cfg = _direct()
    assert t.create_task_engine(cfg) is sentinel
    assert armed == [(sentinel, cfg)]

    kwargs = captured["kwargs"]
    assert "poolclass" not in kwargs, (
        "create_task_engine must no longer force NullPool -- a small warm "
        "pool (pool_size/max_overflow) replaces it (#3145)"
    )
    assert kwargs["pool_size"] == t.TASK_ENGINE_POOL_SIZE == 1
    assert kwargs["max_overflow"] == t.TASK_ENGINE_POOL_MAX_OVERFLOW == 1
    assert kwargs["pool_recycle"] == cfg.pool_recycle
    assert kwargs["pool_pre_ping"] is True


def test_create_task_engine_bounded_pool_also_applies_behind_pooler(monkeypatch):
    """The bounded-pool sizing is not gated on db_pooling_mode -- same as how
    the shared serving engine applies pool_pre_ping/pool_recycle in both
    modes (#3081); only the startup connect_args differ behind a pooler."""
    import sqlalchemy.ext.asyncio as sa_async
    import dynastore.modules.db.db_service as db_service

    captured: dict = {}
    # A real (sqlite) engine, not a bare sentinel: create_task_engine also
    # registers register_pooler_timeout_guard()'s begin-listener in pooler
    # mode, which requires a genuine SQLAlchemy event target.
    fake_engine = create_engine("sqlite://")

    def _fake_create_async_engine(url, **kwargs):  # noqa: ANN001
        captured["kwargs"] = kwargs
        return fake_engine

    monkeypatch.setattr(sa_async, "create_async_engine", _fake_create_async_engine)
    monkeypatch.setattr(
        db_service, "_arm_client_socket_keepalive", lambda engine, cfg: None
    )

    t.create_task_engine(_pooler())

    kwargs = captured["kwargs"]
    assert kwargs["pool_size"] == 1
    assert kwargs["max_overflow"] == 1
    assert kwargs["pool_pre_ping"] is True
