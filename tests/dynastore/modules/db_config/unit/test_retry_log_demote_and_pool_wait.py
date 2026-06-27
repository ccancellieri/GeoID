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

"""Issue #486 / #2511: behavioural assertions for ``query_executor`` retry and pool logging.

1. ``retry_on_transient_connect`` per-attempt retries log at DEBUG (issue #486)
   so a wedged pool no longer floods Cloud Logging. The log line now uses the
   unified format (issue #2511): ``retry attempt=N/M kind=db_connect ...``.
   Terminal exhaustion after ``max_retries`` still logs at WARNING.
2. ``_acquire_async_engine_connection`` emits a structured ``db_pool_acquire``
   log line carrying ``wait_seconds``. INFO when slow (>= threshold), DEBUG
   otherwise. Feeds a GCP log-based metric without requiring a prometheus dep.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import patch

import pytest

from dynastore.modules.db_config import query_executor
from dynastore.modules.db_config.connection_health_config import (
    resolve_slow_pool_acquire_threshold,
)
from dynastore.modules.db_config.query_executor import (
    _acquire_async_engine_connection,
    pool_acquire_scope,
    retry_on_transient_connect,
)

# The slow-pool-acquire threshold is now read live (configs API) rather than
# frozen at import. Resolve it here for the engine-delay baselines below.
_SLOW_POOL_ACQUIRE_THRESHOLD_S = resolve_slow_pool_acquire_threshold()


class _TransientOnce(Exception):
    """Subclass of one of the transient exception types so the decorator retries."""


# Force the decorator to recognise our exception by patching the tuple at runtime.
@pytest.fixture(autouse=True)
def _patch_transient_tuple():
    original = query_executor._TRANSIENT_CONNECT_EXCEPTIONS
    query_executor._TRANSIENT_CONNECT_EXCEPTIONS = (_TransientOnce,)
    try:
        yield
    finally:
        query_executor._TRANSIENT_CONNECT_EXCEPTIONS = original


class TestRetryLogDemote:
    def test_per_attempt_retry_logs_at_debug(self, caplog):
        calls = {"n": 0}

        @retry_on_transient_connect(
            max_retries=3, base_delay=0, max_delay=0, jitter=0
        )
        async def flaky() -> str:
            calls["n"] += 1
            if calls["n"] < 3:
                raise _TransientOnce("simulated transient")
            return "ok"

        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
        result = asyncio.run(flaky())

        assert result == "ok"
        retry_records = [
            r for r in caplog.records
            if "retry attempt=" in r.getMessage()
            and "kind=db_connect" in r.getMessage()
            and "exhausted" not in r.getMessage()
        ]
        assert retry_records, "expected at least one per-attempt retry log"
        for rec in retry_records:
            assert rec.levelno == logging.DEBUG, (
                f"per-attempt retry should be DEBUG, got "
                f"{logging.getLevelName(rec.levelno)}: {rec.getMessage()}"
            )

    def test_terminal_exhaustion_still_warns(self, caplog):
        @retry_on_transient_connect(
            max_retries=2, base_delay=0, max_delay=0, jitter=0
        )
        async def always_fails() -> None:
            raise _TransientOnce("doomed")

        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
        with pytest.raises(_TransientOnce):
            asyncio.run(always_fails())

        exhausted = [
            r for r in caplog.records
            if "exhausted" in r.getMessage()
        ]
        assert len(exhausted) == 1
        assert exhausted[0].levelno == logging.WARNING


class _FakeEngine:
    def __init__(self, delay: float = 0.0, raises: BaseException | None = None) -> None:
        self._delay = delay
        self._raises = raises

    async def connect(self) -> Any:
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._raises is not None:
            raise self._raises

        class _FakeConn:
            async def rollback(self_inner) -> None:
                return None

            async def close(self_inner) -> None:
                return None

            async def invalidate(self_inner) -> None:
                return None

        return _FakeConn()


class TestPoolWaitMetric:
    def test_fast_acquire_logs_debug(self, caplog):
        engine = _FakeEngine(delay=0)
        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
        asyncio.run(_acquire_async_engine_connection(engine))  # type: ignore[arg-type]

        acquires = [
            r for r in caplog.records
            if r.getMessage().startswith("db_pool_acquire ")
            and "slow" not in r.getMessage()
            and "failed" not in r.getMessage()
        ]
        assert acquires, "expected a db_pool_acquire DEBUG record"
        assert all(r.levelno == logging.DEBUG for r in acquires)

    def test_slow_acquire_logs_info(self, caplog):
        engine = _FakeEngine(delay=_SLOW_POOL_ACQUIRE_THRESHOLD_S + 0.05)
        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
        asyncio.run(_acquire_async_engine_connection(engine))  # type: ignore[arg-type]

        slow = [
            r for r in caplog.records
            if "db_pool_acquire slow" in r.getMessage()
        ]
        assert len(slow) == 1
        assert slow[0].levelno == logging.INFO
        assert "wait_seconds=" in slow[0].getMessage()

    def test_failed_acquire_logs_info_with_wait(self, caplog):
        # Use OSError — guaranteed in _TRANSIENT_CONNECT_EXCEPTIONS originally,
        # but we patched that tuple to (_TransientOnce,). Patch it back inline
        # so the decorator does NOT retry and the failure surfaces immediately.
        with patch.object(
            query_executor, "_TRANSIENT_CONNECT_EXCEPTIONS", (RuntimeError,)
        ):
            engine = _FakeEngine(raises=ValueError("boom"))
            caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
            with pytest.raises(ValueError):
                asyncio.run(_acquire_async_engine_connection(engine))  # type: ignore[arg-type]

        failed = [
            r for r in caplog.records
            if "db_pool_acquire failed" in r.getMessage()
        ]
        assert len(failed) == 1
        assert failed[0].levelno == logging.INFO
        assert "wait_seconds=" in failed[0].getMessage()


class TestPoolAcquireScope:
    """Issue #699: pool_acquire_scope() pushes workload tags onto the
    next ``db_pool_acquire`` log line so a 5-min log slice can be
    partitioned by who actually held the pool.
    """

    def test_scope_tags_appear_on_slow_acquire(self, caplog):
        engine = _FakeEngine(delay=_SLOW_POOL_ACQUIRE_THRESHOLD_S + 0.05)
        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)

        async def _go():
            with pool_acquire_scope(
                task_type="gcp_provision_phase3",
                catalog_id="adm2_catalog_7",
            ):
                await _acquire_async_engine_connection(engine)  # type: ignore[arg-type]

        asyncio.run(_go())

        slow = [
            r for r in caplog.records
            if "db_pool_acquire slow" in r.getMessage()
        ]
        assert len(slow) == 1
        msg = slow[0].getMessage()
        assert "task_type=gcp_provision_phase3" in msg
        assert "catalog_id=adm2_catalog_7" in msg

    def test_scope_tags_absent_when_no_scope_pushed(self, caplog):
        engine = _FakeEngine(delay=_SLOW_POOL_ACQUIRE_THRESHOLD_S + 0.05)
        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
        asyncio.run(_acquire_async_engine_connection(engine))  # type: ignore[arg-type]

        slow = [
            r for r in caplog.records
            if "db_pool_acquire slow" in r.getMessage()
        ]
        assert len(slow) == 1
        # No trailing kv pairs beyond the existing service= / wait_seconds= /
        # threshold= triplet — verify by token count not by exact equality so
        # this stays robust to incidental log format tweaks.
        msg = slow[0].getMessage()
        assert "task_type=" not in msg
        assert "catalog_id=" not in msg

    def test_scope_tags_appear_on_failed_acquire(self, caplog):
        with patch.object(
            query_executor, "_TRANSIENT_CONNECT_EXCEPTIONS", (RuntimeError,)
        ):
            engine = _FakeEngine(raises=ValueError("boom"))
            caplog.set_level(logging.DEBUG, logger=query_executor.__name__)

            async def _go():
                with pool_acquire_scope(
                    task_type="capability_publisher", loop="reactive_reaper",
                ):
                    await _acquire_async_engine_connection(engine)  # type: ignore[arg-type]

            with pytest.raises(ValueError):
                asyncio.run(_go())

        failed = [
            r for r in caplog.records
            if "db_pool_acquire failed" in r.getMessage()
        ]
        assert len(failed) == 1
        msg = failed[0].getMessage()
        assert "task_type=capability_publisher" in msg
        assert "loop=reactive_reaper" in msg

    def test_nested_scope_merges_outer_and_inner(self, caplog):
        engine = _FakeEngine(delay=_SLOW_POOL_ACQUIRE_THRESHOLD_S + 0.05)
        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)

        async def _go():
            with pool_acquire_scope(catalog_id="cat1"):
                with pool_acquire_scope(task_type="ingest"):
                    await _acquire_async_engine_connection(engine)  # type: ignore[arg-type]

        asyncio.run(_go())
        slow = [
            r for r in caplog.records
            if "db_pool_acquire slow" in r.getMessage()
        ]
        assert len(slow) == 1
        msg = slow[0].getMessage()
        assert "catalog_id=cat1" in msg
        assert "task_type=ingest" in msg

    def test_empty_and_none_tags_are_skipped(self, caplog):
        engine = _FakeEngine(delay=_SLOW_POOL_ACQUIRE_THRESHOLD_S + 0.05)
        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)

        async def _go():
            # Callers can pass optional context unconditionally; falsy values
            # must not produce ``key=`` or ``key=None`` noise in the log.
            with pool_acquire_scope(
                catalog_id=None, task_type="", collection_id="cset1",
            ):
                await _acquire_async_engine_connection(engine)  # type: ignore[arg-type]

        asyncio.run(_go())
        slow = [
            r for r in caplog.records
            if "db_pool_acquire slow" in r.getMessage()
        ]
        msg = slow[0].getMessage()
        assert "catalog_id=" not in msg
        assert "task_type=" not in msg
        assert "collection_id=cset1" in msg

    def test_scope_resets_after_block_exits(self, caplog):
        engine = _FakeEngine(delay=_SLOW_POOL_ACQUIRE_THRESHOLD_S + 0.05)
        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)

        async def _go():
            with pool_acquire_scope(task_type="ingest"):
                await _acquire_async_engine_connection(engine)  # type: ignore[arg-type]
            # Second acquire is OUTSIDE the scope block — must not inherit it.
            await _acquire_async_engine_connection(engine)  # type: ignore[arg-type]

        asyncio.run(_go())
        slow = [
            r for r in caplog.records
            if "db_pool_acquire slow" in r.getMessage()
        ]
        assert len(slow) == 2
        assert "task_type=ingest" in slow[0].getMessage()
        assert "task_type=" not in slow[1].getMessage()


class TestServiceNameResolution:
    """Issue #699: service= field must come from instance.json (the same
    source the dispatcher uses) rather than from an env var that is unset
    on Cloud Run revisions. Falls back to env, then to ``unknown``.
    """

    def test_resolve_prefers_instance_json(self, monkeypatch):
        monkeypatch.delenv("SERVICE_NAME", raising=False)
        with patch(
            "dynastore.modules.db_config.instance.get_service_name",
            return_value="dynastore-catalog",
        ):
            assert query_executor._resolve_service_name() == "dynastore-catalog"

    def test_resolve_falls_back_to_env(self, monkeypatch):
        monkeypatch.setenv("SERVICE_NAME", "fallback-svc")
        with patch(
            "dynastore.modules.db_config.instance.get_service_name",
            return_value=None,
        ):
            assert query_executor._resolve_service_name() == "fallback-svc"

    def test_resolve_falls_back_to_unknown(self, monkeypatch):
        monkeypatch.delenv("SERVICE_NAME", raising=False)
        with patch(
            "dynastore.modules.db_config.instance.get_service_name",
            return_value=None,
        ):
            assert query_executor._resolve_service_name() == "unknown"

    def test_resolve_swallows_instance_load_errors(self, monkeypatch):
        monkeypatch.setenv("SERVICE_NAME", "envname")
        with patch(
            "dynastore.modules.db_config.instance.get_service_name",
            side_effect=RuntimeError("disk gone"),
        ):
            assert query_executor._resolve_service_name() == "envname"


# ---------------------------------------------------------------------------
# Unified retry log — both wrappers emit the same key=value format.
# Issue #2511: the format is unified; the level is per-site (DEBUG for
# connect per issue #486, WARNING for provisioning).
# ---------------------------------------------------------------------------


class TestUnifiedRetryLog:
    """Both retry wrappers delegate to _run_with_retry_policy and emit
    ``retry attempt=N/M kind=<domain> backoff_s=F err=<type>: <msg>``.
    The level is site-specific: DEBUG for db_connect, WARNING for
    provisioning_write.
    """

    def test_connect_wrapper_emits_unified_line_at_debug(self, caplog):
        calls = {"n": 0}

        @retry_on_transient_connect(max_retries=3, base_delay=0, max_delay=0, jitter=0)
        async def flaky() -> str:
            calls["n"] += 1
            if calls["n"] < 3:
                raise _TransientOnce("transient-connect")
            return "done"

        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
        result = asyncio.run(flaky())

        assert result == "done"
        unified = [
            r for r in caplog.records
            if "retry attempt=" in r.getMessage()
            and "kind=db_connect" in r.getMessage()
            and "backoff_s=" in r.getMessage()
            and "err=" in r.getMessage()
        ]
        assert len(unified) == 2  # 2 failed attempts before success on 3rd
        for rec in unified:
            assert rec.levelno == logging.DEBUG

    @pytest.mark.asyncio
    async def test_provisioning_wrapper_emits_unified_line(self, caplog, monkeypatch):
        import dynastore.modules.db_config.query_executor as qe
        from contextlib import asynccontextmanager

        call_count = 0

        @asynccontextmanager
        async def _fake_managed_transaction(engine):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Simulate a transient closed-connection error on the first attempt.
                err = qe.OperationalError("server closed the connection", {}, None)
                err.connection_invalidated = True  # type: ignore[attr-defined]
                raise err
            yield object()

        monkeypatch.setattr(qe, "managed_transaction", _fake_managed_transaction)

        async def _fast_sleep(_: float) -> None:
            return None

        monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep)

        from dynastore.modules.db_config.query_executor import provisioning_write_with_retry

        caplog.set_level(logging.WARNING, logger=query_executor.__name__)

        async def fn(conn):
            return "provisioned"

        result = await provisioning_write_with_retry(object(), fn, attempts=3)
        assert result == "provisioned"

        unified = [
            r for r in caplog.records
            if "retry attempt=" in r.getMessage()
            and "kind=provisioning_write" in r.getMessage()
            and "backoff_s=" in r.getMessage()
            and "err=" in r.getMessage()
        ]
        assert len(unified) == 1
        assert unified[0].levelno == logging.WARNING
