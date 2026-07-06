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

"""Unit tests for ``ConfigReloadService`` — the Layer A platform-config
hot-reload watcher (#3084).

Mirrors the fixture style of ``test_config_change_runners.py`` (dummy
``PluginConfig`` subclasses + apply-handler registration/cleanup) and
``test_task_services.py`` (monkeypatching the module's ``managed_transaction``
reference with a fake async context manager so no real DB is required).

Covers:
  (a) reconcile re-runs a class's apply handler when its ``updated_at``
      token advances;
  (b) reconcile is a no-op when tokens are unchanged;
  (c) one class failing to reconcile does not block the others in the
      same pass, and its token is left untouched for retry;
  (d) startup seeding populates the last-seen baseline WITHOUT firing any
      apply handlers.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar, List, Optional, Tuple

import pytest

from dynastore.models.plugin_config import PluginConfig, _APPLY_HANDLERS
from dynastore.tools.background_service import Leadership, PodPolicy, ServiceContext


class _ReloadTestConfigA(PluginConfig):
    _address: ClassVar[Tuple[Optional[str], ...]] = ("test", "config_reload", "a")


class _ReloadTestConfigB(PluginConfig):
    _address: ClassVar[Tuple[Optional[str], ...]] = ("test", "config_reload", "b")


def _cleanup(*classes: type) -> None:
    for cls in classes:
        _APPLY_HANDLERS.pop(cls, None)


def _ctx() -> ServiceContext:
    return ServiceContext(
        engine=object(),
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name="test-svc",
    )


def _row(ref_key: str, cls: type, updated_at: datetime) -> Tuple[str, str, Any, datetime]:
    return (ref_key, cls.class_key(), {}, updated_at)


class _FakePcfg:
    """Stand-in for ``PlatformConfigService`` exposing only what
    ``ConfigReloadService`` calls."""

    def __init__(self, rows: List[Tuple[str, str, Any, datetime]]) -> None:
        self.rows = rows
        self.calls = 0
        # Sentinel standing in for ``PlatformConfigService.engine`` — the
        # reconcile transaction runs on THIS engine, not on ctx.engine (which
        # is None at the priority-0 lifespan point where this service starts).
        self.engine = object()

    async def list_configs_versioned(self) -> List[Tuple[str, str, Any, datetime]]:
        self.calls += 1
        return self.rows


@pytest.fixture(autouse=True)
def _patch_managed_transaction(monkeypatch):
    """No real DB is needed: run_apply_handlers only touches ``conn`` if a
    registered handler chooses to, and none of these tests' handlers do."""
    import dynastore.modules.db_config.config_reload_service as crs

    @contextlib.asynccontextmanager
    async def _fake_mt(_engine):
        yield object()

    monkeypatch.setattr(crs, "managed_transaction", _fake_mt)
    yield


def _t(seconds: int = 0) -> datetime:
    return datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=seconds)


class TestConfigReloadServicePolicyFields:
    def test_policy_fields(self):
        from dynastore.modules.db_config.config_reload_service import ConfigReloadService

        svc = ConfigReloadService(_FakePcfg([]))
        assert svc.name == "config_reload"
        assert svc.leadership is Leadership.RUN_EVERYWHERE
        assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL
        assert svc.lock_key is None


class TestConfigReloadServiceSeed:
    async def test_seed_populates_baseline_without_firing_handlers(self):
        from dynastore.modules.db_config.config_reload_service import ConfigReloadService

        calls: List[str] = []
        _ReloadTestConfigA.register_apply_handler(lambda *a: calls.append("a"))
        try:
            pcfg = _FakePcfg([_row("a", _ReloadTestConfigA, _t(0))])
            svc = ConfigReloadService(pcfg)

            await svc._seed()

            assert calls == []
            assert svc._last_seen[_ReloadTestConfigA.class_key()] == _t(0)
        finally:
            _cleanup(_ReloadTestConfigA)


class TestConfigReloadServiceReconcile:
    async def test_reconcile_reruns_handler_when_token_advances(self):
        from dynastore.modules.db_config.config_reload_service import ConfigReloadService

        calls: List[str] = []
        _ReloadTestConfigA.register_apply_handler(lambda *a: calls.append("a"))
        try:
            pcfg = _FakePcfg([_row("a", _ReloadTestConfigA, _t(0))])
            svc = ConfigReloadService(pcfg)
            await svc._seed()
            assert calls == []

            pcfg.rows = [_row("a", _ReloadTestConfigA, _t(5))]
            await svc._reconcile()

            assert calls == ["a"]
            assert svc._last_seen[_ReloadTestConfigA.class_key()] == _t(5)
        finally:
            _cleanup(_ReloadTestConfigA)

    async def test_reconcile_runs_apply_on_pcfg_engine_not_ctx_engine(self, monkeypatch):
        """Regression (#3084): the apply transaction must run on the
        PlatformConfigService engine, NOT ``ServiceContext.engine`` — the
        latter is None at the priority-0 lifespan point where this service
        starts, which made every reconcile raise
        ``Cannot start managed_transaction: db_resource is None`` in prod."""
        import dynastore.modules.db_config.config_reload_service as crs
        from dynastore.modules.db_config.config_reload_service import ConfigReloadService

        seen_engines: List[Any] = []

        @contextlib.asynccontextmanager
        async def _capturing_mt(engine):
            seen_engines.append(engine)
            yield object()

        monkeypatch.setattr(crs, "managed_transaction", _capturing_mt)

        _ReloadTestConfigA.register_apply_handler(lambda *a: None)
        try:
            pcfg = _FakePcfg([_row("a", _ReloadTestConfigA, _t(0))])
            svc = ConfigReloadService(pcfg)
            await svc._seed()

            pcfg.rows = [_row("a", _ReloadTestConfigA, _t(5))]
            await svc._reconcile()

            assert seen_engines == [pcfg.engine]
        finally:
            _cleanup(_ReloadTestConfigA)

    async def test_reconcile_noop_when_tokens_unchanged(self):
        from dynastore.modules.db_config.config_reload_service import ConfigReloadService

        calls: List[str] = []
        _ReloadTestConfigA.register_apply_handler(lambda *a: calls.append("a"))
        try:
            pcfg = _FakePcfg([_row("a", _ReloadTestConfigA, _t(0))])
            svc = ConfigReloadService(pcfg)
            await svc._seed()

            # Same rows, same updated_at — nothing advanced.
            await svc._reconcile()

            assert calls == []
        finally:
            _cleanup(_ReloadTestConfigA)

    async def test_reconcile_one_failing_class_does_not_block_others(self, monkeypatch):
        """A class whose stored row fails to validate must not stop the
        reconcile pass, and must keep its old token so the next wake retries
        it — while a sibling class in the same pass still reconciles."""
        import dynastore.modules.db_config.config_reload_service as crs
        from dynastore.modules.db_config.config_reload_service import ConfigReloadService

        calls: List[str] = []
        _ReloadTestConfigB.register_apply_handler(lambda *a: calls.append("b"))

        real_validate = crs._validate_stored_config

        def _flaky_validate(cls, data):
            if cls is _ReloadTestConfigA:
                raise ValueError("simulated validation failure")
            return real_validate(cls, data)

        monkeypatch.setattr(crs, "_validate_stored_config", _flaky_validate)
        try:
            pcfg = _FakePcfg(
                [_row("a", _ReloadTestConfigA, _t(0)), _row("b", _ReloadTestConfigB, _t(0))]
            )
            svc = ConfigReloadService(pcfg)
            await svc._seed()

            pcfg.rows = [
                _row("a", _ReloadTestConfigA, _t(5)),
                _row("b", _ReloadTestConfigB, _t(5)),
            ]
            await svc._reconcile()

            assert calls == ["b"]
            assert svc._last_seen[_ReloadTestConfigB.class_key()] == _t(5)
            # The failing class's token must NOT advance — retried next wake.
            assert svc._last_seen[_ReloadTestConfigA.class_key()] == _t(0)
        finally:
            _cleanup(_ReloadTestConfigB)

    async def test_reconcile_skips_unregistered_class_key(self):
        """A row whose class_key is no longer a registered PluginConfig must
        be skipped (logged), not raise."""
        from dynastore.modules.db_config.config_reload_service import ConfigReloadService

        pcfg = _FakePcfg([("x", "not_a_registered_class", {}, _t(0))])
        svc = ConfigReloadService(pcfg)

        # Must not raise.
        await svc._reconcile()
        assert "not_a_registered_class" not in svc._last_seen


class TestConfigReloadServiceRunDisabled:
    async def test_run_is_noop_when_disabled(self):
        from dynastore.modules.db_config.config_reload_service import ConfigReloadService

        pcfg = _FakePcfg([_row("a", _ReloadTestConfigA, _t(0))])
        svc = ConfigReloadService(pcfg, enabled=False)

        await svc.run(_ctx())

        # Disabled: never even lists configs.
        assert pcfg.calls == 0
