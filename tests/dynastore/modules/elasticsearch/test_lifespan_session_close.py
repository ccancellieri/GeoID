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

"""ElasticsearchModule.lifespan always closes the aiohttp ClientSession.

Regression test for the session-leak defect: any exception raised between
``es_client.init()`` and the ``yield`` bypassed ``finally: es_client.close()``
because the ``try:`` only wrapped the ``yield``.  After the fix the
``try/finally`` begins immediately after ``init()``, so ``close()`` is
guaranteed regardless of what fails in the startup body.

Three scenarios:
  1. Normal exit — close() is called after clean exit.
  2. Pre-yield import failure — close() is called when an import between
     init() and yield raises (the real production failure mode: the job SCOPE
     does not pip-install the log_backend extras so
     ``from dynastore.modules.elasticsearch.log_backend import ...`` raises
     ModuleNotFoundError).
  3. Body failure — close() is called when an exception escapes the yield.
"""

from __future__ import annotations

import sys
import types
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@contextmanager
def _inject_sys_modules(**stubs: types.ModuleType):
    """Temporarily insert fake modules into sys.modules, restoring on exit."""
    saved = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        yield
    finally:
        for name, old in saved.items():
            if old is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old


def _fake_log_backend_module(*, raise_on_class: Exception | None = None) -> types.ModuleType:
    """Build a minimal stub for dynastore.modules.elasticsearch.log_backend."""
    mod = types.ModuleType("dynastore.modules.elasticsearch.log_backend")
    if raise_on_class:
        mod.ElasticsearchLogBackend = MagicMock(side_effect=raise_on_class)
    else:
        mod.ElasticsearchLogBackend = MagicMock(return_value=MagicMock())
    return mod


def _fake_dashboards_module() -> types.ModuleType:
    """Stub for dashboards_provisioner (no-op provision_dashboards)."""
    mod = types.ModuleType("dynastore.modules.elasticsearch.dashboards_provisioner")
    mod.provision_dashboards = AsyncMock()
    return mod


@pytest.mark.asyncio
async def test_client_closed_when_post_init_import_raises():
    """close() is called when an import between init() and yield raises.

    This mirrors the real production failure mode: a job SCOPE that excludes
    the ``logs`` extension causes ``from dynastore.modules.elasticsearch.log_backend
    import ElasticsearchLogBackend`` to raise ModuleNotFoundError.  Before
    the fix the ``try/finally`` that called ``close()`` was only around the
    ``yield``, so the session was leaked.
    """
    # In this test environment dynastore.extensions.logs is absent, so
    # log_backend genuinely cannot be imported.  We stub only the client so we
    # can track close().
    from dynastore.modules.elasticsearch.module import ElasticsearchModule
    close_mock = AsyncMock()

    with (
        patch("dynastore.modules.elasticsearch.client.init", AsyncMock()),
        patch("dynastore.modules.elasticsearch.client.close", close_mock),
    ):
        module = ElasticsearchModule()
        with pytest.raises(ModuleNotFoundError):
            async with module.lifespan(object()):
                pass  # pragma: no cover — startup fails before the yield

    close_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_client_closed_on_normal_exit():
    """close() is called after the lifespan exits without error."""
    from dynastore.modules.elasticsearch.module import ElasticsearchModule
    close_mock = AsyncMock()

    with _inject_sys_modules(
        **{
            "dynastore.modules.elasticsearch.log_backend": _fake_log_backend_module(),
            "dynastore.modules.elasticsearch.dashboards_provisioner": _fake_dashboards_module(),
        }
    ):
        with (
            patch("dynastore.modules.elasticsearch.client.init", AsyncMock()),
            patch("dynastore.modules.elasticsearch.client.close", close_mock),
            patch("dynastore.modules.elasticsearch.client.get_client", MagicMock(return_value=None)),
            patch("dynastore.tools.discovery.register_plugin", MagicMock()),
        ):
            module = ElasticsearchModule()
            async with module.lifespan(object()):
                pass

    close_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_client_closed_when_body_raises():
    """close() is called when an exception escapes from inside the yield."""
    from dynastore.modules.elasticsearch.module import ElasticsearchModule
    close_mock = AsyncMock()

    with _inject_sys_modules(
        **{
            "dynastore.modules.elasticsearch.log_backend": _fake_log_backend_module(),
            "dynastore.modules.elasticsearch.dashboards_provisioner": _fake_dashboards_module(),
        }
    ):
        with (
            patch("dynastore.modules.elasticsearch.client.init", AsyncMock()),
            patch("dynastore.modules.elasticsearch.client.close", close_mock),
            patch("dynastore.modules.elasticsearch.client.get_client", MagicMock(return_value=None)),
            patch("dynastore.tools.discovery.register_plugin", MagicMock()),
        ):
            module = ElasticsearchModule()
            with pytest.raises(RuntimeError, match="simulated body error"):
                async with module.lifespan(object()):
                    raise RuntimeError("simulated body error")

    close_mock.assert_awaited_once()
