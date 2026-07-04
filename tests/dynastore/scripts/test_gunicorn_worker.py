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

"""Unit tests for ``dynastore.scripts.gunicorn_worker`` (#2925 / #2924).

``gunicorn``/``uvicorn`` are only installed at Docker build time (not a
``pyproject.toml`` dependency), so this module is skipped rather than
failed when they are unavailable in a bare dev venv.
"""
from __future__ import annotations

import importlib

import pytest

pytest.importorskip("gunicorn")
pytest.importorskip("uvicorn")


def test_default_drain_timeout_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no env override, the worker sets a small, finite drain budget.

    An unset (``None``) ``timeout_graceful_shutdown`` makes uvicorn wait
    *indefinitely* for every connection to close before ever running ASGI
    lifespan shutdown — the root cause behind #2925 / #2924. The default
    here must be a real, bounded number.
    """
    monkeypatch.delenv("GUNICORN_DRAIN_TIMEOUT", raising=False)
    import dynastore.scripts.gunicorn_worker as gunicorn_worker
    importlib.reload(gunicorn_worker)

    timeout = gunicorn_worker.DrainAwareUvicornWorker.CONFIG_KWARGS["timeout_graceful_shutdown"]
    assert isinstance(timeout, int)
    assert 0 < timeout < 30


def test_drain_timeout_respects_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """``GUNICORN_DRAIN_TIMEOUT`` overrides the default, same knob shape as
    the other GUNICORN_* server-process flags start.sh already reads."""
    monkeypatch.setenv("GUNICORN_DRAIN_TIMEOUT", "3")
    import dynastore.scripts.gunicorn_worker as gunicorn_worker
    importlib.reload(gunicorn_worker)

    assert gunicorn_worker.DrainAwareUvicornWorker.CONFIG_KWARGS["timeout_graceful_shutdown"] == 3


def test_drain_worker_preserves_upstream_config_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    """The subclass must not drop upstream UvicornWorker.CONFIG_KWARGS keys
    (``loop``/``http``) -- only add the drain-timeout budget on top."""
    monkeypatch.delenv("GUNICORN_DRAIN_TIMEOUT", raising=False)
    import dynastore.scripts.gunicorn_worker as gunicorn_worker
    importlib.reload(gunicorn_worker)
    from uvicorn.workers import UvicornWorker

    for key, value in UvicornWorker.CONFIG_KWARGS.items():
        assert gunicorn_worker.DrainAwareUvicornWorker.CONFIG_KWARGS[key] == value
