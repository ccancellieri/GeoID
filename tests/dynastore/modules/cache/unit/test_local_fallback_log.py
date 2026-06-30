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

"""Regression cover for #629 — bounded LocalAsyncCacheBackend fallback log.

``_log_local_fallback`` logs INFO on first occurrence per process + DEBUG
on subsequent occurrences. Successful Valkey backend registration re-arms
the flag so a flap re-emits at INFO.

``CacheModule`` is engine-driven only: ``ValkeyEngineConfig`` builds the
client and ``socket_connect_timeout_seconds`` is the sole socket-timeout
knob (set via the configs API), so there is no env-var override to cover.
"""

from __future__ import annotations

import logging

import pytest

from dynastore.modules.cache import cache_module
from dynastore.modules.cache.cache_module import (
    _LOCAL_FALLBACK_LOGGED,  # noqa: F401 — pinned by source-grep guard below
    _log_local_fallback,
)


# --------------------------------------------------------------------------
# _log_local_fallback — first-INFO-then-DEBUG bounded log
# --------------------------------------------------------------------------


def _reset_fallback_flag() -> None:
    """Tests should not bleed state into one another; reset between cases."""
    cache_module._LOCAL_FALLBACK_LOGGED = False


def test_log_local_fallback_first_is_info_subsequent_demote_to_debug(caplog):
    """First call logs INFO; every subsequent call logs DEBUG (bounded)."""
    _reset_fallback_flag()
    with caplog.at_level(logging.DEBUG, logger=cache_module.logger.name):
        _log_local_fallback("CACHE BACKEND: LOCAL — test message %s", "first")
        _log_local_fallback("CACHE BACKEND: LOCAL — test message %s", "second")
        _log_local_fallback("CACHE BACKEND: LOCAL — test message %s", "third")

    # Filter to our logger only — caplog can pick up unrelated framework chatter.
    our = [r for r in caplog.records if r.name == cache_module.logger.name]
    assert len(our) == 3
    assert our[0].levelno == logging.INFO
    assert our[1].levelno == logging.DEBUG
    assert our[2].levelno == logging.DEBUG
    # First-call message gets the "further occurrences" tail glued on so
    # operators reading logs know subsequent silence is bounded, not bug.
    assert "further occurrences" in our[0].getMessage()
    # Subsequent calls do NOT carry that tail (pure DEBUG echo).
    assert "further occurrences" not in our[1].getMessage()


def test_log_local_fallback_rearm_after_flag_reset(caplog):
    """After a successful Valkey registration resets the flag, the next
    degrade-to-LOCAL must log at INFO again (operator visibility for a
    flap → re-degrade cycle).
    """
    _reset_fallback_flag()
    with caplog.at_level(logging.DEBUG, logger=cache_module.logger.name):
        _log_local_fallback("CACHE BACKEND: LOCAL — first cycle")
        _log_local_fallback("CACHE BACKEND: LOCAL — first cycle dup")
        # Simulate a successful reconnect re-arming the bounded log.
        cache_module._LOCAL_FALLBACK_LOGGED = False
        _log_local_fallback("CACHE BACKEND: LOCAL — second cycle")

    our = [r for r in caplog.records if r.name == cache_module.logger.name]
    assert len(our) == 3
    assert our[0].levelno == logging.INFO
    assert our[1].levelno == logging.DEBUG
    assert our[2].levelno == logging.INFO  # re-armed


# --------------------------------------------------------------------------
# Source-level pin: re-arm wiring at success paths
# --------------------------------------------------------------------------


def test_module_level_state_pins():
    """Pin the existence of the module-level flag so a rename doesn't
    silently strip the bounded-log mechanism.
    """
    assert hasattr(cache_module, "_LOCAL_FALLBACK_LOGGED")
    assert isinstance(cache_module._LOCAL_FALLBACK_LOGGED, bool)


def test_success_paths_rearm_bounded_log():
    """Source-level pin: both Valkey-registration success paths must
    reset ``_LOCAL_FALLBACK_LOGGED`` so a later degrade-to-LOCAL re-emits
    at INFO instead of staying suppressed at DEBUG.
    """
    import inspect

    src = inspect.getsource(cache_module)
    # The lifespan success path (after `get_cache_manager().register_backend(backend)`).
    # Both success sites should have a `_LOCAL_FALLBACK_LOGGED = False` reset.
    # Count occurrences — must be at least 2 (lifespan + apply-handler re-init).
    assert src.count("_LOCAL_FALLBACK_LOGGED = False") >= 2, (
        "Both Valkey success paths (CacheModule.lifespan + "
        "_on_valkey_engine_config_change) must re-arm the bounded fallback "
        "log; got only %d" % src.count("_LOCAL_FALLBACK_LOGGED = False")
    )


@pytest.fixture(autouse=True)
def _isolate_fallback_flag():
    """Each test starts with a clean flag; clean up after too so other
    test files don't see a tainted flag if they import the module.
    """
    cache_module._LOCAL_FALLBACK_LOGGED = False
    yield
    cache_module._LOCAL_FALLBACK_LOGGED = False
