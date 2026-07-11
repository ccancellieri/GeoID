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

"""SIGTERM disposition in ``main_task.py`` (#3237 Q2(a)).

A Cloud Run Job SIGTERM used to always ``reset_task_to_pending`` — correct for
a mid-run preemption (deploy/scale-in/OOM recycle: a fresh execution can
resume) or a non-final attempt (its retry resumes normally), but wrong for a
*final-attempt task-timeout kill*: resetting that row just gets it respawned
and killed at the same wall again, unbounded, because ``reset_task_to_pending``
never advances ``retry_count``. Meanwhile the row reads as a zombie ``ACTIVE``
until a reconciler probes it.

``_sigterm_is_final_timeout`` classifies which case this is, from container
env alone (``CLOUD_RUN_TASK_ATTEMPT`` is Cloud Run auto-injected;
``MAX_RETRIES`` / ``TASK_TIMEOUT`` arrive via the deployment template and are
optional — older deploys don't set them, and must fall back to the existing
reset-to-PENDING behaviour). It is a pure function so the classification is
unit-testable without touching the DB or the process signal machinery.
"""

from __future__ import annotations

import inspect

import pytest

from dynastore.main_task import _sigterm_is_final_timeout


def _env(**overrides: str) -> dict[str, str]:
    base = {
        "CLOUD_RUN_TASK_ATTEMPT": "1",
        "MAX_RETRIES": "1",
        "TASK_TIMEOUT": "3600",
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize(
    "elapsed_seconds, env, expected",
    [
        # Final attempt (attempt == max_retries) and elapsed at/over the 90%
        # threshold of TASK_TIMEOUT — this is the case Q2(a) targets.
        (3300.0, _env(), True),
        pytest.param(3240.0, _env(), True, id="exactly-at-90-percent-threshold"),
        # Not the final attempt — its own retry will resume normally.
        (3600.0, _env(CLOUD_RUN_TASK_ATTEMPT="0"), False),
        # Final attempt, but killed well before the timeout — a preemption,
        # not a timeout kill; a fresh execution can still be dispatched.
        (100.0, _env(), False),
        pytest.param(3000.0, _env(), False, id="final-attempt-below-90-percent"),
        # Each required env var missing individually -> False.
        (3600.0, {"MAX_RETRIES": "1", "TASK_TIMEOUT": "3600"}, False),
        (3600.0, {"CLOUD_RUN_TASK_ATTEMPT": "1", "TASK_TIMEOUT": "3600"}, False),
        (3600.0, {"CLOUD_RUN_TASK_ATTEMPT": "1", "MAX_RETRIES": "1"}, False),
        # Non-numeric values for each var -> False.
        (3600.0, _env(CLOUD_RUN_TASK_ATTEMPT="not-a-number"), False),
        (3600.0, _env(MAX_RETRIES="not-a-number"), False),
        (3600.0, _env(TASK_TIMEOUT="not-a-number"), False),
        # Paranoia: an attempt index beyond max_retries (shouldn't normally
        # happen, but must still classify as final when elapsed qualifies).
        (3600.0, _env(CLOUD_RUN_TASK_ATTEMPT="5"), True),
    ],
)
def test_sigterm_is_final_timeout(elapsed_seconds, env, expected):
    assert _sigterm_is_final_timeout(elapsed_seconds, env) is expected


def test_sigterm_is_final_timeout_defaults_to_os_environ(monkeypatch):
    """``env=None`` (the default) reads from ``os.environ`` directly."""
    monkeypatch.setenv("CLOUD_RUN_TASK_ATTEMPT", "1")
    monkeypatch.setenv("MAX_RETRIES", "1")
    monkeypatch.setenv("TASK_TIMEOUT", "3600")
    assert _sigterm_is_final_timeout(3600.0) is True


def test_sigterm_is_final_timeout_empty_env_is_false():
    assert _sigterm_is_final_timeout(999999.0, {}) is False


# ---------------------------------------------------------------------------
# Source-level guard: the CancelledError handler must actually wire the
# classifier into a terminal fail_task() call, guarded by owner_id, and must
# still fall back to reset_task_to_pending. ``main()`` needs a live DB + full
# module lifecycle to exercise end-to-end (see test_main_task_claim_guard.py's
# established convention for this function) so this pins the contract at the
# source level, matching the sibling guard tests in this directory.
# ---------------------------------------------------------------------------


def _main_src() -> str:
    from dynastore.main_task import main
    return inspect.getsource(main)


def _cancelled_error_block() -> str:
    src = _main_src()
    start = src.index("except asyncio.CancelledError:")
    end = src.index("except PermanentTaskFailure as exc:", start)
    return src[start:end]


def test_cancelled_error_handler_uses_the_sigterm_classifier():
    block = _cancelled_error_block()
    assert "_sigterm_is_final_timeout(" in block


def test_cancelled_error_handler_writes_terminal_fail_task_on_final_timeout():
    block = _cancelled_error_block()
    assert "await fail_task(" in block
    assert "owner_id=owner_id" in block
    assert "retry=False" in block


def test_cancelled_error_handler_still_falls_back_to_reset_to_pending():
    block = _cancelled_error_block()
    assert "reset_task_to_pending" in block


def test_cancelled_error_handler_guards_fail_task_branch_on_owner_id():
    """The terminal-write branch must only fire when ``owner_id`` was
    actually assigned (inside the ownership-claim block) — never on an
    unbound/None owner_id, which would either crash or race an unscoped
    UPDATE against the wrong row."""
    block = _cancelled_error_block()
    classifier_idx = block.index("_sigterm_is_final_timeout(")
    preceding = block[:classifier_idx]
    assert "owner_id is not None" in preceding[preceding.rindex("if ") :]
