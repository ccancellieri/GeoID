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

"""Source-level invariants for the ``main_task.py`` ownership claim (#726, #2463, #2483).

``main_task.py`` used to take ownership of the task row via an *unconditional*
``update_task(status=ACTIVE, ...)``. With the REST-path spawn lease (60s)
shorter than real Cloud Run cold-start (~1m45s), the reaper reclaimed the row
mid-boot and a second job execution was triggered — which then happily
re-ran an already-``COMPLETED`` task because the ``update_task`` had no status
guard.

The fix: claim via the atomic, status-guarded ``claim_for_execution``; on a
lost claim (already terminal / owned by a live peer), exit cleanly *without*
executing the task. Owner id must use the same ``gcp_cloud_run_`` prefix the
spawner (``GcpJobRunner``) stamps, so the happy-path "ACTIVE & mine" claim
matches instead of being mistaken for a foreign owner.

#2463 extends the lost-claim path: when the task row is FAILED or DEAD_LETTER,
the Cloud Run retry must exit non-zero so the execution is recorded as FAILED
in the GCP console, not as a false SUCCEEDED.  ``_exit_code_for_unclaimed_status``
encodes the mapping and is tested functionally here.

#2483 is the specific retry scenario where this matters: attempt 0 exits 1 (task
failed), Cloud Run retries, attempt 1 finds the task already terminal and used to
exit 0 (skip-path bare return).  Cloud Run then recorded the execution as SUCCEEDED
— a false green.  The fix is that the skip path re-reads the task's terminal status
and exits non-zero when it is FAILED or DEAD_LETTER, so the retry inherits the
correct exit code.  Attempt 1 finding a COMPLETED task still exits 0: that's a
genuine duplicate that should step aside quietly.
"""

from __future__ import annotations

import inspect
import pytest


def _main_src() -> str:
    from dynastore.main_task import main
    return inspect.getsource(main)


def test_main_task_uses_claim_for_execution():
    """Ownership goes through the atomic, status-guarded helper."""
    assert "claim_for_execution" in _main_src()


def test_main_task_does_not_unconditionally_update_ownership():
    """The old unconditional ``update_task`` ownership write — keyed only on
    task_id + schema, no status guard — must be gone."""
    src = _main_src()
    # The legacy ownership write stamped a ``cloud-run-job-`` owner via
    # update_task; both the prefix and that pattern must be retired.
    assert 'cloud-run-job-' not in src


def test_main_task_owner_id_matches_spawner_prefix():
    """``main_task.py`` must claim under the same owner-id family the spawner
    stamps (``gcp_cloud_run_``), otherwise the happy-path "ACTIVE & mine"
    branch of claim_for_execution reads as a foreign owner and the claim is
    refused."""
    assert "gcp_cloud_run_" in _main_src()


def test_main_task_skips_execution_on_lost_claim():
    """A lost claim must short-circuit *before* ``target_task.run`` — the
    whole point is to not re-run a terminal/duplicate task."""
    src = _main_src()
    claim_pos = src.find("claim_for_execution")
    run_pos = src.find("target_task.run")
    assert claim_pos != -1 and run_pos != -1
    assert claim_pos < run_pos, "claim must be evaluated before the task runs"
    # Between the claim and the run there must be a guard that returns early.
    between = src[claim_pos:run_pos]
    assert "return" in between, "lost claim must return before executing the task"
    assert " is None" in between, "must branch on an empty (lost) claim result"


# ---------------------------------------------------------------------------
# Fix #2463 / #2483: correct exit code when task is already FAILED / DEAD_LETTER
# ---------------------------------------------------------------------------

def test_exit_code_for_failed_status_is_nonzero():
    """A Cloud Run retry that finds a FAILED task must exit non-zero so GCP
    records the execution as failed, not as a false SUCCEEDED."""
    from dynastore.main_task import _exit_code_for_unclaimed_status
    assert _exit_code_for_unclaimed_status("FAILED") == 1


def test_exit_code_for_dead_letter_status_is_nonzero():
    """DEAD_LETTER (max retries exhausted) is also a failure — exit non-zero."""
    from dynastore.main_task import _exit_code_for_unclaimed_status
    assert _exit_code_for_unclaimed_status("DEAD_LETTER") == 1


@pytest.mark.parametrize("status", ["COMPLETED", "DISMISSED", "ACTIVE", "PENDING", None])
def test_exit_code_for_non_failure_statuses_is_zero(status):
    """COMPLETED = done, DISMISSED = operator-cancelled, ACTIVE = peer running,
    PENDING / None = unknown.  All are exit 0: this execution is the duplicate."""
    from dynastore.main_task import _exit_code_for_unclaimed_status
    assert _exit_code_for_unclaimed_status(status) == 0


def test_main_task_looks_up_status_on_lost_claim():
    """The lost-claim branch must call ``get_task`` to determine the current
    status before deciding the exit code."""
    src = _main_src()
    claim_pos = src.find("claimed_row is None")
    run_pos = src.find("target_task.run")
    assert claim_pos != -1 and run_pos != -1
    between = src[claim_pos:run_pos]
    assert "get_task" in between, (
        "lost-claim branch must call get_task to look up current status"
    )
    assert "_exit_code_for_unclaimed_status" in between, (
        "lost-claim branch must use _exit_code_for_unclaimed_status to pick exit code"
    )
    assert "sys.exit" in between, (
        "lost-claim branch must call sys.exit for FAILED/DEAD_LETTER tasks"
    )
