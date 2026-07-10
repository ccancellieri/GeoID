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

"""Regression test for the stale ``error_message`` left behind on reclaimed
tasks.

Bug: ``reap_stuck_tasks`` (``GLOBAL_TASKS_REAPER_DDL``) resets a heartbeat-
expired ACTIVE row back to PENDING and stamps
``error_message = 'Reaped by ...'``. When the dispatcher's ``claim_batch``
re-claims that row, its ``UPDATE ... SET`` clause did not clear
``error_message`` — so a task that goes on to COMPLETE successfully on the
retry still carries the reaper's stale error text, confusing any monitoring
keyed on ``error_message IS NOT NULL``.

Every other reset-to-PENDING path (``maintenance.py``'s requeue helpers)
already nulls ``error_message`` on the same UPDATE; ``claim_batch`` was the
one place that didn't. This test pins the fix by asserting the claim SQL's
SET clause nulls ``error_message`` alongside the other per-attempt fields —
i.e. the scenario "reaped row (error_message set, status PENDING) ->
claim_batch -> row ACTIVE with error_message NULL" is satisfied by the SQL
that runs atomically inside the claim itself.
"""

from __future__ import annotations

import inspect
import re

from dynastore.modules.tasks import tasks_module


def _claim_batch_set_clause() -> str:
    """Extract the ``SET ... WHERE`` clause of the ``claim_batch`` UPDATE
    from the function source, so assertions are scoped to the statement
    that actually performs the claim (not the read-only CTE above it)."""
    src = inspect.getsource(tasks_module.claim_batch)
    match = re.search(r"UPDATE \{task_schema\}\.tasks\s+SET(.*?)WHERE", src, re.DOTALL)
    assert match, "could not locate claim_batch's UPDATE ... SET ... WHERE clause"
    return match.group(1)


def test_claim_batch_clears_error_message_on_claim():
    """A fresh claim must start with a clean slate: re-claiming a row that
    the reaper reset to PENDING (with a stale 'Reaped by ...'
    error_message) must null it out in the same UPDATE that flips the row
    to ACTIVE — otherwise a task that goes on to COMPLETE successfully
    still shows a stale error."""
    set_clause = _claim_batch_set_clause()
    assert "error_message = NULL" in set_clause, (
        "claim_batch's UPDATE SET clause must clear error_message so a "
        "reaped-then-reclaimed task doesn't carry a stale 'Reaped by ...' "
        "message into a successful completion."
    )


def test_claim_batch_still_sets_active_and_ownership_fields():
    """Guard against the fix accidentally clobbering the rest of the SET
    clause — the claim must still flip status/ownership/liveness."""
    set_clause = _claim_batch_set_clause()
    assert "status = 'ACTIVE'" in set_clause
    assert "locked_until = :locked_until" in set_clause
    assert "owner_id = :owner_id" in set_clause
    assert "last_heartbeat_at = NOW()" in set_clause
