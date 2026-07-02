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

"""``main()``'s fatal paths must not silently swallow a ``fail_task()``
no-op (#2764 finding 1).

``fail_task()`` returns ``bool`` — ``True`` when its guarded UPDATE matched a
row, ``False`` when it didn't (ownership already lost / row already
terminal / reclaimed). Both fatal-path call sites in ``main()`` used to fire
the call and ignore the return value entirely:

* the ``PermanentTaskFailure`` branch logged "permanently failed" regardless.
* the general ``except Exception`` branch unconditionally logged "Successfully
  reported failure" and stamped ``_failure_already_reported = True`` —
  suppressing the ``__main__`` ``report_failure()`` fallback even when the
  guarded write never landed. That is exactly the zombie-ACTIVE-row scenario:
  the row stays ``ACTIVE``, a Cloud Run retry can't claim it, and the
  no-double-run guard reports a false SUCCEEDED.

The fix threads the boolean through: a no-op is logged at WARNING for
observability, and the sentinel is only stamped on a truthful success — so a
no-op falls through to the ``report_failure()`` fallback instead of being
silently treated as reported.

``main()`` needs a live DB + the full module lifecycle to exercise
end-to-end (see ``test_main_task_report_failure_sentinel.py``), so — matching
that file's established convention for this function — these are
source-level pins of the escalation contract.
"""

from __future__ import annotations

import inspect


def _main_src() -> str:
    from dynastore.main_task import main
    return inspect.getsource(main)


# ---------------------------------------------------------------------------
# PermanentTaskFailure branch (retry=False call)
# ---------------------------------------------------------------------------


def test_permanent_failure_branch_captures_fail_task_return_value():
    src = _main_src()
    exc_branch_start = src.index("except PermanentTaskFailure as exc:")
    finally_start = src.index("finally:", exc_branch_start)
    block = src[exc_branch_start:finally_start]
    assert "retry=False" in block, "PermanentTaskFailure branch must call fail_task(retry=False)"
    assert " = await fail_task(" in block, (
        "PermanentTaskFailure branch must capture fail_task()'s bool return "
        "value instead of firing-and-forgetting the call"
    )


def test_permanent_failure_branch_warns_on_fail_task_noop():
    src = _main_src()
    exc_branch_start = src.index("except PermanentTaskFailure as exc:")
    finally_start = src.index("finally:", exc_branch_start)
    block = src[exc_branch_start:finally_start]
    assert "logger.warning(" in block, (
        "a fail_task() no-op in the PermanentTaskFailure branch must be "
        "logged at WARNING for observability"
    )


# ---------------------------------------------------------------------------
# General `except Exception` branch (retry=True call, in-lifecycle reporter)
# ---------------------------------------------------------------------------


def _general_exception_block(src: str) -> str:
    # The general handler is the outer `except Exception as e:` — locate it
    # after the PermanentTaskFailure branch so we don't match that one.
    start = src.index("except PermanentTaskFailure as exc:")
    outer_start = src.index("except Exception as e:", start)
    return src[outer_start:]


def test_general_exception_branch_captures_fail_task_return_value():
    block = _general_exception_block(_main_src())
    fail_call = block.index("await fail_task(")
    assign_region = block[max(0, fail_call - 40): fail_call]
    assert "=" in assign_region, (
        "the in-lifecycle fail_task(retry=True) call must capture its bool "
        "return value instead of discarding it"
    )


def test_general_exception_branch_only_stamps_sentinel_on_truthful_success():
    block = _general_exception_block(_main_src())
    fail_call = block.index("await fail_task(")
    sentinel = block.index("_failure_already_reported = True")
    # Between the call and the sentinel there must be a conditional guard —
    # the sentinel must NOT be stamped unconditionally on every call.
    between = block[fail_call:sentinel]
    assert "if " in between, (
        "_failure_already_reported must only be set when fail_task() "
        "truthfully reports success, not unconditionally after the call"
    )


def test_general_exception_branch_warns_and_skips_sentinel_on_noop():
    block = _general_exception_block(_main_src())
    fail_call_idx = block.index("await fail_task(")
    sentinel_idx = block.index("_failure_already_reported = True")
    else_idx = block.index("else:", fail_call_idx)
    assert else_idx < sentinel_idx or "else:" in block[sentinel_idx:], (
        "there must be an else-branch alongside the sentinel assignment"
    )
    # The else branch (no-op case) must log a WARNING distinguishing it from
    # the success path's plain info log.
    else_block = block[else_idx:]
    warn_idx = else_block.index("logger.warning(")
    next_except_idx_candidates = [
        i for i in (else_block.find("except Exception as report_error:"),)
        if i != -1
    ]
    boundary = next_except_idx_candidates[0] if next_except_idx_candidates else len(else_block)
    assert warn_idx < boundary, (
        "the fail_task() no-op else-branch must log a WARNING before the "
        "surrounding try's own except clause"
    )
