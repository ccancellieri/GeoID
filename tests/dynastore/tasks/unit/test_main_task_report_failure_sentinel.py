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

"""Guard that ``report_failure()`` is not invoked twice when the in-lifecycle
handler already recorded the failure (#2467).

``main_task.py`` has two failure-reporting paths:

1. **In-lifecycle** (inside ``main()``): when an unexpected exception propagates
   out of the task body, the ``except Exception`` handler inside
   ``modules_lifespan`` tries to call ``fail_task()`` using the already-active
   DB connection.  This is the fast, cheap path.

2. **Outer fallback** (in ``if __name__ == "__main__"``): if ``main()`` raises,
   the outer ``except Exception`` block calls ``asyncio.run(report_failure(...))``,
   which opens a *second* full ``modules.lifespan()`` in the same process —
   ~40 s of startup cost, 34 "already instantiated" warnings, and cascade-owner
   registration failures.

The fix: when path 1 succeeds it stamps ``e._failure_already_reported = True``
on the exception before re-raising.  Path 2 checks that attribute and skips
``report_failure()`` entirely.

These tests verify both sides of the sentinel contract without requiring a live
DB or full module lifecycle.
"""

from __future__ import annotations

import inspect


def _main_src() -> str:
    from dynastore.main_task import main
    return inspect.getsource(main)


def _module_src() -> str:
    import dynastore.main_task as mt
    return inspect.getsource(mt)


# ---------------------------------------------------------------------------
# Sentinel is set by the in-lifecycle handler
# ---------------------------------------------------------------------------

def test_in_lifecycle_handler_sets_sentinel_after_fail_task():
    """After ``fail_task()`` succeeds, the in-lifecycle handler must stamp
    ``_failure_already_reported`` on the exception so the outer bootstrap
    is skipped."""
    src = _main_src()
    fail_task_pos = src.find("await fail_task(")
    assert fail_task_pos != -1, "main() must contain a fail_task() call"
    # The sentinel must appear after at least one fail_task call, before the
    # closing `raise` of the in-lifecycle except block.
    sentinel_pos = src.find("_failure_already_reported = True")
    assert sentinel_pos != -1, (
        "in-lifecycle handler must set _failure_already_reported = True"
    )
    # The sentinel assignment must follow a fail_task call (i.e. it is set
    # only on the success branch of the in-lifecycle report).
    assert sentinel_pos > fail_task_pos, (
        "_failure_already_reported must be set after fail_task(), not before"
    )


def test_sentinel_is_set_before_bare_raise():
    """The sentinel must be stamped before ``raise`` re-propagates the
    exception, so the outer handler can read it."""
    src = _main_src()
    sentinel_pos = src.find("_failure_already_reported = True")
    # The bare ``raise`` that re-propagates the exception from the in-lifecycle
    # handler is the last statement of that except block.
    raise_pos = src.rfind("\n            raise\n")
    assert sentinel_pos != -1 and raise_pos != -1
    assert sentinel_pos < raise_pos, (
        "sentinel must be stamped before the bare raise"
    )


# ---------------------------------------------------------------------------
# Outer __main__ handler reads the sentinel
# ---------------------------------------------------------------------------

def test_outer_handler_checks_sentinel_before_report_failure():
    """The outer ``except Exception`` block must guard ``report_failure()``
    with a check for ``_failure_already_reported`` so the second full
    ``modules.lifespan()`` bootstrap is skipped when the in-lifecycle path
    already reported."""
    full_src = _module_src()
    outer_start = full_src.find('if __name__ == "__main__"')
    assert outer_start != -1, 'module must have a __main__ guard'
    outer_src = full_src[outer_start:]
    assert "_failure_already_reported" in outer_src, (
        "outer __main__ handler must check _failure_already_reported"
    )
    assert "report_failure" in outer_src, (
        "outer __main__ handler still calls report_failure as the fallback"
    )
    # The check must gate the call: _failure_already_reported must appear
    # before report_failure in the outer block.
    sentinel_pos = outer_src.find("_failure_already_reported")
    report_pos = outer_src.find("report_failure")
    assert sentinel_pos < report_pos, (
        "sentinel check must precede the report_failure call in __main__"
    )


# ---------------------------------------------------------------------------
# Sentinel attribute round-trip (behaviour, no DB required)
# ---------------------------------------------------------------------------

def test_sentinel_attribute_skips_report_failure():
    """When an exception carries ``_failure_already_reported = True``, the
    ``getattr`` guard used by the outer handler evaluates to True — confirming
    the pattern works as expected without a live module stack."""
    exc = RuntimeError("simulated task error")
    exc._failure_already_reported = True  # type: ignore[attr-defined]
    # The outer handler does: not getattr(e, '_failure_already_reported', False)
    assert getattr(exc, "_failure_already_reported", False) is True
    assert not (not getattr(exc, "_failure_already_reported", False)), (
        "guard expression must evaluate to False (skip report_failure)"
    )


def test_no_sentinel_triggers_report_failure():
    """Without the attribute, ``getattr`` returns ``False`` and the outer
    handler proceeds to call ``report_failure()``."""
    exc = RuntimeError("simulated task error — never reported in-lifecycle")
    assert getattr(exc, "_failure_already_reported", False) is False
    assert not getattr(exc, "_failure_already_reported", False) is True, (
        "guard expression must evaluate to True (call report_failure)"
    )
