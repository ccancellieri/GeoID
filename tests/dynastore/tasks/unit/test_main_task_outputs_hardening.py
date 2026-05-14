"""``main_task.py`` persists ``outputs`` before the terminal status flip.

#735 (followup to #726): the liveness reconciler can find a Cloud Run execution
that already exited 0 (``TERMINAL_SUCCEEDED``) while its task row is still
``ACTIVE``. For the reconciler to complete that row *correctly* — with the real
result, not an empty one — the outputs must already be on the row by the time
the container exits.

So ``main_task.py``'s success path writes ``outputs`` as a distinct, retried
write *before* calling ``complete_task`` (which flips the status). Cloud Run
reports SUCCEEDED only after the container exits 0 — i.e. only after both
writes — so the ordering guarantee holds.

Source-inspection style: the full ``main()`` path needs a live DB + the module
lifecycle, so the ordering invariant is what we pin here.
"""

from __future__ import annotations

import inspect


def _main_src():
    from dynastore import main_task
    return inspect.getsource(main_task.main)


def test_main_task_persists_outputs_before_status_flip():
    src = _main_src()
    assert "persist_outputs" in src, "main_task must call persist_outputs"
    # Match the call sites (``await ...(``) — ``complete_task(`` also appears
    # in the docstring, which is not what we want to order against.
    persist_idx = src.index("await persist_outputs(")
    complete_idx = src.index("await complete_task(")
    assert persist_idx < complete_idx, (
        "outputs must be persisted BEFORE the terminal status flip"
    )


def test_main_task_outputs_write_is_retried():
    """A lost outputs write would make TERMINAL_SUCCEEDED recovery empty —
    so the write is bounded-retried, not best-effort once."""
    src = _main_src()
    # The retry loop wraps persist_outputs.
    block = src[src.index("persist_outputs(") - 400 : src.index("persist_outputs(") + 200]
    assert "range(" in block or "attempt" in block.lower(), (
        "persist_outputs must be wrapped in a bounded retry loop"
    )
