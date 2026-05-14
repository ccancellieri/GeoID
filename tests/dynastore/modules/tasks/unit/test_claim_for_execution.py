"""Unit tests for ``claim_for_execution`` — the in-job (``main_task.py``)
ownership claim that replaced the unconditional ``update_task(status=ACTIVE)``.

Bug recap (#726): a Cloud Run job container takes ~1m45s to cold-start before
``main_task.py`` reaches its ownership step. The REST-path spawn lease was only
60s, so the pg_cron reaper reclaimed the row mid-boot → a second Cloud Run
execution was triggered. ``main_task.py`` then took ownership via an
*unconditional* ``UPDATE`` — flipping an already-``COMPLETED`` task back to
``ACTIVE`` and re-running it.

``claim_for_execution`` is the atomic, status-guarded claim: it refuses a row
that is already terminal, or ``ACTIVE`` with a still-live lease owned by a
*different* execution. A lost claim (empty ``RETURNING``) tells ``main_task.py``
to exit cleanly without re-running the task.
"""

from __future__ import annotations

import inspect


def _get_claim_for_execution():
    from dynastore.modules.tasks.tasks_module import claim_for_execution
    return claim_for_execution


def test_claim_for_execution_exists_and_is_async():
    """A single helper so the claim SQL has one auditable home."""
    assert inspect.iscoroutinefunction(_get_claim_for_execution())


def test_claim_for_execution_signature():
    """``main_task.py`` calls it with engine, task_id, schema, owner_id and a
    visibility timeout — all required so the claim is unambiguous."""
    sig = inspect.signature(_get_claim_for_execution())
    for param in ("engine", "task_id", "schema", "owner_id", "visibility_timeout"):
        assert param in sig.parameters, f"missing parameter: {param}"


def test_claim_for_execution_refuses_terminal_states():
    """The claim MUST NOT match a row in any terminal state — re-running a
    COMPLETED/FAILED/DISMISSED/DEAD_LETTER task is the #726 regression."""
    src = inspect.getsource(_get_claim_for_execution())
    normalised = " ".join(src.split()).upper()
    assert "STATUS NOT IN ('COMPLETED', 'FAILED', 'DISMISSED', 'DEAD_LETTER')" in normalised


def test_claim_for_execution_refuses_live_lease_owned_by_other():
    """The claim MUST NOT match an ACTIVE row whose lease is still live and
    whose owner is a *different* execution — that is a concurrent duplicate."""
    src = inspect.getsource(_get_claim_for_execution())
    normalised = " ".join(src.split()).upper()
    assert "STATUS = 'ACTIVE'" in normalised
    assert "LOCKED_UNTIL > NOW()" in normalised
    assert "OWNER_ID IS DISTINCT FROM :OWNER_ID" in normalised


def test_claim_for_execution_returns_task_id():
    """RETURNING the matched row lets the caller detect a lost claim (empty
    result) and skip execution instead of running a doomed/duplicate task."""
    src = inspect.getsource(_get_claim_for_execution())
    assert "RETURNING" in src.upper()
    assert "task_id" in src
