"""Source-level invariants for the ``main_task.py`` ownership claim (#726).

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
"""

from __future__ import annotations

import inspect


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
