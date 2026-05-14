"""``GcpJobRunner`` REST-path spawn lease must outlast real Cloud Run
cold-start (#726).

The REST path INSERTs the task row born-claimed ``ACTIVE`` with
``locked_until = now + GCP_RUNNER_SPAWN_LEASE``. The in-job heartbeat only
starts extending that lease once ``main_task.py`` reaches its ownership step —
observed at ~1m45s after the execution is created (container cold-start +
module init). With the old 60s default the lease lapsed mid-boot, the pg_cron
reaper reset the row to PENDING, and a second Cloud Run execution was
triggered. The default must comfortably exceed observed cold-start.
"""

from __future__ import annotations


# Observed cold-start: execution created -> main_task.py "Took ownership"
# was ~105s on the review env. The spawn lease must clear that with margin.
_OBSERVED_COLDSTART_SECONDS = 105


def test_spawn_lease_default_outlasts_observed_coldstart():
    from dynastore.modules.gcp.gcp_runner import _SPAWN_LEASE_SECONDS

    assert _SPAWN_LEASE_SECONDS >= 2 * _OBSERVED_COLDSTART_SECONDS, (
        f"GCP_RUNNER_SPAWN_LEASE default ({_SPAWN_LEASE_SECONDS}s) must clear "
        f"observed Cloud Run cold-start (~{_OBSERVED_COLDSTART_SECONDS}s) with "
        f"margin, or the reaper reclaims the row mid-boot and double-spawns."
    )


def test_spawn_lease_still_env_overridable():
    """The lease stays an env knob — ops can tune it per environment."""
    import inspect

    from dynastore.modules.gcp import gcp_runner

    src = inspect.getsource(gcp_runner)
    assert 'os.getenv("GCP_RUNNER_SPAWN_LEASE"' in src
