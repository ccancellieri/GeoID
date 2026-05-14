"""Unit tests for the Cloud Run liveness probe on ``GcpJobRunner``.

#735: ``GcpJobRunner`` implements :class:`LivenessProbeProtocol` — it can be
asked whether the Cloud Run execution backing a lapsed-lease task is still
alive, by querying the Cloud Run Executions API. The probe MUST NOT raise: any
inconclusive result degrades to ``UNKNOWN`` and the pg_cron reaper backstops.
"""

from __future__ import annotations

import inspect
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def disable_managed_eventing():
    """Neutralize the DB-bound autouse fixture from gcp/conftest.py — these
    tests are pure in-memory."""
    return None


def _runner():
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    return GcpJobRunner()


def _verdict():
    from dynastore.modules.tasks.liveness import LivenessVerdict
    return LivenessVerdict


# --- owns() ----------------------------------------------------------------

def test_owns_matches_gcp_cloud_run_prefix():
    r = _runner()
    assert r.owns("gcp_cloud_run_deadbeef") is True


def test_owns_rejects_foreign_owners():
    r = _runner()
    assert r.owns("dispatcher-pod-3") is False
    assert r.owns("ephemeral-abc") is False
    assert r.owns("") is False
    assert r.owns(None) is False  # type: ignore[arg-type]


def test_gcp_runner_satisfies_liveness_probe_protocol():
    """Structural check — so ``get_protocols(LivenessProbeProtocol)`` discovers it."""
    from dynastore.modules.tasks.liveness import LivenessProbeProtocol
    assert isinstance(_runner(), LivenessProbeProtocol)


# --- _map_execution_state() ------------------------------------------------

def _execution(*, completion_time=None, running=0, succeeded=0, failed=0, cancelled=0):
    return SimpleNamespace(
        completion_time=completion_time,
        running_count=running,
        succeeded_count=succeeded,
        failed_count=failed,
        cancelled_count=cancelled,
    )


def test_map_state_running_is_alive():
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    V = _verdict()
    assert GcpJobRunner._map_execution_state(_execution(running=1)) == V.ALIVE


def test_map_state_pending_is_alive():
    """No counts yet (scheduling / cold start) — the execution exists, so alive."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    V = _verdict()
    assert GcpJobRunner._map_execution_state(_execution()) == V.ALIVE


def test_map_state_completed_succeeded():
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    V = _verdict()
    ex = _execution(completion_time=datetime(2026, 5, 14, tzinfo=timezone.utc), succeeded=1)
    assert GcpJobRunner._map_execution_state(ex) == V.TERMINAL_SUCCEEDED


def test_map_state_completed_failed():
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    V = _verdict()
    ex = _execution(completion_time=datetime(2026, 5, 14, tzinfo=timezone.utc), failed=1)
    assert GcpJobRunner._map_execution_state(ex) == V.TERMINAL_FAILED


def test_map_state_cancelled_is_dead():
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    V = _verdict()
    ex = _execution(completion_time=datetime(2026, 5, 14, tzinfo=timezone.utc), cancelled=1)
    assert GcpJobRunner._map_execution_state(ex) == V.DEAD


# --- probe_liveness() ------------------------------------------------------

@pytest.mark.asyncio
async def test_probe_liveness_null_runner_ref_is_unknown():
    """No handle captured yet (sub-second spawn→capture gap) — UNKNOWN, no API call."""
    r = _runner()
    task = SimpleNamespace(runner_ref=None)
    assert await r.probe_liveness(task) == _verdict().UNKNOWN


def _patch_gcp_module(monkeypatch, *, get_execution):
    """Make ``get_protocol(JobExecutionProtocol)`` return a fake GCP module
    whose executions client routes ``get_execution`` to the supplied mock."""
    client = SimpleNamespace(get_execution=get_execution)
    fake_module = SimpleNamespace(get_executions_client=lambda: client)
    monkeypatch.setattr(
        "dynastore.modules.get_protocol",
        lambda proto: fake_module,
    )
    return fake_module


@pytest.mark.asyncio
async def test_probe_liveness_running_execution_is_alive(monkeypatch):
    r = _runner()
    _patch_gcp_module(
        monkeypatch,
        get_execution=AsyncMock(return_value=_execution(running=1)),
    )
    task = SimpleNamespace(runner_ref="projects/p/locations/r/jobs/j/executions/e")
    assert await r.probe_liveness(task) == _verdict().ALIVE


@pytest.mark.asyncio
async def test_probe_liveness_not_found_is_dead(monkeypatch):
    """A deleted / never-materialized execution → DEAD (fail_task now)."""
    class NotFound(Exception):
        pass

    r = _runner()
    _patch_gcp_module(monkeypatch, get_execution=AsyncMock(side_effect=NotFound("gone")))
    task = SimpleNamespace(runner_ref="projects/p/locations/r/jobs/j/executions/e")
    assert await r.probe_liveness(task) == _verdict().DEAD


@pytest.mark.asyncio
async def test_probe_liveness_generic_error_is_unknown(monkeypatch):
    """Any other error is inconclusive — never crash the reconciler loop."""
    r = _runner()
    _patch_gcp_module(
        monkeypatch,
        get_execution=AsyncMock(side_effect=RuntimeError("transient")),
    )
    task = SimpleNamespace(runner_ref="projects/p/locations/r/jobs/j/executions/e")
    assert await r.probe_liveness(task) == _verdict().UNKNOWN


@pytest.mark.asyncio
async def test_probe_liveness_never_raises(monkeypatch):
    """Even a totally broken protocol resolution must yield a verdict, not raise."""
    r = _runner()

    def _boom(proto):
        raise RuntimeError("registry exploded")

    monkeypatch.setattr("dynastore.modules.get_protocol", _boom)
    task = SimpleNamespace(runner_ref="projects/p/locations/r/jobs/j/executions/e")
    assert await r.probe_liveness(task) == _verdict().UNKNOWN


# --- extract_execution_name() / run_cloud_run_job_async() ------------------

def test_extract_execution_name_from_metadata():
    from dynastore.modules.gcp.tools.jobs import extract_execution_name
    name = "projects/p/locations/europe-west1/jobs/ingest/executions/ingest-xyz"
    op = SimpleNamespace(metadata=SimpleNamespace(name=name))
    assert extract_execution_name(op) == name


def test_extract_execution_name_handles_missing_handle():
    from dynastore.modules.gcp.tools.jobs import extract_execution_name
    assert extract_execution_name(None) is None
    assert extract_execution_name(SimpleNamespace()) is None
    # An LRO name without /executions/ is not an execution handle.
    op = SimpleNamespace(operation=SimpleNamespace(name="projects/p/locations/r/operations/op1"))
    assert extract_execution_name(op) is None


@pytest.mark.asyncio
async def test_run_cloud_run_job_async_returns_execution_name(monkeypatch):
    """The previously-discarded ``Operation`` is mined for the execution handle."""
    from dynastore.modules.gcp.tools import jobs

    name = "projects/p/locations/r/jobs/ingest/executions/ingest-abc"
    fake_op = SimpleNamespace(metadata=SimpleNamespace(name=name))
    fake_runner = SimpleNamespace(run_job=AsyncMock(return_value=fake_op))
    monkeypatch.setattr(jobs, "get_protocol", lambda proto: fake_runner)

    got = await jobs.run_cloud_run_job_async(job_name="ingest", args=[], env_vars={})
    assert got == name


# --- run() persists the handle (source-inspection: full path needs a DB) ---

def test_run_captures_and_persists_runner_ref():
    """After a successful RunJob, ``run()`` captures the execution name and
    calls ``set_runner_ref`` on the row — both REST and dispatcher paths."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    src = inspect.getsource(GcpJobRunner.run)
    assert "set_runner_ref" in src
    # The return value of run_cloud_run_job_async is captured, not discarded.
    assert "= await run_cloud_run_job_async" in src


# --- GCPModule executions client -------------------------------------------

def _make_module_with_credentials():
    from dynastore.modules.gcp.gcp_module import GCPModule

    fake_creds = MagicMock(name="credentials")
    with patch(
        "dynastore.modules.gcp.gcp_module.get_credentials",
        return_value=(fake_creds, MagicMock(name="identity")),
    ), patch("dynastore.modules.gcp.gcp_module.storage.Client"), \
       patch("dynastore.modules.gcp.gcp_module.pubsub_v1.PublisherClient"), \
       patch("dynastore.modules.gcp.gcp_module.pubsub_v1.SubscriberClient"):
        return GCPModule(app_state=MagicMock())


def test_executions_client_not_created_in_init():
    """Async gRPC clients are loop-bound — built lazily, never in sync __init__."""
    mod = _make_module_with_credentials()
    assert mod._executions_client is None


@pytest.mark.asyncio
async def test_get_executions_client_builds_lazily_on_running_loop():
    mod = _make_module_with_credentials()

    fake_jobs = MagicMock(name="JobsAsyncClient")
    fake_run = MagicMock(name="ServicesAsyncClient")
    fake_exec = MagicMock(name="ExecutionsAsyncClient")
    with patch(
        "dynastore.modules.gcp.gcp_module.run_v2.JobsAsyncClient", return_value=fake_jobs,
    ), patch(
        "dynastore.modules.gcp.gcp_module.run_v2.ServicesAsyncClient", return_value=fake_run,
    ), patch(
        "dynastore.modules.gcp.gcp_module.run_v2.ExecutionsAsyncClient", return_value=fake_exec,
    ):
        got = mod.get_executions_client()

    assert got is fake_exec
    assert mod._executions_client is fake_exec
