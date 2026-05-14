"""Unit tests for the runner-agnostic execution-liveness Protocol.

#735 (followup to #726): the fixed spawn-lease timer is replaced by a real
liveness signal. Before the pg_cron reaper blindly reclaims a lapsed-lease
``ACTIVE`` task, the owning runner is asked whether the execution backing the
task is actually alive — via :class:`LivenessProbeProtocol`. ``resolve_probe``
maps a task's ``owner_id`` to the runner that implements the probe; an unmapped
owner (in-process / ephemeral) yields ``None`` and the reconciler no-ops,
leaving the pg_cron reaper as the backstop — exactly today's semantics.
"""

from __future__ import annotations

import inspect


def _liveness():
    from dynastore.modules.tasks import liveness
    return liveness


def test_verdict_enum_members():
    """Every action the reconciler can take needs a distinct verdict."""
    LivenessVerdict = _liveness().LivenessVerdict
    members = {m.name for m in LivenessVerdict}
    assert members == {
        "ALIVE",
        "DEAD",
        "UNKNOWN",
        "TERMINAL_SUCCEEDED",
        "TERMINAL_FAILED",
    }


def test_verdict_enum_is_str():
    """A str-enum so verdicts survive logging / serialization unambiguously."""
    LivenessVerdict = _liveness().LivenessVerdict
    assert issubclass(LivenessVerdict, str)
    assert LivenessVerdict.ALIVE == "alive"


def test_probe_protocol_shape():
    """The Protocol is the runner-agnostic contract: a ``runner_type`` tag,
    a synchronous ``owns(owner_id)`` resolver, and an async ``probe_liveness``."""
    LivenessProbeProtocol = _liveness().LivenessProbeProtocol
    assert hasattr(LivenessProbeProtocol, "owns")
    assert hasattr(LivenessProbeProtocol, "probe_liveness")
    # runtime_checkable so the reconciler can discover implementers structurally.
    assert getattr(LivenessProbeProtocol, "_is_runtime_protocol", False)


def test_probe_liveness_is_async():
    """``probe_liveness`` does network I/O (e.g. Cloud Run Executions API)."""
    LivenessProbeProtocol = _liveness().LivenessProbeProtocol
    assert inspect.iscoroutinefunction(LivenessProbeProtocol.probe_liveness)


class _FakeProbe:
    runner_type = "gcp_cloud_run"

    def owns(self, owner_id: str) -> bool:
        return bool(owner_id) and owner_id.startswith("gcp_cloud_run_")

    async def probe_liveness(self, task):  # pragma: no cover - not called here
        from dynastore.modules.tasks.liveness import LivenessVerdict
        return LivenessVerdict.ALIVE


def test_fake_probe_satisfies_protocol_structurally():
    """A class with the three members IS a LivenessProbeProtocol — no
    inheritance required, so a future runner ships a probe without touching
    RunnerProtocol."""
    LivenessProbeProtocol = _liveness().LivenessProbeProtocol
    assert isinstance(_FakeProbe(), LivenessProbeProtocol)


def test_resolve_probe_matches_gcp_owner(monkeypatch):
    """``resolve_probe`` returns the probe whose ``owns()`` accepts the owner_id."""
    liveness = _liveness()
    probe = _FakeProbe()
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [probe],
    )
    resolved = liveness.resolve_probe("gcp_cloud_run_abc123")
    assert resolved is probe


def test_resolve_probe_unmapped_owner_returns_none(monkeypatch):
    """An in-process / dispatcher / ephemeral owner has no probe — the
    reconciler no-ops and the pg_cron reaper handles it (today's behavior)."""
    liveness = _liveness()
    probe = _FakeProbe()
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [probe],
    )
    assert liveness.resolve_probe("dispatcher-pod-7") is None
    assert liveness.resolve_probe("ephemeral-1234") is None


def test_resolve_probe_none_owner_returns_none():
    """A NULL owner_id resolves to no probe without consulting the registry."""
    assert _liveness().resolve_probe(None) is None
    assert _liveness().resolve_probe("") is None


def test_resolve_probe_swallows_owns_errors(monkeypatch):
    """A probe whose ``owns()`` raises must not break resolution for the rest."""
    liveness = _liveness()

    class _Raises:
        runner_type = "broken"

        def owns(self, owner_id):
            raise RuntimeError("boom")

        async def probe_liveness(self, task):  # pragma: no cover
            ...

    good = _FakeProbe()
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [_Raises(), good],
    )
    assert liveness.resolve_probe("gcp_cloud_run_abc") is good
