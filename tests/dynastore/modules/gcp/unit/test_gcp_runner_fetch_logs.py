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

"""Unit tests for ``GcpJobRunner.fetch_logs`` — #2658 item B.

Root cause: ``LogEntry.severity`` is typed as
``google.logging.type.log_severity_pb2.LogSeverity``, a legacy-style
protobuf enum whose values are plain ``int``s at runtime (no ``.name``
attribute). The pre-fix code unconditionally called ``log_entry.severity
.name`` — since a proto3 enum field is never actually ``None``, the
``is not None`` guard never skipped the call, so ``.name`` raised
``AttributeError`` on every real Cloud Logging response. The method's
lenient ``except Exception`` then swallowed it as
``"log read failed (AttributeError) — logs unavailable"``, defeating the
durable-logs (#2620) read path for every real job.

These tests pin:
  * ``_severity_name`` correctly maps the raw int (0 -> None / unset,
    named values -> their name, unrecognized values -> str fallback).
  * a full ``fetch_logs`` happy path against realistic proto-shaped log
    entries returns populated entries with no AttributeError and no
    "logs unavailable" note.
  * a genuinely unexpected exception (a real code bug) is still swallowed
    into a lenient empty ``LogPage`` (contract preserved) but is now
    logged server-side at ERROR with a traceback, not silently discarded.
  * the pre-existing "no IAM grant" (PermissionDenied) leniency path is
    unaffected and keeps logging at WARNING.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.gcp.gcp_runner import GcpJobRunner


# ---------------------------------------------------------------------------
# _severity_name — direct unit coverage of the root-cause fix
# ---------------------------------------------------------------------------


class TestSeverityName:
    def test_none_maps_to_none(self) -> None:
        assert GcpJobRunner._severity_name(None) is None

    def test_zero_default_maps_to_none(self) -> None:
        # 0 == LogSeverity.DEFAULT — "no severity set", not a real value.
        assert GcpJobRunner._severity_name(0) is None

    def test_known_value_maps_to_name(self) -> None:
        # 500 == LogSeverity.ERROR when google-cloud-logging's raw enum
        # module is importable; otherwise falls back to the numeric string.
        result = GcpJobRunner._severity_name(500)
        assert result in ("ERROR", "500")

    def test_never_raises_attributeerror_on_plain_int(self) -> None:
        """The exact shape of the pre-fix bug: calling this on a plain int
        (what a real LogEntry.severity actually is) must not raise."""
        for value in (0, 100, 200, 300, 400, 500, 600, 700, 800):
            GcpJobRunner._severity_name(value)  # must not raise

    def test_unrecognized_value_falls_back_to_str(self) -> None:
        result = GcpJobRunner._severity_name(999999)
        assert result == "999999"


# ---------------------------------------------------------------------------
# fetch_logs — full read path against realistic proto-shaped fakes
# ---------------------------------------------------------------------------


class _FakeLogEntry:
    """Mirrors the attribute shape of a real
    ``google.cloud.logging_v2.types.LogEntry``: ``severity`` is a plain
    int, ``timestamp`` a ``datetime``, payload fields are a mutually
    exclusive oneof accessed via ``getattr``.
    """

    def __init__(
        self,
        *,
        severity: int = 0,
        text_payload: str = "",
        timestamp: Optional[datetime] = None,
    ) -> None:
        self.severity = severity
        self.text_payload = text_payload
        self.json_payload = None
        self.proto_payload = None
        self.timestamp = timestamp or datetime(2026, 7, 2, tzinfo=timezone.utc)


class _FakeResponse:
    def __init__(self, entries: List[_FakeLogEntry], next_page_token: str = "") -> None:
        self.entries = entries
        self.next_page_token = next_page_token


class _FakePager:
    """Mirrors ``ListLogEntriesAsyncPager.pages`` — an ``async def`` method
    decorated as a ``@property``, i.e. accessing ``.pages`` (no call) yields
    an async generator."""

    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    @property
    async def pages(self) -> AsyncIterator[_FakeResponse]:
        yield self._response


def _fake_task(runner_ref: str = "projects/p/locations/l/jobs/JOB/executions/EXEC") -> Any:
    task = MagicMock(name="task")
    task.runner_ref = runner_ref
    return task


def _patch_identity_and_client(monkeypatch, client: Any) -> None:
    monkeypatch.setattr(
        GcpJobRunner, "_get_logging_client_safe", staticmethod(lambda: client)
    )
    fake_identity = MagicMock()
    fake_identity.get_project_id.return_value = "proj-1"
    monkeypatch.setattr(
        "dynastore.modules.get_protocol", lambda _proto: fake_identity
    )


@pytest.mark.asyncio
async def test_fetch_logs_happy_path_no_attributeerror(monkeypatch) -> None:
    """Realistic log entries (severity as plain int, incl. the common
    unset/DEFAULT=0 case) must be read without tripping the pre-#2658
    AttributeError, and the page must carry no "logs unavailable" note.
    """
    entries = [
        _FakeLogEntry(severity=0, text_payload="starting job"),
        _FakeLogEntry(severity=500, text_payload="boom"),
    ]
    client = MagicMock()
    client.list_log_entries = AsyncMock(return_value=_FakePager(_FakeResponse(entries)))
    _patch_identity_and_client(monkeypatch, client)

    runner = GcpJobRunner()
    page = await runner.fetch_logs(_fake_task())

    assert page.note is None
    assert len(page.entries) == 2
    assert page.entries[0].severity is None  # DEFAULT (0) -> unset
    assert page.entries[0].message == "starting job"
    assert page.entries[1].severity in ("ERROR", "500")
    assert page.entries[1].message == "boom"


@pytest.mark.asyncio
async def test_fetch_logs_permission_denied_stays_lenient_and_warns(
    monkeypatch, caplog
) -> None:
    """Pre-existing behavior preserved: a missing IAM grant is an expected
    degradation, logged at WARNING (not ERROR), with its own note."""

    class _PermissionDenied(Exception):
        pass

    _PermissionDenied.__name__ = "PermissionDenied"

    client = MagicMock()
    client.list_log_entries = AsyncMock(side_effect=_PermissionDenied("no roles/logging.viewer"))
    _patch_identity_and_client(monkeypatch, client)

    runner = GcpJobRunner()
    with caplog.at_level(logging.WARNING, logger="dynastore.modules.gcp.gcp_runner"):
        page = await runner.fetch_logs(_fake_task())

    assert page.entries == []
    assert page.note is not None and "log access not granted" in page.note
    assert any(rec.levelno == logging.WARNING for rec in caplog.records)
    assert not any(rec.levelno == logging.ERROR for rec in caplog.records)


@pytest.mark.asyncio
async def test_fetch_logs_unexpected_bug_is_logged_loudly_not_silently_swallowed(
    monkeypatch, caplog
) -> None:
    """A genuine code bug (AttributeError et al.) in the read path must
    still degrade to a lenient empty page (the endpoint's MUST-NOT-raise
    contract), but MUST be logged server-side at ERROR with a traceback —
    not silently discarded the way the pre-#2658 bug was.
    """
    entries = [_FakeLogEntry(severity=0, text_payload="hi")]
    client = MagicMock()
    client.list_log_entries = AsyncMock(return_value=_FakePager(_FakeResponse(entries)))
    _patch_identity_and_client(monkeypatch, client)

    # Simulate a residual code bug elsewhere in the per-entry extraction —
    # exactly the failure shape #2658 reported.
    with patch.object(
        GcpJobRunner,
        "_extract_log_message",
        staticmethod(MagicMock(side_effect=AttributeError("'int' object has no attribute 'name'"))),
    ):
        runner = GcpJobRunner()
        with caplog.at_level(logging.ERROR, logger="dynastore.modules.gcp.gcp_runner"):
            page = await runner.fetch_logs(_fake_task())

    # Lenient contract preserved: caller still gets HTTP 200 w/ empty page.
    assert page.entries == []
    assert page.note is not None and "AttributeError" in page.note

    # But the real exception must now be surfaced server-side at ERROR
    # with a traceback, not silently downgraded to "no logs".
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert error_records, "the real AttributeError must be logged at ERROR"
    assert error_records[0].exc_info is not None, (
        "the log call must carry exc_info so the traceback is captured"
    )
