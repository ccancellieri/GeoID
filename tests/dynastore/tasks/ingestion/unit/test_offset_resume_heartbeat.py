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

"""Regression coverage for GeoID #2958: resuming an ingestion via ``offset``
must never go completely silent.

Two layers are covered:

- ``_skip_to_offset_with_heartbeat`` — the generic fallback used for any
  reader that does NOT advertise ``supports_offset_seek`` (correctness of
  the discard + periodic heartbeat log).
- ``run_ingestion_task``'s wiring — a reader that DOES advertise
  ``supports_offset_seek`` must receive ``offset`` via ``open()`` instead
  of going through the generic fallback, and ``reader_options`` may not
  override the task's own resume offset.
"""
from __future__ import annotations

import inspect
import logging

from dynastore.tasks.ingestion.main_ingestion import (
    _OFFSET_SKIP_HEARTBEAT_INTERVAL,
    _skip_to_offset_with_heartbeat,
    run_ingestion_task,
)


# ---------------------------------------------------------------------------
# _skip_to_offset_with_heartbeat: correctness + heartbeat visibility
# ---------------------------------------------------------------------------


def test_skip_zero_offset_yields_everything():
    records = list(range(10))
    assert list(_skip_to_offset_with_heartbeat(records, 0)) == records


def test_skip_offset_discards_correct_count():
    records = list(range(10))
    assert list(_skip_to_offset_with_heartbeat(records, 4)) == [4, 5, 6, 7, 8, 9]


def test_skip_offset_beyond_length_yields_nothing():
    records = list(range(5))
    assert list(_skip_to_offset_with_heartbeat(records, 100)) == []


def test_skip_offset_emits_heartbeat_log(monkeypatch, caplog):
    """The bug being fixed: skipping to a large offset used to produce zero
    log output until the first post-offset batch upserted. A heartbeat line
    must fire at least once during a skip that spans multiple intervals."""
    import dynastore.tasks.ingestion.main_ingestion as main_ingestion_mod

    monkeypatch.setattr(main_ingestion_mod, "_OFFSET_SKIP_HEARTBEAT_INTERVAL", 3)
    records = list(range(10))

    with caplog.at_level(logging.INFO, logger=main_ingestion_mod.__name__):
        result = list(_skip_to_offset_with_heartbeat(records, 7))

    assert result == [7, 8, 9]
    heartbeat_lines = [
        rec.message for rec in caplog.records
        if "resuming at offset" in rec.message
    ]
    assert heartbeat_lines, "expected at least one heartbeat log line during the skip"


def test_default_heartbeat_interval_is_reasonably_sized():
    """Guard against an accidental drop to something so small it floods logs
    on a multi-million-row resume, or so large it never fires in practice."""
    assert 1_000 <= _OFFSET_SKIP_HEARTBEAT_INTERVAL <= 1_000_000


# ---------------------------------------------------------------------------
# run_ingestion_task wiring: native-seek readers bypass the generic fallback
# ---------------------------------------------------------------------------


def test_offset_forwarded_to_reader_when_supported():
    src = inspect.getsource(run_ingestion_task)
    assert 'open_kwargs["offset"] = task_request.offset' in src, (
        "run_ingestion_task no longer forwards the resume offset into "
        "open_kwargs for readers that advertise supports_offset_seek — "
        "native seek can no longer be used (GeoID #2958)."
    )
    assert "reader_seeks_offset" in src, (
        "run_ingestion_task no longer branches on supports_offset_seek — "
        "the native-seek / heartbeat-fallback split has been lost."
    )
    assert "_skip_to_offset_with_heartbeat(reader, task_request.offset)" in src, (
        "run_ingestion_task no longer applies the heartbeat-logged fallback "
        "for readers without native offset support."
    )


def test_offset_is_a_protected_reader_options_key():
    """A caller-supplied reader_options must not be able to override the
    task's own resume offset — that would silently skip the wrong number
    of rows on a resumed ingestion."""
    src = inspect.getsource(run_ingestion_task)
    assert '"task_id", "task_schema", "content_type", "offset"' in src, (
        "run_ingestion_task no longer protects 'offset' from being "
        "overridden via reader_options."
    )
