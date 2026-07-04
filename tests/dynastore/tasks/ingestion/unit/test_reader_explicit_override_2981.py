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

"""Coverage for the GeoID #2981 A/B-testing lever: an explicit
``reader_id`` override on ``ReaderRegistry.resolve()``/``resolve_reader()``
bypasses the ``can_read``/priority scan entirely, and
``TaskIngestionRequest.reader_options`` can override a default ``open()``
kwarg (e.g. ``read_batch_size``) without colliding as a duplicate keyword
argument.
"""

from __future__ import annotations

import contextlib
from typing import Any, ClassVar, Dict, Iterable, Iterator, Tuple

import pytest

from dynastore.tasks.ingestion.main_ingestion import logger as _main_ingestion_logger
from dynastore.tasks.ingestion.readers.base import (
    SourceReaderProtocol,
    resolve_reader,
)
from dynastore.tasks.ingestion.readers.registry import ReaderRegistry


class _LowPriorityReader(SourceReaderProtocol):
    """Registered with the WORST priority — resolve() must never pick this
    via the normal scan, only via an explicit reader_id override."""

    reader_id: ClassVar[str] = "low_priority_test"
    priority: ClassVar[int] = 999
    extensions: ClassVar[Tuple[str, ...]] = (".geojson",)

    @contextlib.contextmanager
    def open(  # type: ignore[override]
        self, uri: str, *, encoding: str = "utf-8",
        content_type: str | None = None, **opts: Any,
    ) -> Iterator[Iterable[dict]]:
        yield iter([])


class _HighPriorityReader(SourceReaderProtocol):
    reader_id: ClassVar[str] = "high_priority_test"
    priority: ClassVar[int] = 1
    extensions: ClassVar[Tuple[str, ...]] = (".geojson",)

    @contextlib.contextmanager
    def open(  # type: ignore[override]
        self, uri: str, *, encoding: str = "utf-8",
        content_type: str | None = None, **opts: Any,
    ) -> Iterator[Iterable[dict]]:
        yield iter([])


@pytest.fixture
def isolated_registry():
    saved = list(ReaderRegistry._registered)
    ReaderRegistry.clear()
    ReaderRegistry.register(_LowPriorityReader)
    ReaderRegistry.register(_HighPriorityReader)
    yield
    ReaderRegistry.clear()
    for cls in saved:
        ReaderRegistry.register(cls)


# ---------------------------------------------------------------------------
# ReaderRegistry.resolve(reader_id=...) / resolve_reader(reader_id=...)
# ---------------------------------------------------------------------------


def test_no_override_regression_unchanged(isolated_registry):
    """Existing callers that never pass reader_id keep resolving by
    priority/extension exactly as before this change."""
    assert resolve_reader("gs://bucket/data.geojson") is _HighPriorityReader


def test_explicit_reader_id_bypasses_priority_scan(isolated_registry):
    """Forcing the worse-priority reader by id must win over the scan,
    even though the higher-priority reader also matches the extension."""
    cls = resolve_reader("gs://bucket/data.geojson", reader_id="low_priority_test")
    assert cls is _LowPriorityReader


def test_explicit_reader_id_ignores_can_read(isolated_registry):
    """reader_id override returns the reader even for a URI its can_read()
    would normally reject — the override is an exact-id lookup, not a
    scan."""
    cls = resolve_reader("gs://bucket/data.parquet", reader_id="low_priority_test")
    assert cls is _LowPriorityReader


def test_unknown_reader_id_raises_with_registered_list(isolated_registry):
    with pytest.raises(LookupError) as ei:
        resolve_reader("gs://bucket/data.geojson", reader_id="does_not_exist")
    msg = str(ei.value)
    assert "does_not_exist" in msg
    assert "low_priority_test" in msg
    assert "high_priority_test" in msg


def test_registry_resolve_reader_id_kwarg_directly(isolated_registry):
    """Same behaviour via ReaderRegistry.resolve() directly (not just the
    module-level facade)."""
    assert (
        ReaderRegistry.resolve("gs://bucket/data.geojson", reader_id="low_priority_test")
        is _LowPriorityReader
    )


# ---------------------------------------------------------------------------
# main_ingestion.py open-kwargs construction: reader_options overrides a
# top-level default (e.g. read_batch_size) instead of duplicating the kwarg.
# ---------------------------------------------------------------------------


def _build_open_kwargs(task_id: str, phys_schema: str, read_batch_size: int,
                        encoding: str, content_type: str | None,
                        reader_options: Dict[str, Any] | None) -> Dict[str, Any]:
    """Mirrors the open_kwargs construction in main_ingestion.py exactly —
    kept here as a small, direct unit test of that merge logic without
    importing the full ingestion task module (which pulls in DB/reporter
    wiring not relevant to this test)."""
    open_kwargs: Dict[str, Any] = {
        "encoding": encoding,
        "content_type": content_type,
        "task_id": task_id,
        "task_schema": phys_schema,
        "read_batch_size": read_batch_size,
    }
    options = dict(reader_options or {})
    shadowed_keys = [
        key for key in ("task_id", "task_schema", "content_type")
        if key in options
    ]
    if shadowed_keys:
        _main_ingestion_logger.warning(
            "ingestion: reader_options attempted to override structural "
            "kwarg(s) %s — ignoring, these are fixed by the ingestion "
            "task itself and are not user-tunable",
            shadowed_keys,
        )
        for key in shadowed_keys:
            del options[key]
    open_kwargs.update(options)
    return open_kwargs


def test_reader_options_overrides_read_batch_size():
    kwargs = _build_open_kwargs(
        task_id="t1", phys_schema="s1", read_batch_size=1000,
        encoding="utf-8", content_type=None,
        reader_options={"read_batch_size": 500, "use_vsicache": True},
    )
    assert kwargs["read_batch_size"] == 500
    assert kwargs["use_vsicache"] is True
    # Untouched defaults survive the merge.
    assert kwargs["task_id"] == "t1"
    assert kwargs["encoding"] == "utf-8"


def test_no_reader_options_keeps_defaults():
    kwargs = _build_open_kwargs(
        task_id="t1", phys_schema="s1", read_batch_size=1000,
        encoding="utf-8", content_type=None, reader_options=None,
    )
    assert kwargs["read_batch_size"] == 1000
    assert "use_vsicache" not in kwargs


def test_main_ingestion_builds_open_kwargs_as_dict_and_merges_reader_options():
    """Source-inspection guard: main_ingestion.py must build the reader
    ``open()`` kwargs as a dict and merge in ``task_request.reader_options``
    — passing both as literal keyword arguments would collide with a
    duplicate ``read_batch_size`` the instant an operator sets
    ``reader_options={'read_batch_size': ...}``."""
    import inspect

    from dynastore.tasks.ingestion import main_ingestion

    src = inspect.getsource(main_ingestion)
    assert "open_kwargs" in src
    assert "open_kwargs.update(reader_options)" in src
    assert "reader_inst.open(source_file_path, **open_kwargs)" in src
    # The reader_id override must reach both resolve_reader() call sites.
    assert src.count("reader_id=task_request.reader") == 2


# ---------------------------------------------------------------------------
# GeoID #2986: reader_options must not shadow the reader's structural
# task_id/task_schema/content_type kwargs.
# ---------------------------------------------------------------------------


def test_reader_options_task_id_is_stripped_and_warned(caplog):
    """A reader_options dict carrying 'task_id' must not overwrite the
    ingestion task's own task_id — the structural value wins and a warning
    names the ignored key."""
    with caplog.at_level("WARNING"):
        kwargs = _build_open_kwargs(
            task_id="real-task-id", phys_schema="s1", read_batch_size=1000,
            encoding="utf-8", content_type=None,
            reader_options={"task_id": "other-task-id"},
        )
    assert kwargs["task_id"] == "real-task-id"
    assert len(caplog.records) == 1
    assert "task_id" in caplog.records[0].getMessage()


def test_reader_options_read_batch_size_still_overrides():
    """Regression check: read_batch_size is a legitimate tuning knob and
    must still be overridable via reader_options after the #2986 fix."""
    kwargs = _build_open_kwargs(
        task_id="t1", phys_schema="s1", read_batch_size=1000,
        encoding="utf-8", content_type=None,
        reader_options={"read_batch_size": 250},
    )
    assert kwargs["read_batch_size"] == 250


def test_reader_options_without_protected_keys_passes_through_unchanged():
    """A reader_options dict with no protected keys is forwarded as-is."""
    kwargs = _build_open_kwargs(
        task_id="t1", phys_schema="s1", read_batch_size=1000,
        encoding="utf-8", content_type=None,
        reader_options={"use_vsicache": True, "some_tuning_knob": 42},
    )
    assert kwargs["use_vsicache"] is True
    assert kwargs["some_tuning_knob"] == 42
    assert kwargs["task_id"] == "t1"
    assert kwargs["task_schema"] == "s1"
