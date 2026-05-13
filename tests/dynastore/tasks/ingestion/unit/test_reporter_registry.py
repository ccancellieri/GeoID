"""Reporter id harmonization + fail-loud on unknown reporter (issue #654)."""
from __future__ import annotations

import pytest

# Pre-warm the heavy registration chain before importing the leaf modules
# under test. Without this, ``from dynastore.tasks.tools import
# initialize_reporters`` pulls ``modules.tasks`` mid-import of
# ``models.protocols.authorization``, producing a circular import.
import dynastore.models.protocols  # noqa: F401

from dynastore.tasks.ingestion.reporters import (
    _ingestion_reporter_registry,
    ingestion_reporter,
)
from dynastore.tasks.reporters import ReportingInterface
from dynastore.tasks.tools import initialize_reporters


def test_ingestion_reporter_decorator_uses_snake_case_class_key():
    snapshot = dict(_ingestion_reporter_registry)
    try:
        @ingestion_reporter
        class MyShinyTestReporter(ReportingInterface):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)

        assert "my_shiny_test_reporter" in _ingestion_reporter_registry
        # Old PascalCase key MUST NOT be a parallel registration.
        assert "MyShinyTestReporter" not in _ingestion_reporter_registry
        assert _ingestion_reporter_registry["my_shiny_test_reporter"] is MyShinyTestReporter
    finally:
        _ingestion_reporter_registry.clear()
        _ingestion_reporter_registry.update(snapshot)


def test_initialize_reporters_raises_on_unknown_reporter_listing_available(monkeypatch):
    """Silent skip used to mask user typos (#654). Now raises with the
    available reporter names so the operator can self-correct."""
    fake_registry = {"gcs_detailed_reporter": object}  # value unused on unknown path

    with pytest.raises(ValueError) as exc:
        initialize_reporters(
            engine=None,
            task_id="t-1",
            task_request=None,
            reporting_config={"gcs_detailed": {"report_file_path": "gs://x/y"}},
            registry=fake_registry,  # type: ignore[arg-type]
        )

    msg = str(exc.value)
    assert "gcs_detailed" in msg
    assert "gcs_detailed_reporter" in msg  # available names surfaced
    assert "t-1" in msg


def test_initialize_reporters_empty_config_no_raise():
    # No reporting requested -> DatabaseStatusReporter only, no error.
    out = initialize_reporters(
        engine=None,
        task_id="t-2",
        task_request=None,
        reporting_config=None,
        registry=None,
    )
    assert len(out) == 1  # DatabaseStatusReporter
