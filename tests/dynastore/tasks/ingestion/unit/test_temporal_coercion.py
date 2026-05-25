"""Unit coverage for ingestion-time temporal coercion (GeoID #1333).

A property whose items-schema field is declared ``date`` / ``time`` /
``timestamp`` is normalised from common string representations to canonical
ISO-8601 during ingestion, because the typed write path accepts ISO-8601 but
not arbitrary date formats. These tests pin the two pure helpers directly;
full ``run_ingestion_task`` wiring is exercised by the integration suite.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from dynastore.tasks.ingestion.main_ingestion import (
    _coerce_temporal_value,
    apply_temporal_coercion,
)


# --------------------------------------------------------------------------
# _coerce_temporal_value
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value,expected",
    [
        ("2024-01-31T12:30:00", "2024-01-31T12:30:00"),
        ("2024-01-31 12:30:00", "2024-01-31T12:30:00"),
        ("Jan 31 2024 12:30", "2024-01-31T12:30:00"),
        ("31 December 2024 00:00", "2024-12-31T00:00:00"),
    ],
)
def test_coerce_timestamp_normalises_to_iso(value, expected):
    assert _coerce_temporal_value(value, "timestamp") == expected


def test_coerce_date_returns_date_only_iso():
    assert _coerce_temporal_value("31/12/2024", "date") == "2024-12-31"
    assert _coerce_temporal_value("2024-01-05T09:00:00", "date") == "2024-01-05"


def test_coerce_time_returns_time_only_iso():
    assert _coerce_temporal_value("2024-01-31T08:15:30", "time") == "08:15:30"


def test_coerce_month_first_default_for_ambiguous_numeric():
    # dateutil default is month-first; 01/02/2024 -> 2 Jan 2024.
    assert _coerce_temporal_value("01/02/2024", "date") == "2024-01-02"


# --------------------------------------------------------------------------
# _coerce_temporal_value — explicit parse_format hint (#1350)
# --------------------------------------------------------------------------

def test_explicit_day_first_format_overrides_month_first_default():
    # With %d/%m/%Y the ambiguous 01/02/2024 reads as 1 Feb, not 2 Jan.
    assert (
        _coerce_temporal_value("01/02/2024", "date", "%d/%m/%Y") == "2024-02-01"
    )


def test_explicit_format_timestamp_keeps_time():
    assert (
        _coerce_temporal_value("31-12-2024 09:45", "timestamp", "%d-%m-%Y %H:%M")
        == "2024-12-31T09:45:00"
    )


def test_explicit_format_time_returns_time_only():
    assert (
        _coerce_temporal_value("31-12-2024 09:45", "time", "%d-%m-%Y %H:%M")
        == "09:45:00"
    )


def test_explicit_format_mismatch_returns_value_unchanged():
    # The operator pinned a format; a row that doesn't match it is left for the
    # typed write to reject (207) rather than auto-detected to a different value.
    assert (
        _coerce_temporal_value("2024-12-31", "date", "%d/%m/%Y") == "2024-12-31"
    )


def test_explicit_format_does_not_touch_non_strings():
    dt = datetime(2024, 1, 31)
    assert _coerce_temporal_value(dt, "date", "%d/%m/%Y") is dt


def test_coerce_unparseable_string_is_unchanged():
    assert _coerce_temporal_value("not a date", "timestamp") == "not a date"
    assert _coerce_temporal_value("", "date") == ""
    assert _coerce_temporal_value("   ", "timestamp") == "   "


def test_coerce_non_string_is_unchanged():
    dt = datetime(2024, 1, 31, 12, 0, 0)
    assert _coerce_temporal_value(dt, "timestamp") is dt
    assert _coerce_temporal_value(None, "date") is None
    assert _coerce_temporal_value(42, "timestamp") == 42


# --------------------------------------------------------------------------
# apply_temporal_coercion
# --------------------------------------------------------------------------

def test_apply_coerces_only_declared_temporal_fields():
    props = {
        "start": "31/12/2024",
        "name": "site-a",
        "count": "01/02/2024",  # a string column that merely looks like a date
    }
    out = apply_temporal_coercion(props, {"start": ("date", None)})
    assert out["start"] == "2024-12-31"
    assert out["name"] == "site-a"
    assert out["count"] == "01/02/2024"  # not declared temporal -> untouched


def test_apply_is_in_place_and_returns_same_dict():
    props = {"ts": "2024-01-31 00:00:00"}
    out = apply_temporal_coercion(props, {"ts": ("timestamp", None)})
    assert out is props
    assert props["ts"] == "2024-01-31T00:00:00"


def test_apply_empty_temporal_fields_is_noop():
    props = {"start": "31/12/2024"}
    out = apply_temporal_coercion(props, {})
    assert out is props
    assert props["start"] == "31/12/2024"


def test_apply_tolerates_declared_field_absent_from_properties():
    props = {"other": "x"}
    # 'start' is declared temporal but not present in this row -> no error.
    out = apply_temporal_coercion(
        props, {"start": ("date", None), "other": ("string", None)}
    )
    assert out == {"other": "x"}


def test_apply_leaves_unparseable_temporal_value_for_downstream_rejection():
    props = {"start": "garbage"}
    apply_temporal_coercion(props, {"start": ("timestamp", None)})
    # Unparseable values pass through unchanged; the typed write rejects the
    # row via the 207 IngestionReport rather than failing the whole batch.
    assert props["start"] == "garbage"


def test_apply_threads_per_field_parse_format():
    # Two date columns, one pinned day-first, one left to auto-detection: the
    # same ambiguous string resolves differently per the field's hint.
    props = {"eu": "01/02/2024", "us": "01/02/2024"}
    apply_temporal_coercion(
        props, {"eu": ("date", "%d/%m/%Y"), "us": ("date", None)}
    )
    assert props["eu"] == "2024-02-01"  # day-first: 1 Feb
    assert props["us"] == "2024-01-02"  # month-first default: 2 Jan
