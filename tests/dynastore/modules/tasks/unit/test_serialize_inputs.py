"""Unit tests for ``_serialize_inputs``.

Regression coverage for the 2026-04-21 production bug: ``create_task`` /
``enqueue_task`` called ``json.dumps(inputs)`` without the
:class:`CustomJSONEncoder`, so any producer that put a ``datetime`` /
``UUID`` / ``Decimal`` into ``TaskCreate.inputs`` would crash before the
task row was ever written — the task never ran and the catalog silently
drifted from its search index.

The fix routes both insert call sites through ``_serialize_inputs``
which uses the custom encoder.  These tests pin the contract.
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from dynastore.modules.tasks.tasks_module import _serialize_inputs


def test_none_passes_through_as_none():
    """``None`` → ``None``: column stays NULL, matches legacy behaviour."""
    assert _serialize_inputs(None) is None


def test_empty_dict_passes_through_as_none():
    """Empty dict → ``None``.  Matches the legacy guard
    ``json.dumps(inputs) if inputs else None``."""
    assert _serialize_inputs({}) is None


def test_datetime_serializes_as_iso8601():
    """The 2026-04-21 regression: datetime values in inputs must serialize."""
    ts = datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc)
    result = _serialize_inputs({"created_at": ts})
    assert result is not None
    parsed = json.loads(result)
    assert parsed["created_at"] == "2026-04-22T12:00:00+00:00"


def test_date_serializes_as_iso8601():
    result = _serialize_inputs({"day": date(2026, 4, 22)})
    assert json.loads(result)["day"] == "2026-04-22"


def test_uuid_serializes_as_string():
    u = uuid.UUID("12345678-1234-5678-1234-567812345678")
    result = _serialize_inputs({"id": u})
    assert json.loads(result)["id"] == str(u)


def test_decimal_serializes_as_number():
    """Decimals preserved as numeric type (not string) so downstream
    consumers don't have to parse + cast."""
    assert json.loads(_serialize_inputs({"x": Decimal("1.5")}))["x"] == 1.5
    assert json.loads(_serialize_inputs({"n": Decimal("42")}))["n"] == 42


def test_nested_datetime_in_list_serializes():
    """Pydantic ``model_dump()`` commonly produces nested structures."""
    ts1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2026, 2, 1, tzinfo=timezone.utc)
    result = _serialize_inputs({"window": [ts1, ts2]})
    assert json.loads(result)["window"] == [
        "2026-01-01T00:00:00+00:00",
        "2026-02-01T00:00:00+00:00",
    ]


def test_plain_json_compatible_values_still_work():
    """Regression: simple dicts must still serialize cleanly — no
    unintended encoder side-effects for the non-exotic path."""
    result = _serialize_inputs({"catalog_id": "x", "count": 3, "flag": True})
    assert json.loads(result) == {"catalog_id": "x", "count": 3, "flag": True}


def test_reindex_inputs_shape_survives_roundtrip():
    """Shape of ``BulkCatalogReindexInputs.model_dump()`` — the
    actual payload that triggered the prod incident.  Includes the
    ``after`` datetime that the index task uses as its lookback
    cutoff."""
    payload = {
        "catalog_id": "fao_asis",
        "mode": "stac",
        "after": datetime(2026, 4, 21, 23, 54, 3, tzinfo=timezone.utc),
    }
    result = _serialize_inputs(payload)
    assert result is not None
    parsed = json.loads(result)
    assert parsed["catalog_id"] == "fao_asis"
    assert parsed["mode"] == "stac"
    assert parsed["after"] == "2026-04-21T23:54:03+00:00"
