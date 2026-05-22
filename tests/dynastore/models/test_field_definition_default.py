"""Unit tests for ``FieldDefinition.default`` (P6 of the type-vocab work).

The SSOT field now carries an optional ``default`` whose Python type must back
the canonical ``data_type``. Validation happens at config-parse time (a model
validator), mirroring ``AttributeSchemaEntry.validate_default_type`` but over
the canonical vocabulary — and keeping ``bool`` out of the numeric types.
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols.field_definition import FieldDefinition


# ---------------------------------------------------------------------------
# Accepted defaults — value type matches the canonical data_type
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "data_type, value",
    [
        ("string", "n/a"),
        ("uuid", "00000000-0000-0000-0000-000000000000"),
        ("date", "2026-01-01"),
        ("time", "12:00:00"),
        ("timestamp", "2026-01-01T00:00:00Z"),
        ("binary", "AA=="),
        ("integer", 0),
        ("bigint", 9_000_000_000),
        ("double", 1.5),
        ("numeric", 2),          # int is a valid numeric default
        ("boolean", True),
        ("boolean", False),
        ("jsonb", {"any": "shape"}),
        ("jsonb", [1, 2, 3]),
    ],
)
def test_default_accepted_when_type_matches(data_type: str, value) -> None:
    fd = FieldDefinition(name="f", data_type=data_type, default=value)
    assert fd.default == value


def test_default_none_is_always_allowed() -> None:
    fd = FieldDefinition(name="f", data_type="geometry", default=None)
    assert fd.default is None


# ---------------------------------------------------------------------------
# Rejected defaults — value type does not match
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "data_type, value",
    [
        ("integer", "1"),        # string for an int column
        ("integer", 1.5),        # float for an int column
        ("integer", True),       # bool is NOT an int default
        ("bigint", True),
        ("double", "1.5"),
        ("double", True),        # bool is NOT a number default
        ("boolean", 1),          # int is NOT a boolean default
        ("boolean", "true"),
        ("string", 1),
        ("uuid", 1),
        ("timestamp", 0),        # epoch-int rejected; ISO string required
    ],
)
def test_default_rejected_on_type_mismatch(data_type: str, value) -> None:
    with pytest.raises(ValueError):
        FieldDefinition(name="f", data_type=data_type, default=value)


def test_default_rejected_on_geometry() -> None:
    with pytest.raises(ValueError, match="geometry"):
        FieldDefinition(name="geom", data_type="geometry", default="POINT(0 0)")
