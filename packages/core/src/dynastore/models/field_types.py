#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Canonical field-type vocabulary — the single source of truth for ``data_type``.

The canonical vocabulary is rooted in GDAL/OGR's type system (the natural
source of truth for geospatial field types, surfaced by ``gdalinfo``). Every
storage driver maps *from* its native engine types *to* this vocabulary when
introspecting, and *from* this vocabulary *to* its native types when
materializing — so the persistence type of a value no longer depends on which
reader first saw it.

``data_type`` is **strict**: :func:`canonical_data_type` accepts only the
canonical tokens (case-insensitively) and parametrized geometry
(``geometry(Point,4326)``); anything else raises. There is intentionally **no
alias / backward-compatibility layer** — producers emit canonical directly and
the (native engine type → canonical) translation lives in each driver, not in a
global alias table.

Dates and times are kept deliberately distinct: ``date`` (calendar day, no
time), ``time`` (wall-clock, no day) and ``timestamp`` (an instant, materialized
timezone-aware) never collapse into one another. There is intentionally no
naive-timestamp variant (geospatial instants are treated as UTC).

OGR field *subtypes* (``Boolean``/``Int16``/``Float32``/``JSON``/``UUID``) are
preserved on the optional ``subtype`` axis so they are no longer flattened away;
``Boolean``/``JSON``/``UUID`` additionally *promote* the base ``data_type`` to
``boolean``/``jsonb``/``uuid`` so each driver materializes them natively.
``Int16``/``Float32`` keep their base today and are recorded for the planned
narrowing to ``SMALLINT``/``REAL`` (see ``CANONICAL_TO_PG_DDL`` below).

The user-facing reference — the canonical vocabulary, the deprecated aliases in
:data:`LEGACY_DATA_TYPE_ALIASES`, and the planned changes — is
``docs/components/field-types.md``; keep it in sync with the tables in this module.
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, Optional, Tuple


# ---------------------------------------------------------------------------
# Legacy / SQL spelling → canonical data_type aliases
# ---------------------------------------------------------------------------
# These map pre-canonical configs that used SQL or informal spellings onto the
# strict canonical tokens so existing configs keep validating. Keys are
# lowercase; callers normalize case before lookup. Date/time/timestamp stay
# distinct — only the zoned/`datetime` spellings fold into ``timestamp``.
# The "Deprecated" column in ``docs/components/field-types.md`` is derived
# from this table; keep both in sync when entries change.
LEGACY_DATA_TYPE_ALIASES: Dict[str, str] = {
    # string family
    "text": "string",
    "varchar": "string",
    "char": "string",
    "character": "string",
    "character varying": "string",
    "str": "string",
    "keyword": "string",
    # 32-bit integer family
    "int": "integer",
    "int4": "integer",
    "smallint": "integer",
    "int2": "integer",
    "int32": "integer",
    # 64-bit integer family
    "int8": "bigint",
    "long": "bigint",
    "int64": "bigint",
    # binary floating point
    "float": "double",
    "float8": "double",
    "real": "double",
    "double precision": "double",
    # exact decimal
    "decimal": "numeric",
    "number": "numeric",
    # boolean
    "bool": "boolean",
    # instant (zoned/legacy spellings only — `date` and `time` stay distinct)
    "datetime": "timestamp",
    "timestamptz": "timestamp",
    "timestamp with time zone": "timestamp",
    "timestamp without time zone": "timestamp",
    # json
    "json": "jsonb",
    # uuid
    "guid": "uuid",
    # binary
    "bytea": "binary",
    "blob": "binary",
}


def normalize_legacy_data_type(low: Optional[str]) -> Optional[str]:
    """Map a lowercased legacy/SQL spelling to its canonical token, or ``None``.

    ``low`` must already be stripped + lowercased. Returns a canonical
    ``data_type`` string when ``low`` is a known legacy alias, otherwise
    ``None`` so the caller can fall through to its strict-reject path.
    """
    if not low:
        return None
    return LEGACY_DATA_TYPE_ALIASES.get(low)


class DataType(str, Enum):
    """The canonical, GDAL-rooted field type vocabulary for ``data_type``."""

    STRING = "string"
    INTEGER = "integer"      # 32-bit
    BIGINT = "bigint"        # 64-bit
    DOUBLE = "double"        # binary floating point
    NUMERIC = "numeric"      # exact decimal
    BOOLEAN = "boolean"
    DATE = "date"            # calendar day, no time-of-day
    TIME = "time"            # time-of-day, no date
    TIMESTAMP = "timestamp"  # instant (materialized timezone-aware)
    BINARY = "binary"
    JSONB = "jsonb"
    UUID = "uuid"
    GEOMETRY = "geometry"


class DataSubtype(str, Enum):
    """OGR field subtypes — an optional refinement of the base ``data_type``."""

    BOOLEAN = "boolean"
    INT16 = "int16"
    FLOAT32 = "float32"
    JSON = "json"
    UUID = "uuid"


CANONICAL_DATA_TYPES = frozenset(t.value for t in DataType)
CANONICAL_SUBTYPES = frozenset(s.value for s in DataSubtype)

# GDAL raster band data types (GDT_*), lowercased — the SSOT vocabulary for a
# raster band's type, consumed by STAC-raster / coverage expression via
# :func:`canonical_band_type`. Kept here so the vocabulary is complete and
# discoverable in one place.
GDAL_BAND_TYPES = frozenset({
    "byte", "int8", "uint16", "int16", "uint32", "int32",
    "uint64", "int64", "float32", "float64",
    "cint16", "cint32", "cfloat32", "cfloat64",
})


def canonical_data_type(value: Optional[str]) -> str:
    """Validate / canonicalize a ``data_type`` token.

    - ``None`` / empty → ``"string"`` (the safe universal default).
    - Parametrized geometry (e.g. ``"geometry(Point,4326)"``) is accepted and
      returned lowercased — callers rely on the ``"geometry"`` prefix and an
      embedded SRID, so it must survive unchanged apart from case.
    - A canonical token (case-insensitive) is returned lowercased.
    - A legacy/SQL spelling (``text``, ``int``, ``datetime`` …) is normalized to
      its canonical token via :data:`LEGACY_DATA_TYPE_ALIASES`.
    - Anything else raises ``ValueError``.
    """
    if value is None:
        return DataType.STRING.value
    raw = value.strip()
    if not raw:
        return DataType.STRING.value
    low = raw.lower()
    if low.startswith("geometry"):
        return low
    if low in CANONICAL_DATA_TYPES:
        return low
    aliased = normalize_legacy_data_type(low)
    if aliased is not None:
        return aliased
    raise ValueError(
        f"Unknown data_type {value!r}; expected one of "
        f"{sorted(CANONICAL_DATA_TYPES)} or a parametrized 'geometry(...)'."
    )


def normalize_subtype(value: Optional[str]) -> Optional[str]:
    """Canonicalize a subtype token; drop unknown / ``none`` to ``None``."""
    if value is None:
        return None
    low = str(value).strip().lower()
    if not low or low == "none":
        return None
    return low if low in CANONICAL_SUBTYPES else None


def canonical_band_type(value: Optional[str]) -> str:
    """Validate / canonicalize a raster band type against :data:`GDAL_BAND_TYPES`.

    GDAL emits capitalized band names (``"Byte"``, ``"Float32"``); they are
    accepted case-insensitively and returned in the lowercased canonical form
    (``"byte"``, ``"float32"``) — so the existing capitalized names keep working
    while the full GDAL set (including ``int8`` / ``int64`` / ``uint64``) is
    covered. Raises ``ValueError`` for anything outside the vocabulary; consumers
    that must degrade (e.g. STAC mapping) catch it rather than failing the call.
    """
    if value is None:
        raise ValueError("band type is required")
    low = value.strip().lower()
    if low in GDAL_BAND_TYPES:
        return low
    raise ValueError(
        f"Unknown raster band type {value!r}; expected one of "
        f"{sorted(GDAL_BAND_TYPES)}."
    )


# ---------------------------------------------------------------------------
# GDAL / OGR — the canonical SOURCE of geospatial field types
# ---------------------------------------------------------------------------

# OGR field type name (``ogr.GetFieldTypeName``) -> canonical base ``data_type``.
_OGR_TYPE: dict[str, str] = {
    "Integer": "integer",
    "Integer64": "bigint",
    "Real": "double",
    "String": "string",
    "Date": "date",
    "Time": "time",
    "DateTime": "timestamp",
    "Binary": "binary",
    "IntegerList": "jsonb",
    "Integer64List": "jsonb",
    "RealList": "jsonb",
    "StringList": "jsonb",
}

# OGR field subtype name (``ogr.GetFieldSubTypeName``) -> canonical subtype.
_OGR_SUBTYPE: dict[str, str] = {
    "Boolean": "boolean",
    "Int16": "int16",
    "Float32": "float32",
    "JSON": "json",
    "UUID": "uuid",
}


def ogr_to_canonical(
    type_name: str, subtype_name: Optional[str] = None,
) -> Tuple[str, Optional[str]]:
    """Map an OGR (type, subtype) pair to canonical ``(data_type, subtype)``.

    The ``Boolean`` / ``JSON`` / ``UUID`` subtypes *promote* the base type to
    ``boolean`` / ``jsonb`` / ``uuid`` so the value materializes natively;
    ``Int16`` / ``Float32`` keep their base (``integer`` / ``double``) and only
    record the subtype for drivers that can narrow the storage. Unknown OGR
    types default to ``string`` (every backend can store text).
    """
    base = _OGR_TYPE.get(type_name, "string")
    sub = _OGR_SUBTYPE.get(subtype_name) if subtype_name else None
    if sub == "boolean":
        return ("boolean", "boolean")
    if sub == "json":
        return ("jsonb", "json")
    if sub == "uuid":
        return ("uuid", "uuid")
    if sub in ("int16", "float32"):
        return (base, sub)
    return (base, None)


# ---------------------------------------------------------------------------
# Canonical -> persistence / wire translation tables
# ---------------------------------------------------------------------------

# Canonical ``data_type`` -> PostgreSQL type name, used to materialize sidecar
# columns. Keyed on canonical tokens only; inputs are already canonical
# (validated on FieldDefinition), so the single use-site defaults to TEXT for
# any value that bypassed validation rather than raising mid-DDL. The
# (type, subtype) refinement is intentionally not used yet — Boolean/JSON/UUID
# subtypes already promote their base type to boolean/jsonb/uuid upstream, and
# Int16/Float32 stay INTEGER/FLOAT (a safe widening) until subtype-aware
# narrowing (SMALLINT/REAL) is wired.
CANONICAL_TO_PG_DDL: dict[str, str] = {
    "string": "TEXT",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "double": "FLOAT",       # PG FLOAT == float8 == double precision
    "numeric": "NUMERIC",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "time": "TIME",
    "timestamp": "TIMESTAMPTZ",
    "binary": "BYTEA",
    "jsonb": "JSONB",
    "uuid": "UUID",
}

# Canonical ``data_type`` -> Draft-2020-12 JSON-Schema property fragment, used
# to derive the wire schema for a feature's ``properties``. Keyed on canonical
# tokens; the single use-site normalizes and defaults to ``{"type": "string"}``.
CANONICAL_TO_JSON_SCHEMA: dict[str, dict] = {
    "string": {"type": "string"},
    "uuid": {"type": "string", "format": "uuid"},
    "integer": {"type": "integer"},
    "bigint": {"type": "integer"},
    "double": {"type": "number"},
    "numeric": {"type": "number"},
    "boolean": {"type": "boolean"},
    "timestamp": {"type": "string", "format": "date-time"},
    "date": {"type": "string", "format": "date"},
    "time": {"type": "string", "format": "time"},
    "binary": {"type": "string", "contentEncoding": "base64"},
    "geometry": {"type": "object"},
    "jsonb": {"type": "object"},
}
