"""Derive a Draft-2020-12 JSON-Schema for a feature's ``properties`` from
driver-agnostic ``FieldDefinition`` declarations.

Pure, dependency-light: the collection's items-schema becomes the single source
of truth for both write-validation and the produced feature shape.
"""

from typing import Dict, Literal, Optional

from dynastore.models.protocols.field_definition import FieldDefinition

# Lowercase data_type -> JSON-Schema property fragment.
_TYPE_MAP: Dict[str, dict] = {
    "text": {"type": "string"},
    "varchar": {"type": "string"},
    "varchar_255": {"type": "string"},
    "string": {"type": "string"},
    "uuid": {"type": "string"},
    "integer": {"type": "integer"},
    "int": {"type": "integer"},
    "bigint": {"type": "integer"},
    "float": {"type": "number"},
    "numeric": {"type": "number"},
    "number": {"type": "number"},
    "double": {"type": "number"},
    "boolean": {"type": "boolean"},
    "bool": {"type": "boolean"},
    "timestamp": {"type": "string", "format": "date-time"},
    "timestamptz": {"type": "string", "format": "date-time"},
    "datetime": {"type": "string", "format": "date-time"},
    "date": {"type": "string", "format": "date"},
    "geometry": {"type": "object"},
    "json": {"type": "object"},
    "jsonb": {"type": "object"},
    "object": {"type": "object"},
}


def derive_wire_schema(
    fields: Dict[str, FieldDefinition],
    *,
    strict: bool = False,
    purpose: Literal["read", "write"] = "read",
) -> Optional[dict]:
    """Build a Draft-2020-12 ``object`` schema for a feature's ``properties``.

    Returns ``None`` when ``fields`` is empty (blob collection — nothing to
    validate).

    ``purpose`` selects which structural constraints the schema carries; in
    both cases the per-field VALUE constraints (type, enum, minimum, maximum,
    maxLength, pattern, format) are always emitted:

    - ``"read"`` (default) — the schema published on the wire and used to
      describe a collection. ``required`` is included, and ``strict=True``
      adds ``additionalProperties: false``.
    - ``"write"`` — the schema fed to the write-path value validator. It
      OMITS both ``required`` and ``additionalProperties`` because those are
      already enforced earlier on the write path by separate mechanisms:
      ``required`` by the ``NOT NULL`` sidecar columns derived from the items
      schema (or, for non-native backends, the ``check_required`` app-level
      fallback), and unknown-key rejection by the strict-unknown-fields
      check. Re-asserting them here would raise a second, conflicting 422 for
      the same input.
    """
    if not fields:
        return None

    include_required = purpose == "read"
    include_additional_properties = purpose == "read"

    properties: Dict[str, dict] = {}
    required = []
    for key, fd in fields.items():
        prop = dict(_TYPE_MAP.get((fd.data_type or "").lower(), {"type": "string"}))

        if fd.max_length is not None:
            prop["maxLength"] = fd.max_length
        if fd.minimum is not None:
            prop["minimum"] = fd.minimum
        if fd.maximum is not None:
            prop["maximum"] = fd.maximum
        if fd.enum is not None:
            prop["enum"] = fd.enum
        if fd.pattern is not None:
            prop["pattern"] = fd.pattern
        # Field-level format overrides any date/date-time default.
        if fd.format is not None:
            prop["format"] = fd.format

        properties[key] = prop
        if fd.required is True:
            required.append(key)

    schema: dict = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": properties,
    }
    if include_required:
        schema["required"] = sorted(required)
    if strict and include_additional_properties:
        schema["additionalProperties"] = False
    return schema
