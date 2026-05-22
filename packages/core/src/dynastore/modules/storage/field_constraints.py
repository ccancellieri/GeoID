#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Service-layer fallback enforcement for ``FieldDefinition`` constraints.

Used only when the primary write driver lacks
``Capability.REQUIRED_ENFORCEMENT`` / ``UNIQUE_ENFORCEMENT`` AND
``ItemsSchema.allow_app_level_enforcement=True``.
Drivers that advertise native enforcement rely on their own DDL and must
not call these helpers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Iterable, Mapping, Optional

from dynastore.models.protocols.field_definition import FieldCapability, FieldDefinition
from dynastore.modules.storage.errors import (
    RequiredFieldMissingError,
    UniqueConstraintViolationError,
    UnknownFieldsError,
)


# System-level fields that always pass the strict-unknown-fields check
# regardless of ItemsSchema.fields contents. These are platform
# concerns (item identity, geometry, properties bag) — not user data fields,
# so requiring them in every ItemsSchema would be ceremonial noise.
_STRICT_MODE_SYSTEM_FIELDS = frozenset({
    "id", "geoid", "geometry", "bbox", "type", "properties",
    "links", "assets", "stac_version", "stac_extensions", "collection",
})

if TYPE_CHECKING:
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        FeatureAttributeSidecarConfig,
    )
    from dynastore.modules.storage.driver_config import ItemsSchema


# Canonical ``data_type`` (see ``dynastore.models.field_types``) -> PG type name.
# Keyed on canonical tokens only; inputs are already canonical (validated on
# FieldDefinition), and the lookup defaults to TEXT for any value that bypassed
# validation rather than raising mid-DDL. The (type, subtype) refinement is
# intentionally not used yet — Boolean/JSON/UUID subtypes already promote their
# base type to boolean/jsonb/uuid upstream, and Int16/Float32 stay INTEGER/FLOAT
# (a safe widening) until subtype-aware narrowing (SMALLINT/REAL) is wired.
_DATA_TYPE_TO_PG_NAME: Dict[str, str] = {
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


def pg_native_to_canonical(pg_type: str) -> str:
    """Map a PostgreSQL native type name to the canonical ``data_type``.

    Accepts both ``PostgresType`` enum values (``TEXT``, ``TIMESTAMPTZ``,
    ``VARCHAR(255)``, ``BYTEA`` …) and ``information_schema`` spellings
    (``character varying``, ``timestamp with time zone``, ``double precision``,
    ``USER-DEFINED`` …). This is the PG driver's own native → canonical map; the
    canonical → native direction lives in ``_DATA_TYPE_TO_PG_NAME``. There is no
    canonical range type, so ``*range`` collapses to ``timestamp`` (the precise
    bounds are exposed separately as start/end fields); arrays collapse to
    ``jsonb`` (consistent with OGR list types).
    """
    t = (pg_type or "").strip().lower()
    if not t:
        return "string"
    if "geometry" in t or "geography" in t or t == "user-defined":
        return "geometry"
    if "range" in t:               # tstzrange / tsrange / daterange
        return "timestamp"
    if "timestamp" in t:
        return "timestamp"
    if t.startswith("date"):
        return "date"
    if t.startswith("time"):       # time / time with time zone
        return "time"
    if "bigint" in t or t in ("int8", "bigserial"):
        return "bigint"
    if t == "int" or any(k in t for k in ("smallint", "integer", "int4", "int2", "serial", "tinyint")):
        return "integer"
    if any(k in t for k in ("double", "real", "float")):
        return "double"
    if "numeric" in t or "decimal" in t:
        return "numeric"
    if "bool" in t:
        return "boolean"
    if "uuid" in t:
        return "uuid"
    if "json" in t:
        return "jsonb"
    if "bytea" in t or "binary" in t:
        return "binary"
    if "array" in t or t.endswith("[]"):
        return "jsonb"
    return "string"


def bridge_schema_to_attribute_sidecar(
    schema: "Optional[ItemsSchema]",
    sidecar: "FeatureAttributeSidecarConfig",
) -> "FeatureAttributeSidecarConfig":
    """Merge ``ItemsSchema.fields`` into the attributes sidecar.

    For every ``FieldDefinition`` in ``schema.fields``:

    - If an ``AttributeSchemaEntry`` with the same ``name`` already exists,
      overlay ``nullable = not fd.required`` and ``unique = fd.unique``.
    - Otherwise, append a new entry inferring the PG type from ``fd.data_type``
      (defaults to ``TEXT``). Only fields carrying a constraint
      (``required`` or ``unique``) are synthesised; plain fields are left for
      the existing JSONB / attribute_schema paths.

    Returns a new ``FeatureAttributeSidecarConfig`` so callers can replace it
    in ``col_config.sidecars``. If ``schema`` is None or has no fields,
    the input sidecar is returned unchanged.
    """
    if schema is None or not getattr(schema, "fields", None):
        return sidecar

    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeSchemaEntry,
        PostgresType,
    )

    existing: Dict[str, AttributeSchemaEntry] = {}
    order: list[str] = []
    for entry in sidecar.attribute_schema or []:
        existing[entry.name] = entry
        order.append(entry.name)

    # PR-C: ``materialize_fields_as_columns`` opts into lifting EVERY
    # declared field as a sidecar column, regardless of constraint. Default
    # False keeps the historical "constraints-only" behaviour so sparse
    # schemas don't widen tables unintentionally.
    materialize_all = bool(getattr(schema, "materialize_fields_as_columns", False))

    changed = False
    for name, fd in schema.fields.items():
        has_constraint = bool(fd.required or fd.unique)
        entry = existing.get(name)
        if entry is not None:
            new_nullable = not fd.required if fd.required else entry.nullable
            new_unique = True if fd.unique else entry.unique
            # SSOT field default wins when declared; silence keeps the entry's own.
            new_default = fd.default if fd.default is not None else entry.default
            if (
                entry.nullable != new_nullable
                or entry.unique != new_unique
                or entry.default != new_default
            ):
                existing[name] = entry.model_copy(
                    update={
                        "nullable": new_nullable,
                        "unique": new_unique,
                        "default": new_default,
                    }
                )
                changed = True
            continue
        # Decide whether to synthesise a native column for this field.
        #
        # Precedence (evaluated in order; first matching rule wins):
        #
        # Rule 1 — Hard DB constraint: unique=True or required=True forces a
        #   column regardless of ``fd.materialize``.  A UNIQUE / NOT-NULL
        #   constraint cannot be enforced inside a JSONB blob.
        #
        # Rule 2 — ``fd.materialize is True``: explicit opt-in → COLUMN.
        #
        # Rule 3 — ``fd.materialize is False``: explicit opt-out → JSONB.
        #   This beats capability-driven lifting *and* the schema-level
        #   ``materialize_fields_as_columns`` flag.  Rule 1 was already
        #   checked above, so False here is only reached when there is no
        #   hard constraint.
        #
        # Rule 4/5 — ``fd.materialize is None`` (driver decides):
        #   COLUMN if the schema flag ``materialize_fields_as_columns`` is
        #   set OR the field declares a capability that benefits from a
        #   native column (FILTERABLE, SORTABLE, or INDEXED).
        #   Otherwise the field stays in JSONB.
        #
        # Column-implying capabilities: FILTERABLE, SORTABLE, INDEXED.
        # GROUPABLE / AGGREGATABLE / SPATIAL do not mandate a native column
        # because each can be satisfied via functional JSONB indexes.
        _COLUMN_CAPS = frozenset({
            FieldCapability.FILTERABLE,
            FieldCapability.SORTABLE,
            FieldCapability.INDEXED,
        })
        if has_constraint:
            pass  # Rule 1 — fall through to synthesis
        elif fd.materialize is True:
            pass  # Rule 2 — explicit opt-in
        elif fd.materialize is False:
            continue  # Rule 3 — explicit opt-out; skip column synthesis
        else:
            # Rule 4/5 — materialize is None; driver decides from context
            has_column_cap = bool(set(fd.capabilities or []) & _COLUMN_CAPS)
            if not (materialize_all or has_column_cap):
                continue  # Rule 4 — no trigger; field stays in JSONB
        # ``data_type`` is already canonical (validated on FieldDefinition);
        # tolerant lookup so a bypassed/unknown value degrades to TEXT rather
        # than raising deep in DDL generation.
        pg_name = _DATA_TYPE_TO_PG_NAME.get(
            (fd.data_type or "").lower(), "TEXT"
        )
        try:
            pg_type = PostgresType(pg_name)
        except ValueError:
            pg_type = PostgresType.TEXT
        existing[name] = AttributeSchemaEntry(
            name=name,
            type=pg_type,
            nullable=not fd.required,
            unique=bool(fd.unique),
            default=fd.default,
            description=fd.description if isinstance(fd.description, str) else None,
        )
        order.append(name)
        changed = True

    if not changed:
        return sidecar

    merged = [existing[n] for n in order]
    return sidecar.model_copy(update={"attribute_schema": merged})


def overlay_schema_flags(
    schema: "Optional[ItemsSchema]",
    fields: Dict[str, FieldDefinition],
) -> Dict[str, FieldDefinition]:
    """Overlay ``required``/``unique`` from ``schema`` onto live fields.

    Used by ``get_entity_fields()`` round-trip so callers see the stored
    constraints without needing a second config lookup.
    """
    if schema is None or not getattr(schema, "fields", None):
        return fields
    out: Dict[str, FieldDefinition] = {}
    for name, fd in fields.items():
        ft_field = schema.fields.get(name)
        if ft_field is None or not (ft_field.required or ft_field.unique):
            out[name] = fd
            continue
        out[name] = fd.model_copy(
            update={
                "required": bool(ft_field.required) or fd.required,
                "unique": bool(ft_field.unique) or fd.unique,
            }
        )
    return out


def _resolve_value(feature: Mapping[str, Any], field_name: str) -> Any:
    """Look up a field value in a GeoJSON feature.

    Checks top-level keys first (e.g. ``id``, ``geometry``), then
    ``properties``. Treats empty strings as missing.
    """
    if field_name in feature and feature[field_name] not in (None, ""):
        return feature[field_name]
    props = feature.get("properties") or {}
    if isinstance(props, Mapping):
        value = props.get(field_name)
        if value not in (None, ""):
            return value
    return None


def check_required(
    fields: Dict[str, FieldDefinition],
    features: Iterable[Mapping[str, Any]],
) -> None:
    """Raise ``RequiredFieldMissingError`` if any feature misses a required field."""
    required_names = [n for n, fd in fields.items() if fd.required]
    if not required_names:
        return
    for feature in features:
        for name in required_names:
            if _resolve_value(feature, name) is None:
                raise RequiredFieldMissingError(
                    f"required field '{name}' is missing or null", field=name,
                )


def check_strict_unknown_fields(
    allowed_fields: Iterable[str],
    features: Iterable[Mapping[str, Any]],
) -> None:
    """Raise ``UnknownFieldsError`` when any feature carries a property
    not in ``allowed_fields`` (ItemsSchema strict-mode enforcement).

    Inspects ``feature["properties"]`` and the top-level keys of each
    feature; system fields (id, geoid, geometry, bbox, properties, etc.)
    always pass regardless of ``allowed_fields``. Use when the
    collection's ``ItemsSchema.strict_unknown_fields=True``.

    The first offending feature triggers the raise (fail-fast). Callers
    that need batch-level violation aggregation should iterate manually.
    """
    allowed = set(allowed_fields)
    for feature in features:
        # Property-bag fields
        props = feature.get("properties") if isinstance(feature, Mapping) else None
        if isinstance(props, Mapping):
            offenders = [
                k for k in props.keys()
                if k not in allowed and k not in _STRICT_MODE_SYSTEM_FIELDS
            ]
            if offenders:
                raise UnknownFieldsError(
                    f"feature carries unknown fields under 'properties' "
                    f"not declared in ItemsSchema.fields: {sorted(offenders)}",
                    unknown_fields=sorted(offenders),
                    allowed_fields=sorted(allowed),
                )
        # Top-level keys outside the system-field whitelist
        if isinstance(feature, Mapping):
            top_offenders = [
                k for k in feature.keys()
                if k not in allowed and k not in _STRICT_MODE_SYSTEM_FIELDS
            ]
            if top_offenders:
                raise UnknownFieldsError(
                    f"feature carries unknown top-level fields not declared in "
                    f"ItemsSchema.fields: {sorted(top_offenders)}",
                    unknown_fields=sorted(top_offenders),
                    allowed_fields=sorted(allowed),
                )


async def check_unique(
    fields: Dict[str, FieldDefinition],
    features: Iterable[Mapping[str, Any]],
    *,
    exists: Callable[[str, Any], Awaitable[bool]],
) -> None:
    """Raise ``UniqueConstraintViolationError`` for duplicates.

    Checks both in-batch collisions and against existing storage via the
    caller-supplied ``exists(field_name, value) -> bool`` coroutine —
    typically a driver ``read_entities`` probe.
    """
    unique_names = [n for n, fd in fields.items() if fd.unique]
    if not unique_names:
        return
    seen: Dict[str, set] = {n: set() for n in unique_names}
    for feature in features:
        for name in unique_names:
            value = _resolve_value(feature, name)
            if value is None:
                continue
            if value in seen[name]:
                raise UniqueConstraintViolationError(
                    f"duplicate value for unique field '{name}' in batch",
                    field=name, value=value,
                )
            seen[name].add(value)
            if await exists(name, value):
                raise UniqueConstraintViolationError(
                    f"unique field '{name}' already has value {value!r}",
                    field=name, value=value,
                )
