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

from dynastore.models.field_types import CANONICAL_TO_PG_DDL
from dynastore.models.protocols.field_definition import FieldAccess, FieldCapability, FieldDefinition
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


# Canonical ``data_type`` -> PG type name. The table lives in the canonical
# vocabulary SSOT (``dynastore.models.field_types``); aliased here for the
# single use-site below, which keeps its tolerant TEXT fallback.
_DATA_TYPE_TO_PG_NAME = CANONICAL_TO_PG_DDL


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


# Column-implying capabilities for the AUTO access rule (Rule 4/5). FILTERABLE,
# SORTABLE and INDEXED benefit from a native PG column; GROUPABLE / AGGREGATABLE
# / SPATIAL / FULLTEXT do not (see the precedence comment below). INDEXED stays
# here deliberately (#1291) — it is driver-REPORTED, not authored, but an AUTO
# field that already carries it should keep materialising to a column to preserve
# the #1293 behaviour exactly. Authors request a fast column via FieldAccess.FAST.
_COLUMN_CAPS: "frozenset[FieldCapability]" = frozenset({
    FieldCapability.FILTERABLE,
    FieldCapability.SORTABLE,
    FieldCapability.INDEXED,
})


def schema_field_materializes_as_column(
    fd: FieldDefinition,
    *,
    default_access: FieldAccess = FieldAccess.AUTO,
) -> bool:
    """Decide whether a schema field becomes a native (sidecar) column.

    This is the single column-synthesis precedence used by both the PG bridge
    (:func:`bridge_schema_to_attribute_sidecar`) and the driver-agnostic
    projection (:func:`...field_projection.materialize_feature_fields`). It does
    NOT decide PG-specific layout — only the portable "is this field
    materialised/queryable as a first-class column" question (#1291).

    Geometry is never an attribute column (it is owned by the geometry sidecar /
    driver) and returns ``False`` regardless of access/capabilities.

    Precedence (first match wins):

    1. Hard constraint (``required`` or ``unique``) → column (a NOT-NULL /
       UNIQUE constraint cannot live inside a JSONB blob).
    2. Effective access ``FAST`` → column.
    3. Effective access ``COMPACT`` → JSONB (no column).
    4/5. Effective access ``AUTO`` → column iff the field declares a
       column-implying capability (:data:`_COLUMN_CAPS`); else JSONB.

    ``Effective access`` = the field's own ``access`` unless it is AUTO, in which
    case ``default_access`` (the schema-wide intent) applies.
    """
    if (fd.data_type or "").lower().startswith("geometry"):
        return False
    if bool(fd.required or fd.unique):
        return True  # Rule 1
    field_access = getattr(fd, "access", FieldAccess.AUTO)
    effective_access = (
        field_access if field_access != FieldAccess.AUTO else default_access
    )
    if effective_access == FieldAccess.FAST:
        return True  # Rule 2
    if effective_access == FieldAccess.COMPACT:
        return False  # Rule 3
    # Rule 4/5 — AUTO: capability-driven.
    return bool(set(fd.capabilities or []) & _COLUMN_CAPS)


def bridge_schema_to_attribute_sidecar(
    schema: "Optional[ItemsSchema]",
    sidecar: "FeatureAttributeSidecarConfig",
) -> "FeatureAttributeSidecarConfig":
    """Merge ``ItemsSchema.fields`` into the attributes sidecar.

    For every ``FieldDefinition`` in ``schema.fields``:

    - If an ``AttributeSchemaEntry`` with the same ``name`` already exists,
      overlay ``nullable = not fd.required`` and ``unique = fd.unique``.
    - Otherwise, decide whether to synthesise a native column. The per-field
      precedence (constraint → FAST → COMPACT → AUTO+capability) is the single
      :func:`schema_field_materializes_as_column` rule.

    Silent-drop guard (#1488/#1491 follow-up): the sidecar's
    ``resolved_storage_mode`` (see ``attributes.py:144-150``) becomes
    COLUMNAR-only the moment *any* attribute_schema entry exists under
    AUTOMATIC mode (or when COLUMNAR is set explicitly). The DDL branch in
    ``attributes.py:399-449`` then creates physical columns ONLY — there is
    no JSONB blob on disk to catch fields that the per-field precedence
    would have left for JSONB. To make a silent drop architecturally
    impossible, every non-geometry items_schema field is promoted to an
    ``AttributeSchemaEntry`` whenever the sidecar will resolve COLUMNAR-only.
    When the resolution will be JSONB (AUTOMATIC + no constraint/FAST/cap
    field anywhere in the schema, or explicit JSONB), the blob catches
    un-promoted fields and the per-field precedence is the only gate.

    Returns a new ``FeatureAttributeSidecarConfig`` so callers can replace it
    in ``col_config.sidecars``. If ``schema`` is None or has no fields,
    the input sidecar is returned unchanged.
    """
    if schema is None or not getattr(schema, "fields", None):
        return sidecar

    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeSchemaEntry,
        AttributeStorageMode,
        PostgresType,
    )

    existing: Dict[str, AttributeSchemaEntry] = {}
    order: list[str] = []
    for entry in sidecar.attribute_schema or []:
        existing[entry.name] = entry
        order.append(entry.name)

    # Schema-wide default access intent (#1291). A field that leaves its own
    # ``access`` at AUTO inherits this. ``default_access=FAST`` is the portable
    # successor of the old ``materialize_fields_as_columns=True`` — lift every
    # field into a native column. AUTO keeps the historical "constraints + queryable
    # capabilities only" behaviour so sparse schemas don't widen tables unintentionally.
    schema_default_access = getattr(schema, "default_access", FieldAccess.AUTO)

    # Project the sidecar's eventual ``resolved_storage_mode`` so the loop
    # below can decide whether un-promoted fields will have a JSONB blob to
    # land in. Mirror of the resolution at ``attributes.py:144-150``:
    # explicit COLUMNAR/JSONB pin the mode; AUTOMATIC resolves to COLUMNAR
    # the moment any column entry exists (existing entries already on the
    # sidecar, or schema fields the per-field precedence will promote).
    sidecar_mode = getattr(sidecar, "storage_mode", AttributeStorageMode.AUTOMATIC)
    if sidecar_mode == AttributeStorageMode.COLUMNAR:
        force_all_columnar = True
    elif sidecar_mode == AttributeStorageMode.JSONB:
        force_all_columnar = False
    else:  # AUTOMATIC
        force_all_columnar = bool(existing) or any(
            (not (fd.data_type or "").lower().startswith("geometry"))
            and schema_field_materializes_as_column(
                fd, default_access=schema_default_access,
            )
            for fd in schema.fields.values()
        )

    changed = False
    for name, fd in schema.fields.items():
        # Geometry is owned by the geometry sidecar / driver, never an attribute
        # column. It is neither a constraint column nor an attribute column, so it
        # must be skipped before any column-synthesis decision (and before the
        # existing-entry overlay) — regardless of capabilities or the schema-wide
        # ``default_access`` intent. The tolerant ``startswith`` mirrors the
        # geometry/geography check at the top of this module.
        if (fd.data_type or "").lower().startswith("geometry"):
            continue
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
        # Decide whether to synthesise a native column for this field. The
        # per-field precedence (hard constraint → FAST → COMPACT → AUTO+
        # capability) is the single :func:`schema_field_materializes_as_column`
        # rule, shared with the driver-agnostic projection so PG never
        # re-derives it (#1291). When ``force_all_columnar`` is set, the
        # bridge overrides the JSONB-routing arm of the precedence — the
        # JSONB blob would not exist on disk, so leaving a field there is
        # equivalent to dropping it at ingest (the #1488 class).
        if not (
            force_all_columnar
            or schema_field_materializes_as_column(
                fd, default_access=schema_default_access,
            )
        ):
            continue  # field stays in JSONB (blob exists in this branch)
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


def reconcile_attribute_schema_to_columns(
    entries: "list[Any]",
    physical_columns: "set[str]",
) -> "tuple[list[Any], list[str]]":
    """Drop advertised attribute columns that are not physically present.

    The write-side inverse of :func:`bridge_schema_to_attribute_sidecar`.
    The bridge *adds* an ``AttributeSchemaEntry`` for every items_schema field
    when a sidecar resolves COLUMNAR-only; this *removes* any entry whose
    physical column does not actually exist in the materialised sidecar table.

    Why this is needed (#1489 follow-up on #1488/#1491): ``ensure_storage``
    emits the sidecar table with ``CREATE TABLE IF NOT EXISTS``. On an
    already-materialised collection that DDL is a no-op, so a column the bridge
    newly promoted into ``attribute_schema`` never reaches the table. Persisting
    that config anyway makes the sidecar advertise a ``SELECT`` of a column that
    does not exist → ``UndefinedColumnError`` at read time (the #1491 crash
    class). The app may not ``ALTER TABLE ADD COLUMN`` to heal it (no in-place
    DDL), so the config is reconciled down to physical reality instead: the
    field then degrades to the read-side silent-skip + WARN path, and a fresh
    (re)provision remains the only way to actually add the column.

    Column names are compared exactly — sidecar columns are created with quoted
    identifiers, so PostgreSQL preserves their case in ``information_schema``.

    Returns ``(kept_entries, dropped_names)`` preserving the input order.
    """
    kept = [e for e in entries if e.name in physical_columns]
    dropped = [e.name for e in entries if e.name not in physical_columns]
    return kept, dropped


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
