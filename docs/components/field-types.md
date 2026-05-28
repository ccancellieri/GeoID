# Field Types

This is the single source of truth for the `data_type` vocabulary used in a
collection's `items_schema` and `attribute_schema`. It is rooted in GDAL/OGR's
type system (the natural source of truth for geospatial field types, surfaced by
`gdalinfo`), but uses one normalized, unambiguous token per concept rather than
copying OGR's print names verbatim.

The vocabulary itself lives in `src/dynastore/models/field_types.py`
(`DataType`, `DataSubtype`, the translation tables, and `canonical_data_type()`).
Every storage driver maps *from* its native engine types *to* this vocabulary
when introspecting, and *from* this vocabulary *to* its native types when
materializing — so the persistence type of a value never depends on which reader
saw it first.

## The two type axes

Two distinct vocabularies appear in the configs. They are **not** the same:

- **Logical** — `items_schema.fields[].data_type`. The canonical token the field
  *means*. Case-insensitive (`STRING` == `string`). Validated by
  `canonical_data_type()`; anything outside the canonical set (plus the
  temporary aliases below) is rejected.
- **Physical** — `attribute_schema[].type`. The PostgreSQL column type the value
  is *stored as*. It accepts native PostgreSQL names directly (`TEXT`,
  `INTEGER`, `VARCHAR(255)`, `TIMESTAMPTZ`, …) **and** any canonical `data_type`
  token, which is auto-mapped to the matching PostgreSQL type. The native
  PostgreSQL names are not deprecated.

## Feeding `gdalinfo` types

You do not paste OGR print names into a schema by hand — the **derive** path
translates them for you. `POST …/items-schema/derive` reads a stored `gdalinfo`
blob and runs each OGR `(type, subtype)` through `ogr_to_canonical()`
(`src/dynastore/models/field_types.py`), so the persisted schema holds canonical
tokens. For example OGR `Real` → canonical `double`. This is the supported,
recommended way to use `gdalinfo` output as schema input.

The OGR-name → canonical translation (`_OGR_TYPE` / `_OGR_SUBTYPE`):

| OGR print name (`gdalinfo`) | Canonical `data_type` |
|------|------|
| `Integer` | `integer` |
| `Integer64` | `bigint` |
| `Real` | `double` |
| `String` | `string` |
| `Date` | `date` |
| `Time` | `time` |
| `DateTime` | `timestamp` |
| `Binary` | `binary` |
| `IntegerList` / `Integer64List` / `RealList` / `StringList` | `jsonb` |

OGR subtypes refine the base: `Boolean`/`JSON`/`UUID` *promote* the base to
`boolean`/`jsonb`/`uuid`; `Int16`/`Float32` keep their base (`integer`/`double`)
and record the subtype. Unknown OGR types default to `string`.

> **Caveat (today):** this OGR-name table is consulted **only** on the derive
> path. A hand-authored `items_schema.fields[].data_type` goes through
> `canonical_data_type()`, which knows the canonical tokens plus the deprecated
> aliases below — but **not** the raw OGR print names. So `Integer64` typed
> directly into a config is rejected, while `Real` happens to validate only
> because it is also a deprecated SQL alias (→ `double`). Use `derive`; do not
> paste OGR names into configs.

---

## Works today — canonical vocabulary (use these)

| Canonical `data_type` (logical) | PostgreSQL column (physical) | JSON wire type |
|------|------|------|
| `string` | `TEXT` | string |
| `integer` | `INTEGER` | integer (32-bit) |
| `bigint` | `BIGINT` | integer (64-bit) |
| `double` | `FLOAT` (== `float8` == double precision) | number (binary float) |
| `numeric` | `NUMERIC` | number (exact decimal) |
| `boolean` | `BOOLEAN` | boolean |
| `date` | `DATE` | string (`format: date`) |
| `time` | `TIME` | string (`format: time`) |
| `timestamp` | `TIMESTAMPTZ` | string (`format: date-time`, stored UTC) |
| `binary` | `BYTEA` | string (base64) |
| `jsonb` | `JSONB` | object |
| `uuid` | `UUID` | string (`format: uuid`) |
| `geometry(<type>,<srid>)` | `geometry(<type>,<srid>)` | object (GeoJSON) |

- `geometry` is parametrized — write it with its type and SRID, e.g.
  `geometry(MultiPolygon,4326)`.
- An empty or omitted `data_type` defaults to `string` (the safe universal
  default — every backend can store text).
- `date`, `time`, and `timestamp` are deliberately distinct and never collapse
  into one another. There is no naive-timestamp variant: instants are UTC.

### Subtypes (optional refinement)

`items_schema.fields[].subtype` carries the OGR subtype so it is not flattened
away. One of: `boolean`, `int16`, `float32`, `json`, `uuid`.

- `boolean` / `json` / `uuid` **promote** the base `data_type` to
  `boolean` / `jsonb` / `uuid` so the value materializes natively.
- `int16` / `float32` keep their base (`integer` / `double`) today; the subtype
  is recorded for the planned narrowing below.

---

## Deprecated — accepted now, will be removed

Configs authored before the canonical vocabulary landed may use SQL/legacy
spellings. They live in a separate, explicitly temporary table
(`src/dynastore/models/legacy_type_aliases.py`) so the canonical enum stays
alias-free; `canonical_data_type()` consults it only as a fallback before
raising. **This is a migration window — fold to the canonical token on the left;
the aliases will be removed.**

| Canonical | Deprecated spellings folded to it |
|------|------|
| `string` | `text`, `varchar`, `char`, `character`, `character varying`, `str`, `keyword` |
| `integer` | `int`, `int4`, `int2`, `int32`, `smallint` |
| `bigint` | `int8`, `int64`, `long` |
| `double` | `float`, `float8`, `real`, `double precision` |
| `numeric` | `decimal`, `number` |
| `boolean` | `bool` |
| `timestamp` | `datetime`, `timestamptz`, `timestamp with time zone`, `timestamp without time zone` |
| `jsonb` | `json` |
| `uuid` | `guid` |
| `binary` | `bytea`, `blob` |

`date` and `time` are kept distinct and have no aliases (only the zoned/legacy
spellings fold into `timestamp`).

This folding applies to the **logical** `data_type` only. On the **physical**
`attribute_schema[].type`, `TEXT` / `INTEGER` / `TIMESTAMPTZ` / … are native
PostgreSQL column names — not deprecated.

### Why `double`, not `real`?

`real` is the question this vocabulary most often raises, so it is worth stating
explicitly:

1. **One token per concept.** "Binary floating point" has many spellings — OGR
   `Real`, SQL `double precision`/`float8`/`real`, JSON `number`. If the schema
   accepted all of them, every driver (PG, ES, Iceberg, DuckDB, BigQuery) would
   have to handle the full cross-product on every read and materialize. The SSOT
   collapses them to one token: `double`.
2. **`real` is a footgun.** In SQL, `REAL` is **32-bit** single precision
   (`float4`); OGR `Real` (`OFTReal`) is **64-bit**. Same word, different width.
   Keeping `real` canonical would invite silent precision loss. `double` is
   unambiguous and maps cleanly to PG `FLOAT`, Arrow/Parquet `double`, ES
   `double`.
3. **Translation belongs in the drivers/derive step, not a global alias table.**
   Producers emit canonical directly; the native→canonical translation lives in
   each driver and in `ogr_to_canonical()`. The legacy alias table is a
   crutch, not the design.

Deprecating the `real` *token* does not remove `gdalinfo` support: OGR `Real` is
still translated to `double` at derive time via `_OGR_TYPE`, and always will be.

---

## Coming tomorrow — planned, not yet wired

- **Subtype-aware physical narrowing.** Today `int16` / `float32` widen to PG
  `INTEGER` / `FLOAT` (a safe default). Planned: narrow to `SMALLINT` / `REAL`
  using the recorded subtype. Tracked in the `CANONICAL_TO_PG_DDL` note in
  `src/dynastore/models/field_types.py`.
- **Removal of the deprecated alias table.** Once configs and the derive path
  all emit canonical tokens, `src/dynastore/models/legacy_type_aliases.py` and
  its single fallback call in `canonical_data_type()` are deleted. The
  "Deprecated" table above then stops validating.

---

## Source of truth

| Concern | Location |
|------|------|
| Canonical enum, subtypes, translation tables | `src/dynastore/models/field_types.py` |
| Deprecated alias table (temporary) | `src/dynastore/models/legacy_type_aliases.py` |
| `FieldDefinition` (the schema field model) | `src/dynastore/models/protocols/field_definition.py` |
| `gdalinfo` → schema derive | `src/dynastore/tasks/ingestion/schema_from_gdalinfo.py` |

## See also

- [Items Schema](items_schema.md) — the `items_schema` config that
  *uses* this `data_type` vocabulary: the full field surface, access
  intent, constraints, and validate-time guards.
