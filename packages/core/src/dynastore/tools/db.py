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

import re
from typing import Mapping, Optional, Sequence

# A set of common reserved SQL keywords to prevent identifier collision.
POSTGRES_RESERVED_WORDS = {
    'all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc',
    'asymmetric', 'both', 'case', 'cast', 'check', 'collate', 'column',
    'constraint', 'create', 'current_catalog', 'current_date',
    'current_role', 'current_time', 'current_timestamp', 'current_user',
    'default', 'deferrable', 'desc', 'distinct', 'do', 'else', 'end',
    'except', 'false', 'fetch', 'for', 'foreign', 'from', 'grant', 'group',
    'having', 'in', 'initially', 'intersect', 'into', 'leading', 'limit',
    'localtime', 'localtimestamp', 'not', 'null', 'offset', 'on', 'only',
    'or', 'order', 'placing', 'primary', 'references', 'returning',
    'select', 'session_user', 'some', 'symmetric', 'table', 'then', 'to',
    'trailing', 'true', 'union', 'unique', 'user', 'using', 'variadic',
    'when', 'where', 'window', 'with'
}

def sanitize_for_sql_identifier(value: str) -> str:
    """
    Sanitizes a string to make it a safe PostgreSQL identifier by replacing
    all non-alphanumeric characters (except underscore) with an underscore.
    This is used for creating safe names from arbitrary values (e.g., partition keys).
    """
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(value))

class InvalidIdentifierError(ValueError):
    """Raised when an identifier fails validation."""
    pass


def validate_sql_identifier(identifier: str) -> str:
    """
    Validates a string to ensure it is a safe identifier.
    
    Raises:
        InvalidIdentifierError: If the identifier does not meet constraints.
        
    Returns:
        str: The validated, lowercased identifier.
    """
    if not isinstance(identifier, str):
        raise TypeError("Identifier must be a string.")
    
    if not identifier:
        raise InvalidIdentifierError("Identifier cannot be empty.")
    
    # 0. Reject obviously-templated values up front.  A client that issues a
    #    request against ``/catalogs/{{m.catalog}}/...`` without substituting
    #    the placeholder otherwise sends the literal token down to the routing
    #    resolver, where it surfaces as an opaque ``routed-resolve unavailable``
    #    lookup miss.  Catch it here with an actionable message so the caller
    #    knows to substitute before issuing the request (see issue #1191).
    if "{{" in identifier or "}}" in identifier:
        raise InvalidIdentifierError(
            f"Identifier '{identifier}' contains an unsubstituted template "
            "placeholder ('{{...}}'); substitute it with a real value before "
            "issuing the request."
        )

    identifier_lower = identifier.lower()

    # 1. Check length constraint (max 63 characters).
    if len(identifier_lower) > 63:
        raise InvalidIdentifierError("Identifier must be 63 characters or less.")
        
    # 2. Check for reserved keywords.
    if identifier_lower in POSTGRES_RESERVED_WORDS:
        raise InvalidIdentifierError(f"Identifier '{identifier_lower}' is a reserved keyword.")
        
    # 3. Check character constraints
    #    Authorized chars: a-z, 0-9, _, ., -, > (for JSON paths like 'data->key' or 'schema.table')
    if not re.match(r"^[a-z_][a-z0-9_.>-]*$", identifier_lower):
        raise InvalidIdentifierError(
            "Identifier must start with a letter or underscore, and contain only "
            "lowercase letters, numbers, underscores, dots, or JSON operators (->)."
        )

    return identifier_lower


def validate_column_identifier(identifier: str) -> str:
    """
    Validate a user-supplied physical column name, preserving its case.

    Unlike ``validate_sql_identifier`` (which lowercases and permits ``.``/``>``/``-``
    for JSON/qualified paths), this enforces a plain SQL identifier so the name is
    safe to interpolate — quoted — into DDL/DML and to reuse verbatim as a
    SQLAlchemy bind-parameter name.

    Raises:
        InvalidIdentifierError: If the name is not a plain identifier.

    Returns:
        str: The validated column name, unchanged.
    """
    if not isinstance(identifier, str) or not identifier:
        raise InvalidIdentifierError("Column name must be a non-empty string.")

    if len(identifier) > 63:
        raise InvalidIdentifierError("Column name must be 63 characters or less.")

    if identifier.lower() in POSTGRES_RESERVED_WORDS:
        raise InvalidIdentifierError(
            f"Column name '{identifier}' is a reserved keyword."
        )

    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", identifier):
        raise InvalidIdentifierError(
            f"Column name '{identifier}' is invalid: it must start with a letter or "
            "underscore and contain only letters, digits, and underscores "
            "(no spaces, dots, or symbols)."
        )

    return identifier


def quote_ident(identifier: str, allow_star: bool = False) -> str:
    """
    Double-quote a single PostgreSQL identifier for safe interpolation into
    SQL text, escaping embedded ``"`` per the SQL standard (``"`` -> ``""``).

    Idempotent: an already-quoted identifier is returned unchanged. When
    ``allow_star`` is set, the bare wildcard ``*`` (e.g. a ``SELECT *``
    projection) is returned unquoted rather than wrapped — ``"*"`` is not a
    valid SQL wildcard. See #719.

    This only handles quoting/escaping — it does not validate character
    content. Callers that build identifiers from user input should run them
    through ``validate_sql_identifier``/``validate_column_identifier`` first
    (``qualify_table``/``qualify_column`` below do this for the common
    two-part and single-part cases).
    """
    if not isinstance(identifier, str):
        raise TypeError("Identifier must be a string.")
    if allow_star and identifier == "*":
        return identifier
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'


def qualify_table(schema: str, table: str) -> str:
    """
    Validate and quote a ``schema.table`` reference for safe interpolation
    into SQL text, e.g. ``qualify_table("my_schema", "notebooks")`` ->
    ``'"my_schema"."notebooks"'``.

    Raises:
        InvalidIdentifierError: If either part fails identifier validation.
    """
    validate_sql_identifier(schema)
    validate_column_identifier(table)
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def qualify_column(name: str) -> str:
    """
    Validate and quote a single column identifier for safe interpolation
    into SQL text, e.g. ``qualify_column("geoid")`` -> ``'"geoid"'``.

    Raises:
        InvalidIdentifierError: If the name fails column-identifier validation.
    """
    validate_column_identifier(name)
    return quote_ident(name)


def build_upsert(
    table: str,
    columns: Sequence[str],
    conflict_cols: Sequence[str],
    update_cols: Optional[Sequence[str]] = None,
    literal_values: Optional[Mapping[str, str]] = None,
    returning: Optional[Sequence[str]] = None,
) -> str:
    """
    Render a single-row ``INSERT INTO {table} (...) VALUES (...) ON CONFLICT
    (...) DO UPDATE SET col = EXCLUDED.col`` statement for the common
    single-PK upsert shape hand-written across the storage/config/notebooks
    modules.

    - ``table``: a table reference the caller has already quoted/qualified
      (e.g. via :func:`qualify_table`, or a literal ``"schema"."table"``
      string) — used verbatim, not touched here.
    - ``columns``: every column to insert. Each binds as ``:column_name``
      unless overridden via ``literal_values``. Quoted (escape-only, via
      :func:`quote_ident`) rather than validated — matching the convention
      used for payload-derived column names elsewhere in this module, so a
      name that already worked continues to.
    - ``conflict_cols``: columns forming the ``ON CONFLICT`` target.
    - ``update_cols``: columns refreshed with ``col = EXCLUDED.col`` on
      conflict (defaults to every column not in ``conflict_cols``). Pass an
      empty sequence for ``ON CONFLICT (...) DO NOTHING``.
    - ``literal_values``: column -> raw SQL expression (e.g.
      ``{"updated_at": "NOW()", "config_data": "CAST(:config_data AS jsonb)"}``)
      to inline directly into the VALUES clause instead of a bare bind. On
      conflict, ``col = EXCLUDED.col`` still re-evaluates to the same value
      within one statement, so this doesn't change ``NOW()``-stamping
      semantics.
    - ``returning``: columns to append as ``RETURNING col1, col2, ...``.

    This only covers the plain single-row, EXCLUDED-based case — CAS
    predicates, additive/COALESCE conflict semantics, and multi-row VALUES
    batches stay hand-written.

    Raises:
        ValueError: if ``columns`` or ``conflict_cols`` is empty.
    """
    if not columns:
        raise ValueError("build_upsert requires at least one column.")
    if not conflict_cols:
        raise ValueError("build_upsert requires at least one conflict column.")

    literal_values = literal_values or {}
    if update_cols is None:
        update_cols = [c for c in columns if c not in conflict_cols]

    col_list = ", ".join(quote_ident(c) for c in columns)
    value_list = ", ".join(
        literal_values[c] if c in literal_values else f":{c}" for c in columns
    )
    conflict_target = ", ".join(quote_ident(c) for c in conflict_cols)

    if update_cols:
        set_clause = ", ".join(
            f"{quote_ident(c)} = EXCLUDED.{quote_ident(c)}" for c in update_cols
        )
        conflict_action = f"DO UPDATE SET {set_clause}"
    else:
        conflict_action = "DO NOTHING"

    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({value_list}) "
        f"ON CONFLICT ({conflict_target}) {conflict_action}"
    )
    if returning:
        sql += f" RETURNING {', '.join(quote_ident(c, allow_star=True) for c in returning)}"
    return sql + ";"