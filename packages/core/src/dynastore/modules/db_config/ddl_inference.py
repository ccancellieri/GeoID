#    Copyright 2025 FAO
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

"""Auto-inference of DDL existence checks from CREATE statement patterns.

Extracted from query_executor.py to reduce file size while keeping the
DDL inference system self-contained.
"""

import re

# Process-local cache for DDL existence check results.
# Keyed by (object_type, schema, name).  Only positive results (exists=True)
# are cached — a False result means the DDL must run, and once it does the
# object will exist on the next check.  Cleared naturally on process restart
# (which is the only time previously-existing objects could disappear).
_ddl_existence_cache: dict[tuple[str, ...], bool] = {}


async def _cached_check_async(object_type: str, schema: str, name: str, check_fn, *args):
    """Check existence with a process-local positive-only cache.

    *check_fn* is called (not pre-invoked) only on cache miss, avoiding
    unawaited-coroutine warnings on cache hits.
    """
    key = (object_type, schema, name)
    if _ddl_existence_cache.get(key):
        return True
    result = await check_fn(*args)
    if result:
        _ddl_existence_cache[key] = True
    return result


def _infer_existence_check(sql_template: str):
    """Auto-infer an existence check from a CREATE DDL template.

    Parses the SQL template (which may still contain ``{schema}`` placeholders)
    and returns an async callable ``(conn, params, raw_params) -> bool`` that
    checks whether the target object already exists.

    Returns ``None`` for DDL that cannot be inferred (INSERT, DROP, ALTER, etc.)
    or for patterns that already contain ``IF NOT EXISTS`` on every statement.

    The returned callable resolves ``{schema}`` from *raw_params* at runtime
    (the full kwarg dict before TemplateQueryBuilder consumes identifiers).
    """
    if not sql_template or not isinstance(sql_template, str):
        return None

    trimmed = sql_template.strip()

    upper = trimmed.upper()

    # Skip non-CREATE DDL (INSERT, DROP, ALTER, GRANT, etc.)
    # Multi-statement DDL: check only the first statement
    first_stmt_upper = upper.split(";")[0].strip()
    if not first_stmt_upper.startswith("CREATE"):
        return None

    # Skip UPSERT patterns disguised as DDLQuery (INSERT ... ON CONFLICT)
    if "INSERT" in first_stmt_upper and "ON CONFLICT" in first_stmt_upper:
        return None

    # --- Pattern: CREATE TABLE IF NOT EXISTS {schema}.table_name ---
    # Also matches quoted schemas like "schema".table_name
    m = re.search(
        r'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+'
        r'(?:"?(\{?\w+\}?)"?\.)?'    # optional schema (with or without braces/quotes)
        r'"?(\w+)"?',                  # table name
        trimmed, re.IGNORECASE | re.DOTALL,
    )
    if m:
        raw_schema = m.group(1)  # e.g. "{schema}" or "events" or None
        table_name = m.group(2)

        # Check if it's a PARTITION OF — skip auto-check for partitions
        # as they are created from a parent table
        partition_check = trimmed[m.end():]
        if re.match(r'\s+PARTITION\s+OF\b', partition_check, re.IGNORECASE):
            return None

        async def _check_table(conn, params, raw_params):
            from .locking_tools import check_table_exists
            schema = _resolve_schema(raw_schema, raw_params)
            return await _cached_check_async(
                "table", schema, table_name,
                check_table_exists, conn, table_name, schema,
            )

        return _check_table

    # --- Pattern: CREATE [UNIQUE] INDEX IF NOT EXISTS idx_name ON {schema}.table ---
    m = re.search(
        r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+'
        r'"?(\w+)"?',                  # index name
        trimmed, re.IGNORECASE | re.DOTALL,
    )
    if m:
        index_name = m.group(1)
        # Extract schema from ON clause
        m_on = re.search(
            r'\bON\s+(?:"?(\{?\w+\}?)"?\.)',
            trimmed, re.IGNORECASE | re.DOTALL,
        )
        raw_schema = m_on.group(1) if m_on else None

        async def _check_index_query(conn, schema, idx_name):
            from .locking_tools import DQLQuery, ResultHandler
            q = DQLQuery(
                "SELECT 1 FROM pg_indexes WHERE schemaname = :schema AND indexname = :idx",
                result_handler=ResultHandler.SCALAR,
            )
            try:
                return await q.execute(conn, schema=schema, idx=idx_name) is not None
            except Exception:
                return False

        async def _check_index(conn, params, raw_params):
            schema = _resolve_schema(raw_schema, raw_params)
            return await _cached_check_async(
                "index", schema, index_name,
                _check_index_query, conn, schema, index_name,
            )

        return _check_index

    # --- Pattern: CREATE SCHEMA IF NOT EXISTS "schema" ---
    m = re.search(
        r'CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+'
        r'"?(\{?\w+\}?)"?',
        trimmed, re.IGNORECASE | re.DOTALL,
    )
    if m:
        raw_schema = m.group(1)

        async def _check_schema(conn, params, raw_params):
            from .locking_tools import check_schema_exists
            schema = _resolve_schema(raw_schema, raw_params)
            return await _cached_check_async(
                "schema", schema, schema,
                check_schema_exists, conn, schema,
            )

        return _check_schema

    # --- Pattern: CREATE OR REPLACE FUNCTION {schema}.func_name() ---
    m = re.search(
        r'CREATE\s+OR\s+REPLACE\s+FUNCTION\s+'
        r'(?:"?(\{?\w+\}?)"?\.)?'    # optional schema
        r'"?(\w+)"?'                   # function name
        r'\s*\(',                      # opening paren
        trimmed, re.IGNORECASE | re.DOTALL,
    )
    if m:
        raw_schema = m.group(1)
        func_name = m.group(2)

        async def _check_function(conn, params, raw_params):
            from .locking_tools import check_function_exists
            schema = _resolve_schema(raw_schema, raw_params)
            return await _cached_check_async(
                "function", schema, func_name,
                check_function_exists, conn, func_name, schema,
            )

        return _check_function

    # --- Pattern: CREATE TRIGGER ... ON {schema}.table ---
    # Trigger names in PG are per-relation, so the existence check must be
    # scoped to the specific table — otherwise triggers that share a name
    # across tenant tables (e.g. ``trg_asset_cleanup`` on every asset table)
    # would match incorrectly and skip re-creation.
    m = re.search(
        r'CREATE\s+TRIGGER\s+"?(\w+)"?',
        trimmed, re.IGNORECASE | re.DOTALL,
    )
    if m:
        trigger_name = m.group(1)
        # Extract schema + table from ON clause: ON "schema"."table" or ON schema.table
        m_on = re.search(
            r'\bON\s+(?:"?(\{?\w+\}?)"?\.)?"?(\{?\w+\}?)"?',
            trimmed, re.IGNORECASE | re.DOTALL,
        )
        raw_schema = m_on.group(1) if m_on else None
        raw_table = m_on.group(2) if m_on else None

        async def _check_trigger(conn, params, raw_params):
            from .locking_tools import check_trigger_exists
            schema = _resolve_schema(raw_schema, raw_params)
            table = _resolve_schema(raw_table, raw_params) if raw_table else None
            return await _cached_check_async(
                "trigger", schema, f"{table}.{trigger_name}" if table else trigger_name,
                check_trigger_exists, conn, trigger_name, schema, table,
            )

        return _check_trigger

    return None


def _resolve_schema(raw_schema, raw_params):
    """Resolve a schema reference from the SQL template.

    ``raw_schema`` can be:
    - ``"{schema}"`` or ``{schema}`` → look up ``schema`` in *raw_params*
    - A literal like ``"events"`` → return as-is
    - ``None`` → default to ``"public"`` (PostgreSQL default search_path)
    """
    if not raw_schema:
        return "public"
    # Strip braces: {schema} → schema
    stripped = raw_schema.strip("{}")
    if stripped != raw_schema:
        # It was a template placeholder — resolve from raw_params
        return str(raw_params.get(stripped, "public"))
    return raw_schema
