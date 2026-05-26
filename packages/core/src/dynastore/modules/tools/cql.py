
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

import logging
import re
from typing import Dict, Any, Tuple, Optional, Set, Union, TYPE_CHECKING
from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import column as sql_column

logger = logging.getLogger(__name__)

# A SQLAlchemy ``text()`` placeholder name is matched by a ``\w``-only scanner,
# so a bind-parameter name containing any other character cannot be round-tripped
# when the compiled WHERE fragment is later executed via ``text(sql)``.
_SAFE_PARAM_NAME_RE = re.compile(r"^\w+$")


def _sanitize_bind_param_names(
    where: str, params: Dict[str, Any]
) -> Tuple[str, Dict[str, Any]]:
    """Rename bind-parameter names that are not ``text()``-safe.

    pygeofilter derives each bind-parameter name from the mapped column
    expression, so a queryable that resolves to a quoted or dotted expression —
    a COLUMNAR attribute column ``sc_attributes."col"`` or a JSONB accessor
    ``attributes->>'col'`` — yields a name like ``sc_attributes_"col"_1`` that
    still contains the quote / ``->>`` characters. Both the ``/items`` stream
    path and the STAC ``POST /search`` path later wrap the fragment in
    ``text(sql)``, whose ``:name`` scanner stops at the first non-word character
    and truncates the placeholder, so binding fails ("A value is required for
    bind parameter ...").

    Rewrite every unsafe placeholder in ``where`` and its ``params`` key to a
    synthetic ``cqlp_N`` name. Already-safe names (the common case) are returned
    untouched. The replacement matches a whole ``:name`` token (trailing
    word-boundary guard) so an IN-list expansion whose names differ only in a
    trailing digit (``..._1`` vs ``..._10``) is never partially rewritten, and
    each ``cqlp_N`` is checked against the names already in use so it cannot
    collide with an existing safe parameter.
    """
    unsafe = [k for k in params if not _SAFE_PARAM_NAME_RE.match(k)]
    if not unsafe:
        return where, params
    new_params = {k: v for k, v in params.items() if _SAFE_PARAM_NAME_RE.match(k)}
    used = set(new_params)
    idx = 0
    # Longest first so a name that is a textual prefix of another is rewritten
    # before the shorter one is processed.
    for key in sorted(unsafe, key=len, reverse=True):
        while f"cqlp_{idx}" in used:
            idx += 1
        safe = f"cqlp_{idx}"
        used.add(safe)
        idx += 1
        where = re.sub(rf":{re.escape(key)}(?!\w)", f":{safe}", where)
        new_params[safe] = params[key]
    return where, new_params

# CQL2-Text escapes a single quote inside a string literal by doubling it
# (``'O''Brien'`` → ``O'Brien``). The bundled pygeofilter (0.3.3) lark grammar
# uses a non-greedy ``SINGLE_QUOTED: "'" /.*?/ "'"`` terminal and does NOT
# implement that escape: it tokenises ``'O''Brien'`` as the two separate
# literals ``'O'`` and ``'Brien'`` and raises ``UnexpectedToken`` (HTTP 400).
#
# To accept the spec-compliant escape without forking the grammar, the doubled
# quote is replaced with this sentinel before the text reaches the parser, then
# the sentinel is restored to a single quote in the parsed string literals
# (:func:`_restore_escaped_quotes`). The sentinel uses control characters that
# cannot appear in a CQL identifier or pass through unescaped from a quoted
# literal, and any pre-existing occurrence in the input is stripped first.
_QUOTE_ESCAPE_SENTINEL = "\x00\x01CQLQUOTE\x01\x00"


def _preprocess_cql2_text_quotes(cql_text: str) -> str:
    """Make CQL2-Text doubled-quote escapes parseable by the bundled grammar.

    Scans the text tracking string-literal boundaries and replaces each ``''``
    *inside* a literal (the CQL2-Text escape for a literal single quote) with
    :data:`_QUOTE_ESCAPE_SENTINEL`, leaving the opening/closing delimiters
    intact. The grammar then sees one well-formed string literal instead of two
    (or a stray quote), and the sentinel is undone after parsing by
    :func:`_restore_escaped_quotes`.

    A run of single quotes is interpreted per CQL2: the first quote opens the
    literal, and from there each ``''`` is an escaped quote until a lone ``'``
    closes it. This handles odd boundary runs like ``'''PK'''`` (value
    ``'PK'``) correctly, where a blind ``''`` replacement would leave a stray
    delimiter outside the literal.
    """
    # Defensively drop any pre-existing sentinel so a crafted value cannot
    # smuggle a literal quote past the round-trip.
    text_in = cql_text.replace(_QUOTE_ESCAPE_SENTINEL, "")
    out: list[str] = []
    i = 0
    n = len(text_in)
    in_literal = False
    while i < n:
        ch = text_in[i]
        if ch != "'":
            out.append(ch)
            i += 1
            continue
        # ``ch`` is a single quote.
        if not in_literal:
            # Opening delimiter.
            in_literal = True
            out.append("'")
            i += 1
            continue
        # Inside a literal: a doubled quote is an escape, a lone quote closes.
        if i + 1 < n and text_in[i + 1] == "'":
            out.append(_QUOTE_ESCAPE_SENTINEL)
            i += 2
        else:
            in_literal = False
            out.append("'")
            i += 1
    return "".join(out)


def _restore_escaped_quotes(node: Any) -> None:
    """Restore sentinel-escaped single quotes in a parsed pygeofilter AST.

    Walks the AST in place, replacing :data:`_QUOTE_ESCAPE_SENTINEL` with a
    single quote in every string literal value (``str`` fields and ``str``
    members of list/tuple fields such as ``In.sub_nodes``). The unescaped value
    is what reaches the SQL bind parameter, so matching is exact and
    injection-safe (the value is never interpolated into SQL text).
    """
    if node is None or isinstance(node, (str, bytes, int, float, bool)):
        return
    # dataclass-style AST nodes expose their children via ``__dict__``.
    fields = getattr(node, "__dict__", None)
    if not fields:
        return
    for name, value in list(fields.items()):
        if isinstance(value, str):
            if _QUOTE_ESCAPE_SENTINEL in value:
                setattr(node, name, value.replace(_QUOTE_ESCAPE_SENTINEL, "'"))
        elif isinstance(value, (list, tuple)):
            restored = [
                item.replace(_QUOTE_ESCAPE_SENTINEL, "'")
                if isinstance(item, str)
                else item
                for item in value
            ]
            for item in restored:
                _restore_escaped_quotes(item)
            if isinstance(value, list):
                value[:] = restored
            else:
                setattr(node, name, tuple(restored))
        else:
            _restore_escaped_quotes(value)

# Optional dependency for safe CQL2/ECQL parsing.
# TYPE_CHECKING branch pins the imports as non-Optional for pyright; at runtime
# the try/except installs real modules or raises ImportError from the public
# parse_* entry points via the PYGEOFILTER_AVAILABLE guard.
if TYPE_CHECKING:
    from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
    from pygeofilter.parsers.cql2_json import parse as parse_cql2_json
    from pygeofilter.parsers.ecql import parse as parse_ecql
    from pygeofilter.backends.sqlalchemy import to_filter
    from pygeofilter.ast import Attribute
    PYGEOFILTER_AVAILABLE = True
else:
    try:
        from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
        from pygeofilter.parsers.cql2_json import parse as parse_cql2_json
        from pygeofilter.parsers.ecql import parse as parse_ecql
        from pygeofilter.backends.sqlalchemy import to_filter
        from pygeofilter.ast import Attribute
        PYGEOFILTER_AVAILABLE = True
    except ImportError:
        PYGEOFILTER_AVAILABLE = False
        parse_cql2_text = None
        parse_cql2_json = None
        parse_ecql = None
        to_filter = None
        Attribute = None
        logger.warning("`pygeofilter` not installed. CQL parsing will be disabled.")


def _extract_property_names(node) -> Set[str]:
    """
    Recursively extracts property names from a pygeofilter AST node.
    Used to validate filters against known table properties.
    """
    props = set()
    # Guard against Attribute being None if import failed
    if Attribute and isinstance(node, Attribute):
        props.add(node.name)

    # Recursively check standard pygeofilter AST attributes
    # Binary/Spatial nodes: lhs, rhs
    # Not node: node
    for attr in ['lhs', 'rhs', 'node']:
        child = getattr(node, attr, None)
        if child:
            props.update(_extract_property_names(child))

    # Logical nodes / Functions: nodes, sub_nodes, arguments
    for attr in ['nodes', 'sub_nodes', 'arguments']:
        children = getattr(node, attr, None)
        if children:
            for child in children:
                props.update(_extract_property_names(child))

    return props


def _stamp_geometry_srid(node: Any, srid: int) -> None:
    """Stamp an explicit CRS on every ``Geometry`` literal in a parsed AST.

    pygeofilter's SQLAlchemy backend reads the SRID from the GeoJSON ``crs``
    member on the geometry dict (defaulting to 4326 / CRS84 when absent) and
    emits ``ST_GeomFromEWKT('SRID=<srid>;<wkt>')``. To honour the OGC API
    Features ``filter-crs`` query parameter we walk the AST and inject a
    ``crs`` member on each ``Geometry`` node's dict and a ``crs`` attribute
    on each ``BBox`` node when the caller has specified one — this is the
    only knob the bundled grammar exposes for per-literal CRS overrides.

    Already-set explicit CRSes (in input WKT/EWKT) are not overridden.
    """
    if node is None or isinstance(node, (str, bytes, int, float, bool)):
        return
    geom_attr = getattr(node, "geometry", None)
    # pygeofilter's ``values.Geometry`` carries the geojson dict on the
    # ``geometry`` attribute; ``BBox`` AST nodes carry a ``crs`` directly.
    if isinstance(geom_attr, dict):
        # Only stamp when no explicit CRS member is already present.
        if "crs" not in geom_attr:
            geom_attr["crs"] = {
                "type": "name",
                "properties": {"name": f"urn:ogc:def:crs:EPSG::{srid}"},
            }
    # BBox nodes: stamp crs attribute if missing or default.
    if Attribute is not None and type(node).__name__ == "BBox":
        existing = getattr(node, "crs", None)
        if existing in (None, "", 4326) or (
            isinstance(existing, str) and "4326" in existing
        ):
            try:
                setattr(node, "crs", srid)
            except (AttributeError, TypeError):
                pass

    fields = getattr(node, "__dict__", None)
    if not fields:
        return
    for value in list(fields.values()):
        if isinstance(value, (list, tuple)):
            for child in value:
                _stamp_geometry_srid(child, srid)
        else:
            _stamp_geometry_srid(value, srid)


def parse_cql_filter(
    cql_text: str,
    field_mapping: Optional[Dict[str, Any]] = None,
    valid_props: Optional[Set[str]] = None,
    parser_type: str = 'cql2',
    geometry_srid: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Parses a CQL filter string (CQL2 or ECQL) and converts it to a SQLAlchemy-safe
    SQL string with bind parameters.

    Args:
        cql_text: The CQL filter string.
        field_mapping: A dictionary mapping field names to SQLAlchemy Columns or TextClauses.
                       Required for converting the AST to SQL.
        valid_props: A set of valid property names to validate the filter against.
                     If None, validation against `field_mapping` keys is performed if provided.
        parser_type: 'cql2' (default) or 'ecql'.
        geometry_srid: Optional CRS to stamp on geometry literals (and bbox
                       nodes) that do not carry an explicit CRS. Used to wire
                       the OGC API Features ``filter-crs`` query parameter
                       through to ``ST_GeomFromEWKT('SRID=<srid>;...')``. When
                       ``None`` the pygeofilter default (CRS84 / EPSG:4326) is
                       used.

    Returns:
        A tuple containing:
        - The SQL WHERE clause string (safe for use in TEXT() construction).
        - A dictionary of bind parameters.

    Raises:
        ValueError: If the filter is invalid or contains unknown properties.
        ImportError: If pygeofilter is not installed.
    """
    if not PYGEOFILTER_AVAILABLE:
        raise ImportError("pygeofilter is not installed.")

    if not cql_text:
        return "", {}

    # Strip outer quotes if present
    if len(cql_text) >= 2 and cql_text.startswith('"') and cql_text.endswith('"'):
        cql_text = cql_text[1:-1]

    try:
        # 1. Parse into AST
        if parser_type.lower() == 'cql2':
            # The bundled pygeofilter grammar rejects the CQL2-Text ``''``
            # escape for an embedded single quote; pre-substitute it with a
            # sentinel, parse, then restore the literal quote in the AST.
            ast = parse_cql2_text(_preprocess_cql2_text_quotes(cql_text))
            _restore_escaped_quotes(ast)
        elif parser_type.lower() == 'ecql':
            ast = parse_ecql(cql_text)
        else:
            raise ValueError(f"Unknown parser type: {parser_type}")

        # Stamp the requested CRS on every geometry literal so the backend
        # emits ``ST_GeomFromEWKT('SRID=<srid>;...')`` rather than defaulting
        # to 4326. Honours the OGC API Features ``filter-crs`` parameter; a
        # literal that already carries an explicit CRS is left untouched.
        if geometry_srid is not None:
            _stamp_geometry_srid(ast, geometry_srid)

        # 2. Validation (Optional but recommended)
        if valid_props is None and field_mapping:
            valid_props = set(field_mapping.keys())
        
        if valid_props:
            used_properties = _extract_property_names(ast)
            invalid_props = used_properties - valid_props
            if invalid_props:
                sorted_valid = sorted(list(valid_props))
                raise ValueError(
                    f"Unknown properties: {', '.join(sorted(invalid_props))}. "
                    f"Available properties: {', '.join(sorted_valid)}. "
                    "Hint: If these are intended to be values, ensure they are enclosed in single quotes."
                )

        # 3. Convert to SQLAlchemy Expression
        # If no mapping provided, we try to create a default one from valid_props,
        # but really the caller should provide the mapping for correct handling (e.g. JSONB)
        if field_mapping is None:
             raise ValueError("field_mapping is required for SQL conversion")

        try:
            sql_expr = to_filter(ast, field_mapping=field_mapping)
        except KeyError as ke:
             # Try to provide helpful context if possible
             prop_name = str(ke).strip(chr(39))
             msg = f"Unknown property in filter: {prop_name}"
             if field_mapping:
                 sorted_valid = sorted(list(field_mapping.keys()))
                 msg += f". Available properties: {', '.join(sorted_valid)}"
             raise ValueError(msg) from ke

        # 4. Compile to SQL String
        if isinstance(sql_expr, bool):
            if sql_expr is False:
                 return "1=0", {}
            return "", {} # True means no filter needed

        # CRITICAL FIX: explicit compilation with render_postcompile=True.
        # This ensures that parameters are rendered as named bind parameters (e.g., :param_1)
        # compatible with SQLAlchemy text() and asyncpg, rather than pyformat (%s) or positional.
        compiled = sql_expr.compile(
            compile_kwargs={"render_postcompile": True}
        )

        return _sanitize_bind_param_names(str(compiled), dict(compiled.params))

    except Exception as e:
        # Wrap generic errors into ValueError so callers can handle them as "Bad Request"
        # without knowing internal details
        if isinstance(e, ValueError):
            raise e
        raise ValueError(f"Invalid {parser_type.upper()} filter: {e}") from e


def parse_cql2_json_filter(
    cql_json: Union[str, Dict[str, Any]],
    field_mapping: Optional[Dict[str, Any]] = None,
    valid_props: Optional[Set[str]] = None,
    geometry_srid: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Parses a CQL2-JSON filter dict and converts it to a SQLAlchemy-safe
    SQL string with bind parameters.

    This is the JSON counterpart to ``parse_cql_filter`` (CQL2-Text).
    It follows the exact same validation → to_filter → compile pipeline.

    Args:
        cql_json: The CQL2-JSON filter as a Python dict.
        field_mapping: Maps field names to SQLAlchemy Columns or TextClauses.
        valid_props: Valid property names for filter validation.

    Returns:
        (sql_where_clause, bind_params) tuple.

    Raises:
        ValueError: If the filter is invalid or contains unknown properties.
        ImportError: If pygeofilter is not installed.
    """
    if not PYGEOFILTER_AVAILABLE:
        raise ImportError("pygeofilter is not installed.")

    if not cql_json:
        return "", {}

    try:
        ast = parse_cql2_json(cql_json)

        if geometry_srid is not None:
            _stamp_geometry_srid(ast, geometry_srid)

        if valid_props is None and field_mapping:
            valid_props = set(field_mapping.keys())

        if valid_props:
            used_properties = _extract_property_names(ast)
            invalid_props = used_properties - valid_props
            if invalid_props:
                sorted_valid = sorted(list(valid_props))
                raise ValueError(
                    f"Unknown properties: {', '.join(sorted(invalid_props))}. "
                    f"Available properties: {', '.join(sorted_valid)}. "
                )

        if field_mapping is None:
            raise ValueError("field_mapping is required for SQL conversion")

        try:
            sql_expr = to_filter(ast, field_mapping=field_mapping)
        except KeyError as ke:
            prop_name = str(ke).strip(chr(39))
            msg = f"Unknown property in filter: {prop_name}"
            if field_mapping:
                sorted_valid = sorted(list(field_mapping.keys()))
                msg += f". Available properties: {', '.join(sorted_valid)}"
            raise ValueError(msg) from ke

        if isinstance(sql_expr, bool):
            if sql_expr is False:
                return "1=0", {}
            return "", {}

        compiled = sql_expr.compile(
            compile_kwargs={"render_postcompile": True}
        )

        return _sanitize_bind_param_names(str(compiled), dict(compiled.params))

    except Exception as e:
        if isinstance(e, ValueError):
            raise e
        raise ValueError(f"Invalid CQL2-JSON filter: {e}") from e
