
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
from typing import Dict, Any, Tuple, Optional, Set, Union, TYPE_CHECKING
from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import column as sql_column

logger = logging.getLogger(__name__)

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


def parse_cql_filter(
    cql_text: str,
    field_mapping: Optional[Dict[str, Any]] = None,
    valid_props: Optional[Set[str]] = None,
    parser_type: str = 'cql2'
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
            ast = parse_cql2_text(cql_text)
        elif parser_type.lower() == 'ecql':
            ast = parse_ecql(cql_text)
        else:
            raise ValueError(f"Unknown parser type: {parser_type}")

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
        
        return str(compiled), compiled.params

    except Exception as e:
        # Wrap generic errors into ValueError so callers can handle them as "Bad Request"
        # without knowing internal details
        if isinstance(e, ValueError):
            raise e
        raise ValueError(f"Invalid {parser_type.upper()} filter: {e}") from e


def parse_cql2_json_filter(
    cql_json: Dict[str, Any],
    field_mapping: Optional[Dict[str, Any]] = None,
    valid_props: Optional[Set[str]] = None,
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

        return str(compiled), compiled.params

    except Exception as e:
        if isinstance(e, ValueError):
            raise e
        raise ValueError(f"Invalid CQL2-JSON filter: {e}") from e
