"""CQL2 → BigQuery WHERE-clause translator for OGC /join's BQ secondary.

Mirrors the role that ``modules/tools/cql.py`` plays for the PG primary,
but emits BQ-flavoured SQL: backtick identifier quoting and BQ
GEOGRAPHY-compatible spatial literals. Spatial *operator* names (ST_*)
already overlap between PostGIS and BigQuery — both accept the
mixed-case names emitted by pygeofilter's default spatial map — so only
identifier quoting and ``BBOX(...)`` literal construction need overrides.

The CQL2 parser is also the SQL-injection guard: arbitrary user input is
constrained to nodes pygeofilter recognises; payloads that don't parse
as valid CQL2 are rejected before any BQ SQL is built.
"""

from __future__ import annotations

from typing import Any, Dict, Literal, cast

from pygeofilter import ast
from pygeofilter.backends.evaluator import handle
from pygeofilter.backends.sql.evaluate import SQLEvaluator
from pygeofilter.parsers.cql2_json import parse as parse_cql2_json
from pygeofilter.parsers.cql2_text import parse as parse_cql2_text


def _bbox_to_bq_polygon(minx: Any, miny: Any, maxx: Any, maxy: Any) -> str:
    """Build a BQ GEOGRAPHY literal from the four BBOX corner values."""
    wkt = (
        f"POLYGON(({minx} {miny}, "
        f"{minx} {maxy}, "
        f"{maxx} {maxy}, "
        f"{maxx} {miny}, "
        f"{minx} {miny}))"
    )
    return f"ST_GeogFromText('{wkt}')"


class _BQEvaluator(SQLEvaluator):
    """SQLEvaluator that emits BigQuery-compatible SQL.

    Two overrides vs the upstream PG-flavoured emitter:

    1. Identifier quoting uses backticks (BQ string literals are
       single/double quoted; double-quoted identifiers are a parse error
       in BigQuery's standard SQL).
    2. The CQL2 ``BBOX(minx, miny, maxx, maxy)`` constructor (which
       pygeofilter parses as a ``Function`` node, not an ``ast.BBox``)
       is rewritten to ``ST_GeogFromText('POLYGON(...)')``. Other
       function names defer to ``function_map`` and raise on miss.
    """

    @handle(ast.Attribute)
    def attribute(self, node: "ast.Attribute") -> str:
        return f"`{self.attribute_map[node.name]}`"

    @handle(ast.Function)
    def function(self, node: "ast.Function", *arguments: str) -> str:
        if node.name.lower() == "bbox":
            if len(arguments) != 4:
                raise ValueError(
                    f"BBOX() expects 4 arguments (minx, miny, maxx, maxy); "
                    f"got {len(arguments)}",
                )
            return _bbox_to_bq_polygon(*arguments)
        return super().function(node, *arguments)


def cql_to_bq_where(
    cql: str,
    *,
    cql_lang: Literal["cql2-text", "cql2-json"] = "cql2-text",
    field_mapping: Dict[str, str],
) -> str:
    """Translate a CQL2 expression into a BigQuery WHERE-clause body.

    Returns the SQL fragment without the leading ``WHERE`` — the caller
    appends it to ``SELECT ... FROM <table>``. Raises ``ValueError`` on
    parse / translation failure so the joins handler can surface a 400.

    ``field_mapping`` maps CQL2 property names to BigQuery column names.
    Use the identity mapping (``{col: col}`` for every column the caller
    is allowed to reference) when the BQ table's columns match the CQL2
    surface — typical for a per-request inline target.
    """
    parser = parse_cql2_text if cql_lang == "cql2-text" else parse_cql2_json
    try:
        tree = parser(cql)
    except Exception as exc:
        raise ValueError(f"Invalid CQL2 ({cql_lang}): {exc}") from exc
    try:
        return _BQEvaluator(field_mapping, function_map={}).evaluate(
            cast(ast.AstType, tree),
        )
    except KeyError as exc:
        raise ValueError(
            f"CQL2 references unknown property/function {exc}; "
            f"available properties: {sorted(field_mapping)}"
        ) from exc
