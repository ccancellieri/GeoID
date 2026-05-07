"""
Shared tools for SQL-like expression evaluation in-memory.
"""
import ast
import logging
import operator
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_COMPARE_OPS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Is: operator.is_,
    ast.IsNot: operator.is_not,
}


def _safe_eval_node(node: ast.AST) -> Any:
    """Recursively evaluate a parsed AST node using a strict allowlist of operations."""
    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.BoolOp):
        values = (_safe_eval_node(v) for v in node.values)
        if isinstance(node.op, ast.And):
            result = True
            for v in values:
                result = result and v
            return result
        if isinstance(node.op, ast.Or):
            result = False
            for v in values:
                result = result or v
            return result

    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return not _safe_eval_node(node.operand)

    if isinstance(node, ast.Compare):
        left = _safe_eval_node(node.left)
        for op, comparator in zip(node.ops, node.comparators):
            op_fn = _COMPARE_OPS.get(type(op))
            if op_fn is None:
                raise ValueError(f"Unsupported comparison operator: {type(op).__name__}")
            right = _safe_eval_node(comparator)
            if not op_fn(left, right):
                return False
            left = right
        return True

    raise ValueError(f"Unsupported expression node: {type(node).__name__}")


def evaluate_sql_condition(condition: str, properties: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> bool:
    """
    Evaluates a SQL-like condition (from CQL2) against a set of properties in-memory.
    Uses Python's ast module for safe expression parsing — no eval().
    """
    if not condition:
        return True
    if params is None:
        params = {}

    try:
        eval_expr = condition

        # 1. Replace bind parameters with actual values
        for param_name, param_value in params.items():
            if isinstance(param_value, str):
                eval_expr = eval_expr.replace(f":{param_name}", f"'{param_value}'")
            else:
                eval_expr = eval_expr.replace(f":{param_name}", str(param_value))

        # 2. Replace column references with property values
        # Sort by length descending to avoid partial matches (e.g. 'id' vs 'external_id')
        sorted_props = sorted(properties.keys(), key=len, reverse=True)
        for prop_name in sorted_props:
            prop_value = properties[prop_name]
            if prop_value is None:
                eval_expr = eval_expr.replace(prop_name, "None")
            elif isinstance(prop_value, str):
                escaped = prop_value.replace("\\", "\\\\").replace("'", "\\'")
                eval_expr = eval_expr.replace(prop_name, f"'{escaped}'")
            else:
                eval_expr = eval_expr.replace(prop_name, str(prop_value))

        # 3. Convert SQL operators to Python equivalents
        eval_expr = eval_expr.replace(" = ", " == ")
        eval_expr = eval_expr.replace(" AND ", " and ")
        eval_expr = eval_expr.replace(" OR ", " or ")
        eval_expr = eval_expr.replace(" NOT ", " not ")
        eval_expr = eval_expr.replace(" IS NOT NULL", " is not None")
        eval_expr = eval_expr.replace(" IS NULL", " is None")
        eval_expr = eval_expr.replace("NULL", "None")

        # 4. Parse and safely evaluate using AST (no eval())
        tree = ast.parse(eval_expr, mode="eval")
        return bool(_safe_eval_node(tree.body))

    except Exception as e:
        logger.warning(f"Failed to evaluate SQL condition '{condition}': {e}")
        return False
