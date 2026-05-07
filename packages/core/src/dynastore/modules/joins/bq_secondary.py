"""BigQuery secondary stream adapter for /join.

Wraps Phase 4a's ItemsBigQueryDriver to expose a per-request
target (BigQuerySecondarySpec.target) as a Feature stream, indexable
by the join executor's ``index_secondary``.

Phase 4b PR-2 scope: target identity only (auth via CloudIdentityProtocol).
Phase 4e adds Secret-wrapped credential overrides.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Dict
from unittest.mock import AsyncMock

from dynastore.models.ogc import Feature
from dynastore.modules.joins.bq_filter import cql_to_bq_where
from dynastore.modules.joins.models import BigQuerySecondarySpec
from dynastore.modules.storage.drivers.bigquery import ItemsBigQueryDriver
from dynastore.modules.storage.drivers.bigquery_models import (
    ItemsBigQueryDriverConfig,
)


def _build_secondary_context(
    spec: BigQuerySecondarySpec,
    *,
    secondary_column: str,
) -> Dict[str, Any]:
    """Translate ``spec`` into the ``context`` dict the BQ driver expects.

    ``id_column`` is always set so the join key lands on ``Feature.id``
    (consumed by the executor's ``feat.id`` fallback). When ``spec.filter``
    is present, the CQL2 expression is translated to a BigQuery WHERE
    fragment and added under ``where_clause``; the BQ driver appends it
    to the ``SELECT * FROM <fqn>`` it builds in ``read_entities``.

    Identity field-mapping for the filter: per-request inline targets
    expose the BQ table's columns directly under their own names. The
    CQL2 parser surfaces unknown column references as a 400 via
    ``cql_to_bq_where``'s ValueError.
    """
    context: Dict[str, Any] = {"id_column": secondary_column}
    if spec.filter is None:
        return context

    # First-pass field-mapping: accept every column name the caller
    # references. The CQL2 parser already constrained input to a valid
    # AST, and BQ itself rejects unknown columns at execute time with a
    # clear error message — there's no value-add to a separate
    # introspection round-trip on every request.
    referenced = _properties_in_cql(spec.filter.cql, spec.filter.cql_lang)
    field_mapping = {name: name for name in referenced}
    context["where_clause"] = cql_to_bq_where(
        spec.filter.cql,
        cql_lang=spec.filter.cql_lang,
        field_mapping=field_mapping,
    )
    return context


def _properties_in_cql(cql: str, cql_lang: str) -> set[str]:
    """Walk a CQL2 expression and return the property names it references."""
    from pygeofilter.parsers.cql2_json import parse as parse_cql2_json
    from pygeofilter.parsers.cql2_text import parse as parse_cql2_text

    parser = parse_cql2_text if cql_lang == "cql2-text" else parse_cql2_json
    try:
        tree = parser(cql)
    except Exception as exc:
        raise ValueError(f"Invalid CQL2 ({cql_lang}): {exc}") from exc

    from dynastore.modules.tools.cql import _extract_property_names

    return _extract_property_names(tree)


async def stream_bigquery_secondary(
    spec: BigQuerySecondarySpec,
    *,
    secondary_column: str,
    page_size: int = 1000,
    limit: int = 100_000,
    driver_factory=ItemsBigQueryDriver,
) -> AsyncIterator[Feature]:
    """Yield Features from a per-request BigQuery target.

    The target MUST be fully qualified (project_id, dataset_id,
    table_name); the resolver raises if not. When ``spec.filter`` is
    set, the CQL2 expression is translated to a BigQuery WHERE clause
    and applied BEFORE the join — see ``_build_secondary_context``.
    """
    if not spec.target.is_fully_qualified():
        raise ValueError(
            "BigQuerySecondarySpec.target must include project_id, dataset_id, "
            "and table_name; partial targets are rejected at execution time.",
        )

    driver = driver_factory()
    cfg = ItemsBigQueryDriverConfig(target=spec.target, page_size=page_size)
    # Bypass the platform's config waterfall: this driver instance only
    # serves THIS request, so we monkey-patch its config resolver to
    # return the inline cfg. Phase 4a's read_entities then streams as
    # normal. The ad-hoc catalog/collection identifiers are placeholders
    # — the BQ driver only uses cfg.target for FQN construction.
    driver.get_driver_config = AsyncMock(return_value=cfg)

    context = _build_secondary_context(spec, secondary_column=secondary_column)
    async for feat in driver.read_entities(
        "_inline_", "_inline_",
        limit=limit,
        context=context,
    ):
        yield feat
