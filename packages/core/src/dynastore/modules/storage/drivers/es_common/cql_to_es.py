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

"""CQL2 AST → Elasticsearch Query DSL translator.

The shared translator that lets the STAC ``/search`` dispatch serve a CQL2
``filter`` from Elasticsearch instead of falling back to PostgreSQL. Used by
both ES items drivers (public + tenant-private) via :func:`merge_es_filter`.

Why a custom evaluator
======================

``pygeofilter`` ships an Elasticsearch backend
(``pygeofilter.backends.elasticsearch``) but importing it pulls in
``elasticsearch_dsl`` (not installed, and intentionally not a dependency of this
platform). Its operator coverage is the reference this module mirrors, but the
output here is **plain ES Query DSL dicts** (``term`` / ``terms`` / ``range`` /
``bool`` / ``exists`` / ``wildcard`` / ``geo_shape``) — exactly what
``Q(...).to_dict()`` would have produced — so no ``*_dsl`` package is required.

Injection safety
================

Every user value flows into the DSL as a ``term`` / ``range`` / ``terms`` value
(or a ``geo_shape`` geometry). There is no string interpolation into a query
string anywhere, so a property value carrying quotes or ES query-string
metacharacters is treated as a literal scalar — injection-safe by construction.

Field resolution
================

Property names are resolved through a ``field_mapping`` (built by
:func:`build_es_field_mapping` from the queryables SSOT). An unknown property —
one not present in the mapping — raises :class:`UntranslatableFilterError`; the
caller treats that (and any other translation failure) as "untranslatable" and
falls back to the PostgreSQL path, preserving today's behaviour.
"""

from __future__ import annotations

import datetime as _dt
import re
from typing import Any, Dict, Optional

from pygeofilter import ast, values
from pygeofilter.backends.evaluator import Evaluator, handle


class UntranslatableFilterError(ValueError):
    """Raised when a CQL2 AST cannot be expressed as an ES query.

    Subclasses :class:`ValueError` so existing ``except ValueError`` callers
    (which already gate the PG fallback on unknown-property errors) keep
    working unchanged.
    """


# CQL2 ComparisonOp → ES ``range`` operator key.
_COMPARISON_OP_MAP: Dict[Any, str] = {
    ast.ComparisonOp.LT: "lt",
    ast.ComparisonOp.LE: "lte",
    ast.ComparisonOp.GT: "gt",
    ast.ComparisonOp.GE: "gte",
}


def _like_to_wildcard(
    value: str, wildcard: str, single_char: str, escape_char: str = "\\"
) -> str:
    """Adapt a CQL ``LIKE`` pattern to an ES ``wildcard`` pattern.

    Mirrors ``pygeofilter.backends.elasticsearch.util.like_to_wildcard`` (which
    cannot be imported without ``elasticsearch_dsl``): a non-escaped ``wildcard``
    char becomes ``*`` and a non-escaped ``single_char`` becomes ``?``.
    """
    x_wildcard = re.escape(wildcard)
    x_single_char = re.escape(single_char)

    if escape_char == "\\":
        x_escape_char = "\\\\\\\\"
    else:
        x_escape_char = re.escape(escape_char)

    if wildcard != "*":
        value = re.sub(f"(?<!{x_escape_char}){x_wildcard}", "*", value)
    if single_char != "?":
        value = re.sub(f"(?<!{x_escape_char}){x_single_char}", "?", value)
    return value


def _like_to_match_text(
    value: str, wildcard: str, single_char: str, escape_char: str = "\\"
) -> str:
    """Strip CQL ``LIKE`` wildcards to a plain free-text string for ES ``match``.

    FULLTEXT fields (#1291) are searched with an analyzed ``match`` query, which
    tokenises rather than glob-matches. The CQL ``%``/``_`` wildcards (or
    whatever ``wildcard``/``single_char`` the filter declares) carry no meaning
    to ``match``, so they are removed; an escaped wildcard becomes its literal
    character. The analyzer handles tokenisation and case-folding from there.
    """
    x_wildcard = re.escape(wildcard)
    x_single_char = re.escape(single_char)
    if escape_char == "\\":
        x_escape_char = "\\\\\\\\"
    else:
        x_escape_char = re.escape(escape_char)
    # Drop non-escaped wildcard / single-char markers.
    value = re.sub(f"(?<!{x_escape_char}){x_wildcard}", " ", value)
    value = re.sub(f"(?<!{x_escape_char}){x_single_char}", " ", value)
    # Unescape escaped wildcard / single-char back to their literal form.
    value = value.replace(f"{escape_char}{wildcard}", wildcard)
    value = value.replace(f"{escape_char}{single_char}", single_char)
    return value.strip()


class _CqlToEsEvaluator(Evaluator):
    """Evaluates a pygeofilter AST into a plain ES Query DSL ``dict``.

    Operator coverage (mirrors the reference ES backend, plain-dict output):

    * logical: ``And`` / ``Or`` / ``Not``
    * equality: ``Equal`` (→ ``term``), ``NotEqual`` (→ ``bool.must_not[term]``)
    * comparison: ``LessThan`` / ``LessEqual`` / ``GreaterThan`` /
      ``GreaterEqual`` (→ ``range``)
    * ``Between`` (→ ``range`` gte/lte, negatable)
    * ``In`` (→ ``terms``, negatable)
    * ``Like`` (→ ``wildcard``, negatable)
    * ``IsNull`` (→ ``bool.must_not[exists]``; ``not_`` → ``exists``)
    * ``Exists`` (→ ``exists``, negatable)
    * spatial: ``GeometryIntersects`` / ``GeometryDisjoint`` /
      ``GeometryWithin`` / ``GeometryContains`` / ``BBox`` (→ ``geo_shape``)
    * temporal: ``TemporalPredicate`` subclasses (→ ``range`` / ``term``)
    * ``Attribute`` (resolved via ``field_mapping``), literals, geometry,
      envelope.
    """

    def __init__(
        self,
        field_mapping: Dict[str, str],
        fulltext_mapping: Optional[Dict[str, str]] = None,
    ):
        self.field_mapping = field_mapping
        # Name → analyzed ``.text`` path for FULLTEXT-capable fields (#1291).
        # Consulted only by the ``Like`` handler; empty/None preserves the
        # historical ``wildcard`` behaviour for every field.
        self.fulltext_mapping = fulltext_mapping or {}

    # --- logical -----------------------------------------------------------

    @handle(ast.Not)
    def not_(self, _node, sub):
        return {"bool": {"must_not": [sub]}}

    @handle(ast.And)
    def and_(self, _node, lhs, rhs):
        return {"bool": {"filter": [lhs, rhs]}}

    @handle(ast.Or)
    def or_(self, _node, lhs, rhs):
        return {"bool": {"should": [lhs, rhs], "minimum_should_match": 1}}

    # --- comparison --------------------------------------------------------

    @handle(ast.Equal, ast.NotEqual)
    def equality(self, node, lhs, rhs):
        q: Dict[str, Any] = {"term": {lhs: rhs}}
        if node.op == ast.ComparisonOp.NE:
            return {"bool": {"must_not": [q]}}
        return q

    @handle(ast.LessThan, ast.LessEqual, ast.GreaterThan, ast.GreaterEqual)
    def comparison(self, node, lhs, rhs):
        return {"range": {lhs: {_COMPARISON_OP_MAP[node.op]: rhs}}}

    @handle(ast.Between)
    def between(self, node: ast.Between, lhs, low, high):
        q: Dict[str, Any] = {"range": {lhs: {"gte": low, "lte": high}}}
        if node.not_:
            return {"bool": {"must_not": [q]}}
        return q

    @handle(ast.In)
    def in_(self, node, lhs, *options):
        q: Dict[str, Any] = {"terms": {lhs: list(options)}}
        if node.not_:
            return {"bool": {"must_not": [q]}}
        return q

    @handle(ast.Like)
    def like(self, node: ast.Like, lhs):
        # FULLTEXT fields (#1291): a field that declares analyzed free-text
        # search is queried with an ES ``match`` over its analyzed ``.text``
        # sub-field instead of a ``wildcard`` over the exact ``.keyword`` path.
        # ``match`` runs the same analyzer used at index time, so a LIKE against
        # an analyzed field finds tokenised matches rather than failing on the
        # raw keyword. Every non-FULLTEXT field keeps the historical ``wildcard``
        # behaviour unchanged.
        attr_name = getattr(getattr(node, "lhs", None), "name", None)
        fulltext_path = self.fulltext_mapping.get(attr_name) if attr_name else None
        # pygeofilter types ``pattern`` as ScalarAstType; LIKE patterns are
        # always textual on the wire, so coerce defensively for the helpers.
        raw_pattern = (
            node.pattern if isinstance(node.pattern, str) else str(node.pattern)
        )
        if fulltext_path is not None:
            # Strip the LIKE wildcards/escapes for the free-text query string;
            # ``match`` tokenises rather than glob-matches.
            query_text = _like_to_match_text(
                raw_pattern, node.wildcard, node.singlechar, node.escapechar
            )
            q: Dict[str, Any] = {"match": {fulltext_path: query_text}}
            if node.not_:
                return {"bool": {"must_not": [q]}}
            return q
        pattern = _like_to_wildcard(
            raw_pattern, node.wildcard, node.singlechar, node.escapechar
        )
        expr: Dict[str, Any] = {"value": pattern, "case_insensitive": node.nocase}
        q = {"wildcard": {lhs: expr}}
        if node.not_:
            return {"bool": {"must_not": [q]}}
        return q

    @handle(ast.IsNull)
    def null(self, node: ast.IsNull, lhs):
        exists = {"exists": {"field": lhs}}
        # IS NULL  → field does NOT exist; IS NOT NULL (not_) → field exists.
        if node.not_:
            return exists
        return {"bool": {"must_not": [exists]}}

    @handle(ast.Exists)
    def exists(self, node: ast.Exists, lhs):
        exists = {"exists": {"field": lhs}}
        if node.not_:
            return {"bool": {"must_not": [exists]}}
        return exists

    # --- temporal ----------------------------------------------------------

    @handle(ast.TemporalPredicate, subclasses=True)
    def temporal(self, node: ast.TemporalPredicate, lhs, rhs):
        op = node.op
        if isinstance(rhs, (_dt.date, _dt.datetime)):
            low = high = rhs
        else:
            try:
                low, high = rhs
            except (TypeError, ValueError) as exc:
                raise UntranslatableFilterError(
                    f"Unsupported temporal RHS for operator {op}: {rhs!r}"
                ) from exc

        query = "range"
        negate = False
        predicate: Dict[str, Any]
        if op == ast.TemporalComparisonOp.DISJOINT:
            negate = True
            predicate = {"gte": low, "lte": high}
        elif op == ast.TemporalComparisonOp.AFTER:
            predicate = {"gt": high}
        elif op == ast.TemporalComparisonOp.BEFORE:
            predicate = {"lt": low}
        elif op in (
            ast.TemporalComparisonOp.TOVERLAPS,
            ast.TemporalComparisonOp.OVERLAPPEDBY,
        ):
            predicate = {"gte": low, "lte": high}
        elif op == ast.TemporalComparisonOp.BEGINS:
            query = "term"
            predicate = {"value": low}
        elif op == ast.TemporalComparisonOp.BEGUNBY:
            query = "term"
            predicate = {"value": high}
        elif op == ast.TemporalComparisonOp.DURING:
            predicate = {"gt": low, "lt": high, "relation": "WITHIN"}
        elif op == ast.TemporalComparisonOp.TCONTAINS:
            predicate = {"gt": low, "lt": high, "relation": "CONTAINS"}
        else:
            raise UntranslatableFilterError(f"Unsupported temporal operator: {op}")

        q: Dict[str, Any] = {query: {lhs: predicate}}
        if negate:
            return {"bool": {"must_not": [q]}}
        return q

    # --- spatial -----------------------------------------------------------

    @handle(
        ast.GeometryIntersects,
        ast.GeometryDisjoint,
        ast.GeometryWithin,
        ast.GeometryContains,
    )
    def spatial_comparison(self, node, lhs: str, rhs):
        return {
            "geo_shape": {
                lhs: {"shape": rhs, "relation": node.op.value.lower()},
            }
        }

    @handle(ast.BBox)
    def bbox(self, node: ast.BBox, lhs):
        return {
            "geo_shape": {
                lhs: {
                    "shape": self.envelope(
                        values.Envelope(node.minx, node.maxx, node.miny, node.maxy)
                    ),
                    "relation": "intersects",
                }
            }
        }

    # --- terminals ---------------------------------------------------------

    @handle(ast.Attribute)
    def attribute(self, node: ast.Attribute):
        try:
            return self.field_mapping[node.name]
        except KeyError as exc:
            raise UntranslatableFilterError(
                f"Unknown queryable property '{node.name}' "
                f"(not in field mapping)"
            ) from exc

    @handle(*values.LITERALS)
    def literal(self, node):
        return node

    @handle(values.Geometry)
    def geometry(self, node: values.Geometry):
        return node.geometry

    @handle(values.Envelope)
    def envelope(self, node: values.Envelope):
        return {
            "type": "envelope",
            "coordinates": [
                [min(node.x1, node.x2), max(node.y1, node.y2)],
                [max(node.x1, node.x2), min(node.y1, node.y2)],
            ],
        }

    def adopt(self, node, *sub_args):
        # Any node without a registered handler is untranslatable → PG fallback.
        raise UntranslatableFilterError(
            f"Unsupported CQL2 node for ES translation: {type(node).__name__}"
        )


def cql_ast_to_es_query(
    node: Any,
    field_mapping: Dict[str, str],
    fulltext_mapping: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Translate a pygeofilter CQL2 AST node to an ES Query DSL ``dict``.

    ``field_mapping`` maps each queryable property name to its ES field path
    (see :func:`build_es_field_mapping`). An unknown property — or any node the
    translator does not cover — raises :class:`UntranslatableFilterError`, which
    the caller treats as "untranslatable" and falls back to PostgreSQL.

    ``fulltext_mapping`` (optional; see :func:`build_es_fulltext_mapping`) maps
    FULLTEXT-capable string fields to their analyzed ``.text`` path. When a
    ``LIKE`` targets such a field the translator emits an analyzed ``match``
    query instead of a ``wildcard``; omitting it preserves today's behaviour.

    The returned dict is a single ES query clause (e.g. ``{"term": {...}}`` or a
    ``{"bool": {...}}`` composite) suitable for :func:`merge_es_filter`.
    """
    return _CqlToEsEvaluator(field_mapping, fulltext_mapping).evaluate(node)


# Envelope (top-level doc) fields that are NOT nested under ``properties``.
# Both the public STAC item doc and the private tenant-feature doc keep these
# at the document root (see the public items mapping and
# ``elasticsearch_private/doc_builder.build_tenant_feature_doc``).
#
# ``asset_id`` is the mutable logical label; the items index stores only the
# immutable ``asset_physical_id`` UUID (the logical label is internal-only and
# never written to item _source).  CQL filters on ``asset_id`` are therefore
# routed to the ``asset_physical_id`` ES field so they hit the indexed keyword.
# ``asset_physical_id`` is also registered directly for callers that know the
# UUID.  This map is consumed exclusively by item/STAC-search paths; the
# separate assets index retains its own ``asset_id`` keyword field and is not
# queried through this translator.
_ENVELOPE_FIELD_PATHS: Dict[str, str] = {
    "external_id": "external_id",
    "asset_id": "asset_physical_id",
    "asset_physical_id": "asset_physical_id",
    "geoid": "geoid",
    "id": "id",
    "geometry": "geometry",
    "datetime": "properties.datetime",
}

# Queryable ``data_type`` values that mark a field as geometry — geometry maps
# to the top-level ``geometry`` field regardless of its declared name.
_GEOMETRY_DATA_TYPE_PREFIX = "geometry"

# Canonical string ``data_type`` values whose dynamically-mapped ES field is an
# analyzed ``text`` field with a ``.keyword`` sub-field (the Elasticsearch
# default for a string under a ``dynamic: true`` object — see the private
# tenant ``properties`` sub-tree and the public ``extras`` bucket). CQL2 exact
# matches (``=`` / ``<>`` / ``in`` / ``like``) must target ``.keyword`` or the
# ``term`` query misses the analyzed (lower-cased / tokenized) token. Numeric,
# temporal and boolean dynamic fields are queried directly. Envelope fields with
# an explicit ``keyword`` mapping (external_id, asset_id, …) are resolved before
# this and never need the suffix.
_KEYWORD_SUBFIELD_DATA_TYPES = frozenset({"string", "uuid"})
_KEYWORD_SUBFIELD = ".keyword"
# Analyzed free-text sub-field. A field declaring ``FieldCapability.FULLTEXT``
# (#1291) is offered this ``.text`` path for an ES ``match`` query — analyzed,
# tokenised free-text search — distinct from the exact ``.keyword`` path that the
# default string mapping (``{"type": "keyword", "fields": {"text": ...}}``) and
# the localized-text blocks already emit alongside the keyword.
_FULLTEXT_SUBFIELD = ".text"


def _has_fulltext(field_def: "Any") -> bool:
    """True when a field declares the authorable ``FULLTEXT`` capability."""
    from dynastore.models.protocols.field_definition import FieldCapability

    caps = getattr(field_def, "capabilities", None) or []
    return FieldCapability.FULLTEXT in caps


def build_es_field_mapping(
    queryable_fields: Dict[str, "Any"],
    *,
    private: bool,
) -> Dict[str, str]:
    """Map each queryable field NAME to its Elasticsearch field path.

    ``queryable_fields`` is the queryables SSOT —
    ``QueryOptimizer.get_all_queryable_fields()`` — keyed by property name with
    :class:`~dynastore.models.protocols.field_definition.FieldDefinition` values.

    Resolution rules:

    * Geometry fields always resolve to the root ``geometry`` field.
    * The ``datetime`` envelope path stays at ``properties.datetime`` (STAC
      convention; both public and private docs write it there).
    * Fields with a ``container`` tag route to their canonical ES path via
      :func:`~dynastore.modules.storage.computed_fields.classify_container`
      (refs #1800): ``stats`` → ``stats.<name>``; ``system`` → ``system.<name>``;
      ``identity`` → flat ``<name>`` at root.
    * Remaining fields (container="properties" or no tag): the **private** flat
      tenant doc indexes them under ``properties.<name>``; the **public** doc
      keeps known queryables under ``properties.<name>`` and routes
      unexposed/extension fields to ``properties.extras.<name>``.
    * String / uuid fields under a dynamic ``properties`` sub-tree carry a
      ``.keyword`` suffix for exact CQL2 matches; numeric / temporal / boolean
      don't. Stats and system fields already carry explicit typed mappings in the
      ES index and do not need the ``.keyword`` suffix.

    The live target is the private flat doc; the public mapping is pragmatic.
    """
    from dynastore.modules.storage.computed_fields import classify_container

    mapping: Dict[str, str] = {}
    for name, field_def in queryable_fields.items():
        data_type = str(getattr(field_def, "data_type", "") or "").lower()

        # Geometry fields always resolve to the root ``geometry`` field.
        if data_type.startswith(_GEOMETRY_DATA_TYPE_PREFIX):
            mapping[name] = "geometry"
            continue

        # ``datetime`` special-case: both public and private docs write it at
        # ``properties.datetime`` regardless of container (mirrors the existing
        # _ENVELOPE_FIELD_PATHS entry). Checked before the container router.
        if name == "datetime":
            mapping[name] = _ENVELOPE_FIELD_PATHS["datetime"]
            continue

        # Explicit envelope fields (external_id, asset_id, geoid, id) keep
        # their root / well-known path regardless of any container tag on the
        # FieldDefinition — these are already classified "identity" by
        # classify_container, but the explicit map is the simpler fast path.
        if name in _ENVELOPE_FIELD_PATHS:
            mapping[name] = _ENVELOPE_FIELD_PATHS[name]
            continue

        # Route tagged containers (stats/system/identity) to canonical paths.
        container = classify_container(name, field_def)

        if container == "stats":
            # Stats fields carry an explicit typed mapping at ``stats.<name>``
            # in the ES index; no ``.keyword`` suffix needed.
            mapping[name] = f"stats.{name}"
            continue

        if container == "system":
            # System fields carry an explicit typed mapping at ``system.<name>``.
            mapping[name] = f"system.{name}"
            continue

        if container == "identity":
            # Identity fields (external_id, asset_id, geoid) are flat at root
            # and already handled by _ENVELOPE_FIELD_PATHS above; this branch
            # is a safety net for any identity-tagged field NOT in that map.
            mapping[name] = name
            continue

        # Default: user / STAC properties lane.
        # Strings under a dynamic ``properties`` sub-tree need the ``.keyword``
        # sub-field for exact CQL2 matches; numeric / temporal / boolean don't.
        suffix = (
            _KEYWORD_SUBFIELD
            if data_type in _KEYWORD_SUBFIELD_DATA_TYPES
            else ""
        )

        if private:
            # Private tenant doc: every attribute lives under ``properties.*``.
            mapping[name] = f"properties.{name}{suffix}"
        else:
            # Public doc: known queryables are flat under ``properties``;
            # extension/unknown fields land in the ``extras`` bucket.
            if bool(getattr(field_def, "expose", True)):
                mapping[name] = f"properties.{name}{suffix}"
            else:
                mapping[name] = f"properties.extras.{name}{suffix}"

    return mapping


def build_es_fulltext_mapping(
    queryable_fields: Dict[str, "Any"],
    *,
    private: bool,
) -> Dict[str, str]:
    """Map FULLTEXT-capable string fields to their analyzed ``.text`` ES path.

    Companion to :func:`build_es_field_mapping` (#1291). It returns ONLY the
    fields that declare :attr:`FieldCapability.FULLTEXT` and whose ``data_type``
    is a string — those are the fields for which an analyzed free-text ``match``
    query is a first-class capability. Every other field is omitted, so a caller
    that joins this map with the exact-match mapping leaves non-FULLTEXT fields
    on their existing ``.keyword`` path (no behaviour change for them).

    The resolved path is the same base path the exact mapping uses, with the
    keyword suffix swapped for ``.text`` — the analyzed sub-field that the string
    dynamic template and the localized-text blocks already emit. Geometry and
    explicit envelope fields are skipped (they are never free-text).
    """
    mapping: Dict[str, str] = {}
    for name, field_def in queryable_fields.items():
        if not _has_fulltext(field_def):
            continue
        data_type = str(getattr(field_def, "data_type", "") or "").lower()
        # Free-text only makes sense for string fields. Geometry / envelope /
        # numeric / temporal fields are not analyzed text even if mis-tagged.
        if data_type not in _KEYWORD_SUBFIELD_DATA_TYPES:
            continue
        if name in _ENVELOPE_FIELD_PATHS:
            continue
        if private:
            mapping[name] = f"properties.{name}{_FULLTEXT_SUBFIELD}"
        else:
            if bool(getattr(field_def, "expose", True)):
                mapping[name] = f"properties.{name}{_FULLTEXT_SUBFIELD}"
            else:
                mapping[name] = f"properties.extras.{name}{_FULLTEXT_SUBFIELD}"
    return mapping


def merge_es_filter(
    base_query: Optional[Dict[str, Any]], extra_clause: Dict[str, Any]
) -> Dict[str, Any]:
    """AND ``extra_clause`` into ``base_query``'s ``bool.filter``.

    Used by both ES items drivers to fold a translated CQL2 clause into the
    structural query body built by ``build_items_query``.

    * ``base_query`` is the *inner* query (``{"bool": {...}}`` / ``{"match_all":
      {}}`` / a leaf clause), NOT the ``{"query": ...}`` envelope.
    * A ``match_all`` (or ``None``) base collapses to just the extra clause.
    * A ``bool`` base gains ``extra_clause`` in its ``filter`` list (created if
      absent), without mutating the input.
    * Any other leaf base is wrapped in a fresh ``bool.filter`` alongside the
      extra clause.
    """
    if base_query is None or base_query == {} or "match_all" in base_query:
        return extra_clause

    if "bool" in base_query:
        bool_body = dict(base_query["bool"])
        bool_body["filter"] = list(bool_body.get("filter", [])) + [extra_clause]
        return {"bool": bool_body}

    return {"bool": {"filter": [base_query, extra_clause]}}
