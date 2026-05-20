#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Single source of truth for the 6 composed-config GET query params.

The 3 composed-config handlers (`get_platform_config_composed` /
`get_catalog_config_composed` / `get_collection_config_composed`)
expose the same six query knobs (`resolved`, `meta`, `include`,
`strict`, `links`, `view`).  Two surfaces consume them:

1. **Handler signatures** — FastAPI validates + populates OpenAPI from
   `Annotated[py_type, Query(...)]` aliases below.
2. **`self` link `hrefSchema`** — `ConfigApiService._query_param_schema`
   returns a JSON Schema dict that operators read at runtime to
   discover supported params without scanning OpenAPI separately.

Before this module both surfaces hardcoded the description / enum /
default text independently, so touching one and not the other drifted
the runtime-visible docs.  Now both derive from the `_PARAMS` table
below; touching the description once updates both surfaces.

Defaults live on the parameter (``= "none"``) rather than inside
``Query(...)`` because FastAPI rejects setting both when ``Annotated``
is used.  The `_PARAMS` table still tracks the default so
`QUERY_PARAM_SCHEMA` can advertise it as the JSON Schema ``default``.
"""

from typing import Annotated, Any, Dict, List, Optional

from fastapi import Query


_PARAMS: Dict[str, Dict[str, Any]] = {
    "resolved": {
        "py_type": bool,
        "type": "boolean",
        "default": True,
        "examples": [True, False],
        "description": (
            "When true (default): all registered configs with waterfall-resolved "
            "values; each leaf's ``_meta.source`` tells you which tier "
            "provided the resolved value.  When false: only configs explicitly "
            "stored at this scope (delta-only, safe for read-modify-write flows)."
        ),
    },
    "meta": {
        "py_type": str,
        "type": "string",
        "enum": ["none", "field", "schema"],
        "default": "field",
        "examples": ["field", "schema", "none"],
        "description": (
            "Per-class metadata sibling injected INLINE on each in-scope "
            "plugin leaf as a ``_meta`` block.  ``none`` — no ``_meta`` "
            "key is written; the leaf payload is a clean delta safe to "
            "copy verbatim into a PATCH body (#946).  ``field`` (default) "
            "— ``_meta = {tier, source, docs: {field_name: description}}`` "
            "so dashboards see per-field documentation and provenance.  "
            "``schema`` — ``_meta = {tier, source, json_schema: <full "
            "Pydantic schema 2020-12>}`` (heavier, form-builder ready).  "
            "PUT/PATCH handlers strip ``_meta`` on ingress so payloads "
            "round-trip cleanly regardless of the mode used to fetch them."
        ),
    },
    "include": {
        "py_type": str,
        "type": "string",
        "enum": ["scope", "upstream"],
        "default": "scope",
        "examples": ["scope", "upstream"],
        "description": (
            "Body-rendering mode.  Retained for backward compatibility; "
            "prefer the ``view`` parameter for explicit delta/inherited "
            "filtering at sub-platform tiers.  ``scope`` (default) — at "
            "platform scope (combined with ``strict=True``, the default) "
            "drops ``_visibility=catalog``/``collection`` templates so the "
            "body lists only platform-intrinsic configs.  ``upstream`` — "
            "renders every visible class with its waterfall-resolved value "
            "(at platform scope this also inlines catalog-/collection-tier "
            "templates, mirroring ``strict=False``).  At catalog and "
            "collection scope both values honour the post-#761 'complete "
            "the configurable surface' contract — every config the tier "
            "can see is rendered with ``_meta.source`` reporting the "
            "effective tier.  Use ``view=delta`` or ``view=inherited`` "
            "to filter the body by provenance at sub-platform tiers."
        ),
    },
    "view": {
        "py_type": str,
        "type": "string",
        "enum": ["effective", "delta", "inherited"],
        "default": "effective",
        "examples": ["effective", "delta", "inherited"],
        "description": (
            "Provenance-based leaf filter applied AFTER waterfall "
            "resolution.  ``effective`` (default) — the full composed "
            "surface, identical to today's default response; every leaf "
            "placed by ``_place()`` is rendered regardless of which tier "
            "provided its value.  ``delta`` — only leaves whose "
            "``_meta.source`` equals the active scope (platform / catalog "
            "/ collection); leaves inherited verbatim from upstream tiers "
            "are suppressed.  Equivalent to 'show me what THIS scope "
            "explicitly overrides', making the response safe to copy "
            "verbatim into a PATCH body when combined with "
            "``resolved=true`` (upstream defaults are not re-submitted).  "
            "``inherited`` — only leaves whose ``_meta.source`` differs "
            "from the active scope (platform / catalog / collection); the "
            "active tier has not overridden them.  Useful for operators "
            "who want to audit what they still inherit from upstream "
            "tiers.  Note: at platform scope there is no upstream tier, "
            "so ``delta`` returns every stored leaf and ``inherited`` "
            "returns only leaves resolved from code defaults "
            "(source='default').  The ``view`` param is independent of "
            "``include`` and ``resolved``; combining ``view=delta`` with "
            "``resolved=true`` yields 'local overrides rendered with "
            "their effective values and upstream defaults applied to the "
            "non-overridden fields' — the most useful read-modify-write "
            "view."
        ),
    },
    "strict": {
        "py_type": bool,
        "type": "boolean",
        "default": True,
        "examples": [True, False],
        "description": (
            "Cycle F.7d.2 — at platform scope, narrow the body to "
            "platform-intrinsic configs (``modules``, ``extensions``, "
            "``tasks``, ``engines``).  Catalog-/collection-tier templates "
            "are filtered out.  ``false`` restores the previous always-true "
            "platform-scope inclusion (catalog templates inline in the body). "
            " No effect at catalog or collection scope; accepted for API "
            "symmetry so the same query-string template works at every tier."
        ),
    },
    "links": {
        "py_type": str,
        "type": "string",
        "enum": ["none", "minimal", "full"],
        "default": "minimal",
        "examples": ["minimal", "full", "none"],
        "description": (
            "Per-plugin HATEOAS edit affordances injected INLINE on each "
            "in-scope leaf as a ``_links`` sibling.  Each leaf gets 4 "
            "affordances: ``self`` (GET), ``edit`` (PUT — replace), ``edit`` "
            "(DELETE — clear override), ``describedby`` (GET "
            "registry/{class_key}).  ``minimal`` (default) — "
            "``rel``/``href``/``method`` only.  ``full`` — adds a contextual "
            "``title`` per link naming the class key and tier "
            "(catalog/collection ids included), plus ``rel=schema`` and "
            "``rel=engine`` cross-links.  ``none`` — no ``_links`` on any "
            "leaf, including routing-config ``DriverRef`` entries which "
            "previously emitted a ``driver-config`` link unconditionally "
            "(#946).  PUT/PATCH handlers strip ``_links`` on ingress so "
            "payloads round-trip cleanly regardless of the mode used "
            "to fetch them."
        ),
    },
}


def _alias(name: str) -> Any:
    spec = _PARAMS[name]
    query_kwargs: Dict[str, Any] = {"description": spec["description"]}
    enum: Optional[List[str]] = spec.get("enum")
    if enum is not None:
        query_kwargs["pattern"] = "^(" + "|".join(enum) + ")$"
    return Annotated[spec["py_type"], Query(**query_kwargs)]


def _schema_property(spec: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"type": spec["type"], "default": spec["default"]}
    if "enum" in spec:
        out["enum"] = spec["enum"]
    out["description"] = spec["description"]
    out["examples"] = spec["examples"]
    return out


ResolvedQuery = _alias("resolved")
MetaQuery = _alias("meta")
IncludeQuery = _alias("include")
StrictQuery = _alias("strict")
LinksQuery = _alias("links")
ViewQuery = _alias("view")


QUERY_PARAM_SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {name: _schema_property(spec) for name, spec in _PARAMS.items()},
    "additionalProperties": False,
}
