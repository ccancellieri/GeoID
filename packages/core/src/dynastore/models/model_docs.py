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

"""Shared meta/doc extraction helpers for PluginConfig-like classes.

Extracted from the configs extension so that presets and any future extension
can reuse the same ``_meta`` assembly logic without duplicating it.

Public surface:
  - :func:`extract_field_docs` — ``{field_name: description}`` from JSON schema.
  - :func:`extract_mutability`  — ``{field_name: kind}`` via mutability markers.
  - :func:`build_meta_block`   — compose a ``_meta`` dict for a given mode.
  - :func:`strip_meta_envelopes` — drop ``_meta`` / ``_links`` from a payload dict.

No dependency on any configs extension module — lives entirely in core.
"""

from __future__ import annotations

import functools
from typing import Any, Dict, Optional


@functools.lru_cache(maxsize=256)
def extract_field_docs(model_cls: type) -> Dict[str, str]:
    """Return ``{field_name: description}`` from *model_cls*'s JSON schema.

    Iterates the ``properties`` map produced by ``model_json_schema()`` and
    keeps only fields that declare a non-empty string ``description``.  The
    result is cached per class — schemas are static after class definition.

    Returns an empty dict if the schema cannot be obtained.
    """
    try:
        schema = model_cls.model_json_schema()
    except Exception:
        return {}
    props = schema.get("properties", {})
    out: Dict[str, str] = {}
    for name, spec in props.items():
        if not isinstance(spec, dict):
            continue
        desc = spec.get("description")
        if isinstance(desc, str) and desc:
            out[name] = desc
    return out


@functools.lru_cache(maxsize=256)
def extract_mutability(model_cls: type) -> Dict[str, str]:
    """Return ``{field_name: kind}`` for *model_cls*.

    Delegates to the Protocol-based introspection API
    (``MutabilityIntrospectionProtocol.mutability_map``) so the renderer
    stays decoupled from the concrete ``PluginConfig`` — any class that
    implements the Protocol surface works.

    Falls back to the standalone ``mutability_map`` helper for classes that
    carry Pydantic ``model_fields`` without formally implementing the Protocol.

    Returns an empty dict if introspection fails for any reason.
    """
    from dynastore.models.mutability import (
        MutabilityIntrospectionProtocol,
        mutability_map,
    )
    if isinstance(model_cls, type) and issubclass(
        model_cls, MutabilityIntrospectionProtocol  # type: ignore[arg-type]
    ):
        try:
            return dict(model_cls.mutability_map())
        except Exception:
            pass
    try:
        return mutability_map(model_cls)
    except Exception:
        return {}


def build_meta_block(
    model_cls: type,
    *,
    tier: Optional[str] = None,
    source: Optional[str] = None,
    mode: str = "field",
) -> Dict[str, Any]:
    """Compose a ``_meta`` dict for *model_cls* under *mode*.

    Always-on keys:
      - ``"tier"``   — included only when *tier* is not ``None``.
      - ``"source"`` — included only when *source* is not ``None``.

    Mode-gated extras:
      - ``"none"``   — no extra keys; returns ``{}`` (tier/source also absent).
      - ``"schema"`` — ``"json_schema"`` key holding the full Pydantic schema.
      - ``"field"``  — ``"docs"`` (field-description map) + ``"mutability"``
                       (field-kind map) when non-empty.

    Mirrors the ``_doc_extras`` / ``_meta`` assembly that lives in the configs
    extension composer.
    """
    if mode == "none":
        return {}

    out: Dict[str, Any] = {}
    if tier is not None:
        out["tier"] = tier
    if source is not None:
        out["source"] = source

    if mode == "schema":
        out["json_schema"] = model_cls.model_json_schema()
    elif mode == "field":
        out["docs"] = extract_field_docs(model_cls)
        mutability = extract_mutability(model_cls)
        if mutability:
            out["mutability"] = mutability

    return out


def strip_meta_envelopes(body: Any) -> Any:
    """Drop ``_meta`` and ``_links`` envelope keys from *body*.

    Returns *body* unchanged when it is not a dict.  Safe to call on any
    composed GET payload before round-tripping it through a PUT/PATCH body
    that enforces ``extra="forbid"``.
    """
    if not isinstance(body, dict):
        return body
    return {k: v for k, v in body.items() if k not in ("_meta", "_links")}
