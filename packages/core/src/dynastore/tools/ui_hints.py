#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""
UI hints for Pydantic-generated JSON Schemas.

The admin Configuration Hub frontend is schema-driven: it reads the JSON
Schema emitted by ``/configs/schemas`` and renders generic form components
based on ``x-ui`` vendor extensions merged into property and model schemas.

This module is the single place that defines the ``x-ui`` contract shape,
so evolving the convention (new widgets, new sections) touches one file.

Conventions
-----------
Every hint lives under the top-level ``x-ui`` key (a dict) in the JSON
schema of a property or of a whole model. Known keys:

- ``widget``    — renderer hint: ``password`` | ``textarea`` | ``json`` |
                  ``select`` | ``kvlist`` (None → default per type)
- ``section``   — group label for collapsible grouping of sibling fields
- ``category``  — model-level category used by the left-rail picker
                  (``routing`` | ``storage-drivers`` | ``asset-drivers`` |
                  ``metadata-drivers`` | ``observability`` | ``auth`` |
                  ``other``; frontend falls back to ``other`` if absent)
- ``sensitive`` — mask on display, don't echo back to the server unless
                  the user edited the field (Secret-wrapped fields)
- ``readonly``  — disable input on edit (Immutable / WriteOnce fields)
- ``hint``      — short help text shown under the field
- ``order``     — integer for sort override within a section

Frontend code must treat unknown keys as ignorable so adding new hints is
a one-sided change (backend emits, old frontends ignore).
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def ui(
    widget: Optional[str] = None,
    section: Optional[str] = None,
    category: Optional[str] = None,
    sensitive: Optional[bool] = None,
    readonly: Optional[bool] = None,
    hint: Optional[str] = None,
    order: Optional[int] = None,
) -> Dict[str, Any]:
    """Build a ``json_schema_extra`` dict with an ``x-ui`` sub-object.

    Pass to ``Field(..., json_schema_extra=ui(...))`` on a property, or to
    ``model_config = ConfigDict(json_schema_extra=ui(...))`` on a model.
    Only non-None arguments are emitted, so the schema stays minimal.
    """
    hints: Dict[str, Any] = {}
    if widget is not None:
        hints["widget"] = widget
    if section is not None:
        hints["section"] = section
    if category is not None:
        hints["category"] = category
    if sensitive is not None:
        hints["sensitive"] = sensitive
    if readonly is not None:
        hints["readonly"] = readonly
    if hint is not None:
        hints["hint"] = hint
    if order is not None:
        hints["order"] = order
    return {"x-ui": hints}


def merge_ui(schema: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
    """Merge UI hints into an existing JSON-schema dict, in place.

    Used by ``__get_pydantic_json_schema__`` implementations that wrap a
    base handler's output — e.g. the ``ImmutableMarker`` hook that needs
    to add ``readonly: true`` without replacing the rest of the schema.
    """
    existing = schema.get("x-ui")
    if not isinstance(existing, dict):
        existing = {}
    existing.update({k: v for k, v in kwargs.items() if v is not None})
    schema["x-ui"] = existing
    return schema


__all__ = ["ui", "merge_ui"]
