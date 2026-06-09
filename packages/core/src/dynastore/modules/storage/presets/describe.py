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

"""Pure, DB-free preset descriptor helpers.

Public surface:
  - :func:`build_preview_bundle` — build a ``PresetBundle`` from a
    validated params instance against a synthetic scope, with no DB.
  - :func:`describe_preset` — return a rich descriptor dict (identity +
    params schema + field docs + worked examples with live config).
  - :func:`descriptor_to_markdown` — render a descriptor to Markdown.
  - :func:`descriptor_to_html`    — render a descriptor to HTML.
"""
from __future__ import annotations

import html
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Synthetic scope used when building preview bundles (no real DB required).
_SYNTHETIC_SCOPE: Dict[str, str] = {
    "catalog_id": "example-catalog",
    "collection_id": "example-collection",
}

# Markdown extensions passed to the ``markdown`` package when available.
_MARKDOWN_EXTENSIONS = ["fenced_code", "tables"]


def build_preview_bundle(preset: Any, params_obj: Any) -> Optional[Any]:
    """Build a ``PresetBundle`` from a validated params instance + synthetic scope.

    Uses the ``_build_bundle(params, scope_kwargs)`` entry point on
    ``BundlePreset`` subclasses.  Falls back to ``build(**scope_kwargs)``
    for plain presets.  Returns ``None`` on any exception (e.g. composites
    that genuinely need DB look-ups).  Never touches the database.
    """
    try:
        if hasattr(preset, "_build_bundle"):
            return preset._build_bundle(params_obj, _SYNTHETIC_SCOPE)
        if hasattr(preset, "build"):
            return preset.build(**_SYNTHETIC_SCOPE)
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "build_preview_bundle: preset=%s failed: %s",
            getattr(preset, "name", "?"), exc,
        )
    return None


def _tier_value(preset: Any) -> str:
    tier = getattr(preset, "tier", None)
    if tier is None:
        return ""
    if hasattr(tier, "value"):
        return str(tier.value)
    return str(tier)


def _serialize_bundle(bundle: Any) -> Optional[List[Dict[str, Any]]]:
    """Serialise a ``PresetBundle`` to a list of entry dicts."""
    if bundle is None:
        return None
    try:
        out: List[Dict[str, Any]] = []
        for entry in bundle.iter_apply():
            out.append({
                "slot": entry.slot,
                "config_cls": entry.config_cls.__name__,
                "config": json.loads(entry.instance.model_dump_json()),
            })
        return out
    except Exception as exc:  # noqa: BLE001
        logger.debug("_serialize_bundle failed: %s", exc)
        return None


def _example_entry(preset: Any, example: Any) -> Dict[str, Any]:
    """Build the descriptor dict for one ``PresetExample``."""
    pm = getattr(preset, "params_model", None)
    entry: Dict[str, Any] = {
        "name": example.name,
        "summary": example.summary,
        "params": example.params,
        "resulting_config": None,
    }
    try:
        if pm is not None:
            params_obj = pm.model_validate(example.params)
        else:
            params_obj = None
        bundle = build_preview_bundle(preset, params_obj)
        entry["resulting_config"] = _serialize_bundle(bundle)
    except Exception as exc:  # noqa: BLE001
        entry["error"] = str(exc)
    return entry


def describe_preset(preset: Any, *, mode: str = "field") -> Dict[str, Any]:
    """Return a rich descriptor dict for *preset*.

    Keys:
      - ``name``, ``description``, ``tier``, ``keywords``,
        ``catalog_scopable``, ``params_schema``, ``examples`` — always present.
      - ``_meta`` — present when *mode* != ``"none"`` and the preset has a
        meaningful ``params_model`` (i.e. not ``NoParams`` / absent).

    Each example in ``examples`` contains ``name``, ``summary``, ``params``,
    ``resulting_config`` (list of entry dicts or ``None``), and optionally
    ``error`` when validation or building failed.
    """
    from dynastore.models.model_docs import build_meta_block
    from .preset import NoParams

    pm = getattr(preset, "params_model", None)
    params_schema: Optional[Dict[str, Any]] = None
    if pm is not None and hasattr(pm, "model_json_schema"):
        try:
            params_schema = pm.model_json_schema()
        except Exception:  # noqa: BLE001
            params_schema = None

    examples_raw = getattr(preset, "examples", ()) or ()
    examples_out = [_example_entry(preset, ex) for ex in examples_raw]

    descriptor: Dict[str, Any] = {
        "name": getattr(preset, "name", ""),
        "description": getattr(preset, "description", ""),
        "tier": _tier_value(preset),
        "keywords": list(getattr(preset, "keywords", ())),
        "catalog_scopable": bool(getattr(preset, "catalog_scopable", False)),
        "params_schema": params_schema,
        "examples": examples_out,
    }

    # Inject _meta only when mode != "none" and params_model is meaningful.
    has_real_params = (
        pm is not None
        and pm is not NoParams
        and hasattr(pm, "model_fields")
    )
    if mode != "none" and has_real_params:
        tier_str = _tier_value(preset) or None
        descriptor["_meta"] = build_meta_block(pm, tier=tier_str, mode=mode)

    return descriptor


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def descriptor_to_markdown(descriptor: Dict[str, Any]) -> str:
    """Render *descriptor* to a Markdown string.

    Structure:
      - H1: preset name
      - Description paragraph
      - Parameter fields table (from ``_meta.docs`` when present)
      - One section per example with fenced JSON blocks
    """
    name = descriptor.get("name", "")
    description = descriptor.get("description", "")
    tier = descriptor.get("tier", "")
    keywords = descriptor.get("keywords", [])
    meta = descriptor.get("_meta", {})
    docs: Dict[str, str] = meta.get("docs", {}) if isinstance(meta, dict) else {}

    lines: List[str] = []
    lines.append(f"# {name}")
    lines.append("")
    if description:
        lines.append(description)
        lines.append("")

    lines.append(f"**Tier:** {tier}  ")
    if keywords:
        lines.append(f"**Keywords:** {', '.join(keywords)}")
    lines.append("")

    if descriptor.get("params_schema"):
        lines.append("## Parameters")
        lines.append("")
        if docs:
            lines.append("| Field | Description |")
            lines.append("|-------|-------------|")
            for field_name, field_desc in docs.items():
                lines.append(f"| `{field_name}` | {field_desc} |")
        else:
            lines.append("```json")
            lines.append(json.dumps(descriptor["params_schema"], indent=2))
            lines.append("```")
        lines.append("")

    examples = descriptor.get("examples", [])
    if examples:
        lines.append("## Examples")
        lines.append("")
        for ex in examples:
            lines.append(f"### {ex.get('name', '')}")
            lines.append("")
            summary = ex.get("summary", "")
            if summary:
                lines.append(summary)
                lines.append("")
            lines.append("**Params:**")
            lines.append("")
            lines.append("```json")
            lines.append(json.dumps(ex.get("params", {}), indent=2))
            lines.append("```")
            lines.append("")
            rc = ex.get("resulting_config")
            if rc is not None:
                lines.append("**Resulting config:**")
                lines.append("")
                lines.append("```json")
                lines.append(json.dumps(rc, indent=2))
                lines.append("```")
                lines.append("")
            error = ex.get("error")
            if error:
                lines.append(f"> Preview unavailable: {error}")
                lines.append("")

    return "\n".join(lines)


def descriptor_to_html(descriptor: Dict[str, Any]) -> str:
    """Render *descriptor* to an HTML string.

    Tries to use the ``markdown`` package with fenced-code + tables
    extensions.  Falls back to a ``<pre>`` block if ``markdown`` is not
    installed.  Never raises.
    """
    md_text = descriptor_to_markdown(descriptor)
    try:
        import markdown as _markdown  # lazy import
        return _markdown.markdown(md_text, extensions=_MARKDOWN_EXTENSIONS)
    except ImportError:
        return "<pre>" + html.escape(md_text) + "</pre>"
    except Exception as exc:  # noqa: BLE001
        logger.debug("descriptor_to_html: markdown render failed: %s", exc)
        return "<pre>" + html.escape(md_text) + "</pre>"
