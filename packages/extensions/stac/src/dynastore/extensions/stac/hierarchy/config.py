"""Unified config for all hierarchy-provider kinds."""

from __future__ import annotations

from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field, model_validator

from dynastore.modules.stac.stac_config import HierarchyRule


ProviderKind = Literal["data-derived", "dimension-backed", "static", "external-skos"]


class HierarchyProviderConfig(BaseModel):
    """Single config entry for one hierarchy — dispatched by `kind`."""

    kind: ProviderKind = Field(
        ...,
        description="Which provider implementation produces this hierarchy.",
    )
    hierarchy_id: str = Field(..., description="Stable id for the hierarchy (path key).")
    level_name: Optional[str] = None
    collection_title_template: Optional[str] = None
    collection_description_template: Optional[str] = None

    # data-derived only
    rule: Optional[HierarchyRule] = Field(
        default=None,
        description="SQL rule driving a data-derived provider.",
    )

    # dimension-backed only
    dimension_id: Optional[str] = Field(
        default=None,
        description="ogc-dimensions id whose hierarchical provider drives this.",
    )

    # extension point for static / external-skos / future kinds
    params: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _coerce_bare_rule(cls, data: Any) -> Any:
        """Back-compat: a bare `HierarchyRule`-shaped dict becomes `kind=data-derived`."""
        if isinstance(data, dict) and "kind" not in data:
            if "rule" in data or "item_code_field" in data:
                rule_payload = data.get("rule", data)
                hid = (
                    data.get("hierarchy_id")
                    or rule_payload.get("hierarchy_id")
                    if isinstance(rule_payload, dict)
                    else None
                )
                return {
                    "kind": "data-derived",
                    "hierarchy_id": hid or "default",
                    "rule": rule_payload,
                }
        return data

    @model_validator(mode="after")
    def _check_kind_requirements(self) -> "HierarchyProviderConfig":
        if self.kind == "data-derived" and self.rule is None:
            raise ValueError("kind='data-derived' requires `rule`")
        if self.kind == "dimension-backed" and not self.dimension_id:
            raise ValueError("kind='dimension-backed' requires `dimension_id`")
        if self.kind == "static" and "tree" not in self.params:
            raise ValueError("kind='static' requires params.tree")
        return self
