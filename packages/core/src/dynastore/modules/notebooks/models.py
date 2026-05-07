"""Notebook domain models.

title and description are ``LocalizedText`` (multilanguage) stored as JSONB.
tags are auto-generated from the English title if not explicitly provided.
"""
from enum import StrEnum
from pydantic import BaseModel, Field, model_validator
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from dynastore.models.localization import LocalizedText


# ---------------------------------------------------------------------------
# Stop-words used for auto-tag generation (English)
# ---------------------------------------------------------------------------

_STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "of", "in", "on", "at", "to", "for",
    "is", "are", "was", "were", "be", "been", "by", "with", "this", "that",
    "it", "its", "as", "from", "how", "use", "using", "demo", "example",
    "notebook", "guide", "setup",
})


def _auto_tags_from_title(title: Union[LocalizedText, Dict, str, None]) -> List[str]:
    """Generate tags from the English (or first available) title value."""
    if title is None:
        return []
    if isinstance(title, str):
        text = title
    elif isinstance(title, dict):
        text = title.get("en") or next(iter(title.values()), "") or ""
    elif hasattr(title, "en") and title.en:
        text = title.en
    elif hasattr(title, "model_dump"):
        vals = [v for v in title.model_dump().values() if v]
        text = vals[0] if vals else ""
    else:
        text = str(title)
    tokens = [w.lower() for w in text.split() if w.isalpha() and len(w) > 2]
    return sorted({t for t in tokens if t not in _STOP_WORDS})


class OwnerType(StrEnum):
    MODULE = "module"
    SYSADMIN = "sysadmin"


class NotebookBase(BaseModel):
    """Base model for notebook data."""

    notebook_id: str = Field(..., description="Logical ID/slug for the notebook.")
    title: LocalizedText = Field(..., description="Multilanguage human-readable title.")
    description: Optional[LocalizedText] = Field(
        default=None,
        description="Optional multilanguage description.",
    )
    tags: List[str] = Field(
        default_factory=list,
        description=(
            "Search tags. Auto-generated from English title tokens if empty. "
            "Stored as JSONB; supports exact-match filtering."
        ),
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary extra metadata (kernel specs, requirements, etc.).",
    )

    @model_validator(mode="after")
    def _fill_tags(self) -> "NotebookBase":
        if not self.tags:
            self.tags = _auto_tags_from_title(self.title)
        return self


class NotebookCreate(NotebookBase):
    """Model for creating/saving a tenant notebook."""

    content: Dict[str, Any] = Field(..., description="The full .ipynb JSON structure.")


class Notebook(NotebookCreate):
    """Full tenant notebook model including system fields."""

    catalog_id: str = Field(..., description="The catalog this notebook belongs to.")
    created_at: datetime = Field(..., description="Timestamp when created.")
    updated_at: datetime = Field(..., description="Timestamp when last updated.")
    deleted_at: Optional[datetime] = Field(default=None, description="Soft-delete timestamp.")
    owner_id: Optional[str] = Field(default=None, description="Principal ID of the owner.")
    copied_from: Optional[str] = Field(
        default=None, description="Platform notebook_id this was copied from."
    )


class PlatformNotebookCreate(NotebookBase):
    """Model for creating/saving a platform-level notebook."""

    content: Dict[str, Any] = Field(..., description="The full .ipynb JSON structure.")
    registered_by: str = Field(..., description="Module name or 'sysadmin'.")
    owner_type: OwnerType = Field(..., description="'module' or 'sysadmin'.")


class PlatformNotebook(PlatformNotebookCreate):
    """Full platform notebook model including system fields."""

    created_at: datetime = Field(..., description="Timestamp when created.")
    updated_at: datetime = Field(..., description="Timestamp when last updated.")
    deleted_at: Optional[datetime] = Field(default=None, description="Soft-delete timestamp.")
