"""Per-plugin notebook contribution input model.

A `NotebookContribution` is what a plugin's `get_notebooks()` returns.
The aggregator in `NotebooksModule.lifespan()` consumes the list and
seeds `notebooks.platform_notebooks` (ON CONFLICT DO NOTHING).
"""
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from dynastore.models.localization import LocalizedText


class NotebookContribution(BaseModel):
    """Input shape produced by `NotebookContributorProtocol.get_notebooks()`.

    Either `notebook_path` (a `.ipynb` on disk) or `notebook_content`
    (an in-memory nb-format dict) must be provided; not both.
    """

    notebook_id: str = Field(..., description="Stable slug, primary key in platform_notebooks.")
    title: LocalizedText = Field(..., description="Multilanguage human-readable title.")
    description: LocalizedText | None = Field(
        default=None, description="Optional multilanguage description."
    )
    tags: list[str] = Field(default_factory=list, description="Search keywords.")
    notebook_path: Path | None = Field(
        default=None, description="Absolute path to the .ipynb on disk."
    )
    notebook_content: dict[str, Any] | None = Field(
        default=None, description="In-memory nb-format JSON, alternative to path."
    )
    default_catalog_id: str | None = Field(
        default=None,
        description=(
            "UI hint: when the user clicks 'Copy to Catalog', this catalog id "
            "is pre-selected. Purely advisory; the user can override."
        ),
    )
    applies_to: list[str] | None = Field(
        default=None,
        description=(
            "Allowlist of catalog ids the notebook is meant for. "
            "Canonical 'applies to every catalog' value is None; an empty list "
            "is normalised to None at construction so downstream consumers only "
            "ever see one representation."
        ),
    )
    registered_by: str = Field(
        ...,
        description=(
            "Plugin identifier. Recorded on the row; used for audit "
            "and grouping in the admin view. No default — orphan rows are bugs."
        ),
    )

    @field_validator("applies_to", mode="after")
    @classmethod
    def _empty_applies_to_is_none(cls, value: list[str] | None) -> list[str] | None:
        """Normalise an empty allowlist to ``None``.

        Both ``None`` and ``[]`` semantically mean "applies to every catalog";
        collapsing to a single canonical form (``None``) keeps aggregator and
        SQL filter code simple downstream.
        """
        if value is not None and len(value) == 0:
            return None
        return value

    @model_validator(mode="after")
    def _exactly_one_source(self) -> "NotebookContribution":
        has_path = self.notebook_path is not None
        has_content = self.notebook_content is not None
        if has_path and has_content:
            raise ValueError(
                "notebook_path and notebook_content are mutually exclusive."
            )
        if not has_path and not has_content:
            raise ValueError(
                "Provide one of notebook_path or notebook_content."
            )
        return self
