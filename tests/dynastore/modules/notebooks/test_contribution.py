from pathlib import Path

import pytest
from pydantic import ValidationError

from dynastore.models.localization import LocalizedText
from dynastore.modules.notebooks.contribution import NotebookContribution


def test_path_or_content_required():
    with pytest.raises(
        ValidationError, match="Provide one of notebook_path or notebook_content"
    ):
        NotebookContribution(
            notebook_id="x",
            title=LocalizedText(en="X"),
            registered_by="t",
        )


def test_path_and_content_mutually_exclusive(tmp_path: Path):
    nb = tmp_path / "x.ipynb"
    nb.write_text("{}")
    with pytest.raises(ValidationError, match="mutually exclusive"):
        NotebookContribution(
            notebook_id="x",
            title=LocalizedText(en="X"),
            registered_by="t",
            notebook_path=nb,
            notebook_content={},
        )


def test_minimal_path_form(tmp_path: Path):
    nb = tmp_path / "x.ipynb"
    nb.write_text("{}")
    c = NotebookContribution(
        notebook_id="x",
        title=LocalizedText(en="X"),
        registered_by="t",
        notebook_path=nb,
    )
    assert c.applies_to is None
    assert c.default_catalog_id is None


def test_applies_to_accepts_list_of_ids():
    c = NotebookContribution(
        notebook_id="x",
        title=LocalizedText(en="X"),
        registered_by="t",
        notebook_content={},
        applies_to=["demo-catalog", "spanner-catalog"],
    )
    assert c.applies_to == ["demo-catalog", "spanner-catalog"]


def test_applies_to_empty_list_normalised_to_none():
    c = NotebookContribution(
        notebook_id="x",
        title=LocalizedText(en="X"),
        registered_by="t",
        notebook_content={},
        applies_to=[],
    )
    assert c.applies_to is None
