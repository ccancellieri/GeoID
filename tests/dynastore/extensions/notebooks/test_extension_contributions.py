"""Pin every extension's ``build_contributions()`` against the
``NotebookContribution`` model contract.

#587: when ``registered_by`` was made required in #409, the two
extensions with explicit constructors regressed at startup because no
test exercised the discovery path. This file walks every package that
publishes a ``notebooks.py`` module and asserts the model validates.
"""
from __future__ import annotations

import importlib

import pytest

EXTENSION_NOTEBOOK_MODULES = [
    "dynastore.extensions.stac.notebooks",
    "dynastore.extensions.features.notebooks",
    "dynastore.extensions.records.notebooks",
    "dynastore.extensions.coverages.notebooks",
    "dynastore.extensions.assets.notebooks",
    "dynastore.extensions.web.notebooks",
]


@pytest.mark.parametrize("module_path", EXTENSION_NOTEBOOK_MODULES)
def test_build_contributions_returns_valid_contributions(module_path: str) -> None:
    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        pytest.skip(f"{module_path} not installed: {exc}")

    contribs = mod.build_contributions()
    assert isinstance(contribs, list)
    for c in contribs:
        # `registered_by` is required (#409). Re-validating the
        # model would mask attribute access bugs, so we touch fields
        # directly.
        assert c.notebook_id
        assert c.notebook_path
        assert c.registered_by, (
            f"{module_path}: contribution {c.notebook_id!r} missing registered_by"
        )
