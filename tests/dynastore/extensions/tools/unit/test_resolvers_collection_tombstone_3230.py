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

"""Regression coverage for #3230: the shared ``resolve_collection_or_404``
resolver must state the tombstoned-catalog contract explicitly rather than
depending on a generic ``ValueError``-message-sniffing exception handler.

Every collection-level read route (OGC Features / STAC / EDR / Records /
Volumes collection GET and items listing) reaches its catalog/collection
through this one resolver. ``get_collection`` resolves the physical schema
via ``resolve_physical_schema(allow_missing=False)``, which raises
``ValueError`` for a tombstoned (or genuinely missing) catalog — this
resolver now catches that directly and maps it to the same 404 a missing
collection gets, independent of the exception's message text.

The catch is deliberately narrowed to a ``"not found"`` message: in
Pydantic v2 ``pydantic.ValidationError`` is itself a ``ValueError``
subclass, and ``get_collection`` ends in ``Collection.model_validate(...)``
— a malformed ``Extent``/``SpatialExtent`` field (see
``models/shared_models.py``'s ``validate_bbox``) raises exactly that. A
blanket ``except ValueError`` would mask that data-integrity failure as a
plain 404 instead of surfacing it (422/500) via the generic
``ValidationExceptionHandler``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from dynastore.extensions.tools.resolvers import resolve_collection_or_404
from dynastore.models.shared_models import Collection


class _FakeCatalogsService:
    def __init__(self, *, collection: Any = None, not_found_error: Exception | None = None) -> None:
        if not_found_error is not None:
            async def _raise(*_a: Any, **_kw: Any) -> Any:
                raise not_found_error

            self.get_collection = AsyncMock(side_effect=_raise)
        else:
            self.get_collection = AsyncMock(return_value=collection)


@pytest.mark.asyncio
async def test_resolve_collection_or_404_404s_on_a_tombstoned_parent_catalog():
    """A tombstoned parent catalog surfaces as ``ValueError`` from
    ``get_collection`` (via ``resolve_physical_schema(allow_missing=False)``)
    — the resolver must map it to 404 explicitly, not rely on a downstream
    handler pattern-matching the message text."""
    catalogs_svc = _FakeCatalogsService(
        not_found_error=ValueError("Catalog 'deleted-cat' not found.")
    )

    with pytest.raises(HTTPException) as exc_info:
        await resolve_collection_or_404(catalogs_svc, "deleted-cat", "col-a")

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_resolve_collection_or_404_404s_a_missing_collection():
    """Sibling guard: the pre-existing falsy-result path is unchanged."""
    catalogs_svc = _FakeCatalogsService(collection=None)

    with pytest.raises(HTTPException) as exc_info:
        await resolve_collection_or_404(catalogs_svc, "cat", "missing-col")

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_resolve_collection_or_404_returns_an_active_collection():
    """Sibling guard: an active collection on a live catalog still resolves."""
    active = Collection.model_validate({"id": "col-a"})
    catalogs_svc = _FakeCatalogsService(collection=active)

    result = await resolve_collection_or_404(catalogs_svc, "live-cat", "col-a")

    assert result is active


@pytest.mark.asyncio
async def test_resolve_collection_or_404_does_not_mask_a_validation_error():
    """A ``ValidationError`` from ``Collection.model_validate`` (e.g. a
    malformed ``Extent``/``SpatialExtent`` bbox) is a ``ValueError``
    subclass in Pydantic v2 but is not a "not found" signal — it must
    propagate unchanged so the generic ``ValidationExceptionHandler`` can
    render it as 422/500, not be swallowed into a misleading 404 that hides
    a data-integrity bug behind a resource-absence status."""
    try:
        Collection.model_validate(
            {
                "id": "col-a",
                "extent": {
                    "spatial": {"bbox": [[10.0, 0.0, 0.0, 0.0]]},
                    "temporal": {"interval": [[None, None]]},
                },
            }
        )
        raise AssertionError("expected Collection.model_validate to raise")
    except ValidationError as validation_error:
        assert "not found" not in str(validation_error).lower()
        catalogs_svc = _FakeCatalogsService(not_found_error=validation_error)

        with pytest.raises(ValidationError):
            await resolve_collection_or_404(catalogs_svc, "cat", "col-a")


@pytest.mark.asyncio
async def test_resolve_collection_or_404_does_not_mask_an_unrelated_value_error():
    """Sibling guard with a plain (non-Pydantic) ``ValueError`` that also
    lacks the "not found" signal — same propagate-unchanged contract."""
    catalogs_svc = _FakeCatalogsService(
        not_found_error=ValueError("Collection payload is malformed.")
    )

    with pytest.raises(ValueError, match="malformed"):
        await resolve_collection_or_404(catalogs_svc, "cat", "col-a")
