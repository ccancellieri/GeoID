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

"""Regression coverage for #3166: the shared ``resolve_catalog_or_404``
resolver must apply the same fail-closed visibility contract #3164 gave the
direct STAC catalog GET to every other consumer, in one place.

``get_catalog``/``get_catalog_model`` deliberately fall through to a
tombstone lookup and return a populated model with ``deleted_at`` set for a
soft-deleted catalog (the 200+deleted-state reclaim contract). Read-surface
callers of the shared resolver must not resurface that model as if the
catalog were live; admin/reclaim callers opt in explicitly via
``include_tombstoned=True``.
"""

from __future__ import annotations

import datetime
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from dynastore.extensions.tools.resolvers import resolve_catalog_or_404
from dynastore.models.shared_models import Catalog


def _tombstoned_catalog(catalog_id: str = "deleted-cat") -> Catalog:
    ts = datetime.datetime(2026, 7, 9, 22, 30, 0, tzinfo=datetime.timezone.utc)
    return Catalog.model_validate({"id": catalog_id, "deleted_at": ts})


def _active_catalog(catalog_id: str = "live-cat") -> Catalog:
    return Catalog.model_validate({"id": catalog_id})


class _FakeCatalogsService:
    def __init__(self, model) -> None:
        self.get_catalog = AsyncMock(return_value=model)
        self.get_catalog_model = AsyncMock(return_value=model)


@pytest.mark.asyncio
async def test_resolve_catalog_or_404_404s_a_tombstoned_catalog_get_catalog():
    """Default (read-surface) behaviour: a tombstoned catalog via
    ``get_catalog`` is indistinguishable from a missing one."""
    svc = _FakeCatalogsService(_tombstoned_catalog())

    with pytest.raises(HTTPException) as exc_info:
        await resolve_catalog_or_404(svc, "deleted-cat")

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_resolve_catalog_or_404_404s_a_tombstoned_catalog_get_catalog_model():
    """Same contract via ``use_model=True`` (``get_catalog_model``)."""
    svc = _FakeCatalogsService(_tombstoned_catalog())

    with pytest.raises(HTTPException) as exc_info:
        await resolve_catalog_or_404(svc, "deleted-cat", use_model=True)

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_resolve_catalog_or_404_include_tombstoned_returns_the_model():
    """Admin/reclaim opt-in: ``include_tombstoned=True`` still observes the
    200+deleted-state model instead of being 404ed."""
    tombstoned = _tombstoned_catalog()
    svc = _FakeCatalogsService(tombstoned)

    result = await resolve_catalog_or_404(
        svc, "deleted-cat", use_model=True, include_tombstoned=True,
    )

    assert result is tombstoned
    assert result.deleted_at is not None


@pytest.mark.asyncio
async def test_resolve_catalog_or_404_returns_an_active_catalog():
    """Sibling guard: an active catalog (deleted_at=None) is unaffected —
    the fix must not turn every catalog lookup into a 404."""
    active = _active_catalog()
    svc = _FakeCatalogsService(active)

    result = await resolve_catalog_or_404(svc, "live-cat")

    assert result is active


@pytest.mark.asyncio
async def test_resolve_catalog_or_404_still_404s_a_missing_catalog():
    """A genuinely missing catalog (getter returns None) still 404s, with
    or without include_tombstoned."""
    svc = _FakeCatalogsService(None)

    with pytest.raises(HTTPException) as exc_info:
        await resolve_catalog_or_404(svc, "ghost-cat", include_tombstoned=True)

    assert exc_info.value.status_code == 404
