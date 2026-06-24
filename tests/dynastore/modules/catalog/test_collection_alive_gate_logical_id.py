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

"""Regression: collection liveness/activation gates must key the registry
lookup on the immutable internal id, resolving the public ``external_id``
first.

The lifecycle/registry rows and routing config are keyed on the internal
``id`` (the create path pins config under ``collection_model.id``).  The
write-path boundary methods (``ensure_alive``/``is_alive``/``is_active``/
``activate_collection``) receive the *logical* (external) id from the request
layer, so they must map external -> internal before the ``WHERE id = …``
query.  Without that mapping every collection — including long-lived ones —
resolves to MISSING and writes 404 ``collection-not-alive``.

Pure unit: no live DB.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from dynastore.modules.catalog.collection_service import (
    CollectionLifecycle,
    CollectionNotAliveError,
    CollectionService,
)

EXTERNAL_ID = "my-public-collection"
INTERNAL_ID = "c_8f3a1b9c2d4e"
CATALOG_ID = "demo_catalog"


def _make_service() -> CollectionService:
    """Bare CollectionService without running __init__/setup."""
    return CollectionService.__new__(CollectionService)


@pytest.mark.asyncio
async def test_ensure_alive_resolves_external_id_before_lifecycle_query() -> None:
    """``ensure_alive`` must hand ``_get_lifecycle`` the internal id, not the
    public external_id it was called with."""
    svc = _make_service()
    svc.resolve_collection_id = AsyncMock(return_value=INTERNAL_ID)  # type: ignore[method-assign]
    svc._get_lifecycle = AsyncMock(return_value=CollectionLifecycle.ACTIVE)  # type: ignore[method-assign]

    await svc.ensure_alive(CATALOG_ID, EXTERNAL_ID)

    svc.resolve_collection_id.assert_awaited_once_with(
        CATALOG_ID, EXTERNAL_ID, allow_missing=True
    )
    # The lifecycle lookup keyed on the INTERNAL id, never the external label.
    assert svc._get_lifecycle.await_args.args[1] == INTERNAL_ID


@pytest.mark.asyncio
async def test_ensure_alive_raises_with_internal_id_when_missing() -> None:
    """A genuinely missing collection still raises — the resolver passes the
    id through unchanged and the registry lookup decides MISSING."""
    svc = _make_service()
    svc.resolve_collection_id = AsyncMock(return_value=None)  # type: ignore[method-assign]
    svc._get_lifecycle = AsyncMock(return_value=CollectionLifecycle.MISSING)  # type: ignore[method-assign]

    with pytest.raises(CollectionNotAliveError) as excinfo:
        await svc.ensure_alive(CATALOG_ID, EXTERNAL_ID)

    assert excinfo.value.reason == "missing"
    # Passthrough: the unresolved value reached the lookup unchanged.
    assert svc._get_lifecycle.await_args.args[1] == EXTERNAL_ID


@pytest.mark.asyncio
async def test_is_alive_resolves_external_id_before_lifecycle_query() -> None:
    svc = _make_service()
    svc.resolve_collection_id = AsyncMock(return_value=INTERNAL_ID)  # type: ignore[method-assign]
    svc._get_lifecycle = AsyncMock(return_value=CollectionLifecycle.ACTIVE)  # type: ignore[method-assign]

    assert await svc.is_alive(CATALOG_ID, EXTERNAL_ID) is True
    assert svc._get_lifecycle.await_args.args[1] == INTERNAL_ID


@pytest.mark.asyncio
async def test_is_active_resolves_external_id_before_physical_table_lookup() -> None:
    """``is_active`` must key the routing/physical-table lookup on the internal
    id — config is pinned under the internal id at create time."""
    svc = _make_service()
    svc.resolve_collection_id = AsyncMock(return_value=INTERNAL_ID)  # type: ignore[method-assign]
    svc.resolve_physical_table = AsyncMock(return_value=None)  # type: ignore[method-assign]

    # physical_table None -> returns False without probing; we only assert the
    # lookup key here.
    assert await svc.is_active(CATALOG_ID, EXTERNAL_ID) is False
    assert svc.resolve_physical_table.await_args.args[1] == INTERNAL_ID


@pytest.mark.asyncio
async def test_activate_collection_marks_confirmed_with_internal_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``activate_collection`` must provision and mark the confirmed-active set
    under the internal id so a subsequent ``is_active`` fast-path hit aligns."""
    import dynastore.modules.catalog.collection_service as mod

    svc = _make_service()
    svc.engine = None
    svc.resolve_collection_id = AsyncMock(return_value=INTERNAL_ID)  # type: ignore[method-assign]
    svc._activate_collection = AsyncMock(return_value=None)  # type: ignore[method-assign]

    marked: list[tuple[str, str]] = []
    monkeypatch.setattr(
        mod, "_mark_confirmed_active", lambda c, col: marked.append((c, col))
    )

    class _NullCM:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(mod, "managed_transaction", lambda *a, **k: _NullCM())

    await svc.activate_collection(CATALOG_ID, EXTERNAL_ID)

    assert svc._activate_collection.await_args.args[:2] == (CATALOG_ID, INTERNAL_ID)
    assert marked == [(CATALOG_ID, INTERNAL_ID)]


@pytest.mark.asyncio
async def test_to_internal_collection_id_passthrough_when_already_internal() -> None:
    """When the resolver finds no external match the id is already internal
    (or absent) and is returned unchanged."""
    svc = _make_service()
    svc.resolve_collection_id = AsyncMock(return_value=None)  # type: ignore[method-assign]

    out = await svc._to_internal_collection_id(CATALOG_ID, INTERNAL_ID)

    assert out == INTERNAL_ID
