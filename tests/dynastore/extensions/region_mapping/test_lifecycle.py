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

"""DB-free unit tests for ``lifecycle`` -- the best-effort referential-
integrity cleanup listeners (dynastore#443). ``registry_store`` is fully
mocked; these tests only exercise the listener's own guard clauses and
error handling, never real SQL.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_on_collection_gone_deletes_matching_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import lifecycle

    monkeypatch.setattr(lifecycle, "get_engine", lambda: object())
    delete_calls = []

    async def _delete(engine: Any, catalog_id: str, collection_id: str) -> int:
        delete_calls.append((catalog_id, collection_id))
        return 2

    monkeypatch.setattr(lifecycle._store, "delete_claims_by_source_collection", _delete)

    await lifecycle._on_collection_gone(catalog_id="fao", collection_id="countries")

    assert delete_calls == [("fao", "countries")]


@pytest.mark.asyncio
async def test_on_collection_gone_missing_ids_is_a_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import lifecycle

    delete = AsyncMock()
    monkeypatch.setattr(lifecycle._store, "delete_claims_by_source_collection", delete)

    await lifecycle._on_collection_gone(catalog_id=None, collection_id="countries")
    await lifecycle._on_collection_gone(catalog_id="fao", collection_id=None)

    delete.assert_not_called()


@pytest.mark.asyncio
async def test_on_collection_gone_no_engine_is_a_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import lifecycle

    monkeypatch.setattr(lifecycle, "get_engine", lambda: None)
    delete = AsyncMock()
    monkeypatch.setattr(lifecycle._store, "delete_claims_by_source_collection", delete)

    await lifecycle._on_collection_gone(catalog_id="fao", collection_id="countries")

    delete.assert_not_called()


@pytest.mark.asyncio
async def test_on_collection_gone_swallows_store_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    """A DB hiccup here must never propagate -- the collection-delete path
    that fired this event has already completed and moved on."""
    from dynastore.extensions.region_mapping import lifecycle

    monkeypatch.setattr(lifecycle, "get_engine", lambda: object())

    async def _boom(engine: Any, catalog_id: str, collection_id: str) -> int:
        raise RuntimeError("db hiccup")

    monkeypatch.setattr(lifecycle._store, "delete_claims_by_source_collection", _boom)

    await lifecycle._on_collection_gone(catalog_id="fao", collection_id="countries")  # must not raise


@pytest.mark.asyncio
async def test_on_catalog_gone_deletes_matching_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import lifecycle

    monkeypatch.setattr(lifecycle, "get_engine", lambda: object())
    delete_calls = []

    async def _delete(engine: Any, catalog_id: str) -> int:
        delete_calls.append(catalog_id)
        return 3

    monkeypatch.setattr(lifecycle._store, "delete_claims_by_source_catalog", _delete)

    await lifecycle._on_catalog_gone(catalog_id="fao")

    assert delete_calls == ["fao"]


@pytest.mark.asyncio
async def test_on_catalog_gone_swallows_store_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import lifecycle

    monkeypatch.setattr(lifecycle, "get_engine", lambda: object())

    async def _boom(engine: Any, catalog_id: str) -> int:
        raise RuntimeError("db hiccup")

    monkeypatch.setattr(lifecycle._store, "delete_claims_by_source_catalog", _boom)

    await lifecycle._on_catalog_gone(catalog_id="fao")  # must not raise


def test_register_subscriber_wires_both_event_types(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import lifecycle
    from dynastore.modules.catalog.event_service import CatalogEventType

    registered = []

    def _fake_async_event_listener(event_type: Any):
        def _decorator(func: Any) -> Any:
            registered.append(event_type)
            return func

        return _decorator

    monkeypatch.setattr(lifecycle, "async_event_listener", _fake_async_event_listener)

    lifecycle.register_region_mapping_cleanup_subscriber()

    assert set(registered) == {
        CatalogEventType.COLLECTION_HARD_DELETION,
        CatalogEventType.CATALOG_HARD_DELETION,
    }
