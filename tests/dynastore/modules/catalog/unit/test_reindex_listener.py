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

"""Unit tests for the M3.1b production-wiring adapter :mod:`reindex_listener`.

Covers:

- ``handle_catalog_metadata_changed`` extracts ``payload`` from kwargs
  and builds a one-event batch for ``ReindexWorker.handle_batch``.
- Missing / non-dict payload raises loud so emission-shape drift is
  caught at the listener boundary.
- ``should_retry=True`` result → raises (so EventDrainTask retries the row).
- ``succeeded=True`` with degraded errors → logs WARNING but returns
  (so EventDrainTask completes the row).
- Joined retry-error message preserves every per-driver failure.
- ``register_reindex_listener`` calls ``async_event_listener`` with the
  canonical event-type string.
- Second registration on the same event_service is a no-op (idempotent).
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# handle_catalog_metadata_changed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_listener_builds_one_event_batch_from_payload_kwarg():
    """The listener receives ``payload={...}`` in kwargs; builds a batch."""
    from dynastore.modules.catalog.reindex_listener import (
        handle_catalog_metadata_changed,
    )

    worker = MagicMock()
    worker.handle_batch = AsyncMock(return_value=[])

    payload = {"catalog_id": "cat-42", "domain": "core", "operation": "upsert"}
    await handle_catalog_metadata_changed(
        worker, catalog_id="cat-42", payload=payload,
    )

    worker.handle_batch.assert_awaited_once()
    (events,) = worker.handle_batch.await_args.args
    assert len(events) == 1
    evt = events[0]
    assert evt["event_type"] == "catalog_metadata_changed"
    # event_id is None in this wiring — the real row id lives in the
    # EventDrainTask's scope, not the listener API.
    assert evt["event_id"] is None
    assert evt["payload"] is payload


@pytest.mark.asyncio
async def test_listener_missing_payload_raises():
    """No ``payload`` kwarg — fail loud at the boundary, don't silently drop."""
    from dynastore.modules.catalog.reindex_listener import (
        handle_catalog_metadata_changed,
    )

    worker = MagicMock()
    worker.handle_batch = AsyncMock()

    with pytest.raises(RuntimeError, match="missing or non-dict 'payload'"):
        await handle_catalog_metadata_changed(worker, catalog_id="cat-42")

    worker.handle_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_listener_non_dict_payload_raises():
    """``payload`` is a string (JSON not yet parsed?) — fail loud."""
    from dynastore.modules.catalog.reindex_listener import (
        handle_catalog_metadata_changed,
    )

    worker = MagicMock()
    worker.handle_batch = AsyncMock()

    with pytest.raises(RuntimeError, match="missing or non-dict 'payload'"):
        await handle_catalog_metadata_changed(
            worker, payload='{"catalog_id": "c"}',
        )

    worker.handle_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_listener_retry_result_raises_with_joined_errors():
    """``should_retry=True`` → raise, so EventDrainTask retries the row."""
    from dynastore.modules.catalog.reindex_listener import (
        handle_catalog_metadata_changed,
    )
    from dynastore.modules.catalog.reindex_worker import _DispatchResult

    worker = MagicMock()
    worker.handle_batch = AsyncMock(return_value=[
        _DispatchResult(
            event_id=None, succeeded=False, should_retry=True,
            errors=["ES 500", "timeout on core indexer"],
        ),
    ])

    with pytest.raises(RuntimeError, match="ES 500; timeout on core indexer"):
        await handle_catalog_metadata_changed(
            worker, payload={"catalog_id": "c", "operation": "upsert"},
        )


@pytest.mark.asyncio
async def test_listener_retry_without_error_list_has_default_message():
    """``should_retry=True`` with empty ``errors`` still raises a useful msg."""
    from dynastore.modules.catalog.reindex_listener import (
        handle_catalog_metadata_changed,
    )
    from dynastore.modules.catalog.reindex_worker import _DispatchResult

    worker = MagicMock()
    worker.handle_batch = AsyncMock(return_value=[
        _DispatchResult(
            event_id=None, succeeded=False, should_retry=True, errors=[],
        ),
    ])

    with pytest.raises(RuntimeError, match="reindex dispatch failed"):
        await handle_catalog_metadata_changed(
            worker, payload={"catalog_id": "c", "operation": "upsert"},
        )


@pytest.mark.asyncio
async def test_listener_degraded_result_warns_and_returns(caplog):
    """``succeeded=True`` with errors → WARNING log, return normally (ACK)."""
    from dynastore.modules.catalog.reindex_listener import (
        handle_catalog_metadata_changed,
    )
    from dynastore.modules.catalog.reindex_worker import _DispatchResult

    worker = MagicMock()
    worker.handle_batch = AsyncMock(return_value=[
        _DispatchResult(
            event_id=None, succeeded=True, should_retry=False,
            errors=["ES degraded: timeout (on_timeout=degrade)"],
        ),
    ])

    with caplog.at_level(
        logging.WARNING, logger="dynastore.modules.catalog.reindex_listener",
    ):
        # Must not raise — the SLA said degrade.
        await handle_catalog_metadata_changed(
            worker, payload={"catalog_id": "c-7", "operation": "upsert"},
        )

    assert any(
        "degraded errors" in r.message and "c-7" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_listener_clean_success_no_warning(caplog):
    """``succeeded=True`` with no errors → silent ACK (no WARNING)."""
    from dynastore.modules.catalog.reindex_listener import (
        handle_catalog_metadata_changed,
    )
    from dynastore.modules.catalog.reindex_worker import _DispatchResult

    worker = MagicMock()
    worker.handle_batch = AsyncMock(return_value=[
        _DispatchResult(
            event_id=None, succeeded=True, should_retry=False, errors=[],
        ),
    ])

    with caplog.at_level(
        logging.WARNING, logger="dynastore.modules.catalog.reindex_listener",
    ):
        await handle_catalog_metadata_changed(
            worker, payload={"catalog_id": "c", "operation": "upsert"},
        )

    assert not any(
        "degraded errors" in r.message for r in caplog.records
    )


# ---------------------------------------------------------------------------
# register_reindex_listener
# ---------------------------------------------------------------------------


class _FakeEventService:
    """Minimal EventService double recording listener registrations.

    Plain class rather than MagicMock so ``getattr(svc, marker, None)``
    actually returns ``None`` the first time (MagicMock auto-creates
    truthy attrs on access, which would trip the idempotency guard
    immediately).
    """

    def __init__(self) -> None:
        self._registered: list = []

    def async_event_listener(self, event_type):
        def _decorator(func):
            self._registered.append((event_type, func))
            return func
        return _decorator


def _fake_event_service() -> _FakeEventService:
    return _FakeEventService()


def test_register_invokes_async_event_listener_decorator():
    """The canonical event-type string is passed to ``async_event_listener``."""
    from dynastore.modules.catalog.reindex_listener import (
        CATALOG_METADATA_CHANGED_EVENT, register_reindex_listener,
    )

    svc = _fake_event_service()
    register_reindex_listener(svc)

    assert len(svc._registered) == 1
    event_type, func = svc._registered[0]
    assert event_type == CATALOG_METADATA_CHANGED_EVENT == "catalog_metadata_changed"
    assert callable(func)


def test_register_is_idempotent_per_event_service_instance(caplog):
    """Second call on the same EventService must not duplicate the listener."""
    from dynastore.modules.catalog.reindex_listener import (
        register_reindex_listener,
    )

    svc = _fake_event_service()
    with caplog.at_level(
        logging.DEBUG, logger="dynastore.modules.catalog.reindex_listener",
    ):
        first = register_reindex_listener(svc)
        second = register_reindex_listener(svc)

    assert first is second  # same bound listener returned
    assert len(svc._registered) == 1  # only registered once
    assert any("already registered" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_registered_listener_delegates_to_handle_fn():
    """The function registered on the event_service wraps ``handle_catalog_metadata_changed``."""
    from dynastore.modules.catalog.reindex_listener import register_reindex_listener
    from dynastore.modules.catalog.reindex_worker import _DispatchResult

    worker = MagicMock()
    worker.handle_batch = AsyncMock(return_value=[
        _DispatchResult(
            event_id=None, succeeded=True, should_retry=False, errors=[],
        ),
    ])

    svc = _fake_event_service()
    register_reindex_listener(svc, worker=worker)
    (_, listener) = svc._registered[0]

    # Invoke the listener as dispatch_to_listeners would.
    await listener(
        catalog_id="cat-42",
        payload={"catalog_id": "cat-42", "domain": "stac", "operation": "delete"},
    )

    worker.handle_batch.assert_awaited_once()
    (events,) = worker.handle_batch.await_args.args
    assert events[0]["payload"]["domain"] == "stac"


def test_register_default_worker_is_fresh_reindex_worker():
    """Default-path ``register_reindex_listener`` builds a ``ReindexWorker``."""
    from dynastore.modules.catalog.reindex_listener import register_reindex_listener

    svc = _fake_event_service()
    listener = register_reindex_listener(svc)
    # The listener closes over a ReindexWorker; construction succeeded
    # means the default-worker branch ran without imports blowing up.
    assert callable(listener)
