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

"""Regression coverage for #2256 gap #1 — the /logs half.

create_collection emits COLLECTION_CREATION (the /events half, covered by
test_collection_create_emits_event.py), but the logs extension only
registered listeners for CATALOG_* events, so collection lifecycle never
reached /logs. This pins the collection lifecycle listeners + handlers in
LogExtension so a regression that drops them fails loudly.

Two layers:

1. Source-shape — the three COLLECTION_* listeners are registered in
   _register_listeners.

2. Runtime — each handler forwards to append_log with the matching
   event_type, the collection_id set, and is_system=False so log_event
   routes the row into the tenant's physical schema (surfacing under both
   /catalogs/{cat}/logs and /catalogs/{cat}/collections/{coll}/logs).
"""

from __future__ import annotations

import inspect

import pytest
from unittest.mock import AsyncMock

from dynastore.extensions.logs.log_extension import LogExtension
from dynastore.modules.catalog.event_service import CatalogEventType


# ---------------------------------------------------------------------------
# Source-shape: cheap, deterministic regression guard
# ---------------------------------------------------------------------------


def test_register_listeners_wires_collection_lifecycle() -> None:
    src = inspect.getsource(LogExtension._register_listeners)
    for event in (
        "CatalogEventType.COLLECTION_CREATION",
        "CatalogEventType.COLLECTION_DELETION",
        "CatalogEventType.COLLECTION_HARD_DELETION",
    ):
        assert event in src, (
            f"LogExtension._register_listeners no longer wires {event} — "
            "collection lifecycle will reach /events but not /logs "
            "(#2256 gap #1, /logs half)."
        )


# ---------------------------------------------------------------------------
# Runtime: each handler appends a tenant-scoped, collection-keyed log row
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "handler_name, expected_event_type",
    [
        ("_on_collection_created", CatalogEventType.COLLECTION_CREATION.value),
        ("_on_collection_deleted", CatalogEventType.COLLECTION_DELETION.value),
        (
            "_on_collection_hard_deleted",
            CatalogEventType.COLLECTION_HARD_DELETION.value,
        ),
    ],
)
async def test_collection_handler_appends_tenant_scoped_log(
    handler_name: str, expected_event_type: str
) -> None:
    ext = LogExtension.__new__(LogExtension)  # bypass heavy __init__
    ext.append_log = AsyncMock()  # type: ignore[method-assign]

    handler = getattr(ext, handler_name)
    await handler(catalog_id="cat_x", collection_id="coll_y")

    ext.append_log.assert_awaited_once()
    entry = ext.append_log.await_args.args[0]
    assert entry.catalog_id == "cat_x"
    assert entry.collection_id == "coll_y"
    assert entry.event_type == expected_event_type
    # is_system=True routes to the flat catalog.system_logs (with collection_id
    # set), not the tenant {schema}.logs partitioned table: the listener runs
    # in-band with the create/delete transaction that mutates that collection's
    # log partition, so an immediate write on a separate connection cannot see
    # it. search_logs filters the system row back by catalog_id + collection_id.
    assert entry.is_system is True
    # immediate=True bypasses the batch aggregator: lifecycle events are sparse,
    # and the aggregator's timer flush is lost when an idle Cloud Run instance
    # is CPU-throttled and scales to zero before the buffer drains.
    assert ext.append_log.await_args.kwargs.get("immediate") is True
