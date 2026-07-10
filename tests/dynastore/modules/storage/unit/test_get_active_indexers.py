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

"""get_active_indexers — multi-driver fan-out driver_ref resolution.

Mocks ``_resolve_entity_operations`` so the test exercises the helper's
projection logic in isolation, without bringing up ConfigsProtocol.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dynastore.modules.storage.routing_config import (
    Operation,
    OperationDriverEntry,
    get_active_indexers,
)


def _ops(*, INDEX=None, **by_op):
    """Build a fake operations dict from keyword pairs of OP -> [driver_ids].

    The ``INDEX`` kwarg maps to ``operations[Operation.INDEX]`` — the
    lane ``index_entries`` (which backs ``get_active_indexers``) reads
    directly. Other op kwargs map straight to their own op-key.
    """
    ops = {
        op: [OperationDriverEntry(driver_ref=did) for did in dids]
        for op, dids in by_op.items()
    }
    if INDEX is not None:
        ops[Operation.INDEX] = [OperationDriverEntry(driver_ref=did) for did in INDEX]
    return ops


@pytest.mark.asyncio
async def test_returns_all_INDEX_driver_ids():
    fake = _ops(INDEX=["es_items_driver", "pg_items_driver"], READ=["es_items_driver"])
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake,
    ):
        result = await get_active_indexers("c", entity="item", collection_id="col")
    assert result == {"es_items_driver", "pg_items_driver"}


@pytest.mark.asyncio
async def test_returns_empty_set_when_no_INDEX_entries():
    fake = _ops(READ=["es_items_driver"])  # No INDEX-lane entry
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake,
    ):
        result = await get_active_indexers("c", entity="item", collection_id="col")
    assert result == set()


@pytest.mark.asyncio
async def test_works_for_each_entity_kind():
    """Helper is entity-agnostic — same shape for item / collection / catalog / asset."""
    fake = _ops(INDEX=["es_driver"])
    for entity in ("item", "collection", "catalog", "asset"):
        with patch(
            "dynastore.modules.storage.routing_config._resolve_entity_operations",
            return_value=fake,
        ):
            result = await get_active_indexers("c", entity=entity, collection_id="col")
        assert result == {"es_driver"}, f"failed for entity={entity}"


@pytest.mark.asyncio
async def test_dedupes_via_set_semantics():
    """Same driver_ref listed twice as an INDEX entry collapses to one entry."""
    fake = _ops(INDEX=["es_items_driver", "es_items_driver"])
    with patch(
        "dynastore.modules.storage.routing_config._resolve_entity_operations",
        return_value=fake,
    ):
        result = await get_active_indexers("c", entity="item", collection_id="col")
    assert result == {"es_items_driver"}
