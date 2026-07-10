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

"""#733 — routing-config-driven privacy detection.

After #1047 Phase 2, privacy is expressed solely by the presence of
``items_elasticsearch_private_driver`` in ``ItemsRoutingConfig.operations``.
The catalog and collection cascade handlers were removed because
CatalogElasticsearchPrivateDriver and CollectionElasticsearchPrivateDriver
no longer exist.
"""
from __future__ import annotations

from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    _items_routing_has_private_driver,
)


def _items_routing_with_private(*, operation: str = Operation.INDEX) -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            operation: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    source="auto",
                ),
            ],
        },
    )


def _items_routing_without_private() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


# ---------------------------------------------------------------------------
# Items-private detection helpers
# ---------------------------------------------------------------------------


def test_items_has_private_driver_detects_pinned_entry():
    assert _items_routing_has_private_driver(_items_routing_with_private()) is True


def test_items_has_private_driver_returns_false_when_absent():
    assert _items_routing_has_private_driver(_items_routing_without_private()) is False


def test_items_has_private_driver_finds_entry_in_any_operation():
    for op in (Operation.WRITE, Operation.READ, Operation.INDEX):
        routing = _items_routing_with_private(operation=op)
        assert _items_routing_has_private_driver(routing) is True, (
            f"private driver in operations[{op}] must satisfy the cascade gate"
        )
