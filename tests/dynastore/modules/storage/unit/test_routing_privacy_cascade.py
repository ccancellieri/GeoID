"""#733 — routing-config-driven privacy detection.

After #1047 Phase 2, privacy is expressed solely by the presence of
``items_elasticsearch_private_driver`` in ``ItemsRoutingConfig.operations``.
The catalog and collection cascade handlers were removed because
CatalogElasticsearchPrivateDriver and CollectionElasticsearchPrivateDriver
no longer exist.
"""
from __future__ import annotations

import pytest

from dynastore.modules.storage.routing_config import (
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
    _items_routing_has_private_driver,
)


def _items_routing_with_private(*, operation: str = Operation.INDEX) -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            operation: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    on_failure=FailurePolicy.OUTBOX,
                    write_mode=WriteMode.ASYNC,
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
    for op in (Operation.WRITE, Operation.READ, Operation.SEARCH, Operation.INDEX):
        routing = _items_routing_with_private(operation=op)
        assert _items_routing_has_private_driver(routing) is True, (
            f"private driver in operations[{op}] must satisfy the cascade gate"
        )


# ---------------------------------------------------------------------------
# apply_catalog_default_routing_seed — happy-path regression
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_catalog_default_routing_seed_writes_both_templates():
    """Happy path — CatalogRoutingTemplates.collection_defaults with both templates
    set -> helper writes both per-collection configs."""
    from unittest.mock import AsyncMock, MagicMock

    from dynastore.modules.catalog.catalog_config import (
        CatalogRoutingTemplates,
        _build_private_items_routing,
        apply_catalog_default_routing_seed,
    )
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingDefaults,
        FailurePolicy,
        Operation,
        OperationDriverEntry,
    )

    pg_only_coll = CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )

    policy = CatalogRoutingTemplates(
        collection_defaults=CatalogRoutingDefaults(
            items_routing=_build_private_items_routing(),
            collection_routing=pg_only_coll,
        ),
    )
    proto = MagicMock()
    proto.get_config = AsyncMock(return_value=policy)
    proto.set_config = AsyncMock()

    applied = await apply_catalog_default_routing_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is True
    assert proto.set_config.await_count == 2
    classes_written = [
        call.args[0] for call in proto.set_config.await_args_list
    ]
    assert classes_written == [ItemsRoutingConfig, CollectionRoutingConfig]
