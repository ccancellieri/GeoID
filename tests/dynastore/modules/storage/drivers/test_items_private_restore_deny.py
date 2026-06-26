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

"""#733, #1047 — pin the cascade-aware ``_restore_deny_policies`` behaviour
on the items-private driver.

After #1047 Phase 2, privacy is expressed solely via
``items_elasticsearch_private_driver`` in ``ItemsRoutingConfig``.
Collection-envelope privacy via a separate ES driver is removed.
The lifespan startup hook scans each catalog's collections and
re-applies the catalog-wide DENY policy idempotently for any catalog
with at least one private-items collection.

These tests exercise the static helper
``_catalog_has_private_collection`` directly (pure logic, fully
mockable) and the lifespan loop's integration with that helper.

#2464 — the lifespan also skips the scan entirely in ephemeral job
contexts (Cloud Run Jobs, local dev) where there is no prior in-memory
DENY state to recover.  Detection is via ``K_SERVICE``, which Cloud Run
Services always set and Jobs never do.
"""
from __future__ import annotations

import re
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
    ItemsElasticsearchPrivateDriver,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_collection(col_id: str) -> MagicMock:
    col = MagicMock()
    col.id = col_id
    return col


def _stub_catalog(cat_id: str) -> MagicMock:
    cat = MagicMock()
    cat.id = cat_id
    return cat


def _items_private_routing() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    on_failure=FailurePolicy.OUTBOX,
                    write_mode=WriteMode.ASYNC,
                    secondary_index=True,
                ),
            ],
        },
    )


def _items_public_routing() -> ItemsRoutingConfig:
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



def _catalogs_proto_with(
    catalogs: List[MagicMock],
    collections_by_cat: dict[str, List[MagicMock]],
) -> MagicMock:
    proto = MagicMock()

    async def list_catalogs(*, limit: int, offset: int) -> List[MagicMock]:
        return catalogs[offset:offset + limit]

    async def list_collections(
        catalog_id: str, *, limit: int, offset: int,
    ) -> List[MagicMock]:
        cols = collections_by_cat.get(catalog_id, [])
        return cols[offset:offset + limit]

    proto.list_catalogs = list_catalogs
    proto.list_collections = list_collections
    return proto


def _configs_proto_with_routing(
    routing_by_pair: Dict[tuple, object],
) -> MagicMock:
    """Returns the supplied routing config for (cls, cat, col) lookups.

    ``routing_by_pair`` maps ``(catalog_id, collection_id, cls.__name__)``
    → routing instance; missing pairs return ``None``.
    """
    proto = MagicMock()

    async def get_config(cls, *, catalog_id, collection_id="", **kwargs):
        return routing_by_pair.get(
            (catalog_id, collection_id, cls.__name__),
        )

    proto.get_config = get_config
    return proto


# ---------------------------------------------------------------------------
# _catalog_has_private_collection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_helper_returns_true_when_items_routing_pins_private_driver():
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-a")],
        {"cat-a": [_stub_collection("col-public"), _stub_collection("col-private")]},
    )
    configs = _configs_proto_with_routing({
        ("cat-a", "col-public", "ItemsRoutingConfig"): _items_public_routing(),
        ("cat-a", "col-private", "ItemsRoutingConfig"): _items_private_routing(),
    })
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-a",
    )
    assert result is True


@pytest.mark.asyncio
async def test_helper_returns_false_when_all_collections_items_public():
    """After #1047 Phase 2, only items routing is checked — collection
    routing is not scanned for the DENY decision."""
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-a")],
        {"cat-a": [_stub_collection("col-1"), _stub_collection("col-2")]},
    )
    configs = _configs_proto_with_routing({
        ("cat-a", "col-1", "ItemsRoutingConfig"): _items_public_routing(),
        ("cat-a", "col-2", "ItemsRoutingConfig"): _items_public_routing(),
    })
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-a",
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_returns_false_when_catalog_has_no_collections():
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-empty")], {"cat-empty": []},
    )
    configs = _configs_proto_with_routing({})
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-empty",
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_returns_false_on_list_collections_exception():
    """A transient ``list_collections`` failure should NOT propagate or
    be misinterpreted as 'has private' — defer to the next apply."""
    catalogs_proto = MagicMock()
    catalogs_proto.list_collections = AsyncMock(side_effect=RuntimeError("transient"))
    configs = _configs_proto_with_routing({})
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-flaky",
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_skips_collection_when_get_config_raises():
    """A transient items-routing lookup failure must not stop the scan
    — it should still check the other collections."""
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-a")],
        {"cat-a": [_stub_collection("col-bad"), _stub_collection("col-good")]},
    )
    proto = MagicMock()

    async def get_config(cls, *, catalog_id, collection_id="", **kwargs):
        if collection_id == "col-bad":
            raise RuntimeError("transient")
        if cls is ItemsRoutingConfig:
            return _items_private_routing()
        return None

    proto.get_config = get_config
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, proto, "cat-a",
    )
    assert result is True


@pytest.mark.asyncio
async def test_helper_paginates_through_collections():
    """The N+1 collection scan paginates in batches of 100; with 250
    collections we expect three list_collections calls (offsets
    0/100/200) before terminating on the short batch."""
    cols = [_stub_collection(f"col-{i:03d}") for i in range(250)]
    routing_map: Dict[tuple, object] = {}
    for c in cols:
        routing_map[("cat-big", c.id, "ItemsRoutingConfig")] = _items_public_routing()
    # Only the very last is private (via items routing).
    routing_map[("cat-big", "col-249", "ItemsRoutingConfig")] = _items_private_routing()

    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-big")], {"cat-big": cols},
    )
    configs = _configs_proto_with_routing(routing_map)
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-big",
    )
    assert result is True


# ---------------------------------------------------------------------------
# _restore_deny_policies — full lifespan loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restore_applies_deny_only_for_catalogs_with_private_collections():
    """End-to-end of the lifespan loop: two catalogs, one has a
    private-routing collection and one is fully public; ``_apply_deny_policy``
    must fire only for the catalog with the private collection."""
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-private"), _stub_catalog("cat-public")],
        {
            "cat-private": [_stub_collection("col-secret")],
            "cat-public": [_stub_collection("col-1"), _stub_collection("col-2")],
        },
    )
    configs = _configs_proto_with_routing({
        ("cat-private", "col-secret", "ItemsRoutingConfig"): _items_private_routing(),
        ("cat-public", "col-1", "ItemsRoutingConfig"): _items_public_routing(),
        ("cat-public", "col-2", "ItemsRoutingConfig"): _items_public_routing(),
    })

    # Patch get_protocol so the lifespan helper resolves both protos.
    def _get_protocol(p):
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol
        if p is CatalogsProtocol:
            return catalogs_proto
        if p is ConfigsProtocol:
            return configs
        return None

    driver = ItemsElasticsearchPrivateDriver()
    apply_calls: list[str] = []

    async def fake_apply(cat_id):
        apply_calls.append(cat_id)

    with patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ), patch.object(driver, "_apply_deny_policy", side_effect=fake_apply):
        await driver._restore_deny_policies()

    assert apply_calls == ["cat-private"]


@pytest.mark.asyncio
async def test_restore_is_no_op_when_protocols_unavailable():
    """No CatalogsProtocol or ConfigsProtocol → bail silently."""
    driver = ItemsElasticsearchPrivateDriver()
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=None,
    ), patch.object(driver, "_apply_deny_policy", new=AsyncMock()) as apply_mock:
        await driver._restore_deny_policies()
    apply_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_swallows_unexpected_failures():
    """The outer try/except in _restore_deny_policies must catch
    surprises — lifespan should never abort because of a
    privacy-recovery hiccup."""
    driver = ItemsElasticsearchPrivateDriver()

    def _get_protocol(p):
        raise RuntimeError("boom")

    with patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ):
        # No exception should propagate.
        await driver._restore_deny_policies()


# ---------------------------------------------------------------------------
# DENY resource pattern is built from the OGCServiceMixin registry
# (issue #454 item 2 — Fix 1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_deny_policy_uses_ogc_prefix_registry():
    """``_apply_deny_policy`` must build its resource regex from
    ``get_ogc_service_prefixes()`` so the pattern self-maintains as new
    OGC protocols come online — no hardcoded protocol list to drift."""
    captured: list = []

    fake_perm = MagicMock()
    fake_perm.register_policy.side_effect = lambda p: captured.append(p) or p
    fake_perm.register_role.return_value = None
    fake_perm.create_policy = AsyncMock()

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=fake_perm,
    ), patch(
        "dynastore.extensions.tools.conformance.get_ogc_service_prefixes",
        return_value=["features", "maps", "records", "stac", "tiles"],
    ):
        await ItemsElasticsearchPrivateDriver._apply_deny_policy("cat-x")

    assert len(captured) == 1
    pol = captured[0]
    assert pol.effect == "DENY"
    assert pol.actions == ["GET"]
    pat = pol.resources[0]
    # All registry-supplied prefixes appear in the alternation
    for p in ("features", "maps", "records", "stac", "tiles"):
        assert p in pat
    # Catalog id is regex-escaped and present
    assert re.escape("cat-x") in pat
    # No legacy hardcoded entries remain
    assert "wfs" not in pat
    # `catalog` (singular) is not auto-included unless a real prefix exists
    assert "/(catalog|" not in pat


@pytest.mark.asyncio
async def test_apply_deny_policy_falls_back_to_wildcard_when_registry_empty():
    """If discovery returns no OGC contributors (early lifecycle / test
    fixture), DENY must still fail-closed by emitting a wildcard
    pattern and logging a warning rather than skipping the policy."""
    captured: list = []

    fake_perm = MagicMock()
    fake_perm.register_policy.side_effect = lambda p: captured.append(p) or p
    fake_perm.register_role.return_value = None
    fake_perm.create_policy = AsyncMock()

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=fake_perm,
    ), patch(
        "dynastore.extensions.tools.conformance.get_ogc_service_prefixes",
        return_value=[],
    ):
        await ItemsElasticsearchPrivateDriver._apply_deny_policy("cat-y")

    assert len(captured) == 1
    pat = captured[0].resources[0]
    assert pat.startswith("/[^/]+/catalogs/")
    assert re.escape("cat-y") in pat


@pytest.mark.asyncio
async def test_apply_deny_policy_pattern_matches_singular_catalog_and_item_urls():
    """#960 scope 3 — the DENY regex must cover BOTH the singular catalog
    envelope (``/stac/catalogs/{cat}``) and item paths
    (``/stac/catalogs/{cat}/collections/X/items/Y``) under one policy."""
    captured: list = []

    fake_perm = MagicMock()
    fake_perm.register_policy.side_effect = lambda p: captured.append(p) or p
    fake_perm.register_role.return_value = None
    fake_perm.create_policy = AsyncMock()

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=fake_perm,
    ), patch(
        "dynastore.extensions.tools.conformance.get_ogc_service_prefixes",
        return_value=["stac", "features"],
    ):
        await ItemsElasticsearchPrivateDriver._apply_deny_policy("cat-42")

    assert len(captured) == 1
    pat = captured[0].resources[0]
    rx = re.compile(pat)
    # Singular catalog envelope URL (catalog-private leak path)
    assert rx.fullmatch("/stac/catalogs/cat-42") is not None, pat
    assert rx.fullmatch("/features/catalogs/cat-42") is not None, pat
    # Item URL (existing coverage)
    assert rx.fullmatch(
        "/stac/catalogs/cat-42/collections/x/items/y"
    ) is not None, pat
    # Collection envelope URL
    assert rx.fullmatch(
        "/stac/catalogs/cat-42/collections/x"
    ) is not None, pat
    # Other catalogs must NOT be matched
    assert rx.fullmatch("/stac/catalogs/other-cat") is None, pat


def test_get_ogc_service_prefixes_filters_to_path_prefixes():
    """The helper must accept only ``"/x"``-shaped prefixes (drops empty
    string defaults from the mixin and any non-path values)."""
    from dynastore.extensions.tools.conformance import get_ogc_service_prefixes

    fake_a = MagicMock(prefix="/maps")
    fake_b = MagicMock(prefix="")  # default mixin value — must be dropped
    fake_c = MagicMock(prefix="/records")
    fake_d = MagicMock(spec=[])  # no `prefix` attribute at all
    with patch(
        "dynastore.extensions.tools.conformance.get_protocols",
        return_value=[fake_a, fake_b, fake_c, fake_d],
    ):
        result = get_ogc_service_prefixes()

    assert result == ["maps", "records"]


# ---------------------------------------------------------------------------
# lifespan — job-context skip (#2464)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lifespan_skips_restore_deny_in_job_context():
    """In a Cloud Run Job (K_SERVICE absent), lifespan must skip
    ``_restore_deny_policies()`` entirely.

    The O(N_catalogs × M_collections) startup scan is pure waste in an
    ephemeral job that runs one task and exits — it has no prior in-memory
    DENY state to recover.
    """
    driver = ItemsElasticsearchPrivateDriver()
    restore_called = False

    async def _should_not_be_called() -> None:
        nonlocal restore_called
        restore_called = True

    # Patch at the source; the driver imports it lazily so the source location
    # (dynastore.tools.env) is the right target.
    with patch.object(driver, "_restore_deny_policies", side_effect=_should_not_be_called), \
         patch("dynastore.tools.env.is_running_as_job", return_value=True):
        async with driver.lifespan(object()):
            pass

    assert not restore_called, "_restore_deny_policies must not run in job context"


@pytest.mark.asyncio
async def test_lifespan_calls_restore_deny_in_service_context():
    """In a Cloud Run Service (K_SERVICE present), lifespan must call
    ``_restore_deny_policies()`` as usual — service behavior is unchanged.
    """
    driver = ItemsElasticsearchPrivateDriver()
    restore_called = False

    async def _record_call() -> None:
        nonlocal restore_called
        restore_called = True

    with patch.object(driver, "_restore_deny_policies", side_effect=_record_call), \
         patch("dynastore.tools.env.is_running_as_job", return_value=False):
        async with driver.lifespan(object()):
            pass

    assert restore_called, "_restore_deny_policies must run in service context"


@pytest.mark.asyncio
async def test_lifespan_job_skip_emits_debug_log(caplog):
    """The job-context skip path must emit exactly one DEBUG message so
    operators have a trace without WARNING noise on every job cold-boot.
    """
    import logging
    driver = ItemsElasticsearchPrivateDriver()
    driver_logger = "dynastore.modules.storage.drivers.elasticsearch_private.driver"

    with patch.object(driver, "_restore_deny_policies", new=AsyncMock()), \
         patch("dynastore.tools.env.is_running_as_job", return_value=True), \
         caplog.at_level(logging.DEBUG, logger=driver_logger):
        async with driver.lifespan(object()):
            pass

    skip_records = [
        r for r in caplog.records
        if "skipping _restore_deny_policies" in r.message
    ]
    assert len(skip_records) == 1
    assert skip_records[0].levelname == "DEBUG"
