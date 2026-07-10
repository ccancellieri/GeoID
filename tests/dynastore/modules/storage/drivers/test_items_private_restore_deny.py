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
The lifespan startup hook re-applies the catalog-wide DENY policy
idempotently for any catalog whose items routing pins the private
driver — at catalog scope or in any stored collection-level delta
row (#3160: the check reads only persisted rows; it no longer lists
collections or resolves the config waterfall per collection).

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
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_catalog(cat_id: str) -> MagicMock:
    cat = MagicMock()
    cat.id = cat_id
    return cat


def _items_private_routing() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    source="auto",
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



def _catalogs_proto_with(catalogs: List[MagicMock]) -> MagicMock:
    proto = MagicMock()

    async def list_catalogs(*, limit: int, offset: int) -> List[MagicMock]:
        return catalogs[offset:offset + limit]

    proto.list_catalogs = list_catalogs
    return proto


def _private_items_delta() -> dict:
    """A collection-level ``ItemsRoutingConfig`` delta row exactly as
    persisted (string keys, plain dicts) pinning the private driver."""
    return {
        "operations": {
            "index": [
                {
                    "driver_ref": "items_elasticsearch_private_driver",
                    "source": "auto",
                },
            ],
        },
    }


def _public_items_delta() -> dict:
    return {
        "operations": {
            "write": [
                {"driver_ref": "items_postgresql_driver", "on_failure": "fatal"},
            ],
        },
    }


def _configs_proto_with(
    catalog_scope_by_cat: Dict[str, object],
    deltas_by_cat: Dict[str, List[dict]],
) -> MagicMock:
    """Mocks the two reads the #3160 helper performs: the resolved
    catalog-scope config and the stored collection-level delta rows."""
    proto = MagicMock()

    async def get_config(cls, *, catalog_id, **kwargs):
        return catalog_scope_by_cat.get(catalog_id)

    async def list_collection_config_deltas(cls, catalog_id, **kwargs):
        return deltas_by_cat.get(catalog_id, [])

    proto.get_config = get_config
    proto.list_collection_config_deltas = list_collection_config_deltas
    return proto


# ---------------------------------------------------------------------------
# _catalog_has_private_collection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_helper_returns_true_when_stored_delta_pins_private_driver():
    configs = _configs_proto_with(
        {"cat-a": _items_public_routing()},
        {"cat-a": [_public_items_delta(), _private_items_delta()]},
    )
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        configs, "cat-a",
    )
    assert result is True


@pytest.mark.asyncio
async def test_helper_returns_true_when_catalog_scope_pins_private_driver():
    """#3160 deliberate widening: a catalog-scope private pin restores the
    DENY without touching any collection row (fails closed for privacy)."""
    configs = _configs_proto_with(
        {"cat-a": _items_private_routing()},
        {"cat-a": []},
    )
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        configs, "cat-a",
    )
    assert result is True


@pytest.mark.asyncio
async def test_helper_returns_false_when_all_stored_deltas_public():
    """After #1047 Phase 2, only items routing is checked — collection
    routing is not scanned for the DENY decision."""
    configs = _configs_proto_with(
        {"cat-a": _items_public_routing()},
        {"cat-a": [_public_items_delta(), _public_items_delta()]},
    )
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        configs, "cat-a",
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_returns_false_when_no_deltas_stored():
    """Collections without a stored ItemsRoutingConfig row inherit the
    catalog scope — with a public catalog scope and no rows there is
    nothing private to protect."""
    configs = _configs_proto_with(
        {"cat-empty": _items_public_routing()},
        {},
    )
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        configs, "cat-empty",
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_returns_false_on_delta_listing_exception():
    """A transient delta-listing failure should NOT propagate or be
    misinterpreted as 'has private' — defer to the next apply."""
    configs = MagicMock()

    async def get_config(cls, *, catalog_id, **kwargs):
        return _items_public_routing()

    configs.get_config = get_config
    configs.list_collection_config_deltas = AsyncMock(
        side_effect=RuntimeError("transient"),
    )
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        configs, "cat-flaky",
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_survives_catalog_scope_lookup_failure():
    """A transient catalog-scope resolution failure must not stop the
    check — the stored deltas are still consulted."""
    configs = MagicMock()
    configs.get_config = AsyncMock(side_effect=RuntimeError("transient"))

    async def list_collection_config_deltas(cls, catalog_id, **kwargs):
        return [_private_items_delta()]

    configs.list_collection_config_deltas = list_collection_config_deltas
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        configs, "cat-a",
    )
    assert result is True


@pytest.mark.asyncio
async def test_helper_tolerates_delta_without_operations():
    """Stored deltas are partial by design — a row that only overrides
    non-routing fields must not break the scan."""
    configs = _configs_proto_with(
        {"cat-a": _items_public_routing()},
        {"cat-a": [{"default_failure_policy": "fatal"}, _private_items_delta()]},
    )
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        configs, "cat-a",
    )
    assert result is True


# ---------------------------------------------------------------------------
# _restore_deny_policies — full lifespan loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restore_applies_deny_only_for_catalogs_with_private_collections():
    """End-to-end of the lifespan loop: two catalogs, one has a stored
    private-routing collection delta and one is fully public;
    ``_apply_deny_policy`` must fire only for the catalog with the
    private collection."""
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-private"), _stub_catalog("cat-public")],
    )
    configs = _configs_proto_with(
        {
            "cat-private": _items_public_routing(),
            "cat-public": _items_public_routing(),
        },
        {
            "cat-private": [_private_items_delta()],
            "cat-public": [_public_items_delta(), _public_items_delta()],
        },
    )

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
