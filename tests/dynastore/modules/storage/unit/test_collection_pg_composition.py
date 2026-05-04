"""Composition-driver tests for ``CollectionPostgresqlDriver``.

The wrapper is library-only at the time of landing — no
``[project.entry-points]`` registration, so production routing still
fans out to ``CollectionCorePostgresqlDriver`` +
``CollectionStacPostgresqlDriver`` directly.  These tests pin the
composition surface so the eventual cutover commit can flip the
entry-point registration as a one-line change.
"""

from __future__ import annotations

from typing import Any, Dict, FrozenSet, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.protocols.entity_store import (
    CollectionStore,
    EntityStoreCapability,
)
from dynastore.modules.storage.drivers.collection_metadata_postgresql import (
    CollectionPostgresqlDriver,
    CollectionPostgresqlDriverConfig,
    MetadataCoreSidecarConfig,
    MetadataPgSidecarRegistry,
    MetadataStacSidecarConfig,
)


# ---------------------------------------------------------------------------
# Test doubles — minimal CollectionStore stand-ins
# ---------------------------------------------------------------------------


class _FakeInner:
    """Minimal inner driver — captures every call so tests can assert
    fan-out happened on every configured sidecar.
    """

    def __init__(
        self,
        *,
        slice_value: Optional[Dict[str, Any]] = None,
        capabilities: Optional[FrozenSet[str]] = None,
    ):
        self.slice_value = slice_value
        self.capabilities = capabilities or frozenset({
            EntityStoreCapability.READ,
            EntityStoreCapability.WRITE,
            EntityStoreCapability.SOFT_DELETE,
        })
        self.upsert_calls: List[Dict[str, Any]] = []
        self.delete_calls: List[Dict[str, Any]] = []
        self.search_calls: List[Dict[str, Any]] = []

    async def is_available(self) -> bool:
        return True

    async def get_metadata(
        self, catalog_id: str, collection_id: str, *, context=None, db_resource=None,
    ) -> Optional[Dict[str, Any]]:
        return self.slice_value

    async def upsert_metadata(
        self, catalog_id: str, collection_id: str, metadata: Dict[str, Any],
        *, db_resource=None,
    ) -> None:
        self.upsert_calls.append({
            "catalog_id": catalog_id, "collection_id": collection_id,
            "metadata": metadata,
        })

    async def delete_metadata(
        self, catalog_id: str, collection_id: str,
        *, soft: bool = False, db_resource=None,
    ) -> None:
        self.delete_calls.append({
            "catalog_id": catalog_id, "collection_id": collection_id,
            "soft": soft,
        })

    async def search_metadata(
        self, catalog_id: str, *, q=None, bbox=None, datetime_range=None,
        filter_cql=None, limit: int = 100, offset: int = 0,
        context=None, db_resource=None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        self.search_calls.append({"catalog_id": catalog_id, "q": q})
        return [{"id": "from-search"}], 1

    async def get_driver_config(self, catalog_id: str, *, db_resource=None) -> Any:
        return None

    async def location(self, catalog_id: str, collection_id: str) -> Any:
        from dynastore.modules.storage.storage_location import StorageLocation
        return StorageLocation(
            backend="postgresql",
            canonical_uri="postgresql://test",
            identifiers={"catalog_id": catalog_id, "collection_id": collection_id},
            display_label="fake",
        )

    async def ensure_storage(
        self, catalog_id: str, collection_id: Optional[str] = None, **kwargs,
    ) -> None:
        return None


class _FakeCoreCls:
    """Class wrapper — registry expects classes, not instances."""
    _instance: Optional[_FakeInner] = None

    def __new__(cls):  # type: ignore[misc]
        if cls._instance is None:
            cls._instance = _FakeInner(
                slice_value={"title": "core-title", "description": "core-desc"},
                capabilities=frozenset({
                    EntityStoreCapability.READ, EntityStoreCapability.WRITE,
                    EntityStoreCapability.SEARCH, EntityStoreCapability.SOFT_DELETE,
                    EntityStoreCapability.PHYSICAL_ADDRESSING,
                }),
            )
        return cls._instance


class _FakeStacCls:
    _instance: Optional[_FakeInner] = None

    def __new__(cls):  # type: ignore[misc]
        if cls._instance is None:
            cls._instance = _FakeInner(
                slice_value={"extent": {"bbox": [[1, 2, 3, 4]]}, "providers": []},
                capabilities=frozenset({
                    EntityStoreCapability.READ, EntityStoreCapability.WRITE,
                    EntityStoreCapability.SPATIAL_FILTER, EntityStoreCapability.SOFT_DELETE,
                }),
            )
        return cls._instance


@pytest.fixture(autouse=True)
def _reset_registry():
    """Replace the production registry with our two fake classes for the
    duration of each test, then restore.  Also clears the wrapper's
    per-catalog sidecar cache (PR 1e step 4) so cache entries from
    previous tests don't leak across instances — the cache key
    deliberately ignores ``self`` for production efficiency, which
    means without this clear, test 2 would see test 1's resolved inners.
    """
    saved = dict(MetadataPgSidecarRegistry._registry)
    saved_loaded = MetadataPgSidecarRegistry._defaults_loaded
    MetadataPgSidecarRegistry.clear()
    MetadataPgSidecarRegistry._registry["metadata_core"] = _FakeCoreCls  # type: ignore[assignment]
    MetadataPgSidecarRegistry._registry["metadata_stac"] = _FakeStacCls  # type: ignore[assignment]
    MetadataPgSidecarRegistry._defaults_loaded = True
    # Clear singletons between tests.
    _FakeCoreCls._instance = None
    _FakeStacCls._instance = None
    # Flush the wrapper's per-catalog sidecar cache.
    try:
        CollectionPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        yield
    finally:
        MetadataPgSidecarRegistry.clear()
        MetadataPgSidecarRegistry._registry.update(saved)
        MetadataPgSidecarRegistry._defaults_loaded = saved_loaded
        try:
            CollectionPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()  # type: ignore[attr-defined]
        except Exception:
            pass


# ---------------------------------------------------------------------------
# TypedDriver bind + capability set
# ---------------------------------------------------------------------------


def test_typed_driver_bind_resolves():
    assert CollectionPostgresqlDriver.config_cls() is CollectionPostgresqlDriverConfig
    assert CollectionPostgresqlDriverConfig.class_key() == "collection_postgresql_driver"


def test_capabilities_union_covers_inner_capabilities():
    """Wrapper advertises union — every cap declared by either inner today
    must appear so the routing layer recognises the wrapper as a drop-in
    for the two raw drivers.
    """
    caps = CollectionPostgresqlDriver.capabilities
    for required in (
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SOFT_DELETE,
        EntityStoreCapability.SEARCH,         # CORE
        EntityStoreCapability.SPATIAL_FILTER, # STAC
        EntityStoreCapability.PHYSICAL_ADDRESSING,
    ):
        assert required in caps


# ---------------------------------------------------------------------------
# Sidecar registry
# ---------------------------------------------------------------------------


def test_default_sidecars_includes_core_and_stac_when_both_registered():
    sidecars = MetadataPgSidecarRegistry.default_sidecars()
    types = [s.sidecar_type for s in sidecars]
    assert types == ["metadata_core", "metadata_stac"]


def test_default_sidecars_omits_stac_when_unregistered():
    """Simulates a deployment without the stac extra installed."""
    MetadataPgSidecarRegistry._registry.pop("metadata_stac", None)
    sidecars = MetadataPgSidecarRegistry.default_sidecars()
    assert [s.sidecar_type for s in sidecars] == ["metadata_core"]


def test_unknown_sidecar_type_skipped_with_warning(caplog):
    """A sidecar config whose type isn't in the registry skips with a
    warning — better than crashing the whole write.
    """
    driver = CollectionPostgresqlDriver()
    bogus = MetadataCoreSidecarConfig.model_construct(sidecar_type="metadata_does_not_exist")
    with caplog.at_level("WARNING"):
        inners = driver._resolve_inner_drivers([bogus])
    assert inners == []
    assert any("metadata_does_not_exist" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Composition fan-out — write/delete go to every inner
# ---------------------------------------------------------------------------


async def test_upsert_metadata_fans_out_to_every_configured_sidecar():
    driver = CollectionPostgresqlDriver()
    payload = {"title": "T", "description": "D", "extent": {"bbox": [[0, 0, 1, 1]]}}
    await driver.upsert_metadata("cat-a", "col-a", payload)
    core = _FakeCoreCls()
    stac = _FakeStacCls()
    assert len(core.upsert_calls) == 1
    assert len(stac.upsert_calls) == 1
    # Wrapper hands the FULL payload to every inner — each inner is
    # responsible for filtering to its own column set (existing
    # `_PgCollectionMetadataBase._filter_payload` invariant).
    assert core.upsert_calls[0]["metadata"] == payload
    assert stac.upsert_calls[0]["metadata"] == payload


async def test_delete_metadata_fans_out_with_soft_flag_preserved():
    driver = CollectionPostgresqlDriver()
    await driver.delete_metadata("cat-a", "col-a", soft=True)
    core, stac = _FakeCoreCls(), _FakeStacCls()
    assert core.delete_calls == [{"catalog_id": "cat-a", "collection_id": "col-a", "soft": True}]
    assert stac.delete_calls == [{"catalog_id": "cat-a", "collection_id": "col-a", "soft": True}]


# ---------------------------------------------------------------------------
# Read fan-in — slices merged shallow, last-wins on key collision
# ---------------------------------------------------------------------------


async def test_get_metadata_merges_slices_from_every_inner():
    driver = CollectionPostgresqlDriver()
    out = await driver.get_metadata("cat-a", "col-a")
    assert out is not None
    # Both slices present in the merge.
    assert out["title"] == "core-title"
    assert out["extent"] == {"bbox": [[1, 2, 3, 4]]}


async def test_get_metadata_returns_none_when_every_inner_returns_none():
    _FakeCoreCls._instance = _FakeInner(slice_value=None)
    _FakeStacCls._instance = _FakeInner(slice_value=None)
    driver = CollectionPostgresqlDriver()
    assert await driver.get_metadata("cat-a", "col-a") is None


async def test_get_metadata_swallows_per_inner_failure_and_returns_other_slices():
    """An inner driver crashing on read must not blank out the whole
    response — mirrors the router's `_safe_get` graceful-degrade shape.
    """
    failing = AsyncMock(spec=CollectionStore)
    failing.get_metadata.side_effect = RuntimeError("boom")
    failing.is_available.return_value = True
    failing.capabilities = frozenset({EntityStoreCapability.READ})

    class _FailingCls:
        def __new__(cls):  # type: ignore[misc]
            return failing

    MetadataPgSidecarRegistry._registry["metadata_core"] = _FailingCls  # type: ignore[assignment]
    driver = CollectionPostgresqlDriver()
    out = await driver.get_metadata("cat-a", "col-a")
    assert out is not None
    # Only the STAC slice survived.
    assert "extent" in out
    assert "title" not in out


# ---------------------------------------------------------------------------
# Search — first SEARCH-capable inner wins
# ---------------------------------------------------------------------------


async def test_search_metadata_delegates_to_first_search_capable_inner():
    """CORE has SEARCH; STAC doesn't.  Wrapper must route the search call
    to CORE only — never to STAC (which would return [], 0 today and
    silently shadow CORE results).
    """
    driver = CollectionPostgresqlDriver()
    rows, total = await driver.search_metadata("cat-a", q="foo")
    assert total == 1
    assert rows == [{"id": "from-search"}]
    core, stac = _FakeCoreCls(), _FakeStacCls()
    assert len(core.search_calls) == 1
    assert stac.search_calls == []


async def test_search_metadata_returns_empty_when_no_inner_has_search():
    _FakeCoreCls._instance = _FakeInner(
        capabilities=frozenset({EntityStoreCapability.READ}),
    )
    _FakeStacCls._instance = _FakeInner(
        capabilities=frozenset({EntityStoreCapability.READ}),
    )
    driver = CollectionPostgresqlDriver()
    rows, total = await driver.search_metadata("cat-a", q="foo")
    assert rows == [] and total == 0


# ---------------------------------------------------------------------------
# Wrapper config — sidecar discriminated union round-trip
# ---------------------------------------------------------------------------


def test_wrapper_config_sidecars_discriminated_union_round_trips():
    cfg = CollectionPostgresqlDriverConfig(
        sidecars=[
            MetadataCoreSidecarConfig(),
            MetadataStacSidecarConfig(),
        ],
    )
    dumped = cfg.model_dump(exclude_unset=True)
    restored = CollectionPostgresqlDriverConfig.model_validate(dumped)
    assert [s.sidecar_type for s in restored.sidecars] == [
        "metadata_core", "metadata_stac",
    ]


def test_wrapper_config_default_sidecars_is_empty_list():
    """Empty default — driver falls back to registry default at use time
    (mirrors the items-tier default-fast invariant from M1b.2).
    """
    cfg = CollectionPostgresqlDriverConfig()
    assert cfg.sidecars == []


# ---------------------------------------------------------------------------
# STAC capability marker — wrapper must structurally satisfy
# ``StacCollectionEntityStoreCapability`` IFF a STAC inner is loaded.
# Regression guard for PR 1e step 3b: before the fix, the wrapper had no
# ``stac_metadata_columns`` method, so ``isinstance(wrapper,
# StacCollectionEntityStoreCapability)`` returned False and
# ``stac_service._assert_stac_capable_metadata_stack`` warned
# "STAC slice will be dropped on write" even though the wrapper's
# STAC sidecar actually persisted it.
# ---------------------------------------------------------------------------


def test_wrapper_satisfies_stac_capability_when_stac_inner_loaded():
    from dynastore.extensions.stac.protocols import (
        StacCollectionEntityStoreCapability,
    )

    # Make the STAC fake actually expose the marker method so the wrapper
    # can delegate to it.
    def _stac_cols(self):
        return ("extent", "providers", "stac_version")
    _FakeStacCls._instance = _FakeInner()
    _FakeStacCls._instance.stac_metadata_columns = _stac_cols.__get__(  # type: ignore[attr-defined]
        _FakeStacCls._instance, _FakeInner,
    )
    driver = CollectionPostgresqlDriver()
    assert isinstance(driver, StacCollectionEntityStoreCapability)
    cols = driver.stac_metadata_columns()
    assert "extent" in cols
    assert "stac_version" in cols


def test_wrapper_returns_empty_columns_when_no_stac_inner_loaded():
    """Deployment without the stac extra: registry has only metadata_core,
    wrapper's stac_metadata_columns() must return () so
    ``stac_service._has_stac`` correctly identifies STAC as unavailable.
    """
    MetadataPgSidecarRegistry._registry.pop("metadata_stac", None)
    driver = CollectionPostgresqlDriver()
    assert driver.stac_metadata_columns() == ()


# ---------------------------------------------------------------------------
# Apply-handler warning — surfaces silent-drop of operator sidecars override
# until runtime fetch is wired.  Honest acknowledgment of the half-finished
# state of the ``sidecars`` config field; without this warning an operator
# patching the routing config with custom sidecars would see no behaviour
# change and assume their override took effect.
# ---------------------------------------------------------------------------


async def test_apply_handler_invalidates_cache_and_logs_info(caplog):
    """PR 1e step 4: handler clears the per-catalog sidecar cache and
    logs INFO so the operator-submitted override takes effect on the
    next metadata operation instead of waiting for the TTL to expire.
    """
    from dynastore.modules.storage.drivers.collection_metadata_postgresql import (
        _on_apply_collection_pg_driver_config,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    wrapper = CollectionPostgresqlDriver()
    register_plugin(wrapper)
    try:
        cfg = CollectionPostgresqlDriverConfig(
            sidecars=[MetadataCoreSidecarConfig()],  # non-empty → log INFO
        )
        with caplog.at_level("INFO"):
            await _on_apply_collection_pg_driver_config(
                cfg, catalog_id="some-catalog", collection_id=None,
                db_resource=None,
            )
        assert any(
            "sidecar cache invalidated" in r.message and "metadata_core" in r.message
            for r in caplog.records
        ), "Apply handler must log INFO when invalidating cache for non-empty override"
    finally:
        unregister_plugin(wrapper)


async def test_apply_handler_silent_for_empty_sidecars(caplog):
    """Default-empty config: handler still invalidates the cache (cheap)
    but suppresses the INFO log to avoid noise — operator expressed no
    explicit override worth surfacing.
    """
    from dynastore.modules.storage.drivers.collection_metadata_postgresql import (
        _on_apply_collection_pg_driver_config,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    wrapper = CollectionPostgresqlDriver()
    register_plugin(wrapper)
    try:
        cfg = CollectionPostgresqlDriverConfig()  # empty default
        with caplog.at_level("INFO"):
            await _on_apply_collection_pg_driver_config(
                cfg, catalog_id="cat", collection_id=None, db_resource=None,
            )
        assert not any(
            "sidecar cache invalidated" in r.message
            for r in caplog.records
        ), "Empty override must not emit the INFO log"
    finally:
        unregister_plugin(wrapper)


async def test_apply_handler_ignores_non_wrapper_configs():
    """Apply-handler is registered globally on the config-apply pipeline;
    must no-op cleanly when handed a config of a different class.
    """
    from dynastore.modules.storage.drivers.collection_metadata_postgresql import (
        _on_apply_collection_pg_driver_config,
    )

    class _Unrelated:
        pass

    # Must not raise on unrelated config types.
    await _on_apply_collection_pg_driver_config(
        _Unrelated(), catalog_id="cat", collection_id=None, db_resource=None,
    )


async def test_apply_handler_safe_when_wrapper_not_yet_registered():
    """Early-lifespan apply flushes can run before the wrapper is
    register_plugin'd — handler must no-op cleanly, not crash.  The
    cold cache will start fresh on first use anyway.
    """
    from dynastore.modules.storage.drivers.collection_metadata_postgresql import (
        _on_apply_collection_pg_driver_config,
    )

    cfg = CollectionPostgresqlDriverConfig(
        sidecars=[MetadataCoreSidecarConfig()],
    )
    # No register_plugin — handler should silently return.
    await _on_apply_collection_pg_driver_config(
        cfg, catalog_id="cat", collection_id=None, db_resource=None,
    )


# ---------------------------------------------------------------------------
# Operator-override behavior — the whole point of PR 1e step 4.
# Pins that a non-empty CollectionPostgresqlDriverConfig.sidecars
# fetched via ConfigsProtocol actually changes which inner drivers
# get fanned to at runtime, vs. registry-default fallback.
# ---------------------------------------------------------------------------


async def test_operator_override_actually_changes_runtime_fanout():
    """Mock ConfigsProtocol.get_config to return a config with ONLY
    metadata_core in sidecars (no metadata_stac).  Verify the wrapper
    fans out to ONLY core, not both — proving the runtime override
    is honored, not silently dropped (which was the v0.5.70 state).
    """
    custom_cfg = CollectionPostgresqlDriverConfig(
        sidecars=[MetadataCoreSidecarConfig()],  # core only — no stac
    )
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(return_value=custom_cfg)

    from dynastore.models.protocols.configs import ConfigsProtocol

    # The wrapper imports get_protocol locally inside the resolver
    # method, so patch it where it's used (the discovery module).
    with patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=lambda p: fake_configs if p is ConfigsProtocol else None,
    ):
        CollectionPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()
        driver = CollectionPostgresqlDriver()
        payload = {"title": "T", "extent": {"bbox": [[0, 0, 1, 1]]}}
        await driver.upsert_metadata("cat-a", "col-a", payload)

    # Core inner saw the call (operator opted in).
    assert len(_FakeCoreCls().upsert_calls) == 1
    # STAC inner did NOT (operator opted out via config).
    assert len(_FakeStacCls().upsert_calls) == 0


async def test_registry_default_used_when_no_operator_override():
    """When ConfigsProtocol.get_config returns a config with EMPTY
    sidecars (or fetch fails / no ConfigsProtocol), wrapper falls back
    to MetadataPgSidecarRegistry.default_sidecars — both inners get fanned.
    """
    empty_cfg = CollectionPostgresqlDriverConfig()  # no sidecars
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(return_value=empty_cfg)

    from dynastore.models.protocols.configs import ConfigsProtocol

    with patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=lambda p: fake_configs if p is ConfigsProtocol else None,
    ):
        CollectionPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()
        driver = CollectionPostgresqlDriver()
        await driver.upsert_metadata("cat-a", "col-a", {"title": "T"})

    # Both inners reached (registry default).
    assert len(_FakeCoreCls().upsert_calls) == 1
    assert len(_FakeStacCls().upsert_calls) == 1


# ---------------------------------------------------------------------------
# Discovery integration — wrapper IS the discovered CollectionStore
# plugin after register_plugin().  Closes a real gap: existing router tests
# inject mocks via ``drivers=`` and don't exercise the production discovery
# path.  This test catches "I forgot to register the wrapper" or "I forgot
# to remove a raw driver entry-point" — exactly the cutover-class
# regressions PR 1e step 3b would have introduced if not caught.
# ---------------------------------------------------------------------------


async def test_wrapper_is_discoverable_via_get_protocols():
    """Verify ``get_protocols(CollectionStore)`` returns the
    wrapper after ``register_plugin``, NOT a raw inner driver.  Mirrors
    the production discovery path that ``collection_metadata_router._resolve_drivers``
    uses when called without an explicit ``drivers=`` kwarg.
    """
    from dynastore.models.protocols.entity_store import (
        CollectionStore,
    )
    from dynastore.tools.discovery import (
        get_protocols,
        register_plugin,
        unregister_plugin,
    )

    wrapper = CollectionPostgresqlDriver()
    register_plugin(wrapper)
    try:
        discovered = list(get_protocols(CollectionStore))
        # Wrapper IS in the discovery results.
        assert wrapper in discovered
        # And every PG-tier discovered instance is a wrapper, not a raw inner.
        # (Other CollectionStore implementers — e.g. ES drivers — may
        # also appear; this test only pins the PG-tier shape.)
        from dynastore.modules.storage.drivers.metadata_postgresql import (
            CollectionCorePostgresqlDriver,
        )
        for d in discovered:
            assert not isinstance(d, CollectionCorePostgresqlDriver), (
                "Raw CollectionCorePostgresqlDriver must NOT surface as a "
                "discovered plugin after PR 1e step 3b cutover — composition "
                "is the only PG-tier path."
            )
    finally:
        unregister_plugin(wrapper)
