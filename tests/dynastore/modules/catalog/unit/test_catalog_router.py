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

"""Unit tests for the M2.3b catalog-metadata router.

Covers:

- Read fan-out merges CORE + STAC dicts.
- Read returns ``None`` only when every driver returned ``None``.
- Read degrades: a driver that raises is logged at WARNING and omitted
  from the merged envelope (other drivers still contribute).
- Write fan-out calls every driver in order, sharing the same
  ``db_resource``.
- Delete fan-out honours ``soft``.
- Default driver resolution pulls from ``get_protocols(CatalogStore)``.
- Absence of registered drivers → router no-ops everything (no raise).
- CatalogRoutingConfig defaults (the other half of M2.3b) point at the
  canonical M2.1 driver names under both WRITE and READ.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _silence_event_emission(monkeypatch):
    """Stub ``emit_event`` on the router.

    The tests in this file focus on driver fan-out semantics, not on
    event plumbing (which has its own suite below).  Stubbing emit
    here keeps the existing assertions unchanged after the M3.0
    emission wiring landed on the router.
    """
    monkeypatch.setattr(
        "dynastore.modules.catalog.event_service.emit_event",
        AsyncMock(return_value=None),
    )


# ---------------------------------------------------------------------------
# READ fan-out + merge
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_catalog_metadata_merges_core_and_stac():
    """Router awaits both drivers concurrently and merges their dicts."""
    from dynastore.modules.catalog.catalog_router import (
        get_catalog_metadata,
    )

    core = MagicMock()
    core.get_catalog_metadata = AsyncMock(return_value={"title": {"en": "T"}})
    stac = MagicMock()
    stac.get_catalog_metadata = AsyncMock(return_value={"stac_version": "1.1.0"})

    result = await get_catalog_metadata("cat-42", drivers=[core, stac])

    assert result == {"title": {"en": "T"}, "stac_version": "1.1.0"}
    core.get_catalog_metadata.assert_awaited_once_with(
        "cat-42", context=None, db_resource=None,
    )
    stac.get_catalog_metadata.assert_awaited_once_with(
        "cat-42", context=None, db_resource=None,
    )


@pytest.mark.asyncio
async def test_get_catalog_metadata_returns_none_when_all_drivers_return_none():
    """Envelope absence is a domain-wide signal — not a partial dict."""
    from dynastore.modules.catalog.catalog_router import (
        get_catalog_metadata,
    )

    d1 = MagicMock()
    d1.get_catalog_metadata = AsyncMock(return_value=None)
    d2 = MagicMock()
    d2.get_catalog_metadata = AsyncMock(return_value=None)

    assert await get_catalog_metadata("cat", drivers=[d1, d2]) is None


@pytest.mark.asyncio
async def test_get_catalog_metadata_returns_partial_when_one_driver_has_data():
    """One domain populated + one empty → dict of the populated domain."""
    from dynastore.modules.catalog.catalog_router import (
        get_catalog_metadata,
    )

    core = MagicMock()
    core.get_catalog_metadata = AsyncMock(return_value={"title": "T"})
    stac = MagicMock()
    stac.get_catalog_metadata = AsyncMock(return_value=None)

    assert await get_catalog_metadata("cat", drivers=[core, stac]) == {"title": "T"}


@pytest.mark.asyncio
async def test_get_catalog_metadata_degrades_on_driver_exception(caplog):
    """Driver raising on READ is logged at WARNING; merge keeps the rest."""
    from dynastore.modules.catalog import catalog_router as mod
    from dynastore.modules.catalog.catalog_router import (
        get_catalog_metadata,
    )

    core = MagicMock()
    core.get_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("pg connection reset"),
    )
    stac = MagicMock()
    stac.get_catalog_metadata = AsyncMock(return_value={"stac_version": "1.1.0"})

    with caplog.at_level(logging.WARNING, logger=mod.__name__):
        result = await get_catalog_metadata("cat-42", drivers=[core, stac])

    assert result == {"stac_version": "1.1.0"}
    assert any(
        "Catalog-metadata READ failed" in r.message for r in caplog.records
    )


@pytest.mark.asyncio
async def test_get_catalog_metadata_noop_without_registered_drivers():
    """Empty driver list → None with no crash and no warning per call."""
    from dynastore.modules.catalog.catalog_router import (
        get_catalog_metadata,
    )

    # Explicit empty list — caller opted in to "no drivers".
    assert await get_catalog_metadata("cat", drivers=[]) is None


# ---------------------------------------------------------------------------
# WRITE fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_catalog_metadata_calls_every_driver_sequentially():
    """Sequential upsert — shared db_resource, deterministic order."""
    from dynastore.modules.catalog.catalog_router import (
        upsert_catalog_metadata,
    )

    order: list[str] = []

    def _make(name):
        m = MagicMock()
        async def _upsert(catalog_id, metadata, *, db_resource=None):
            order.append(name)
        m.upsert_catalog_metadata = AsyncMock(side_effect=_upsert)
        return m

    core = _make("core")
    stac = _make("stac")
    payload = {"title": {"en": "T"}, "stac_version": "1.1.0"}
    fake_conn = MagicMock()

    await upsert_catalog_metadata(
        "cat-42", payload, db_resource=fake_conn, drivers=[core, stac],
    )

    assert order == ["core", "stac"]
    core.upsert_catalog_metadata.assert_awaited_once_with(
        "cat-42", payload, db_resource=fake_conn,
    )
    stac.upsert_catalog_metadata.assert_awaited_once_with(
        "cat-42", payload, db_resource=fake_conn,
    )


@pytest.mark.asyncio
async def test_upsert_catalog_metadata_bubbles_driver_exceptions():
    """WRITE failures MUST propagate — dual-write needs all-or-nothing."""
    from dynastore.modules.catalog.catalog_router import (
        upsert_catalog_metadata,
    )

    core = MagicMock()
    core.upsert_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("FK violation"),
    )

    with pytest.raises(RuntimeError, match="FK violation"):
        await upsert_catalog_metadata("cat", {}, drivers=[core])


@pytest.mark.asyncio
async def test_upsert_catalog_metadata_second_driver_skipped_on_first_failure():
    """W2 regression: first-driver exception must STOP the fan-out.

    The drivers share one ``db_resource``; if driver 1's SAVEPOINT
    rolls back and the router continued to driver 2, driver 2 would
    run on a connection whose inner SAVEPOINT had aborted.  The router
    must raise on first failure so the enclosing lifecycle-hook
    SAVEPOINT catches the exception and rolls back cleanly.
    """
    from dynastore.modules.catalog.catalog_router import (
        upsert_catalog_metadata,
    )

    core = MagicMock()
    core.upsert_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("pg error on CORE"),
    )
    stac = MagicMock()
    stac.upsert_catalog_metadata = AsyncMock()

    with pytest.raises(RuntimeError, match="pg error on CORE"):
        await upsert_catalog_metadata("cat", {}, drivers=[core, stac])

    # The key assertion: STAC driver must NOT have been invoked after
    # CORE raised.  Any continuation would run on a poisoned connection.
    stac.upsert_catalog_metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_upsert_degrades_when_secondary_index_driver_fails(caplog):
    """A secondary-index (ES) WRITE failure degrades to async reindex.

    The catalog ES driver marks itself ``is_catalog_indexer = True`` and is
    reindexed off the ``catalog_metadata_changed`` event emitted after the
    fan-out.  When its client is unavailable (e.g. on an env with no catalog
    ES), the synchronous write must NOT abort the canonical PG write — it is
    logged at WARNING and the primary driver still commits (geoid#2482).
    """
    from dynastore.modules.catalog import catalog_router as mod
    from dynastore.modules.catalog.catalog_router import (
        upsert_catalog_metadata,
    )

    core = MagicMock()
    core.is_catalog_indexer = False
    core.upsert_catalog_metadata = AsyncMock()

    es = MagicMock()
    es.is_catalog_indexer = True
    es.upsert_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("Elasticsearch client not available"),
    )

    with caplog.at_level(logging.WARNING, logger=mod.__name__):
        # Must NOT raise — secondary failure degrades.
        await upsert_catalog_metadata("cat-42", {"title": "T"}, drivers=[core, es])

    core.upsert_catalog_metadata.assert_awaited_once()
    es.upsert_catalog_metadata.assert_awaited_once()
    degrade = [r for r in caplog.records if "secondary-index driver" in r.message]
    assert degrade
    # The handled degradation must not log a traceback — exc_info would render
    # as a false ERROR in aggregated logs on ES-less envs.
    assert all(r.exc_info is None for r in degrade)


@pytest.mark.asyncio
async def test_upsert_primary_failure_still_fatal_even_with_indexer_present():
    """A primary (PG) WRITE failure stays fatal regardless of indexers.

    Guards the asymmetry: only ``is_catalog_indexer`` drivers degrade; the
    canonical PG store must still propagate so the lifecycle SAVEPOINT rolls
    back (and so the existing all-or-nothing dual-write contract holds).
    """
    from dynastore.modules.catalog.catalog_router import (
        upsert_catalog_metadata,
    )

    core = MagicMock()
    core.is_catalog_indexer = False
    core.upsert_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("pg error on CORE"),
    )

    with pytest.raises(RuntimeError, match="pg error on CORE"):
        await upsert_catalog_metadata("cat", {}, drivers=[core])


# ---------------------------------------------------------------------------
# DELETE fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_degrades_when_secondary_index_driver_fails(caplog):
    """DELETE mirrors UPSERT: a secondary-index failure degrades, not aborts."""
    from dynastore.modules.catalog import catalog_router as mod
    from dynastore.modules.catalog.catalog_router import (
        delete_catalog_metadata,
    )

    core = MagicMock()
    core.is_catalog_indexer = False
    core.delete_catalog_metadata = AsyncMock()

    es = MagicMock()
    es.is_catalog_indexer = True
    es.delete_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("Elasticsearch client not available"),
    )

    with caplog.at_level(logging.WARNING, logger=mod.__name__):
        await delete_catalog_metadata("cat", drivers=[core, es])

    core.delete_catalog_metadata.assert_awaited_once()
    es.delete_catalog_metadata.assert_awaited_once()
    degrade = [r for r in caplog.records if "secondary-index driver" in r.message]
    assert degrade
    assert all(r.exc_info is None for r in degrade)


@pytest.mark.asyncio
async def test_delete_catalog_metadata_forwards_soft_flag():
    """``soft=True`` reaches every driver."""
    from dynastore.modules.catalog.catalog_router import (
        delete_catalog_metadata,
    )

    core = MagicMock()
    core.delete_catalog_metadata = AsyncMock()
    stac = MagicMock()
    stac.delete_catalog_metadata = AsyncMock()

    await delete_catalog_metadata("cat", soft=True, drivers=[core, stac])

    core.delete_catalog_metadata.assert_awaited_once_with(
        "cat", soft=True, db_resource=None,
    )
    stac.delete_catalog_metadata.assert_awaited_once_with(
        "cat", soft=True, db_resource=None,
    )


# ---------------------------------------------------------------------------
# Default driver resolution
# ---------------------------------------------------------------------------


def test_resolve_catalog_store_drivers_goes_through_get_protocols():
    """Default path resolves every registered CatalogStore."""
    from dynastore.modules.catalog import catalog_router as mod

    fake_driver = MagicMock()
    with patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[fake_driver],
    ) as gp:
        result = mod._resolve_catalog_store_drivers()
    assert result == [fake_driver]
    gp.assert_called_once()
    # Called with the CatalogStore protocol — the one arg to get_protocols.
    from dynastore.models.protocols.entity_store import CatalogStore
    assert gp.call_args.args[0] is CatalogStore


# ---------------------------------------------------------------------------
# CatalogRoutingConfig defaults (partner commit)
# ---------------------------------------------------------------------------


class TestMetadataChangedEventEmission:
    """M3.0 — the router emits catalog_metadata_changed on every mutation.

    These tests override the autouse emit stub so they can assert the
    exact calls made.  One event per driver class is expected; multiple
    instances of the same driver class de-dup to a single event per call.
    """

    @pytest.mark.asyncio
    async def test_upsert_emits_per_driver_class_event(self, monkeypatch):
        from dynastore.modules.catalog.catalog_router import (
            upsert_catalog_metadata,
        )

        # Two distinct driver classes — emit dedups by class name.
        class CoreDriver:
            upsert_catalog_metadata = AsyncMock()

        class StacDriver:
            upsert_catalog_metadata = AsyncMock()

        core = CoreDriver()
        stac = StacDriver()

        emit = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "dynastore.modules.catalog.event_service.emit_event", emit,
        )

        await upsert_catalog_metadata(
            "cat-42", {"title": {"en": "T"}},
            drivers=[core, stac],  # type: ignore[arg-type]
        )
        # One event per driver class — two distinct classes → two events.
        assert emit.await_count == 2
        classes = [
            call.kwargs["payload"]["driver_class"]
            for call in emit.call_args_list
        ]
        assert set(classes) == {"CoreDriver", "StacDriver"}
        # Every event carries ``operation`` and ``catalog_id``.
        for call in emit.call_args_list:
            payload = call.kwargs["payload"]
            assert payload["catalog_id"] == "cat-42"
            assert payload["operation"] == "upsert"

    @pytest.mark.asyncio
    async def test_delete_emits_delete_operation(self, monkeypatch):
        from dynastore.modules.catalog.catalog_router import (
            delete_catalog_metadata,
        )

        core = MagicMock()
        core.delete_catalog_metadata = AsyncMock()

        emit = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "dynastore.modules.catalog.event_service.emit_event", emit,
        )

        await delete_catalog_metadata("cat", soft=False, drivers=[core])
        emit.assert_awaited_once()
        assert emit.call_args.kwargs["payload"]["operation"] == "delete"

    @pytest.mark.asyncio
    async def test_soft_delete_emits_soft_delete_operation(self, monkeypatch):
        from dynastore.modules.catalog.catalog_router import (
            delete_catalog_metadata,
        )

        core = MagicMock()
        core.delete_catalog_metadata = AsyncMock()
        emit = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "dynastore.modules.catalog.event_service.emit_event", emit,
        )

        await delete_catalog_metadata("cat", soft=True, drivers=[core])
        assert emit.call_args.kwargs["payload"]["operation"] == "soft_delete"

    @pytest.mark.asyncio
    async def test_duplicate_driver_class_dedup_to_one_event(self, monkeypatch):
        """Two instances of the same driver class → one event, not two."""
        from dynastore.modules.catalog.catalog_router import (
            upsert_catalog_metadata,
        )

        class PgCore:
            upsert_catalog_metadata = AsyncMock()

        pg_a = PgCore()
        pg_b = PgCore()  # second instance of the same class

        emit = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "dynastore.modules.catalog.event_service.emit_event", emit,
        )

        await upsert_catalog_metadata(
            "cat", {}, drivers=[pg_a, pg_b],  # type: ignore[arg-type]
        )
        assert emit.await_count == 1   # one PgCore event, not two
        assert emit.call_args.kwargs["payload"]["driver_class"] == "PgCore"

    @pytest.mark.asyncio
    async def test_upsert_emit_receives_same_db_resource_as_drivers(
        self, monkeypatch,
    ):
        """Transactional-outbox contract pin: driver writes + the
        ``catalog_metadata_changed`` emission must land on the IDENTICAL
        connection object the caller passed in.

        If the router (or any downstream helper) ever acquires a fresh
        pooled connection instead of threading ``db_resource`` through,
        the emitted event would be committed independently of the
        driver write — breaking the outbox guarantee that an event
        disappears with its transaction on rollback.

        The router docstring's "Transaction-scope contract (load-
        bearing)" section depends on this identity propagation; this
        test is the regression fuse against silent breakage of it.
        """
        from dynastore.modules.catalog.catalog_router import (
            upsert_catalog_metadata,
        )

        # Two distinct driver classes → two events emitted.
        class _RouterTestCore:
            upsert_catalog_metadata = AsyncMock()

        class _RouterTestStac:
            upsert_catalog_metadata = AsyncMock()

        core = _RouterTestCore()
        stac = _RouterTestStac()

        emit = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "dynastore.modules.catalog.event_service.emit_event", emit,
        )

        # Sentinel used as the caller's ``db_resource``.  Identity
        # (``is``) checks below pin that NONE of the router's call
        # sites substitute a different object.
        live_conn = MagicMock(name="live_AsyncConnection")

        await upsert_catalog_metadata(
            "cat-42", {"title": {"en": "T"}, "stac_version": "1.1.0"},
            db_resource=live_conn,
            drivers=[core, stac],  # type: ignore[arg-type]
        )

        # Driver writes see the same connection.
        assert core.upsert_catalog_metadata.await_args.kwargs["db_resource"] is live_conn
        assert stac.upsert_catalog_metadata.await_args.kwargs["db_resource"] is live_conn
        # Emits see the same connection (one per driver class, two total).
        assert emit.await_count == 2
        for call in emit.call_args_list:
            assert call.kwargs["db_resource"] is live_conn, (
                "catalog_metadata_changed event emitted with a different "
                "db_resource than the driver writes — transactional-outbox "
                "contract violated"
            )

    @pytest.mark.asyncio
    async def test_emit_failure_logs_but_does_not_raise(self, monkeypatch, caplog):
        """A broken emit_event must not turn a successful write into a 5xx."""
        from dynastore.modules.catalog import catalog_router as mod
        from dynastore.modules.catalog.catalog_router import (
            upsert_catalog_metadata,
        )

        core = MagicMock()
        core.upsert_catalog_metadata = AsyncMock()

        async def _boom(*args, **kwargs):
            raise RuntimeError("outbox unavailable")

        monkeypatch.setattr(
            "dynastore.modules.catalog.event_service.emit_event", _boom,
        )

        with caplog.at_level(logging.WARNING, logger=mod.__name__):
            # Must not raise.
            await upsert_catalog_metadata("cat", {}, drivers=[core])

        core.upsert_catalog_metadata.assert_awaited_once()
        assert any(
            "event emission failed" in r.message for r in caplog.records
        )


def test_catalog_routing_config_defaults_use_canonical_names():
    """The defaults must reference the canonical registered driver_ref.

    ``catalog_core_postgresql_driver`` / ``catalog_stac_postgresql_driver``
    were never registered as entry-points — the registered ``CatalogStore``
    is ``catalog_postgresql_driver``, a composition wrapper that fans CRUD
    across the catalog_core + catalog_stac PG sidecars internally. Drift
    here silently breaks ``_validate_routing_entries`` on any deployment
    that loads ``CatalogRoutingConfig``'s defaults without an explicit
    platform override.
    """
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig, Operation,
    )

    cfg = CatalogRoutingConfig()
    write_ids = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    read_ids = {e.driver_ref for e in cfg.operations[Operation.READ]}
    assert write_ids == {"catalog_postgresql_driver"}
    # READ has two entries: PG (SoR, untagged) + ES (hint-routed).
    assert "catalog_postgresql_driver" in read_ids
    assert "catalog_elasticsearch_driver" in read_ids
    assert len(read_ids) == 2
