"""Call-site dedup test — closes followup #2a from PR #420 / #427 / #432.

Drives ``_trigger_configured_actions`` (the real Pub/Sub→ingestion call
site in ``packages/extensions/gcp/src/dynastore/extensions/gcp/gcp_events.py``)
twice with the **same** OBJECT_FINALIZE payload and asserts:

* ``processes_module.execute_process`` is invoked twice (the call site
  itself does not dedup — that contract lives downstream in
  ``create_task``)
* both invocations carry the **same** ``dedup_key`` derived from the
  call site's formula:
  ``ingestion:{catalog_id}:{collection_id}:{asset_id}:{generation}``
* a third call with a different ``generation`` (new object version)
  derives a **different** ``dedup_key`` — the per-version contract that
  makes operator re-uploads work.

PR #432 already pinned the downstream behavior (DB-level dedup) using a
synthetic ``task_type`` to defuse the BackgroundRunner race. This test
covers the *upstream* half: the call site correctly extracts
``generation`` from ``gcs_payload`` and threads the right key through
``execute_process``. Together they pin the full path
GCS-event → execute_process → create_task → DB.

Hermetic: providers and the process executor are stubbed, so no DB,
no module bring-up, no runner.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.extensions.gcp import gcp_events
from dynastore.modules.gcp.gcp_config import (
    GcpCollectionBucketConfig,
    GcsNotificationEventType,
    TriggeredAction,
)


def _make_payload(
    *, bucket: str, name: str, asset_id: str, generation: str
) -> Dict[str, Any]:
    return {
        "bucket": bucket,
        "name": name,
        "generation": generation,
        "metadata": {"asset_id": asset_id},
    }


def _ingestion_action() -> TriggeredAction:
    return TriggeredAction(
        process_id="ingestion",
        execute_request_template={
            "asset_id": "{asset_id}",
            "source_uri": "gs://{bucket}/{name}",
        },
    )


def _stub_providers(monkeypatch: pytest.MonkeyPatch, config: GcpCollectionBucketConfig) -> None:
    """Replace ``_get_providers`` with stubs that return the supplied config."""
    fake_configs = MagicMock()

    async def _get_config(cls, *_args, **_kwargs):
        if cls is GcpCollectionBucketConfig:
            return config
        return None

    fake_configs.get_config = AsyncMock(side_effect=_get_config)
    fake_eventing = MagicMock()
    monkeypatch.setattr(
        gcp_events,
        "_get_providers",
        lambda: (MagicMock(), fake_eventing, MagicMock(), fake_configs),
    )


@pytest.mark.asyncio
async def test_trigger_actions_redelivery_threads_same_dedup_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_id = "cat_a"
    collection_id = "col_b"
    asset_id = "asset_42"
    generation = "1715250000000001"
    bucket = "bkt"
    name = f"collections/{collection_id}/{asset_id}.tif"

    config = GcpCollectionBucketConfig(
        event_actions={GcsNotificationEventType.OBJECT_FINALIZE: [_ingestion_action()]},
    )
    _stub_providers(monkeypatch, config)

    captured: List[Tuple[str, Optional[str]]] = []

    async def _fake_execute_process(*, dedup_key: Optional[str] = None, process_id: str, **kwargs):  # noqa: ARG001
        captured.append((process_id, dedup_key))
        return MagicMock()  # truthy → "successfully deferred" log path

    monkeypatch.setattr(
        gcp_events.processes_module, "execute_process", _fake_execute_process
    )
    # Engine is required upstream of execute_process.
    monkeypatch.setattr(gcp_events, "get_engine", lambda: MagicMock())

    payload = _make_payload(
        bucket=bucket, name=name, asset_id=asset_id, generation=generation
    )

    # Two redeliveries of the same OBJECT_FINALIZE.
    for _ in range(2):
        await gcp_events._trigger_configured_actions(
            catalog_id, collection_id, GcsNotificationEventType.OBJECT_FINALIZE, payload
        )

    assert len(captured) == 2, (
        "_trigger_configured_actions itself does not dedup (that contract "
        f"lives in create_task downstream); expected 2 invocations, got {len(captured)}"
    )
    expected_key = f"ingestion:{catalog_id}:{collection_id}:{asset_id}:{generation}"
    assert captured[0] == ("ingestion", expected_key)
    assert captured[1] == ("ingestion", expected_key), (
        "Pub/Sub redelivery of the same OBJECT_FINALIZE must derive the "
        "same dedup_key both times so the downstream create_task pre-check "
        "and DB partial unique index can collapse them."
    )


@pytest.mark.asyncio
async def test_trigger_actions_new_generation_yields_new_dedup_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_id = "cat_a"
    collection_id = "col_b"
    asset_id = "asset_42"
    bucket = "bkt"
    name = f"collections/{collection_id}/{asset_id}.tif"

    config = GcpCollectionBucketConfig(
        event_actions={GcsNotificationEventType.OBJECT_FINALIZE: [_ingestion_action()]},
    )
    _stub_providers(monkeypatch, config)

    captured: List[Optional[str]] = []

    async def _fake_execute_process(*, dedup_key: Optional[str] = None, **kwargs):  # noqa: ARG001
        captured.append(dedup_key)
        return MagicMock()

    monkeypatch.setattr(
        gcp_events.processes_module, "execute_process", _fake_execute_process
    )
    monkeypatch.setattr(gcp_events, "get_engine", lambda: MagicMock())

    gen_a = "1715250000000001"
    gen_b = "1715250000000002"
    await gcp_events._trigger_configured_actions(
        catalog_id,
        collection_id,
        GcsNotificationEventType.OBJECT_FINALIZE,
        _make_payload(bucket=bucket, name=name, asset_id=asset_id, generation=gen_a),
    )
    await gcp_events._trigger_configured_actions(
        catalog_id,
        collection_id,
        GcsNotificationEventType.OBJECT_FINALIZE,
        _make_payload(bucket=bucket, name=name, asset_id=asset_id, generation=gen_b),
    )

    assert captured == [
        f"ingestion:{catalog_id}:{collection_id}:{asset_id}:{gen_a}",
        f"ingestion:{catalog_id}:{collection_id}:{asset_id}:{gen_b}",
    ], (
        "Per-version dedup contract regressed: a new GCS object generation "
        "must produce a fresh dedup_key so operator re-uploads create a new "
        f"task. Got {captured!r}."
    )


@pytest.mark.asyncio
async def test_trigger_actions_missing_generation_falls_through_without_dedup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive fallback documented in gcp_events.py:516-517 — when
    ``generation`` is absent, dedup_key is None and the original
    execute_process path still runs (no key threaded)."""
    catalog_id = "cat_a"
    collection_id = "col_b"
    asset_id = "asset_42"
    config = GcpCollectionBucketConfig(
        event_actions={GcsNotificationEventType.OBJECT_FINALIZE: [_ingestion_action()]},
    )
    _stub_providers(monkeypatch, config)

    captured: List[Optional[str]] = []

    async def _fake_execute_process(*, dedup_key: Optional[str] = None, **kwargs):  # noqa: ARG001
        captured.append(dedup_key)
        return MagicMock()

    monkeypatch.setattr(
        gcp_events.processes_module, "execute_process", _fake_execute_process
    )
    monkeypatch.setattr(gcp_events, "get_engine", lambda: MagicMock())

    payload = {
        "bucket": "bkt",
        "name": f"collections/{collection_id}/{asset_id}.tif",
        # generation deliberately absent
        "metadata": {"asset_id": asset_id},
    }
    await gcp_events._trigger_configured_actions(
        catalog_id, collection_id, GcsNotificationEventType.OBJECT_FINALIZE, payload
    )

    assert captured == [None], (
        "When GCS object generation is missing, the call site must NOT "
        "fabricate a dedup_key (that would conflate distinct uploads); "
        f"got {captured!r}."
    )
