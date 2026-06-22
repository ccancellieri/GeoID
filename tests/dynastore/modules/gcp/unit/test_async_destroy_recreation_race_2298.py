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

"""Recreation-race guard for ``GcpCatalogOpsMixin._on_async_destroy_catalog``.

A catalog hard-deleted via ``DELETE /stac/catalogs/{id}?force=true`` fires an
un-awaited background teardown. If the same id is recreated before that task
runs, two deterministic-name resources collide:

* The default Pub/Sub topic ``ds-{catalog_id}-events`` is keyed on catalog_id
  only, so the new catalog adopts the exact same topic. The stale teardown must
  NOT delete it, or the live catalog loses its eventing channel.
* The bucket name embeds ``physical_schema``, so the old, orphaned bucket has a
  name distinct from the new catalog's. Teardown must target the OLD bucket by
  its schema-derived name, never a catalog_id-keyed DB lookup (which now returns
  the NEW catalog's bucket).

Detection: compare the schema captured at delete time (``context.physical_schema``)
against the schema currently registered for the id. A mismatch ⇒ recreated.
"""

from __future__ import annotations

from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

import dynastore.modules.gcp.gcp_catalog_ops as ops_mod
from dynastore.modules.gcp.gcp_catalog_ops import GcpCatalogOpsMixin
from dynastore.modules.gcp.gcp_config import (
    GcpCatalogBucketConfig,
    GcpEventingConfig,
    ManagedBucketEventing,
)
from dynastore.modules.catalog.lifecycle_manager import LifecycleContext


OLD_SCHEMA = "s_old1111"
NEW_SCHEMA = "s_new2222"
# What the host stub's generate_bucket_name() reconstructs from OLD_SCHEMA.
DERIVED_OLD_BUCKET = "proj-s-old1111"


class _Host(GcpCatalogOpsMixin):
    """Concrete host exposing only the collaborators ``_on_async_destroy_catalog``
    invokes."""

    def __init__(self) -> None:
        self.teardown_managed_eventing_channel = AsyncMock(return_value=None)
        self.teardown_catalog_eventing = AsyncMock(return_value=None)

        self._bucket_svc = MagicMock()
        # Deterministic, schema-embedding name (mirrors BucketService).
        self._bucket_svc.generate_bucket_name = MagicMock(
            side_effect=lambda cid, physical_schema=None: (
                f"proj-{(physical_schema or cid).replace('_', '-')}"
            )
        )
        self._bucket_svc.drop_storage = AsyncMock(return_value=True)
        # If teardown ever resolves the bucket name via the DB it would get the
        # NEW catalog's bucket — this must never be consulted.
        self._bucket_svc.get_storage_identifier = AsyncMock(
            return_value="proj-s-new2222"
        )

    def get_bucket_service(self) -> Any:
        return self._bucket_svc


def _eventing_context(persisted_bucket: Optional[str] = None) -> LifecycleContext:
    cfg = GcpEventingConfig(managed_eventing=ManagedBucketEventing(enabled=True))
    snapshot = {GcpEventingConfig.class_key(): cfg.model_dump(mode="json")}
    if persisted_bucket is not None:
        # Mirror the pre-deletion config snapshot carrying the authoritative
        # (possibly legacy-named) bucket name.
        snapshot[GcpCatalogBucketConfig.class_key()] = {"bucket_name": persisted_bucket}
    return LifecycleContext(physical_schema=OLD_SCHEMA, config=snapshot)


@pytest.fixture(autouse=True)
def _silence_logs(monkeypatch):
    """Lifecycle log writes hit the DB; stub them for unit isolation."""
    monkeypatch.setattr(ops_mod, "log_info", AsyncMock())
    monkeypatch.setattr(ops_mod, "log_warning", AsyncMock())
    monkeypatch.setattr(ops_mod, "log_error", AsyncMock())


def _patch_current_schema(monkeypatch, current: Optional[str]) -> None:
    """Make the recreation probe (``get_protocol(CatalogsProtocol)``) report
    ``current`` as the schema now registered for the catalog id."""
    catalogs_svc = MagicMock()
    catalogs_svc.resolve_physical_schema = AsyncMock(return_value=current)
    monkeypatch.setattr(ops_mod, "get_protocol", lambda _proto: catalogs_svc)


@pytest.mark.asyncio
async def test_recreation_preserves_eventing_and_targets_old_bucket(monkeypatch):
    """Catalog recreated under a NEW schema: eventing teardown is skipped (the
    deterministic topic now belongs to the live catalog) and only the OLD,
    schema-named bucket is removed."""
    host = _Host()
    _patch_current_schema(monkeypatch, NEW_SCHEMA)

    await host._on_async_destroy_catalog("cat", _eventing_context())

    # Eventing is protected — neither the managed channel nor the default
    # topic/subscription cleanup may run.
    host.teardown_managed_eventing_channel.assert_not_called()
    host.teardown_catalog_eventing.assert_not_called()

    # The old bucket is targeted explicitly (schema-derived here, since the
    # snapshot carried no persisted name), never by a catalog_id DB lookup.
    host._bucket_svc.drop_storage.assert_awaited_once_with(
        "cat", physical_schema=OLD_SCHEMA, bucket_name=DERIVED_OLD_BUCKET
    )
    host._bucket_svc.get_storage_identifier.assert_not_called()


@pytest.mark.asyncio
async def test_recreation_uses_persisted_bucket_name_from_snapshot(monkeypatch):
    """When the pre-deletion config snapshot carries the authoritative bucket
    name (the legacy/general case where the name may not embed the schema), that
    exact name is the deletion target — not a reconstructed one."""
    host = _Host()
    _patch_current_schema(monkeypatch, NEW_SCHEMA)
    legacy_name = "proj-legacy-cat-name"

    await host._on_async_destroy_catalog("cat", _eventing_context(legacy_name))

    host.teardown_catalog_eventing.assert_not_called()
    host._bucket_svc.drop_storage.assert_awaited_once_with(
        "cat", physical_schema=OLD_SCHEMA, bucket_name=legacy_name
    )
    host._bucket_svc.get_storage_identifier.assert_not_called()


@pytest.mark.asyncio
async def test_no_recreation_runs_full_teardown(monkeypatch):
    """Catalog truly gone (no current schema): full teardown runs — managed
    eventing + default eventing cleanup — and the bucket is still targeted by the
    deleted catalog's schema."""
    host = _Host()
    _patch_current_schema(monkeypatch, None)

    await host._on_async_destroy_catalog("cat", _eventing_context())

    host.teardown_managed_eventing_channel.assert_awaited_once()
    host.teardown_catalog_eventing.assert_awaited_once()
    host._bucket_svc.drop_storage.assert_awaited_once_with(
        "cat", physical_schema=OLD_SCHEMA, bucket_name=DERIVED_OLD_BUCKET
    )
    host._bucket_svc.get_storage_identifier.assert_not_called()


@pytest.mark.asyncio
async def test_same_schema_is_not_treated_as_recreation(monkeypatch):
    """Defensive: if the id still resolves to the SAME schema it is the same
    instance, not a recreation — full teardown proceeds."""
    host = _Host()
    _patch_current_schema(monkeypatch, OLD_SCHEMA)

    await host._on_async_destroy_catalog("cat", _eventing_context())

    host.teardown_managed_eventing_channel.assert_awaited_once()
    host.teardown_catalog_eventing.assert_awaited_once()
