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

"""Pin the catalog post-create enqueue decision for GCP bucket provisioning.

``GCPModule._on_post_create_catalog`` is the post-INSERT lifecycle hook that, for
``provision_enabled=True`` catalogs, must enqueue a ``gcp_provision_catalog`` task
(the async worker that creates the GCS bucket and marks the ``gcp_bucket``
checklist step terminal). When ``provision_enabled=False`` it must NOT enqueue —
it links the deterministic bucket name and marks the catalog ready inline.

These tests drive the hook directly with a stubbed config service and a captured
``create_task`` so the enqueue decision is verified at the seam, independent of a
live database / GCP credentials (the integration suite needs both).
"""
from __future__ import annotations

from typing import Any, List, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.gcp.gcp_catalog_ops import GcpCatalogOpsMixin
from dynastore.modules.gcp.gcp_config import GcpCatalogBucketConfig


class _Host(GcpCatalogOpsMixin):
    """Minimal host exposing only what ``_on_post_create_catalog`` touches."""

    def __init__(self) -> None:
        self._bucket_service = MagicMock()
        self._bucket_service.generate_bucket_name.return_value = "bkt-cat1"

    def get_bucket_service(self) -> Any:  # type: ignore[override]
        return self._bucket_service


@pytest.fixture
def captured_tasks(monkeypatch) -> List[Tuple[Any, Any, Any]]:
    """Patch the symbols the hook resolves at call time and capture enqueues."""
    calls: List[Tuple[Any, Any, Any]] = []

    async def _fake_create_task(conn, task_request, physical_schema):
        calls.append((conn, task_request, physical_schema))
        return MagicMock()

    import dynastore.modules.tasks.tasks_module as tasks_module

    monkeypatch.setattr(tasks_module, "create_task", _fake_create_task)
    return calls


def _stub_config_service(monkeypatch, *, provision_enabled: bool) -> None:
    """Make ``get_protocol(ConfigsProtocol)`` (and CatalogsProtocol) resolvable
    with the desired ``provision_enabled``."""
    cfg = GcpCatalogBucketConfig(provision_enabled=provision_enabled)

    config_mgr = MagicMock()
    config_mgr.get_config = AsyncMock(return_value=cfg)

    catalogs_svc = MagicMock()
    catalogs_svc.update_provisioning_status = AsyncMock(return_value=True)

    from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol

    def _fake_get_protocol(proto, *a, **kw):
        if proto is ConfigsProtocol:
            return config_mgr
        if proto is CatalogsProtocol:
            return catalogs_svc
        return None

    # The hook resolves ConfigsProtocol via the module-level ``get_protocol``
    # imported into gcp_catalog_ops, and CatalogsProtocol/log helpers via lazy
    # imports off the same discovery module.
    import dynastore.modules.gcp.gcp_catalog_ops as ops
    monkeypatch.setattr(ops, "get_protocol", _fake_get_protocol)

    # ``DriverContext`` validates ``db_resource`` against the real DB handle
    # types; the hook wraps the connection in one purely to pass it through to
    # the config service. Stub it to a passthrough so a mock connection works.
    class _PassthroughCtx:
        def __init__(self, db_resource=None, **_: Any) -> None:
            self.db_resource = db_resource

    monkeypatch.setattr(ops, "DriverContext", _PassthroughCtx)

    import dynastore.tools.discovery as discovery
    monkeypatch.setattr(discovery, "get_protocol", _fake_get_protocol)

    # The provision_enabled=False branch now persists the bucket name via
    # config_mgr.set_config rather than a direct SQL link query.
    config_mgr.set_config = AsyncMock(return_value=None)

    import dynastore.modules.catalog.log_manager as log_manager
    monkeypatch.setattr(log_manager, "log_info", AsyncMock(return_value=None))
    monkeypatch.setattr(log_manager, "log_error", AsyncMock(return_value=None))


@pytest.mark.asyncio
async def test_enqueues_provision_task_when_provision_enabled(
    monkeypatch, captured_tasks
):
    _stub_config_service(monkeypatch, provision_enabled=True)
    host = _Host()
    conn = MagicMock()

    await host._on_post_create_catalog(conn, "phys_s", "cat1")

    assert len(captured_tasks) == 1, (
        "provision_enabled=True must enqueue exactly one gcp_provision_catalog "
        f"task; got {len(captured_tasks)} enqueues."
    )
    _conn, task_request, physical_schema = captured_tasks[0]
    assert task_request.task_type == "gcp_provision_catalog"
    assert task_request.inputs == {"catalog_id": "cat1"}
    assert physical_schema == "phys_s"


@pytest.mark.asyncio
async def test_does_not_enqueue_when_provision_disabled(
    monkeypatch, captured_tasks
):
    _stub_config_service(monkeypatch, provision_enabled=False)
    host = _Host()
    conn = MagicMock()

    await host._on_post_create_catalog(conn, "phys_s", "cat1")

    assert captured_tasks == [], (
        "provision_enabled=False must NOT enqueue a provision task (it links the "
        "bucket name and marks the catalog ready inline)."
    )
