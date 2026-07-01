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

"""Unit tests for ``GCPModule``'s ``PlatformScalingProtocol`` implementation.

Stubs the module-level ``run_v2`` name with a minimal fake (this dev
environment does not have ``google-cloud-run`` installed — see the module's
conditional import) so the tests exercise ``set_min_instances`` /
``get_min_instances``'s own logic without depending on the real proto
package being present.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


class _FakeServiceScaling:
    def __init__(self, min_instance_count=None):
        self.min_instance_count = min_instance_count


class _FakeService:
    def __init__(self, name=None, scaling=None, template=None):
        self.name = name
        self.scaling = scaling
        self.template = template

    def HasField(self, field_name: str) -> bool:
        return getattr(self, field_name, None) is not None


class _FakeUpdateServiceRequest:
    def __init__(self, service=None, update_mask=None):
        self.service = service
        self.update_mask = update_mask


_FAKE_RUN_V2 = SimpleNamespace(
    Service=_FakeService,
    ServiceScaling=_FakeServiceScaling,
    UpdateServiceRequest=_FakeUpdateServiceRequest,
)


@pytest.fixture(autouse=True)
def disable_managed_eventing():
    """Neutralize the DB-bound autouse fixture from gcp/conftest.py — these
    tests are pure in-memory GCPModule stub construction."""
    return None


@pytest.fixture(autouse=True)
def _fake_run_v2(monkeypatch):
    from dynastore.modules.gcp import gcp_module

    monkeypatch.setattr(gcp_module, "run_v2", _FAKE_RUN_V2)


def _make_gcp_module(
    *,
    has_credentials: bool = True,
    project_id: str = "my-project",
    region: str = "europe-west1",
    service_name: str = "my-service",
):
    from dynastore.modules.gcp.gcp_module import GCPModule

    with patch.object(GCPModule, "__init__", lambda self: None):
        module = GCPModule.__new__(GCPModule)

    module._credentials = object() if has_credentials else None
    module.get_project_id = lambda: project_id
    module.get_region = lambda: region
    module.get_service_name = lambda: service_name
    return module


# --- set_min_instances -------------------------------------------------------


@pytest.mark.asyncio
async def test_set_min_instances_sends_minimal_update_mask():
    module = _make_gcp_module()
    mock_client = AsyncMock()
    mock_client.service_path.return_value = (
        "projects/my-project/locations/europe-west1/services/my-service"
    )
    module.get_run_client = lambda: mock_client

    await module.set_min_instances(3)

    mock_client.update_service.assert_awaited_once()
    request = mock_client.update_service.await_args.kwargs["request"]
    assert list(request.update_mask.paths) == ["scaling.min_instance_count"]
    assert request.service.scaling.min_instance_count == 3
    assert not request.service.HasField("template"), (
        "must never touch template.* — that rolls a new revision (cold start)"
    )


@pytest.mark.asyncio
async def test_set_min_instances_noop_without_credentials():
    module = _make_gcp_module(has_credentials=False)
    mock_client = AsyncMock()
    module.get_run_client = lambda: mock_client

    await module.set_min_instances(3)

    mock_client.update_service.assert_not_awaited()


@pytest.mark.asyncio
async def test_set_min_instances_noop_when_not_a_named_service():
    module = _make_gcp_module(service_name=None)
    mock_client = AsyncMock()
    module.get_run_client = lambda: mock_client

    await module.set_min_instances(3)

    mock_client.update_service.assert_not_awaited()


@pytest.mark.asyncio
async def test_set_min_instances_swallows_update_service_failure():
    module = _make_gcp_module()
    mock_client = AsyncMock()
    mock_client.service_path.return_value = (
        "projects/my-project/locations/europe-west1/services/my-service"
    )
    mock_client.update_service = AsyncMock(side_effect=RuntimeError("boom"))
    module.get_run_client = lambda: mock_client

    # Must not raise — actuation failures must never abort the reconciler tick.
    await module.set_min_instances(3)


# --- get_min_instances -------------------------------------------------------


@pytest.mark.asyncio
async def test_get_min_instances_reads_current_value():
    module = _make_gcp_module()
    mock_client = AsyncMock()
    mock_client.service_path.return_value = (
        "projects/my-project/locations/europe-west1/services/my-service"
    )
    mock_client.get_service = AsyncMock(
        return_value=SimpleNamespace(scaling=SimpleNamespace(min_instance_count=4))
    )
    module.get_run_client = lambda: mock_client

    result = await module.get_min_instances()

    assert result == 4


@pytest.mark.asyncio
async def test_get_min_instances_none_without_credentials():
    module = _make_gcp_module(has_credentials=False)
    mock_client = AsyncMock()
    module.get_run_client = lambda: mock_client

    result = await module.get_min_instances()

    assert result is None
    mock_client.get_service.assert_not_awaited()
