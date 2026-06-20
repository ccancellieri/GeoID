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

"""Unit tests for GCPModule.get_self_url() push-endpoint URL resolution.

Regression guard for the bug where a dev catalog's push subscription was wired
to the prod service URL because SERVICE_URL was read before K_SERVICE, causing
GCS OBJECT_FINALIZE events to be delivered to prod instead of dev and leaving
dev assets permanently PENDING.

Covered invariants:
1. When K_SERVICE is set (Cloud Run), the discovered URI from the Admin API is
   returned — SERVICE_URL env var is not consulted at all.
2. When K_SERVICE is set and the Admin API call fails, RuntimeError is raised
   (no silent fallback to SERVICE_URL which could point at the prod service).
3. When K_SERVICE is absent (local/test), SERVICE_URL is accepted.
4. When K_SERVICE is absent and SERVICE_URL is also absent, RuntimeError is
   raised immediately.
5. The push_endpoint assembled by setup_push_subscription uses the dev URL,
   not the prod URL, even when SERVICE_URL is set to prod in the environment.
"""
from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gcp_module(
    k_service: str | None,
    project_id: str = "my-project",
    region: str = "europe-west1",
):
    """Return a GCPModule stub with the minimum surface for get_self_url tests."""
    from dynastore.modules.gcp.gcp_module import GCPModule

    with patch.object(GCPModule, "__init__", lambda self: None):
        module = GCPModule.__new__(GCPModule)

    module._module_config = None
    module._run_client = None
    module.get_service_name = lambda: k_service
    module.get_project_id = lambda: project_id
    module.get_region = lambda: region

    return module


@pytest.fixture(autouse=True)
def _clear_self_url_cache():
    """Clear the get_self_url cache before and after each test.

    get_self_url is decorated with @cached(maxsize=1) — a module-level cache
    shared across test instances. Without clearing it, earlier test results
    bleed into later tests through the single LRU slot.
    """
    from dynastore.modules.gcp.gcp_module import GCPModule

    GCPModule.get_self_url.cache_clear()
    yield
    GCPModule.get_self_url.cache_clear()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cloud_run_uses_admin_api_not_service_url_env():
    """K_SERVICE set → Admin API URI is returned; SERVICE_URL is not consulted.

    This is the core regression test: even if SERVICE_URL is set to the prod URL
    (e.g. leftover from a mis-deploy), the Cloud Run Admin API result must win.
    """
    module = _make_gcp_module(k_service="dev-dynastore-catalog")
    dev_url = "https://dev-dynastore-catalog-abcdef-ew.a.run.app"
    prod_url = "https://dynastore-catalog-w5jfih2xxa-ew.a.run.app"

    mock_run_client = AsyncMock()
    mock_run_client.service_path.return_value = (
        "projects/my-project/locations/europe-west1/services/dev-dynastore-catalog"
    )
    mock_run_client.get_service = AsyncMock(return_value=SimpleNamespace(uri=dev_url))
    module.get_run_client = lambda: mock_run_client

    with patch.dict(os.environ, {"SERVICE_URL": prod_url}):
        url = await module.get_self_url()

    assert url == dev_url, (
        f"Expected dev URL '{dev_url}', got '{url}'. "
        f"SERVICE_URL='{prod_url}' must not override Cloud Run Admin API discovery."
    )
    mock_run_client.get_service.assert_awaited_once()


@pytest.mark.asyncio
async def test_cloud_run_admin_api_failure_raises_not_falls_back_to_service_url():
    """K_SERVICE set but Admin API fails → RuntimeError, not silent fallback.

    Falling back to SERVICE_URL is dangerous: if it holds the prod URL, newly
    created push subscriptions would be wired to prod. Failing loudly causes the
    provisioning task to retry instead of committing a bad endpoint.
    """
    module = _make_gcp_module(k_service="dev-dynastore-catalog")
    prod_url = "https://dynastore-catalog-w5jfih2xxa-ew.a.run.app"

    mock_run_client = AsyncMock()
    mock_run_client.service_path.return_value = (
        "projects/my-project/locations/europe-west1/services/dev-dynastore-catalog"
    )
    mock_run_client.get_service = AsyncMock(
        side_effect=PermissionError("run.services.get denied")
    )
    module.get_run_client = lambda: mock_run_client

    with patch.dict(os.environ, {"SERVICE_URL": prod_url}):
        with pytest.raises(RuntimeError, match="dev-dynastore-catalog"):
            await module.get_self_url()


@pytest.mark.asyncio
async def test_local_env_without_k_service_uses_service_url():
    """K_SERVICE absent → SERVICE_URL is accepted (local dev / CI path)."""
    module = _make_gcp_module(k_service=None)
    local_url = "http://localhost:8000"

    env = {k: v for k, v in os.environ.items() if k != "SERVICE_URL"}
    env["SERVICE_URL"] = local_url
    with patch.dict(os.environ, env, clear=True):
        url = await module.get_self_url()

    assert url == local_url


@pytest.mark.asyncio
async def test_local_env_without_k_service_or_service_url_raises():
    """Neither K_SERVICE nor SERVICE_URL set → RuntimeError immediately."""
    module = _make_gcp_module(k_service=None)

    env = {k: v for k, v in os.environ.items() if k not in ("SERVICE_URL", "K_SERVICE")}
    with patch.dict(os.environ, env, clear=True):
        with pytest.raises(RuntimeError, match="K_SERVICE"):
            await module.get_self_url()


@pytest.mark.asyncio
async def test_setup_push_subscription_uses_dev_url_not_service_url_env():
    """Regression: push_endpoint in the PushConfig must use the dev service host.

    Mirrors the exact failure observed with ds-gaul-default-sub where the push
    subscription was created pointing at the prod endpoint because SERVICE_URL
    was evaluated before K_SERVICE in the old code path.
    """
    from dynastore.modules.gcp.gcp_eventing_ops import GcpEventingOpsMixin
    from dynastore.modules.gcp.models import PushSubscriptionConfig
    import dynastore.modules.gcp.gcp_eventing_ops as ops_mod

    dev_url = "https://dev-dynastore-catalog-abcdef-ew.a.run.app"
    prod_url = "https://dynastore-catalog-w5jfih2xxa-ew.a.run.app"

    class _Stub(GcpEventingOpsMixin):
        def get_project_id(self):
            return "my-project"

        def get_region(self):
            return "europe-west1"

        def get_account_email(self):
            return "svc@my-project.iam.gserviceaccount.com"

        async def get_self_url(self):
            # Simulates the Admin API returning the correct dev URL after the fix.
            return dev_url

        def get_publisher_client(self):
            return MagicMock()

        def get_subscriber_client(self):
            sub = MagicMock()
            sub.subscription_path.return_value = (
                "projects/my-project/subscriptions/ds-gaul-default-sub"
            )
            sub.create_subscription.return_value = MagicMock()
            return sub

        def get_storage_client(self):
            return MagicMock()

        def get_bucket_service(self):
            return MagicMock()

    stub = _Stub()

    # Capture the push_endpoint argument passed to PushConfig construction.
    captured_endpoints: list[str] = []

    mock_pubsub = MagicMock()

    def _capture_push_config_call(**kwargs):
        ep = kwargs.get("push_endpoint", "")
        captured_endpoints.append(ep)
        return MagicMock()

    mock_pubsub.types.PushConfig.side_effect = lambda **kw: _capture_push_config_call(**kw)
    mock_pubsub.types.PushConfig.OidcToken.return_value = MagicMock()

    async def _immediate_run_in_thread(fn, *args, **kw):
        return fn(*args, **kw)

    with (
        patch.object(ops_mod, "pubsub_v1", mock_pubsub),
        patch.object(ops_mod, "run_in_thread", _immediate_run_in_thread),
        patch.dict(os.environ, {"SERVICE_URL": prod_url}),
    ):
        sub_cfg = PushSubscriptionConfig(
            subscription_id="ds-gaul-default-sub", push_endpoint=None
        )
        await stub.setup_push_subscription(
            "projects/my-project/topics/ds-gaul-events",
            sub_cfg,
            custom_attributes={},
        )

    assert len(captured_endpoints) == 1, (
        f"Expected exactly one PushConfig construction, got {len(captured_endpoints)}."
    )
    endpoint = captured_endpoints[0]
    assert "dev-dynastore-catalog" in endpoint, (
        f"Push endpoint '{endpoint}' must contain the dev service host. "
        f"Prod URL '{prod_url}' must not appear in dev subscriptions."
    )
    assert "dynastore-catalog-w5jfih2xxa" not in endpoint, (
        f"Push endpoint '{endpoint}' must not contain the prod service hostname."
    )
    assert endpoint == f"{dev_url}/gcp/events/pubsub-push", (
        f"Expected '{dev_url}/gcp/events/pubsub-push', got '{endpoint}'."
    )
