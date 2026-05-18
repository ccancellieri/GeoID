"""Regression test for #935.

The logs-dashboard page used origin-relative paths (``/logs/_dashboards_health``
/ ``_dashboards_config``) without the active FastAPI ``root_path`` prefix.
When the API was mounted under e.g. ``/geospatial/v2/api``, the in-browser
fetches 404'd, the JS fell into its silent catch block, ``health`` stayed at
the default-all-False shape, and the page falsely reported
*"Elasticsearch is not reachable from the geoid service"*.

This test pins the contract: the served HTML must substitute the
``__API_PREFIX__`` placeholder with the active ``root_path``.
"""

import pytest


class _StubScope(dict):
    pass


class _StubRequest:
    def __init__(self, root_path: str):
        self.scope = _StubScope(root_path=root_path)


@pytest.mark.asyncio
async def test_logs_dashboard_page_injects_root_path_prefix():
    from dynastore.extensions.logs.log_extension import LogExtension

    ext = LogExtension()
    request = _StubRequest("/geospatial/v2/api")
    response = await ext.provide_logs_dashboard_page(request)

    body = response.body.decode("utf-8")
    assert "__API_PREFIX__" not in body, (
        "Placeholder should be replaced before reaching the browser"
    )
    assert "const API_PREFIX = '/geospatial/v2/api'" in body, (
        "Active root_path must be injected so client-side fetches hit the API mount"
    )


@pytest.mark.asyncio
async def test_logs_dashboard_page_empty_prefix_at_root_mount():
    from dynastore.extensions.logs.log_extension import LogExtension

    ext = LogExtension()
    request = _StubRequest("")
    response = await ext.provide_logs_dashboard_page(request)

    body = response.body.decode("utf-8")
    assert "__API_PREFIX__" not in body
    assert "const API_PREFIX = ''" in body, (
        "Empty root_path collapses to '' so fetches resolve correctly at origin root"
    )


@pytest.mark.asyncio
async def test_logs_dashboard_page_strips_trailing_slash():
    from dynastore.extensions.logs.log_extension import LogExtension

    ext = LogExtension()
    request = _StubRequest("/geospatial/v2/api/")
    response = await ext.provide_logs_dashboard_page(request)

    body = response.body.decode("utf-8")
    # No double-slash from trailing root_path + leading-slash in the fetch path.
    assert "const API_PREFIX = '/geospatial/v2/api'" in body
