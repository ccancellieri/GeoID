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

"""Integration tests for configs extension static asset serving.

Verifies that:
1. Static files (configuration.js, configuration.html) are served correctly
2. No trailing slash redirects occur for static file paths
3. Files are served with correct content types
"""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create a test client with the web and configs extensions loaded."""
    from dynastore.main import create_app
    app = create_app()
    return TestClient(app, raise_server_exceptions=False)


def test_configuration_js_serves_correctly(client: TestClient):
    """Test that configuration.js is served as a JavaScript file."""
    response = client.get("/web/static/configs/configuration.js")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    assert "javascript" in response.headers.get("content-type", "").lower(), \
        f"Expected JavaScript content type, got {response.headers.get('content-type')}"
    assert b"configuration.js" in response.content or b"function" in response.content, \
        "configuration.js content should contain JavaScript code"


def test_configuration_html_serves_correctly(client: TestClient):
    """Test that configuration.html is served without trailing slash redirect."""
    response = client.get("/web/static/configs/configuration.html")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    assert "html" in response.headers.get("content-type", "").lower(), \
        f"Expected HTML content type, got {response.headers.get('content-type')}"
    assert b"<!DOCTYPE html>" in response.content or b"<html" in response.content, \
        "configuration.html content should be valid HTML"


def test_configuration_html_no_redirect(client: TestClient):
    """Test that configuration.html does NOT redirect to configuration.html/."""
    response = client.get("/web/static/configs/configuration.html", allow_redirects=False)
    assert response.status_code == 200, \
        f"Expected 200 (no redirect), got {response.status_code}. Location: {response.headers.get('location')}"
    assert "location" not in response.headers, \
        f"Unexpected redirect to: {response.headers.get('location')}"


def test_presets_js_serves_correctly(client: TestClient):
    """Test that presets.js is served correctly."""
    response = client.get("/web/static/configs/presets.js")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    assert "javascript" in response.headers.get("content-type", "").lower(), \
        f"Expected JavaScript content type, got {response.headers.get('content-type')}"


def test_presets_html_serves_correctly(client: TestClient):
    """Test that presets.html is served without trailing slash redirect."""
    response = client.get("/web/static/configs/presets.html", allow_redirects=False)
    assert response.status_code == 200, \
        f"Expected 200 (no redirect), got {response.status_code}. Location: {response.headers.get('location')}"
    assert "html" in response.headers.get("content-type", "").lower(), \
        f"Expected HTML content type, got {response.headers.get('content-type')}"


def test_nonexistent_static_file_returns_404(client: TestClient):
    """Test that requesting a non-existent static file returns 404."""
    response = client.get("/web/static/configs/nonexistent.js")
    assert response.status_code == 404, f"Expected 404, got {response.status_code}"


def test_configs_static_prefix_registered(client: TestClient):
    """Test that the configs static prefix is registered."""
    response = client.get("/web/config/static-prefixes")
    assert response.status_code == 200
    data = response.json()
    prefixes = [p["prefix"] for p in data]
    assert "configs" in prefixes, f"'configs' prefix not found in {prefixes}"
