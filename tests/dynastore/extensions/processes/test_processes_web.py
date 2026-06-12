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

"""Web-page registration smoke tests for the Processes Browser page.

Asserts that ProcessesService contributes:
- a web page with id ``processes_browser`` via ``get_web_pages()``
- a static-asset prefix ``processes`` via ``get_static_assets()``
- a page handler that returns HTTP 200 HTML with the ``{{VERSION}}`` token
  substituted
- the HTML and JS reference the schema-form.js and api.js common helpers
  via the correct static-path rule paths (../static/common/)
- the JS imports the execution-related URL helper (apiUrl / url.js)

No browser testing is performed — all assertions are string checks against
the static files.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os

import pytest

MODULE_PATH = "dynastore.extensions.processes.processes_service"
CLASS_NAME = "ProcessesService"
PAGE_ID = "processes_browser"
STATIC_PREFIX = "processes"
HANDLER_NAME = "provide_processes_browser"


def _bare_instance():
    mod = importlib.import_module(MODULE_PATH)
    cls = getattr(mod, CLASS_NAME)
    return cls.__new__(cls)


# ---------------------------------------------------------------------------
# Registration tests
# ---------------------------------------------------------------------------


def test_page_id_registered():
    svc = _bare_instance()
    page_ids = {spec.page_id for spec in svc.get_web_pages()}
    assert PAGE_ID in page_ids, (
        f"{CLASS_NAME} did not contribute page '{PAGE_ID}'; got {page_ids}"
    )


def test_static_prefix_registered():
    svc = _bare_instance()
    prefixes = {asset.prefix.strip("/") for asset in svc.get_static_assets()}
    assert STATIC_PREFIX in prefixes, (
        f"{CLASS_NAME} did not contribute static prefix '{STATIC_PREFIX}'; got {prefixes}"
    )


def test_static_files_non_empty():
    svc = _bare_instance()
    for asset in svc.get_static_assets():
        if asset.prefix.strip("/") == STATIC_PREFIX:
            files = asset.files_provider()
            assert len(files) > 0, "provide_static_files() returned an empty list"
            assert all(os.path.isfile(f) for f in files), (
                "provide_static_files() returned paths that do not exist on disk"
            )
            return
    pytest.fail(f"Static asset provider for prefix '{STATIC_PREFIX}' not found")


# ---------------------------------------------------------------------------
# Handler smoke test
# ---------------------------------------------------------------------------


def test_handler_returns_200_html():
    svc = _bare_instance()
    handler = getattr(svc, HANDLER_NAME)

    if "request" in inspect.signature(handler).parameters:
        result = handler(request=None)
    else:
        result = handler()
    if inspect.isawaitable(result):
        result = asyncio.get_event_loop().run_until_complete(result)

    assert getattr(result, "status_code", 200) == 200
    body = result.body.decode() if hasattr(result, "body") else str(result)
    assert "<" in body, "handler did not return HTML"
    assert "{{VERSION}}" not in body, "VERSION template token was not substituted"


# ---------------------------------------------------------------------------
# Static-path rule: content assertions (no browser)
# ---------------------------------------------------------------------------


def _static_file(filename: str) -> str:
    path = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..", "..",
        "packages", "extensions", "processes", "src",
        "dynastore", "extensions", "processes", "static",
        filename,
    )
    path = os.path.normpath(path)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def test_html_version_token_present():
    """The raw HTML must contain {{VERSION}} so the handler can substitute it."""
    html = _static_file("processes_browser.html")
    assert "{{VERSION}}" in html, (
        "processes_browser.html is missing the {{VERSION}} template token"
    )


def test_html_references_own_js_as_module():
    html = _static_file("processes_browser.html")
    assert "../processes/processes_browser.js" in html, (
        "HTML does not load processes_browser.js via the correct ../processes/ path"
    )
    assert 'type="module"' in html, (
        "processes_browser.js must be loaded as an ES module"
    )


def test_html_references_common_assets():
    html = _static_file("processes_browser.html")
    assert "../static/common/admin.css" in html, (
        "HTML does not reference admin.css via ../static/common/"
    )
    assert "../static/common/page-shell.css" in html, (
        "HTML does not reference page-shell.css via ../static/common/"
    )


def test_js_imports_common_api():
    js = _static_file("processes_browser.js")
    assert 'from "../static/common/api.js"' in js, (
        "processes_browser.js does not import api.js via ../static/common/"
    )


def test_js_imports_schema_form():
    js = _static_file("processes_browser.js")
    assert 'from "../static/common/schema-form.js"' in js, (
        "processes_browser.js does not import schema-form.js via ../static/common/"
    )


def test_js_imports_url():
    js = _static_file("processes_browser.js")
    assert 'from "../static/common/url.js"' in js, (
        "processes_browser.js does not import url.js (needed for apiUrl in raw fetch)"
    )


def test_js_imports_i18n():
    js = _static_file("processes_browser.js")
    assert 'from "../static/common/i18n.js"' in js, (
        "processes_browser.js does not import i18n.js via ../static/common/"
    )


def test_js_has_execution_endpoint():
    js = _static_file("processes_browser.js")
    assert "/execution" in js, (
        "processes_browser.js does not reference an OGC execution endpoint path"
    )


def test_js_has_job_polling():
    js = _static_file("processes_browser.js")
    assert "pollJob" in js, (
        "processes_browser.js does not define a job polling function"
    )


def test_js_has_prefer_header():
    js = _static_file("processes_browser.js")
    assert "respond-async" in js, (
        "processes_browser.js does not set the Prefer: respond-async header for async execution"
    )


def test_js_has_403_handling():
    js = _static_file("processes_browser.js")
    assert "403" in js, (
        "processes_browser.js does not handle 403 Forbidden from the execution endpoint"
    )
