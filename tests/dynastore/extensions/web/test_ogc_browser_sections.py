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

"""Registration + handler smoke tests for the shared-OGC-browser web sections.

Records, MovingFeatures, Coverages, and EDR each contribute a navigation page
and a static-asset prefix via the ``@expose_web_page`` / ``@expose_static``
decorators (mirroring the STAC/Features/Assets sections). These tests assert the
contribution is discoverable (the page id and static prefix surface through
``get_web_pages`` / ``get_static_assets``) and that the page handler returns a
200 HTML response with the ``{{VERSION}}`` template token substituted.

The services are instantiated via ``cls.__new__`` to avoid their full
``__init__``; ``collect_web_pages`` inspects the class MRO and binds methods with
``getattr`` (it never triggers ``@property`` descriptors), so a bare instance is
sufficient.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect

import pytest

# (module path, class name, page id, static prefix, handler name)
SECTIONS = [
    (
        "dynastore.extensions.records.records_service",
        "RecordsService",
        "records_browser",
        "records",
        "provide_records_browser",
    ),
    (
        "dynastore.extensions.moving_features.mf_service",
        "MovingFeaturesService",
        "movingfeatures_browser",
        "movingfeatures",
        "provide_movingfeatures_browser",
    ),
    (
        "dynastore.extensions.coverages.coverages_service",
        "CoveragesService",
        "coverages_browser",
        "coverages",
        "provide_coverages_browser",
    ),
    (
        "dynastore.extensions.edr.edr_service",
        "EDRService",
        "edr_browser",
        "edr",
        "provide_edr_browser",
    ),
]


def _bare_instance(module_path: str, cls_name: str):
    mod = importlib.import_module(module_path)
    cls = getattr(mod, cls_name)
    return cls.__new__(cls)


@pytest.mark.parametrize(
    "module_path, cls_name, page_id, prefix, _handler",
    SECTIONS,
    ids=[s[1] for s in SECTIONS],
)
def test_section_page_and_static_register(module_path, cls_name, page_id, prefix, _handler):
    svc = _bare_instance(module_path, cls_name)

    page_ids = {spec.page_id for spec in svc.get_web_pages()}
    assert page_id in page_ids, f"{cls_name} did not contribute page '{page_id}'"

    prefixes = {asset.prefix.strip("/") for asset in svc.get_static_assets()}
    assert prefix in prefixes, f"{cls_name} did not contribute static prefix '{prefix}'"


@pytest.mark.parametrize(
    "module_path, cls_name, _page_id, _prefix, handler_name",
    SECTIONS,
    ids=[s[1] for s in SECTIONS],
)
def test_section_handler_returns_html(module_path, cls_name, _page_id, _prefix, handler_name):
    svc = _bare_instance(module_path, cls_name)
    handler = getattr(svc, handler_name)

    if "request" in inspect.signature(handler).parameters:
        result = handler(request=None)
    else:
        result = handler()
    if inspect.isawaitable(result):
        result = asyncio.new_event_loop().run_until_complete(result)

    assert getattr(result, "status_code", 200) == 200
    body = result.body.decode() if hasattr(result, "body") else str(result)
    assert "<" in body, "handler did not return HTML"
    assert "{{VERSION}}" not in body, "VERSION template token was not substituted"
