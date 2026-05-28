#    Copyright 2025 FAO
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

"""Unit tests for the documentation help-box.

Exercises the help-box injected by ``enrich_extension_metadata`` (which links to
the embeddable ``/extension-docs/{name}`` route). No DB, no external HTTP.
"""

from dynastore.extensions.documentation.service import _render_help_box


def test_render_help_box_contains_marker_and_url():
    html = _render_help_box("/extension-docs/catalog")

    # ``help-box`` is the idempotency marker relied on by enrich_extension_metadata.
    assert 'class="help-box"' in html
    assert 'href="/extension-docs/catalog"' in html
    assert "<b>Documentation</b>" in html
