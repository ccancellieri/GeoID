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

"""Cache-busting and the canonical no-cache policy for web shell entry-points.

Shell HTML is served ``no-cache`` so it always revalidates, while its
long-cached JS/CSS are busted with a ``?v=<token>`` query derived from the
assets' modification time. A package-version token would not change between two
same-version redeploys; the mtime token does. These tests guard both halves.
"""

import os
from pathlib import Path

import pytest

# File lives at packages/extensions/web/tests/unit/test_*.py
_STATIC = (
    Path(__file__).parents[3]
    / "web" / "src" / "dynastore" / "extensions" / "web" / "static"
)

# Shell page -> the asset hrefs/srcs it references (as written in the markup).
_SHELLS = {
    "website/index.html": (
        "static/custom.js",
        "static/common/tailwind.css",
        "static/vendor/vendor.css",
    ),
    "dashboard/index.html": ("../static/common/admin.css", "styles.css"),
    "dashboard/processes.html": ("../static/common/admin.css", "styles.css"),
}


@pytest.mark.parametrize("page,assets", list(_SHELLS.items()))
def test_shell_assets_are_cache_busted(page, assets):
    """Every long-cached asset a shell references must carry the ?v token."""
    html = (_STATIC / page).read_text(encoding="utf-8")
    for asset in assets:
        needle = asset + "?v={{ASSET_V}}"
        assert needle in html, (
            f"{page} references {asset!r} without the cache-busting token "
            f"{needle!r}; stale JS/CSS will survive a deploy for up to a day."
        )


def test_asset_version_token_uses_newest_mtime(tmp_path):
    from dynastore.extensions.web.web import Web

    older = tmp_path / "a.js"
    older.write_text("x")
    os.utime(older, (1000, 1000))
    newer = tmp_path / "b.css"
    newer.write_text("y")
    os.utime(newer, (2000, 2000))

    assert Web._asset_version_token([str(older), str(newer)]) == "2000"


def test_asset_version_token_falls_back_to_version():
    from dynastore._version import VERSION
    from dynastore.extensions.web.web import Web

    assert Web._asset_version_token([]) == VERSION
    assert Web._asset_version_token(["/no/such/file.js"]) == VERSION


def test_html_entry_points_revalidate():
    """HTML shells get no-cache so a freshly stamped ?v token is seen at once."""
    from dynastore.extensions.web.web import Web

    assert Web._cache_headers_for("static", "index.html") == {
        "Cache-Control": "no-cache, must-revalidate"
    }
