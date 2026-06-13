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

"""Supply-chain guard: no shipped HTML page may load Tailwind from the CDN.

The compiled stylesheet at static/common/tailwind.css replaces the runtime
CDN dependency. These tests enforce that replacement is permanent.

Refs #2043
"""

from pathlib import Path

# Root of the packages/extensions tree.
# File lives at: packages/extensions/web/tests/unit/test_*.py
# parents: [0]=unit, [1]=tests, [2]=web, [3]=extensions, [4]=packages
_EXTENSIONS_ROOT = Path(__file__).parents[3]
assert _EXTENSIONS_ROOT.name == "extensions", (
    f"Unexpected path: {_EXTENSIONS_ROOT}; check test file location"
)

_TAILWIND_CSS = (
    _EXTENSIONS_ROOT
    / "web"
    / "src"
    / "dynastore"
    / "extensions"
    / "web"
    / "static"
    / "common"
    / "tailwind.css"
)

CDN_MARKER = "cdn.tailwindcss.com"


def _all_html_files():
    return list(_EXTENSIONS_ROOT.rglob("*.html"))


def test_compiled_tailwind_css_exists_and_is_nontrivial():
    """tailwind.css must be committed and contain real CSS (> 1 KB)."""
    assert _TAILWIND_CSS.exists(), (
        f"Compiled stylesheet not found at {_TAILWIND_CSS}. "
        "Run: cd packages/extensions/web/src/dynastore/extensions/web/static "
        "&& npm run build:css"
    )
    size = _TAILWIND_CSS.stat().st_size
    assert size > 1024, (
        f"tailwind.css is suspiciously small ({size} bytes); "
        "the build may have produced an empty file."
    )


def test_no_html_page_references_tailwind_cdn():
    """Every HTML file under packages/extensions/ must be CDN-free."""
    offenders = [
        str(p.relative_to(_EXTENSIONS_ROOT))
        for p in _all_html_files()
        if CDN_MARKER in p.read_text(encoding="utf-8")
    ]
    assert not offenders, (
        f"{len(offenders)} HTML file(s) still reference {CDN_MARKER!r}:\n"
        + "\n".join(f"  {o}" for o in offenders)
    )


def test_compiled_tailwind_css_contains_dyna_theme():
    """The dyna color palette tokens actually used as Tailwind utilities must survive the build.

    Tailwind v4 tree-shakes @theme values: only tokens referenced by a utility
    class in a scanned source file are emitted into the compiled output.  The 4
    tokens below are used as ``bg-dyna-*`` / ``text-dyna-*`` / ``border-dyna-*``
    classes across the extension browser pages.  Tokens declared in @theme but
    never consumed by a utility class (secondary, success, warning, purple,
    danger) are intentionally absent — that is correct v4 behaviour, not a bug.
    """
    css = _TAILWIND_CSS.read_text(encoding="utf-8")
    for token in ("dyna-dark", "dyna-darker", "dyna-primary", "dyna-accent"):
        assert token in css, (
            f"Custom theme token {token!r} not found in compiled tailwind.css. "
            "The @theme block in input.css may be missing or malformed."
        )


def test_compiled_tailwind_css_contains_font_tokens():
    """Inter and Fira Code must resolve via --font-sans / --font-mono (v4 key names)."""
    css = _TAILWIND_CSS.read_text(encoding="utf-8")
    assert "Inter" in css, (
        "Custom font 'Inter' not found in compiled tailwind.css. "
        "Check that --font-sans (not --font-family-sans) is used in the @theme block."
    )
    assert "Fira Code" in css, (
        "Custom font 'Fira Code' not found in compiled tailwind.css. "
        "Check that --font-mono (not --font-family-mono) is used in the @theme block."
    )


def test_compiled_tailwind_css_contains_utility_classes():
    """A representative sample of utility classes must be present."""
    css = _TAILWIND_CSS.read_text(encoding="utf-8")
    for fragment in ("flex", "grid-cols-2", "max-w-7xl", "ring-2", "blue-700"):
        assert fragment in css, (
            f"Utility fragment {fragment!r} not found in compiled tailwind.css. "
            "Check that the @source globs in input.css cover all extension pages."
        )
