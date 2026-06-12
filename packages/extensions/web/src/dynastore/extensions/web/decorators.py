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

"""Convenience decorators for web-page / static-asset contribution.

The decorators attach pure metadata to the function.  A contributing
class uses `collect_web_pages(self)` / `collect_static_assets(self)` from
`dynastore.extensions.tools.web_collect` to materialise bound-method
`WebPageSpec` / `StaticAsset` instances in its own
`get_web_pages()` / `get_static_assets()` implementation.

No reflective class-walking happens outside the contributing class.


ds-page-shell contract
----------------------
``static/common/page-shell.js`` registers the ``ds-page-shell`` custom element.
``static/common/page-shell.css`` contains the shared header rules and opt-in
utility classes (``.ds-page-body``, ``.ds-sidebar``).

Attributes accepted by ``<ds-page-shell>``:

    data-title       Page title text (pass a pre-localised string; server-side
                     template substitution runs before the browser parses HTML).
    data-breadcrumb  Optional supplementary breadcrumb shown before the title.
    data-logo-src    URL to the brand logo.  Defaults to the ``dynastore.png``
                     resolved from the script's own directory, which works in
                     both serving contexts (see below).
    data-back-label  Label for the back-link; defaults to "Back to Home".
    data-back-href   Override the back-link href; defaults to ``../``.

Serving context and path rules:

    Every page — admin and extension — is served at ``/web/pages/{page_id}``.
    Common assets therefore always resolve via ``../static/common/``, e.g.:

        ``<link rel="stylesheet" href="../static/common/page-shell.css">``
        ``<script src="../static/common/page-shell.js"></script>``

    The component derives the logo URL from ``document.currentScript.src``
    so it lands on ``/web/static/dynastore.png`` without a hard-coded depth.

Adding a new page:

    1. Load the stylesheet:
       ``<link rel="stylesheet" href="../static/common/page-shell.css">``
    2. Load the script (plain ``<script>``, not ``type="module"``):
       ``<script src="../static/common/page-shell.js"></script>``
    3. Place the element as the first child of ``<body>`` (before any layout
       containers), passing a pre-localised ``data-title``::

           <ds-page-shell data-title="My Page Title"></ds-page-shell>
"""

from typing import Any, Callable, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


def expose_static(
    virtual_path: str,
    owner: str = "",
    description: str = "",
    public: bool = True,
):
    """Mark a method as a static-files provider.

    The decorated method must return a ``List[str]`` of absolute file
    paths that are allowed to be served under the virtual prefix.

    ``owner`` identifies the extension that contributes this prefix
    (e.g. ``"web"``, ``"geoid"``).  ``description`` is a short
    human-readable summary surfaced by the registry endpoint at
    ``GET /web/config/static-prefixes`` so page authors can discover
    available CSS/JS namespaces without hardcoding URL paths.

    ``public`` controls whether anonymous users may read files under this
    prefix. Defaults to ``True`` (the common case: browser pages load their
    JS/CSS anonymously). Set to ``False`` for prefixes that serve assets
    behind authenticated routes (e.g. admin-only dashboards). The web
    policy builder reads this flag when constructing the anonymous ALLOW
    list so future extensions are covered without manual policy edits.
    """

    def decorator(func: Callable[..., List[str]]) -> Callable[..., List[str]]:
        setattr(func, "_web_static_prefix", virtual_path)
        setattr(func, "_web_static_owner", owner)
        setattr(func, "_web_static_description", description)
        setattr(func, "_web_static_public", public)
        return func

    return decorator


def expose_web_page(
    page_id: str,
    title: Union[str, Dict[str, str]],
    icon: str = "fa-circle",
    description: Union[str, Dict[str, str]] = "",
    required_roles: Optional[List[str]] = None,
    audience_policy_id: Optional[str] = None,
    priority: int = 0,
    section: Optional[Union[str, Dict[str, str]]] = None,
    is_embed: bool = False,
    enabled: bool = True,
):
    """Mark a method as a web-page provider (metadata only).

    Visibility metadata — preferred to least-preferred:

    1. ``audience_policy_id``: id of a registered ``Policy``. The
       visibility filter resolves the policy's role bindings at request
       time and admits any caller whose flat role list intersects.
       Operators rebind the policy via REST to extend the audience —
       no decorator change needed for custom roles.
    2. ``required_roles``: literal role-name list. Legacy / explicit
       audience declaration. Kept for pages that haven't migrated.
    3. Neither set: the page is anonymous-visible.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        setattr(
            func,
            "_web_page_spec",
            {
                "page_id": page_id,
                "title": title,
                "icon": icon,
                "description": description,
                "required_roles": (
                    tuple(required_roles) if required_roles else None
                ),
                "audience_policy_id": audience_policy_id,
                "priority": priority,
                "section": section,
                "is_embed": is_embed,
                "enabled": enabled,
            },
        )
        return func

    return decorator
