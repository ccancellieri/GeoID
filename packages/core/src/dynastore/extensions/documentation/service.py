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

import inspect
import logging
import os
import re
from enum import Enum

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse

from dynastore.extensions.registry import ExtensionConfig
from dynastore.extensions.documentation._fastapi_docs import custom_swagger_ui_html

logger = logging.getLogger(__name__)

# Markdown extensions used wherever a README is rendered to HTML.
_MARKDOWN_EXTENSIONS = ["fenced_code", "tables"]


class RendererUnavailable(Exception):
    """Raised when the ``markdown`` package is not installed."""


class ReadmeNotFound(Exception):
    """Raised when the registry has no entry for ``name`` or the path is absent."""


def render_extension_readme_body(registry: dict, name: str) -> str:
    """Look up ``name`` in *registry*, read its README, and return rendered HTML.

    The returned string is the **bare rendered body** — plain markdown-to-HTML
    with no surrounding template. Callers are responsible for wrapping it (or
    not) in a full HTML document.

    Raises:
        RendererUnavailable: if the ``markdown`` package is not importable.
        ReadmeNotFound: if *name* is absent from *registry* or the file on
            disk does not exist.
    """
    try:
        import markdown
    except ImportError:
        raise RendererUnavailable("markdown package is not installed")

    readme_path = registry.get(name)
    if not readme_path or not os.path.exists(readme_path):
        raise ReadmeNotFound(f"No README registered or file absent for {name!r}")

    with open(readme_path, "r", encoding="utf-8") as f:
        md_content = f.read()

    return markdown.markdown(md_content, extensions=_MARKDOWN_EXTENSIONS)


def _render_help_box(help_url: str) -> str:
    """Right-aligned "Documentation" link injected into OpenAPI tag descriptions.

    ``help_url`` points at the extension's rendered README page. The ``help-box``
    class is also the idempotency marker used to avoid prepending the box twice
    when the lifespan re-runs.
    """
    return (
        '<div class="help-box" style="display: flex; justify-content: flex-end;">'
        f'<a href="{help_url}" target="_blank" title="Open documentation in a new tab" '
        'style="display: inline-flex; align-items: center; gap: 6px;">'
        '<b>Documentation</b> ❓</a>'
        '</div>'
    )


def configure_swagger_ui(app: FastAPI):
    """
    Configures the Swagger UI with custom parameters and injects
    JavaScript for persistent state management, navigation, and Dark Mode.
    """
    if app.swagger_ui_parameters is None:
        app.swagger_ui_parameters = {}

    app.swagger_ui_parameters.update({
        "docExpansion": "none",
        "persistAuthorization": True,
        "deepLinking": True,
        "displayOperationId": False,
        "tagsSorter": "alpha",
    })

    # Build the OAuth init dict from environment. ``IDP_CLIENT_ID`` is the
    # SPA login client (e.g. ``geoid-fe``) — Swagger UI's Authorize popup
    # uses it to start the Authorization Code + PKCE flow against Keycloak.
    # If unset, omit ``clientId`` entirely so the user notices the missing
    # config in the popup rather than authenticating as a stale default.
    #
    # ``usePkceWithAuthorizationCodeGrant`` is a current Swagger UI v5 OAuth2
    # field (bundled via swagger-ui-dist@5); it enables PKCE for the public
    # SPA client and is required by our Keycloak Authorization Code flow.
    init_oauth: dict = {
        "usePkceWithAuthorizationCodeGrant": True,
        "scopes": "openid email profile",
    }
    client_id = os.getenv("IDP_CLIENT_ID")
    if client_id:
        init_oauth["clientId"] = client_id

    # ``IDP_AUDIENCE`` (e.g. ``geoid-be``) — Keycloak setups that gate API
    # access on the ``aud`` claim need the SPA to request the audience
    # explicitly. Swagger UI forwards ``additionalQueryStringParams`` to
    # both the authorize and token requests.
    audience = os.getenv("IDP_AUDIENCE")
    if audience:
        init_oauth["additionalQueryStringParams"] = {"audience": audience}

    app.swagger_ui_init_oauth = init_oauth

    # FastAPI's canonical OAuth2 callback page. Swagger UI's popup posts
    # the auth-code response to this URL; the page parses it and posts a
    # message back to the opener so the popup auto-closes. Must be set
    # BEFORE the /docs handler is built — _fastapi_docs.py reads this
    # attribute and threads it into get_swagger_ui_html().
    app.swagger_ui_oauth2_redirect_url = "/docs/oauth2-redirect"

    # Remove FastAPI's default /docs route so our custom Swagger UI wrapper
    # (which injects the dark-theme CSS/JS) can take its place below.
    for route in list(app.router.routes):
        if getattr(route, "path", "") == "/docs":
            app.router.routes.remove(route)
            logger.info("Removed default FastAPI /docs route to inject custom Swagger UI wrapper.")

    async def _docs_handler(req: Request):
        return await custom_swagger_ui_html(app, req)

    app.add_api_route("/docs", _docs_handler, include_in_schema=False)

    # OAuth2 redirect callback — pure FastAPI default page, no customization.
    from fastapi.openapi.docs import get_swagger_ui_oauth2_redirect_html

    async def _oauth2_redirect_handler():
        return get_swagger_ui_oauth2_redirect_html()

    app.add_api_route(
        "/docs/oauth2-redirect",
        _oauth2_redirect_handler,
        include_in_schema=False,
    )


def enrich_extension_metadata(app: FastAPI, config: ExtensionConfig, router: APIRouter):
    """
    Checks for a README, creates the help route, and enriches OpenAPI tags.
    """
    extension_name = getattr(config.cls, "_registered_name", config.cls.__name__)

    try:
        extension_dir = os.path.dirname(inspect.getfile(config.cls))
        readme_path = os.path.join(extension_dir, "readme.md")

        if os.path.exists(readme_path):
            # Register the README path so the help routes can serve it.
            if not hasattr(app.state, "extension_docs_registry"):
                app.state.extension_docs_registry = {}
            app.state.extension_docs_registry[extension_name] = readme_path

            # Read content for the OpenAPI tag description.
            readme_content = ""
            try:
                with open(readme_path, "r", encoding="utf-8") as f:
                    readme_content = f.read()
            except Exception as e:
                logger.error(f"Error reading readme for description: {e}")
                readme_content = "Documentation available. Click to open full view."

            # Calculate safe_help_url — at enrichment time we have no Request,
            # so fall back to the app's root_path (set at FastAPI() construction).
            root_path_prefix = app.root_path.rstrip("/")

            if not root_path_prefix.startswith("/"):
                root_path_prefix = "/" + root_path_prefix

            safe_help_url = f"{root_path_prefix.rstrip('/')}/extension-docs/{extension_name}"

            # Determine the tags this extension contributes; the help box is
            # injected into each tag's description below.
            tags_to_enrich = set()
            if router.tags:
                tags_to_enrich.update(router.tags)
            for route in router.routes:
                route_tags = getattr(route, "tags", None)
                if route_tags:
                    tags_to_enrich.update(route_tags)
            if not tags_to_enrich:
                tags_to_enrich.add(extension_name)

            # --- HEADER GENERATION ---
            header_html = _render_help_box(safe_help_url)

            # Prepend header to content
            readme_content = header_html + "\n\n" + readme_content

            # --- LINK FIXING ---
            def replace_anchor(match):
                text = match.group(1)
                anchor = match.group(2)
                return f'<a href="{safe_help_url}{anchor}" target="_blank">{text}</a>'

            readme_content = re.sub(r'\[([^\]]+)\]\((#[^)]+)\)', replace_anchor, readme_content)

            # Add the visible endpoint to the "Help" section of the docs.
            async def _specific_help_handler(request: Request, ext_name: str = extension_name):
                registry = getattr(request.app.state, "extension_docs_registry", {})
                try:
                    body = render_extension_readme_body(registry, ext_name)
                except RendererUnavailable:
                    return HTMLResponse("Markdown renderer not installed", 500)
                except ReadmeNotFound:
                    return HTMLResponse("Docs not found", 404)
                return HTMLResponse(body)

            app.add_api_route(
                f"/extension-docs/{extension_name}",
                _specific_help_handler,
                methods=["GET"],
                tags=["Help"],
                summary=f"Read {extension_name} documentation",
                operation_id=f"read_{extension_name}_docs",
                description=readme_content,
            )

            # Enrich each tag's OpenAPI description with the help box.
            if not app.openapi_tags:
                app.openapi_tags = []

            for tag in tags_to_enrich:
                tag_name = str(tag.value) if isinstance(tag, Enum) else str(tag)
                existing_tag = next((t for t in app.openapi_tags if t.get('name') == tag_name), None)

                if existing_tag:
                    if "description" not in existing_tag:
                        existing_tag["description"] = ""
                    # Avoid duplication if lifespan runs multiple times
                    if "help-box" not in existing_tag["description"]:
                        existing_tag["description"] = header_html + "\n\n" + existing_tag["description"]
                else:
                    app.openapi_tags.append({
                        "name": tag_name,
                        "description": header_html,
                    })

    except Exception as e:
        logger.error(f"Failed to setup docs for {extension_name}: {e}", exc_info=True)
