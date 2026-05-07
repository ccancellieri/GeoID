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
    })

    app.swagger_ui_init_oauth = {
        "clientId": "dynastore",
        "usePkceWithAuthorizationCodeGrant": True,
        "scopes": "openid email profile",
    }

    # --- CRITICAL FIX: Remove existing /docs route ---
    for route in list(app.router.routes):
        if getattr(route, "path", "") == "/docs":
            app.router.routes.remove(route)
            logger.info("Removed default FastAPI /docs route to inject custom Swagger UI wrapper.")

    # Now we can safely add our own /docs handler using the extracted function
    async def _docs_handler(req: Request):
        return await custom_swagger_ui_html(app, req)

    app.add_api_route("/docs", _docs_handler, include_in_schema=False)


def setup_global_help_endpoint(app: FastAPI):
    """
    Sets up the generic /_help/{name} endpoint that renders Markdown.
    """
    if not hasattr(app.state, "extension_docs_registry"):
        app.state.extension_docs_registry = {}

    async def global_extension_help_handler(request: Request, name: str):
        from dynastore.extensions.tools.url import get_root_url
        try:
            import markdown
        except ImportError:
            return HTMLResponse("<h1>Documentation renderer (markdown) not installed</h1>", status_code=500)

        registry = getattr(request.app.state, "extension_docs_registry", {})
        readme_path = registry.get(name)

        if not readme_path or not os.path.exists(readme_path):
            return HTMLResponse("<h1>Documentation not found</h1>", status_code=404)

        try:
            with open(readme_path, "r", encoding="utf-8") as f:
                md_content = f.read()

            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>{name.capitalize()} Extension Help</title>
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <style>
                    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 900px; margin: 0 auto; padding: 20px; background-color: #f9f9f9; }}
                    header {{ margin-bottom: 30px; padding-bottom: 10px; border-bottom: 1px solid #eaeaea; }}
                    a {{ color: #007bff; text-decoration: none; }}
                    a:hover {{ text-decoration: underline; }}
                    pre {{ background: #2d2d2d; color: #f8f8f2; padding: 15px; border-radius: 5px; overflow-x: auto; }}
                    code {{ background: #eaeaea; padding: 2px 5px; border-radius: 3px; font-family: monospace; font-size: 0.9em; }}
                    pre code {{ background: transparent; color: inherit; padding: 0; }}
                    table {{ border-collapse: collapse; width: 100%; margin: 20px 0; background: white; }}
                    th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
                    th {{ background-color: #f2f2f2; font-weight: 600; }}
                    blockquote {{ border-left: 4px solid #007bff; margin: 0; padding-left: 15px; color: #555; }}
                    img {{ max-width: 100%; height: auto; }}
                </style>
            </head>
            <body>
                <header>
                    <p><a href="#" onclick="window.close(); return false;">&larr; Close Window</a> | <a href="{get_root_url(request=request)}/docs">Back to Main API Docs</a></p>
                </header>
                {markdown.markdown(md_content, extensions=['fenced_code', 'tables'])}
            </body>
            </html>
            """
            return HTMLResponse(content=html_content)
        except Exception as e:
            return HTMLResponse(content=f"Error reading docs: {e}", status_code=500)

    # Register hidden route
    route_path = "/_help/{name}"
    if not any(getattr(route, "path", None) == route_path for route in app.routes):
        app.add_api_route(
            route_path,
            global_extension_help_handler,
            methods=["GET"],
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
            # 1. Register path in registry
            if not hasattr(app.state, "extension_docs_registry"):
                app.state.extension_docs_registry = {}
            app.state.extension_docs_registry[extension_name] = readme_path

            # 2. Read content
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

            # --- 4. Determine Tags (MOVED UP for Header generation) ---
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
            help_link = (
                f'<a href="{safe_help_url}" target="_blank" title="Open documentation in a new tab" '
                f'style="display: inline-flex; align-items: center; gap: 6px;">'
                f'<b>Documentation</b> \u2753</a>'
            )

            header_html = (
                f'<div class="help-box" style="display: flex; justify-content: flex-end;">'
                f'{help_link}'
                f'</div>'
            )

            # Prepend header to content
            readme_content = header_html + "\n\n" + readme_content

            # --- LINK FIXING ---
            def replace_anchor(match):
                text = match.group(1)
                anchor = match.group(2)
                return f'<a href="{safe_help_url}{anchor}" target="_blank">{text}</a>'

            readme_content = re.sub(r'\[([^\]]+)\]\((#[^)]+)\)', replace_anchor, readme_content)

            # 3. Add visible endpoint to "Help" section
            async def _specific_help_handler(request: Request, ext_name: str = extension_name):
                registry = getattr(request.app.state, "extension_docs_registry", {})
                path = registry.get(ext_name)
                if not path:
                    return HTMLResponse("Docs not found", 404)
                try:
                    import markdown
                except ImportError:
                    return HTMLResponse("Markdown renderer not installed", 500)
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
                return HTMLResponse(markdown.markdown(content, extensions=['fenced_code', 'tables']))

            op_id = f"read_{extension_name}_docs"

            app.add_api_route(
                f"/extension-docs/{extension_name}",
                _specific_help_handler,
                methods=["GET"],
                tags=["Help"],
                summary=f"Read {extension_name} documentation",
                operation_id=op_id,
                description=readme_content,
            )

            # 5. Enrich Tags Metadata
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
