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

"""Custom Swagger UI HTML handler extracted from the documentation module."""

from fastapi import FastAPI, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse


async def custom_swagger_ui_html(app: FastAPI, req: Request) -> HTMLResponse:
    """Render the custom Swagger UI HTML with injected JS/CSS overrides."""
    root_path = req.scope.get("root_path", "").rstrip("/")
    openapi_url = root_path + app.openapi_url
    oauth2_redirect_url = app.swagger_ui_oauth2_redirect_url
    if oauth2_redirect_url:
        oauth2_redirect_url = root_path + oauth2_redirect_url

    response = get_swagger_ui_html(
        openapi_url=openapi_url,
        title=app.title + " - Swagger UI",
        oauth2_redirect_url=oauth2_redirect_url,
        init_oauth=app.swagger_ui_init_oauth,
        swagger_ui_parameters=app.swagger_ui_parameters,
        swagger_favicon_url=f"{root_path}/web/static/dynastore.png",
    )

    static_url = f"{root_path}/web/extension-static"

    # Inject BOTH URLs so the JS can load the common layout and dark theme separately.
    custom_js = f"""
        <script>
            window.SWAGGER_LAYOUT_CSS_URL = "{static_url}/swagger_common.css";
            window.SWAGGER_THEME_CSS_URL = "{static_url}/swagger_theme.css";
        </script>
        <script src="{static_url}/swagger_custom.js"></script>
        """

    original_content = bytes(response.body).decode("utf-8")
    new_content = original_content.replace("</body>", f"{custom_js}</body>")
    return HTMLResponse(new_content)
