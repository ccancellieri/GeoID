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

"""Unit tests for IAM's custom OpenAPI security-scheme injection.

Exercises ``build_iam_openapi_schema`` directly so the IamMiddleware /
TenantScopeMiddleware stack does not need to be booted. Also verifies
that ``install_filtered_openapi`` (the platform-disable filter wrapper)
preserves the security schemes and the top-level ``security`` field.
"""

from types import SimpleNamespace

from fastapi import FastAPI

from dynastore.extensions.iam.service import build_iam_openapi_schema
from dynastore.extensions.tools.exposure_openapi import install_filtered_openapi


def _fresh_app() -> FastAPI:
    app = FastAPI(title="t", version="0.0.0", description="d")

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    return app


def test_security_schemes_present(monkeypatch):
    monkeypatch.delenv("IDP_ISSUER_URL", raising=False)
    monkeypatch.delenv("IDP_PUBLIC_URL", raising=False)

    schema = build_iam_openapi_schema(_fresh_app())
    schemes = schema["components"]["securitySchemes"]
    assert "HTTPBearer" in schemes
    assert "OAuth2AuthorizationCode" in schemes
    # HTTPBearer description should reference Keycloak login, not /auth/token.
    assert "Keycloak" in schemes["HTTPBearer"]["description"]


def test_top_level_security_requirement(monkeypatch):
    monkeypatch.delenv("IDP_ISSUER_URL", raising=False)
    monkeypatch.delenv("IDP_PUBLIC_URL", raising=False)

    schema = build_iam_openapi_schema(_fresh_app())
    assert schema["security"] == [
        {"HTTPBearer": []},
        {"OAuth2AuthorizationCode": ["openid", "email", "profile"]},
    ]


def test_oauth2_absolute_urls_when_idp_set(monkeypatch):
    monkeypatch.setenv("IDP_ISSUER_URL", "https://kc.example.com/realms/test")
    monkeypatch.delenv("IDP_PUBLIC_URL", raising=False)

    schema = build_iam_openapi_schema(_fresh_app())
    flow = schema["components"]["securitySchemes"]["OAuth2AuthorizationCode"]["flows"][
        "authorizationCode"
    ]
    assert flow["authorizationUrl"] == (
        "https://kc.example.com/realms/test/protocol/openid-connect/auth"
    )
    assert flow["tokenUrl"] == (
        "https://kc.example.com/realms/test/protocol/openid-connect/token"
    )


def test_oauth2_falls_back_to_relative_when_idp_unset(monkeypatch):
    monkeypatch.delenv("IDP_ISSUER_URL", raising=False)
    monkeypatch.delenv("IDP_PUBLIC_URL", raising=False)

    schema = build_iam_openapi_schema(_fresh_app())
    flow = schema["components"]["securitySchemes"]["OAuth2AuthorizationCode"]["flows"][
        "authorizationCode"
    ]
    assert flow["authorizationUrl"] == "/auth/authorize"
    assert flow["tokenUrl"] == "/auth/token"


def test_oauth2_uses_public_url_when_set(monkeypatch):
    monkeypatch.setenv("IDP_ISSUER_URL", "http://internal-kc:8080/realms/test")
    monkeypatch.setenv("IDP_PUBLIC_URL", "https://kc.public.example.com")

    schema = build_iam_openapi_schema(_fresh_app())
    flow = schema["components"]["securitySchemes"]["OAuth2AuthorizationCode"]["flows"][
        "authorizationCode"
    ]
    assert flow["authorizationUrl"] == (
        "https://kc.public.example.com/realms/test/protocol/openid-connect/auth"
    )
    assert flow["tokenUrl"] == (
        "https://kc.public.example.com/realms/test/protocol/openid-connect/token"
    )


def test_install_filtered_openapi_preserves_security(monkeypatch):
    monkeypatch.setenv("IDP_ISSUER_URL", "https://kc.example.com/realms/test")
    monkeypatch.delenv("IDP_PUBLIC_URL", raising=False)

    app = _fresh_app()

    # Mimic IAM extension's wiring without booting middleware.
    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        app.openapi_schema = build_iam_openapi_schema(app)
        return app.openapi_schema

    app.openapi = custom_openapi

    # ExposureMatrix.get_sync() returns an object with a ``platform`` dict.
    # No extensions disabled -> filter is a passthrough.
    matrix = SimpleNamespace(get_sync=lambda: SimpleNamespace(platform={}))
    app.state.extension_prefixes = []

    install_filtered_openapi(app, matrix)  # type: ignore[arg-type]

    schema = app.openapi()
    schemes = schema["components"]["securitySchemes"]
    assert "HTTPBearer" in schemes
    assert "OAuth2AuthorizationCode" in schemes
    assert schema["security"] == [
        {"HTTPBearer": []},
        {"OAuth2AuthorizationCode": ["openid", "email", "profile"]},
    ]
    flow = schemes["OAuth2AuthorizationCode"]["flows"]["authorizationCode"]
    assert flow["authorizationUrl"].startswith("https://kc.example.com/realms/test/")
