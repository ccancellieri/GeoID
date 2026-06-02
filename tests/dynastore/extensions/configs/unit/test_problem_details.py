"""Tests for the RFC 9457 Problem Details surface in extensions/configs.

Covers:
  * The four constructors (plugin_not_registered / validation_failed /
    value_error / unexpected_failure) produce the right type/title/status.
  * ``validation_failed`` extracts a structured ``errors[]`` array from a
    Pydantic ``ValidationError``.
  * The exception handler renders ``application/problem+json`` with the
    correct status code, body shape, and request-path-derived ``instance``.
  * The handler defaults ``instance`` only when the raiser left it null.
"""

import json
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field, ValidationError

from dynastore.extensions.configs import problem_details


# ---------------------------------------------------------------------------
# Constructor shape tests
# ---------------------------------------------------------------------------

def test_plugin_not_registered_shape():
    exc = problem_details.plugin_not_registered("FooBarConfig")
    p = exc.problem
    assert p.type == "https://errors.dynastore.fao.org/config/plugin-not-registered"
    assert p.title == "Configuration plugin not registered"
    assert p.status == 404
    assert "FooBarConfig" in (p.detail or "")
    assert p.instance is None  # set by the handler from request.url.path
    assert p.errors is None


def test_value_error_shape():
    exc = problem_details.value_error(ValueError("unknown plugin 'X'"))
    p = exc.problem
    assert p.type == "https://errors.dynastore.fao.org/config/not-found"
    assert p.status == 404
    assert "unknown plugin" in (p.detail or "")


def test_global_registry_surfaces_problem_status_not_500():
    """Regression: ``GlobalExceptionHandlingMiddleware`` was demoting every
    ``ProblemException`` to a generic 500 because the registry didn't know
    how to render one — the configs extension's RFC 9457 handler is registered
    with FastAPI but the middleware swallows the exception before per-class
    dispatch fires. ``ProblemExceptionHandler`` (registered first in
    ``_register_builtin_handlers``) surfaces the explicit 4xx status and the
    Problem Details ``detail`` instead of the fallback ``"An unexpected error
    occurred."`` body.
    """
    from dynastore.extensions.tools.exception_handlers import _global_registry

    exc = problem_details.value_error(
        ValueError("Unknown config class 'items_schema'"),
    )
    result = _global_registry.handle(exc, context={}, reraise_unhandled=False)
    assert result.status_code == 404, "must keep the configs ProblemException's status"
    assert "Unknown config class" in (result.detail or "")
    assert "An unexpected error" not in (result.detail or "")


def test_unexpected_failure_shape():
    exc = problem_details.unexpected_failure(RuntimeError("connection reset by peer"))
    p = exc.problem
    assert p.type == "https://errors.dynastore.fao.org/config/internal"
    assert p.status == 500
    assert "connection reset" in (p.detail or "")


def test_validation_failed_shape():
    class _Model(BaseModel):
        target_srid: int = Field(ge=1024, le=998999)

    try:
        _Model.model_validate({"target_srid": 999999999})
    except ValidationError as ve:
        exc = problem_details.validation_failed(ve)

    p = exc.problem
    assert p.type == "https://errors.dynastore.fao.org/config/validation-failed"
    assert p.status == 422
    assert p.errors is not None and len(p.errors) >= 1
    err = p.errors[0]
    assert err.field == "target_srid"
    assert "less than or equal" in err.message.lower() or "le=" in err.message.lower()
    assert err.value == 999999999


def test_validation_failed_multiple_errors():
    class _Model(BaseModel):
        srid: int = Field(ge=0)
        name: str = Field(min_length=1)

    try:
        _Model.model_validate({"srid": -1, "name": ""})
    except ValidationError as ve:
        exc = problem_details.validation_failed(ve)

    p = exc.problem
    assert p.errors is not None
    fields = {e.field for e in p.errors}
    assert "srid" in fields
    assert "name" in fields


def test_validation_failed_nested_field_path_dotted():
    class _Inner(BaseModel):
        port: int = Field(ge=0, le=65535)

    class _Outer(BaseModel):
        inner: _Inner

    try:
        _Outer.model_validate({"inner": {"port": -1}})
    except ValidationError as ve:
        exc = problem_details.validation_failed(ve)

    p = exc.problem
    assert p.errors is not None
    assert any(e.field == "inner.port" for e in p.errors)


# ---------------------------------------------------------------------------
# Handler integration tests — register handler on a tiny FastAPI app and
# confirm the rendered response matches the spec.
# ---------------------------------------------------------------------------

@pytest.fixture()
def app_with_handler():
    app = FastAPI()
    problem_details.register(app)

    @app.get("/raises-not-found")
    def _raises_not_found():
        raise problem_details.plugin_not_registered("MissingPlugin")

    @app.get("/raises-validation")
    def _raises_validation():
        class _Model(BaseModel):
            srid: int = Field(ge=0)
        try:
            _Model.model_validate({"srid": -1})
        except ValidationError as ve:
            raise problem_details.validation_failed(ve) from ve

    @app.get("/raises-with-explicit-instance")
    def _raises_with_explicit_instance():
        from dynastore.extensions.configs.problem_details import ProblemDetails, ProblemException
        raise ProblemException(ProblemDetails(
            type="https://errors.dynastore.fao.org/config/custom",
            title="Custom",
            status=409,
            detail="boom",
            instance="/explicit/path",
        ))

    return app


def test_handler_renders_application_problem_json_media_type(app_with_handler):
    client = TestClient(app_with_handler)
    r = client.get("/raises-not-found")
    assert r.status_code == 404
    assert r.headers["content-type"].startswith("application/problem+json")


def test_handler_body_matches_rfc_9457(app_with_handler):
    client = TestClient(app_with_handler)
    r = client.get("/raises-not-found")
    body = r.json()
    assert body["type"] == "https://errors.dynastore.fao.org/config/plugin-not-registered"
    assert body["title"] == "Configuration plugin not registered"
    assert body["status"] == 404
    assert "MissingPlugin" in body["detail"]
    # instance defaulted to request path
    assert body["instance"] == "/raises-not-found"
    # errors[] not populated for non-validation errors
    assert "errors" not in body


def test_handler_validation_response_carries_errors_array(app_with_handler):
    client = TestClient(app_with_handler)
    r = client.get("/raises-validation")
    assert r.status_code == 422
    body = r.json()
    assert body["type"] == "https://errors.dynastore.fao.org/config/validation-failed"
    assert body["instance"] == "/raises-validation"
    assert isinstance(body["errors"], list) and len(body["errors"]) >= 1
    assert body["errors"][0]["field"] == "srid"
    assert body["errors"][0]["value"] == -1


def test_handler_does_not_overwrite_explicit_instance(app_with_handler):
    """When the raiser sets ``instance`` explicitly, the handler must NOT
    overwrite it with the request path.
    """
    client = TestClient(app_with_handler)
    r = client.get("/raises-with-explicit-instance")
    assert r.status_code == 409
    body = r.json()
    assert body["instance"] == "/explicit/path"  # not "/raises-with-explicit-instance"


def test_handler_omits_null_fields(app_with_handler):
    """Per RFC 9457, optional fields with null values should be omitted from
    the response (we use ``model_dump(exclude_none=True)``)."""
    client = TestClient(app_with_handler)
    r = client.get("/raises-not-found")
    body = r.json()
    # `errors` is None on this path → should be omitted
    assert "errors" not in body
