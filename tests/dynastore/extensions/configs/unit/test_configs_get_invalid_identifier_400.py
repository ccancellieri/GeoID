"""#1201 — configs GET must map a client identifier error to 400, not 500.

A templated/malformed resource id (``{{m.catalog}}``) raises
``InvalidIdentifierError`` (a ``ValueError`` subclass, from
``validate_sql_identifier`` — #1196) deep inside ``get_config``. The three
configs GET handlers previously funneled it into the broad
``except Exception -> unexpected_failure`` branch and returned **500**. A
malformed identifier is a *client* error and must be **400** — consistent with
the write path, which already maps it via ``handle_exception`` (#1191).

Covers:
  * the new ``problem_details.invalid_identifier`` 400 constructor;
  * each GET handler (collection/catalog/platform) -> 400 for an
    ``InvalidIdentifierError``;
  * regression guards: a genuine non-identifier failure stays 500, and an
    unregistered plugin stays 404.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.configs import problem_details
from dynastore.extensions.configs.service import ConfigsService
from dynastore.tools.db import InvalidIdentifierError

_TEMPLATED = "{{m.catalog}}"


def _svc_with_configs(get_config_side_effect):
    """A ConfigsService whose ``configs`` property yields a mock store."""
    svc = object.__new__(ConfigsService)
    store = MagicMock()
    store.get_config = AsyncMock(side_effect=get_config_side_effect)
    return svc, store


# ---------------------------------------------------------------------------
# problem_details.invalid_identifier constructor
# ---------------------------------------------------------------------------

def test_invalid_identifier_shape():
    exc = problem_details.invalid_identifier(
        InvalidIdentifierError(f"Identifier {_TEMPLATED!r} contains an "
                               "unsubstituted template placeholder")
    )
    p = exc.problem
    assert p.status == 400
    assert p.type == "https://errors.dynastore.fao.org/config/invalid-identifier"
    assert "placeholder" in (p.detail or "")
    assert p.errors is None


# ---------------------------------------------------------------------------
# GET handlers — InvalidIdentifierError -> 400
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_collection_config_invalid_identifier_400():
    svc, store = _svc_with_configs(InvalidIdentifierError("bad id " + _TEMPLATED))
    with patch.object(type(svc), "configs", property(lambda self: store)), patch(
        "dynastore.extensions.configs.service.resolve_config_class",
        return_value=MagicMock(),
    ):
        with pytest.raises(problem_details.ProblemException) as ei:
            await svc.get_collection_config(_TEMPLATED, "coll", "items_write_policy")
    assert ei.value.problem.status == 400


@pytest.mark.asyncio
async def test_get_catalog_config_invalid_identifier_400():
    svc, store = _svc_with_configs(InvalidIdentifierError("bad id " + _TEMPLATED))
    with patch.object(type(svc), "configs", property(lambda self: store)), patch(
        "dynastore.extensions.configs.service.resolve_config_class",
        return_value=MagicMock(),
    ):
        with pytest.raises(problem_details.ProblemException) as ei:
            await svc.get_catalog_config(_TEMPLATED, "items_write_policy")
    assert ei.value.problem.status == 400


@pytest.mark.asyncio
async def test_get_platform_config_invalid_identifier_400():
    svc, store = _svc_with_configs(InvalidIdentifierError("bad id"))
    with patch.object(type(svc), "configs", property(lambda self: store)), patch(
        "dynastore.extensions.configs.service.resolve_config_class",
        return_value=MagicMock(),
    ):
        with pytest.raises(problem_details.ProblemException) as ei:
            await svc.get_platform_config("items_write_policy")
    assert ei.value.problem.status == 400


# ---------------------------------------------------------------------------
# Regression guards — non-identifier failures keep their status
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_catalog_config_generic_failure_still_500():
    svc, store = _svc_with_configs(RuntimeError("connection reset by peer"))
    with patch.object(type(svc), "configs", property(lambda self: store)), patch(
        "dynastore.extensions.configs.service.resolve_config_class",
        return_value=MagicMock(),
    ):
        with pytest.raises(problem_details.ProblemException) as ei:
            await svc.get_catalog_config("real-catalog", "items_write_policy")
    assert ei.value.problem.status == 500


@pytest.mark.asyncio
async def test_get_catalog_config_unregistered_plugin_still_404():
    svc, store = _svc_with_configs(AssertionError("should not be reached"))
    with patch.object(type(svc), "configs", property(lambda self: store)), patch(
        "dynastore.extensions.configs.service.resolve_config_class",
        return_value=None,
    ):
        with pytest.raises(problem_details.ProblemException) as ei:
            await svc.get_catalog_config("real-catalog", "no_such_plugin")
    assert ei.value.problem.status == 404
