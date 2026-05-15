"""Integration regression for the #738/#747 silent-swallow fix.

Closes the #755 residual: an HTTP-level regression test that proves
``PUT /configs/.../plugins/items_routing_config`` with an invalid
payload now returns **400 + problem-details body**, rather than the
pre-#747 ``200 + null body`` while persisting the invalid config.

These tests live at the integration tier because the bug they pin is
specifically the route → service → validate-handler → 400 wiring; the
unit-tier coverage (``test_config_change_runners.py`` from #747) pins
the runner semantics in isolation but cannot catch a regression in
``update_collection_config``'s ``handle_exception`` mapping or the
``ConfigValidationError → HTTP 400`` exception-handler registration.

The invalid case used here is the same shape as Furkan's original
#738 report: an entry whose ``hints`` set contains a hint the target
driver does not advertise in ``supported_hints``. We pick
``items_postgresql_driver`` + ``Hint.ANALYTICS`` because the PG driver
deterministically does NOT carry ``ANALYTICS`` (analytics workloads
route to DuckDB / Iceberg). ``GEOMETRY_SIMPLIFIED`` on the private ES
driver was the historical trigger and is now valid (private driver
implements ``simplify_to_fit`` and the foundation PR added the hint),
so it can no longer be used as the invalid case.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient


pytestmark = [
    pytest.mark.enable_modules("db_config", "db", "catalog", "storage"),
    pytest.mark.enable_extensions("features", "configs"),
]


def _routing_body_with_unsupported_hint() -> dict:
    """An ``ItemsRoutingConfig`` body whose READ entry hands PG a hint
    the PG driver does not advertise (``analytics``). The validate
    handler must reject this pre-persist."""
    # NOTE: ``Operation`` is a ``StrEnum`` whose values are UPPERCASE
    # (``WRITE``/``READ``/...) while ``FailurePolicy`` values are
    # lowercase — mirror that casing exactly or step-3 of
    # ``_validate_routing_entries`` (operation-support) will fire
    # before step-2 (hint check) and mask the assertion we care about.
    return {
        "operations": {
            "READ": [
                {
                    "driver_ref": "items_postgresql_driver",
                    "hints": ["analytics"],
                    "on_failure": "fatal",
                }
            ]
        }
    }


def _routing_body_valid_minimal() -> dict:
    """A trivially-valid ``ItemsRoutingConfig`` body: PG WRITE with no
    hints (PG advertises ``WRITE`` capability). Used as the baseline
    that should survive an invalid follow-up PUT untouched."""
    return {
        "operations": {
            "WRITE": [
                {
                    "driver_ref": "items_postgresql_driver",
                    "on_failure": "fatal",
                }
            ]
        }
    }


@pytest.mark.asyncio(loop_scope="module")
async def test_put_invalid_items_routing_config_returns_400_not_200_null(
    shared_catalog: str,
    shared_collection_factory,
    sysadmin_in_process_client_module: AsyncClient,
) -> None:
    """The #738 regression: invalid PUT must surface as 400, not 200+null.

    The bug, verbatim from the issue: the route returned ``200`` with a
    JSON ``null`` body and the invalid config was persisted, because the
    ``ValueError`` raised by ``_validate_routing_entries`` was swallowed
    by the post-upsert ``except Exception: logger.error`` in the apply
    runner. Post-#747 the validation runs pre-upsert via
    ``_VALIDATE_HANDLERS``, the exception propagates as
    ``ConfigValidationError``, and the exception-handler maps it to
    HTTP 400 with a problem-details body.
    """
    col_id = await shared_collection_factory()
    url = (
        f"/configs/catalogs/{shared_catalog}"
        f"/collections/{col_id}/plugins/items_routing_config"
    )

    # Capture the pre-PUT state so we can prove rollback.
    before = await sysadmin_in_process_client_module.get(url)
    assert before.status_code == 200, before.text
    before_body = before.json()

    resp = await sysadmin_in_process_client_module.put(
        url, json=_routing_body_with_unsupported_hint()
    )

    # The headline assertion: 400, NOT 200, NOT 500.
    assert resp.status_code == 400, (
        f"expected 400 from invalid items_routing_config PUT, got "
        f"{resp.status_code}: {resp.text}"
    )

    # The bug's literal signature was a JSON ``null`` body — guard
    # against any regression that re-introduces it.
    body = resp.json()
    assert body is not None, "response body was JSON null (the #738 signature)"
    assert isinstance(body, dict), f"expected problem-details object, got: {body!r}"

    # The validate handler's error message mentions both the offending
    # hint and the driver — assert at least one piece is surfaced.
    serialised = repr(body).lower()
    assert "analytics" in serialised or "items_postgresql_driver" in serialised, (
        f"problem-details body should reference the validation failure "
        f"context, got: {body!r}"
    )

    # Rollback proof: the persisted config must match the pre-PUT state.
    after = await sysadmin_in_process_client_module.get(url)
    assert after.status_code == 200, after.text
    assert after.json() == before_body, (
        "invalid PUT must not mutate persisted state"
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_put_valid_items_routing_config_returns_200_with_body(
    shared_catalog: str,
    shared_collection_factory,
    sysadmin_in_process_client_module: AsyncClient,
) -> None:
    """The #747 commit-2 fix: ``set_config`` now ``return config``.

    Pre-#747 the route returned ``200`` with a JSON ``null`` body for
    every valid PUT because ``set_config`` returned ``None``. This is
    the positive companion to the invalid-case test above — a valid
    PUT must round-trip a non-null config object.
    """
    col_id = await shared_collection_factory()
    url = (
        f"/configs/catalogs/{shared_catalog}"
        f"/collections/{col_id}/plugins/items_routing_config"
    )

    resp = await sysadmin_in_process_client_module.put(
        url, json=_routing_body_valid_minimal()
    )

    assert resp.status_code == 200, (
        f"expected 200 on valid PUT, got {resp.status_code}: {resp.text}"
    )

    body = resp.json()
    assert body is not None, "valid PUT returned JSON null (#747 regression)"
    assert isinstance(body, dict)
    # The persisted object should expose the operations dict we sent.
    # ``Operation`` is a StrEnum with uppercase values; the response
    # keys round-trip with that casing.  Other operations may appear
    # in the response too: the validate handler now augments via
    # ``_self_register_indexers/searchers_into`` pre-upsert (post-#747),
    # so auto-discovered ``source="auto"`` entries land alongside the
    # operator-supplied WRITE entry.  Asserting on WRITE only is the
    # robust shape; the self-register persistence is pinned separately
    # below.
    assert "operations" in body
    assert "WRITE" in body["operations"], (
        f"WRITE op not in persisted body: {body!r}"
    )
    write_entries = body["operations"]["WRITE"]
    assert any(
        e.get("driver_ref") == "items_postgresql_driver" for e in write_entries
    ), f"PG write entry not surfaced in response: {body!r}"


@pytest.mark.asyncio(loop_scope="module")
async def test_routing_self_register_entries_persisted(
    shared_catalog: str,
    shared_collection_factory,
    sysadmin_in_process_client_module: AsyncClient,
) -> None:
    """The latent #747-commit-1 persistence fix: ``_self_register_*``
    runs pre-upsert (validate phase), so auto-discovered entries with
    ``source="auto"`` actually land in the stored row.

    Pre-#747 the self-register mutated ``config.operations`` in place
    AFTER the upsert had already serialised the config, so the auto
    entries were dropped before they reached the DB. This test PUTs a
    minimal config and asserts that the persisted row contains
    auto-augmented entries — proving the mutation is now persisted.
    """
    col_id = await shared_collection_factory()
    url = (
        f"/configs/catalogs/{shared_catalog}"
        f"/collections/{col_id}/plugins/items_routing_config"
    )

    put_resp = await sysadmin_in_process_client_module.put(
        url, json=_routing_body_valid_minimal()
    )
    assert put_resp.status_code == 200, put_resp.text

    # Round-trip via a fresh GET so we're reading from the persisted
    # row, not the returned config object.
    get_resp = await sysadmin_in_process_client_module.get(url)
    assert get_resp.status_code == 200, get_resp.text
    persisted = get_resp.json()

    # Even a single-op PUT body picks up self-registered SEARCH /
    # INDEX entries when the validate phase populates them.  At minimum
    # we assert SOMETHING beyond the lone WRITE entry the operator sent
    # — anything less means the self-register persistence is broken.
    ops = persisted.get("operations", {})
    op_count = sum(len(v) for v in ops.values())
    assert op_count > 1, (
        f"only the operator-supplied entry persisted; self-register "
        f"did not augment: {persisted!r}"
    )
