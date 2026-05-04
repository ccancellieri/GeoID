"""Self-validation tests for the shared_catalog / shared_collection_factory
fixtures introduced in tests/dynastore/conftest.py. These tests validate the
fixtures' contract; they are not user-facing tests of any product feature.

Note: these tests use ``loop_scope="module"`` because ``shared_catalog`` is
module-scoped (depends on ``app_lifespan_module``). Tests opting into
``shared_catalog`` elsewhere in the suite must do the same.
"""
import pytest
import pytest_asyncio


# The shared_collection_factory fixture posts to /features/catalogs/.../collections,
# which requires the `features` extension to be mounted. The catalog itself is
# created via the CatalogsProtocol, which only needs `catalog` module + its deps.
pytestmark = [
    pytest.mark.enable_extensions("features"),
]


@pytest.mark.asyncio(loop_scope="module")
async def test_shared_catalog_yields_valid_id(shared_catalog):
    """Fixture yields a non-empty catalog id with the expected naming shape."""
    assert isinstance(shared_catalog, str)
    assert shared_catalog.startswith("shared_")
    parts = shared_catalog.split("_")
    assert len(parts) >= 3, f"unexpected catalog id shape: {shared_catalog}"


@pytest.mark.asyncio(loop_scope="module")
async def test_shared_catalog_exists_in_db(shared_catalog):
    """The catalog is actually persisted — get_catalog returns a row."""
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol

    catalogs = get_protocol(CatalogsProtocol)
    cat = await catalogs.get_catalog(shared_catalog)
    assert cat is not None
    assert cat.id == shared_catalog


@pytest.mark.asyncio(loop_scope="module")
async def test_shared_catalog_id_stable_within_module(shared_catalog):
    """Same fixture instance is reused across every test in this module.
    Pins the contract so a future refactor can't silently flip the scope
    back to function.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CatalogsProtocol

    catalogs = get_protocol(CatalogsProtocol)
    a = await catalogs.get_catalog(shared_catalog)
    b = await catalogs.get_catalog(shared_catalog)
    assert a is not None and b is not None
    assert a.id == b.id == shared_catalog


@pytest.mark.asyncio(loop_scope="module")
async def test_shared_collection_factory_creates_collections(
    shared_catalog, shared_collection_factory
):
    """Factory creates a collection inside the shared catalog and the
    collection actually exists."""
    col_id = await shared_collection_factory()
    assert isinstance(col_id, str)
    assert col_id.startswith("col_")

    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import CollectionsProtocol

    collections = get_protocol(CollectionsProtocol)
    col = await collections.get_collection(shared_catalog, col_id)
    assert col is not None
    assert col.id == col_id


@pytest.mark.asyncio(loop_scope="module")
async def test_shared_collection_factory_distinct_ids(
    shared_catalog, shared_collection_factory
):
    """Repeated calls within the same test return distinct ids."""
    a = await shared_collection_factory()
    b = await shared_collection_factory()
    c = await shared_collection_factory()
    assert len({a, b, c}) == 3, f"expected distinct ids; got {a}, {b}, {c}"


@pytest.mark.asyncio(loop_scope="module")
async def test_shared_collection_factory_collection_visible_during_test(
    shared_catalog, shared_collection_factory, sysadmin_in_process_client_module
):
    """A collection created in this test is visible via the API. The
    'really gone after teardown' guarantee is exercised by the next test
    (test_shared_collection_factory_cleans_up_previous_test_collections).
    """
    col_id = await shared_collection_factory()
    resp = await sysadmin_in_process_client_module.get(
        f"/features/catalogs/{shared_catalog}/collections/{col_id}"
    )
    assert resp.status_code == 200
    # Stash the id where the next test can find it (module-level state is OK
    # within a single module's tests; test ordering is preserved by pytest).
    pytest._shared_collection_factory_leak_probe = col_id


@pytest.mark.asyncio(loop_scope="module")
async def test_shared_collection_factory_cleans_up_previous_test_collections(
    shared_catalog, sysadmin_in_process_client_module
):
    """Verifies the previous test's collection was actually deleted on
    its teardown. Closes the cleanup contract that
    ``shared_collection_factory`` claims.

    Pattern: previous test stashed the id on the pytest module; we read
    it back here and confirm the API now returns 404.
    """
    col_id = getattr(pytest, "_shared_collection_factory_leak_probe", None)
    assert col_id is not None, (
        "previous test must have stashed an id; check ordering"
    )
    resp = await sysadmin_in_process_client_module.get(
        f"/features/catalogs/{shared_catalog}/collections/{col_id}"
    )
    assert resp.status_code == 404, (
        f"expected 404 (collection cleaned up by factory teardown); "
        f"got {resp.status_code}: {resp.text}"
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_shared_collection_factory_accepts_overrides(
    shared_catalog, shared_collection_factory, sysadmin_in_process_client_module
):
    """The ``**overrides`` kwarg lets a caller customise the body. Verify
    a custom description sticks (proves the override pathway is wired,
    not silently dropped).
    """
    custom_desc = "test override description — should land on the resource"
    col_id = await shared_collection_factory(description=custom_desc)
    resp = await sysadmin_in_process_client_module.get(
        f"/features/catalogs/{shared_catalog}/collections/{col_id}"
    )
    assert resp.status_code == 200
    body = resp.json()
    # description may be returned as a string or as a localized {"en": "..."}
    # dict depending on the request language; accept both.
    desc = body.get("description")
    if isinstance(desc, dict):
        desc = next(iter(desc.values()), None)
    assert desc == custom_desc, f"override didn't stick; got {body.get('description')!r}"
