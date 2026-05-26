import pytest
from tests.dynastore.test_utils import generate_test_id
from dynastore.modules.iam.iam_service import IamService as IamManager
from dynastore.modules.iam.models import Principal, Role
from dynastore.modules.db_config.query_executor import managed_transaction


@pytest.fixture
async def iam_manager(app_lifespan):
    from dynastore.modules.iam.postgres_iam_storage import PostgresIamStorage
    from dynastore.modules.iam.postgres_policy_storage import PostgresPolicyStorage
    from dynastore.modules.iam.policies import PolicyService as PolicyManager  # PolicyService class name unchanged

    storage = PostgresIamStorage(app_lifespan)
    policy_storage = PostgresPolicyStorage(app_lifespan)
    policy_manager = PolicyManager(app_lifespan, storage=policy_storage)

    manager = IamManager(storage, policy_manager, app_state=app_lifespan)
    async with managed_transaction(app_lifespan.engine) as conn:
        await storage.initialize(conn, schema="iam")
    await policy_manager.initialize()
    return manager


@pytest.mark.asyncio
async def test_principal_crud(iam_manager):
    """Test creating and retrieving a principal with roles."""
    test_subject = f"test_{generate_test_id()}"
    principal = Principal(
        provider="local",
        subject_id=test_subject,
        roles=["admin", "special_viewer"],
        attributes={"name": "Test User"},
    )
    created_principal = await iam_manager.create_principal(principal)
    assert created_principal.id is not None
    assert "special_viewer" in created_principal.roles

    # Retrieve
    fetched = await iam_manager.get_principal(created_principal.id)
    assert fetched is not None
    assert fetched.subject_id == test_subject


@pytest.mark.asyncio
async def test_role_hierarchy(iam_manager):
    """Test role hierarchy resolution."""
    try:
        await iam_manager.storage.create_role(
            Role(name="viewer"), schema="iam"
        )
    except Exception:
        pass  # Ignore if role already exists

    try:
        await iam_manager.storage.create_role(
            Role(name="editor"), schema="iam"
        )
    except Exception:
        pass

    await iam_manager.storage.add_role_hierarchy(
        "editor", "viewer", schema="iam"
    )

    effective = await iam_manager.storage.get_role_hierarchy(
        ["editor"], schema="iam"
    )
    assert "viewer" in effective
    assert "editor" in effective
