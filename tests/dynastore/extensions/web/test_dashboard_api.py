"""
Integration tests for the Web extension's dashboard API endpoints.

All tests run against a real in-process FastAPI application (no mocks) with a
live database, using the sysadmin_in_process_client_module fixture which already
implies app_lifespan startup.

Enabled extensions: web, logs, features, stac
Enabled modules:    db_config, db, catalog, stats, iam

Fixture notes:
- sysadmin_in_process_client_module  — authenticated HTTP client; implicitly starts app
- my_catalog_id               — creates/tears down a real catalog around the test
- catalog_admin_dashboard_ctx — non-sysadmin client + two catalogs (admin grant on A only)
"""

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.authentication import AuthenticatorProtocol
from dynastore.modules.catalog.models import Catalog
# IamService imported here only for test-fixture setup (create_principal
# + identity_link + grant). Production code MUST consume IAM via the
# Protocol layer (models/protocols/) — gating happens via policies registered
# through PermissionProtocol (see register_web_policies() in web.py).
from dynastore.modules.iam.iam_service import IamService
from dynastore.models.auth import Principal as IamPrincipal
from tests.dynastore.test_utils import generate_test_id

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.enable_extensions("logs", "events", "features", "stac"),
    # 'stac' MODULE (not extension) registers CollectionStacPostgresqlDriver +
    # creates the collection_stac sidecar table at lifespan DDL time;
    # without it any catalog write blows up at first STAC-slice insert.
    pytest.mark.enable_modules("db_config", "db", "catalog", "stats", "iam", "stac"),
]


@pytest.fixture
async def my_catalog_id(app_lifespan_module, catalog_id):
    """Fixture that actually creates a catalog in the database for these tests."""
    catalogs = get_protocol(CatalogsProtocol)
    cat = Catalog(id=catalog_id, title={"en": "Test Dashboard Catalog"})
    await catalogs.create_catalog(cat.model_dump(), lang="*")
    yield catalog_id
    try:
        await catalogs.delete_catalog(catalog_id, force=True)
    except Exception:
        pass


async def _mint_internal_jwt(roles, subject):
    """Mint an HS256 JWT recognised by IamService's internal fallback."""
    import jwt as pyjwt
    from datetime import datetime, timezone, timedelta

    iam_svc = get_protocol(AuthenticatorProtocol)
    secret = await iam_svc.get_jwt_secret()
    payload = {
        "sub": subject,
        "roles": roles,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "iss": "dynastore-test",
    }
    return pyjwt.encode(payload, secret, algorithm="HS256")


@pytest_asyncio.fixture
async def catalog_admin_dashboard_ctx(app_lifespan_module):
    """Provision two catalogs and a non-sysadmin admin granted on catalog A only.

    Yields a dict:
        client       — AsyncClient with Bearer header for the catalog admin
        catalog_a    — id of the catalog where the principal holds an admin grant
        catalog_b    — id of the second catalog (no grant — used for cross-tenant denial)
        subject_id   — principal's external subject id (for assertions)

    The subject is created with provider="internal" so it matches the JWT decoded
    by IamService's HS256 fallback path (used by all in-process test fixtures).
    """
    catalogs_svc = get_protocol(CatalogsProtocol)
    iam = get_protocol(IamService)

    suffix = generate_test_id()
    catalog_a = f"dash-a-{suffix}"
    catalog_b = f"dash-b-{suffix}"

    await catalogs_svc.create_catalog(
        Catalog(id=catalog_a, title={"en": "Dashboard A"}).model_dump(), lang="*"
    )
    await catalogs_svc.create_catalog(
        Catalog(id=catalog_b, title={"en": "Dashboard B"}).model_dump(), lang="*"
    )

    subject_id = f"dash-admin-{suffix}"
    principal = IamPrincipal(
        provider="internal",
        subject_id=subject_id,
        roles=["admin"],
    )
    # catalog_id targets the schema where the admin grant lands; identity_link
    # is platform-global and resolves regardless.
    await iam.create_principal(principal, catalog_id=catalog_a)

    token = await _mint_internal_jwt(roles=["admin"], subject=subject_id)
    headers = {"Authorization": f"Bearer {token}"}
    transport = ASGITransport(app=app_lifespan_module.app)
    async with AsyncClient(
        transport=transport, base_url="http://test", headers=headers
    ) as client:
        yield {
            "client": client,
            "catalog_a": catalog_a,
            "catalog_b": catalog_b,
            "subject_id": subject_id,
        }

    # Best-effort teardown — deleting the catalog cascades grants in its schema.
    for cid in (catalog_a, catalog_b):
        try:
            await catalogs_svc.delete_catalog(cid, force=True)
        except Exception:
            pass


@pytest_asyncio.fixture
async def anonymous_dashboard_client(app_lifespan_module):
    """In-process client with no Authorization header — anonymous caller."""
    transport = ASGITransport(app=app_lifespan_module.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

# @pytest.mark.skip()
async def test_dashboard_stats_endpoint(
    sysadmin_in_process_client_module: AsyncClient, my_catalog_id: str
):
    """Test the dashboard stats endpoint returns 200 (path-based catalog scope)."""
    response = await sysadmin_in_process_client_module.get(
        f"/web/dashboard/catalogs/{my_catalog_id}/stats"
    )
    assert response.status_code == 200
    data = response.json()
    assert "total_requests" in data
    assert "average_latency_ms" in data


async def test_canonical_logs_endpoint(
    sysadmin_in_process_client_module: AsyncClient, my_catalog_id: str
):
    """Per-catalog logs flow through the canonical /logs/ surface; the
    dashboard proxy was removed in favour of direct browser calls."""
    response = await sysadmin_in_process_client_module.get(
        f"/logs/catalogs/{my_catalog_id}?limit=5"
    )
    assert response.status_code == 200
    data = response.json()
    assert "logs" in data
    assert isinstance(data["logs"], list)


async def test_canonical_events_endpoint(
    sysadmin_in_process_client_module: AsyncClient, my_catalog_id: str
):
    """Per-catalog events flow through the canonical /events/ surface."""
    response = await sysadmin_in_process_client_module.get(
        f"/events/catalogs/{my_catalog_id}/events?limit=5"
    )
    assert response.status_code == 200


async def test_dashboard_catalogs_endpoint(
    sysadmin_in_process_client_module: AsyncClient,
):
    """Test the dashboard catalogs endpoint returns 200 and a list."""
    response = await sysadmin_in_process_client_module.get("/web/dashboard/catalogs")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


async def test_dashboard_collections_endpoint(
    sysadmin_in_process_client_module: AsyncClient, my_catalog_id: str
):
    """Test the dashboard collections endpoint returns 200 and a list."""
    response = await sysadmin_in_process_client_module.get(
        f"/web/dashboard/catalogs/{my_catalog_id}/collections"
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


async def test_health_endpoint(sysadmin_in_process_client_module: AsyncClient):
    """Test the health endpoint returns ok status."""
    response = await sysadmin_in_process_client_module.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data.get("status") == "ok"


async def test_web_pages_config_endpoint(sysadmin_in_process_client_module: AsyncClient):
    """Test the web pages config endpoint returns a list."""
    response = await sysadmin_in_process_client_module.get("/web/config/pages?language=en")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


async def test_docs_manifest_endpoint(sysadmin_in_process_client_module: AsyncClient):
    """Test the docs manifest endpoint returns a dict bucketed by category."""
    response = await sysadmin_in_process_client_module.get("/web/docs-manifest")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    for category, items in data.items():
        assert isinstance(items, list)
        for item in items:
            assert "id" in item
            assert "title" in item


async def test_canonical_logs_pagination(sysadmin_in_process_client_module: AsyncClient, my_catalog_id: str):
    """Canonical /logs/ surface respects the limit parameter."""
    response = await sysadmin_in_process_client_module.get(
        f"/logs/catalogs/{my_catalog_id}?limit=1"
    )
    assert response.status_code == 200
    data = response.json()
    assert "logs" in data
    assert len(data["logs"]) <= 1


async def test_dashboard_collections_unknown_catalog(
    sysadmin_in_process_client_module: AsyncClient,
):
    """
    Requesting collections for a non-existent catalog returns 200 with an empty
    list — the endpoint delegates to CollectionsProtocol which returns [] when
    the catalog has no collections (or does not exist), rather than raising 404.
    """
    response = await sysadmin_in_process_client_module.get(
        "/web/dashboard/catalogs/nonexistent-catalog-xyz/collections"
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert data == []


# ---------------------------------------------------------------------------
# Two dashboard tiers — both gated by Policies registered via PermissionProtocol:
#   - Platform-tier (sysadmin-only): bare /web/dashboard/{stats|logs|events|
#     tasks|ogc-compliance} — gated by web_dashboard_platform_access policy.
#   - Per-catalog: /web/dashboard/catalogs/{catalog_id}[/collections/{col}]/...
#     — gated by web_dashboard_per_catalog_access policy + the
#     catalog_membership_required ConditionHandler.
# The old /web/dashboard/catalogs/_system_/... URL shape no longer exists.
# ---------------------------------------------------------------------------

# Endpoint suffix → kept short so the parametrize stays readable.
# logs and events are intentionally absent: their /web/dashboard/{logs,events}
# proxy routes were removed; callers use the canonical /logs/ and /events/
# surfaces directly. The per-catalog access-control tests below cover those.
_DASHBOARD_DATA_SUFFIXES = ["stats", "tasks", "ogc-compliance"]


@pytest.mark.parametrize("suffix", _DASHBOARD_DATA_SUFFIXES)
async def test_dashboard_data_endpoints_reject_anonymous(
    anonymous_dashboard_client: AsyncClient, suffix: str
):
    """Anonymous callers must not read any per-catalog dashboard data."""
    response = await anonymous_dashboard_client.get(
        f"/web/dashboard/catalogs/some_catalog/{suffix}"
    )
    # IamMiddleware default-deny returns 403 when no policy ALLOWs.
    assert response.status_code in (401, 403)


@pytest.mark.parametrize("suffix", _DASHBOARD_DATA_SUFFIXES)
async def test_dashboard_platform_endpoints_sysadmin_allowed(
    sysadmin_in_process_client_module: AsyncClient, suffix: str
):
    """Sysadmin may read every bare platform-tier dashboard endpoint."""
    response = await sysadmin_in_process_client_module.get(
        f"/web/dashboard/{suffix}"
    )
    assert response.status_code == 200


@pytest.mark.parametrize("suffix", _DASHBOARD_DATA_SUFFIXES)
async def test_dashboard_platform_endpoints_anonymous_denied(
    anonymous_dashboard_client: AsyncClient, suffix: str
):
    """Anonymous callers may not read any platform-tier dashboard endpoint."""
    response = await anonymous_dashboard_client.get(f"/web/dashboard/{suffix}")
    assert response.status_code in (401, 403)


async def test_dashboard_stats_catalog_admin_allowed_on_own_catalog(
    catalog_admin_dashboard_ctx,
):
    ctx = catalog_admin_dashboard_ctx
    response = await ctx["client"].get(
        f"/web/dashboard/catalogs/{ctx['catalog_a']}/stats"
    )
    assert response.status_code == 200
    body = response.json()
    assert "total_requests" in body


async def test_dashboard_stats_catalog_admin_denied_on_other_catalog(
    catalog_admin_dashboard_ctx,
):
    ctx = catalog_admin_dashboard_ctx
    response = await ctx["client"].get(
        f"/web/dashboard/catalogs/{ctx['catalog_b']}/stats"
    )
    assert response.status_code == 403


async def test_dashboard_platform_stats_catalog_admin_denied(
    catalog_admin_dashboard_ctx,
):
    """Catalog admin may not read the bare platform-tier /web/dashboard/stats."""
    ctx = catalog_admin_dashboard_ctx
    response = await ctx["client"].get("/web/dashboard/stats")
    assert response.status_code in (401, 403)


@pytest.mark.parametrize("endpoint,url_fn,assert_fn", [
    # logs canonical surface returns {"logs": [...], "links": [...]}
    ("logs", lambda cat: f"/logs/catalogs/{cat}?limit=5", lambda body: "logs" in body),
    # events canonical surface returns a list of event dicts
    ("events", lambda cat: f"/events/catalogs/{cat}/events?limit=5", lambda body: isinstance(body, list)),
])
async def test_canonical_logs_events_catalog_admin_allowed_on_own_catalog(
    catalog_admin_dashboard_ctx, endpoint: str, url_fn, assert_fn
):
    """Catalog admin can reach the canonical logs/events surfaces for their own catalog."""
    ctx = catalog_admin_dashboard_ctx
    response = await ctx["client"].get(url_fn(ctx["catalog_a"]))
    assert response.status_code == 200
    assert assert_fn(response.json())


@pytest.mark.parametrize("endpoint,url_fn", [
    ("logs", lambda cat: f"/logs/catalogs/{cat}?limit=5"),
    ("events", lambda cat: f"/events/catalogs/{cat}/events?limit=5"),
])
async def test_canonical_logs_events_catalog_admin_denied_on_other_catalog(
    catalog_admin_dashboard_ctx, endpoint: str, url_fn
):
    """Catalog admin is denied on canonical logs/events for a catalog they don't own."""
    ctx = catalog_admin_dashboard_ctx
    response = await ctx["client"].get(url_fn(ctx["catalog_b"]))
    assert response.status_code == 403


@pytest.mark.parametrize("endpoint", ["tasks", "ogc-compliance"])
async def test_dashboard_phase_b_plus_catalog_admin_owned(
    catalog_admin_dashboard_ctx, endpoint: str
):
    """Tasks + ogc-compliance are gated under the per-catalog path too."""
    ctx = catalog_admin_dashboard_ctx
    response = await ctx["client"].get(
        f"/web/dashboard/catalogs/{ctx['catalog_a']}/{endpoint}"
    )
    assert response.status_code == 200


@pytest.mark.parametrize("endpoint", ["tasks", "ogc-compliance"])
async def test_dashboard_phase_b_plus_catalog_admin_denied_on_other_catalog(
    catalog_admin_dashboard_ctx, endpoint: str
):
    ctx = catalog_admin_dashboard_ctx
    response = await ctx["client"].get(
        f"/web/dashboard/catalogs/{ctx['catalog_b']}/{endpoint}"
    )
    assert response.status_code == 403


@pytest.mark.parametrize("endpoint", ["tasks", "ogc-compliance"])
async def test_dashboard_platform_tasks_compliance_catalog_admin_denied(
    catalog_admin_dashboard_ctx, endpoint: str
):
    """Catalog admin may not read bare /web/dashboard/{tasks|ogc-compliance}."""
    ctx = catalog_admin_dashboard_ctx
    response = await ctx["client"].get(f"/web/dashboard/{endpoint}")
    assert response.status_code in (401, 403)


# ---------------------------------------------------------------------------
# Per-catalog HTML shell + per-catalog processes shell
# ---------------------------------------------------------------------------


async def test_dashboard_per_catalog_shell_anonymous_blocked(
    anonymous_dashboard_client: AsyncClient,
):
    """The per-catalog dashboard HTML shell is gated; anonymous denied."""
    response = await anonymous_dashboard_client.get(
        "/web/dashboard/catalogs/some_catalog/"
    )
    assert response.status_code in (401, 403)


async def test_dashboard_per_catalog_processes_shell_anonymous_blocked(
    anonymous_dashboard_client: AsyncClient,
):
    """The per-catalog processes HTML shell is gated; anonymous denied."""
    response = await anonymous_dashboard_client.get(
        "/web/dashboard/catalogs/some_catalog/processes/"
    )
    assert response.status_code in (401, 403)


async def test_dashboard_per_catalog_shell_owned(
    catalog_admin_dashboard_ctx,
):
    """Catalog admin can load the per-catalog HTML shell for their own catalog."""
    ctx = catalog_admin_dashboard_ctx
    response = await ctx["client"].get(
        f"/web/dashboard/catalogs/{ctx['catalog_a']}/"
    )
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# Collection-scoped routes (NEW): /catalogs/{cat}/collections/{col}/{stats,logs,events}
# ---------------------------------------------------------------------------

# logs and events collection-scoped dashboard proxies were deleted too.
_COLLECTION_DATA_SUFFIXES = ["stats"]


@pytest.mark.parametrize("suffix", _COLLECTION_DATA_SUFFIXES)
async def test_collection_scoped_endpoints_reject_anonymous(
    anonymous_dashboard_client: AsyncClient, suffix: str
):
    response = await anonymous_dashboard_client.get(
        f"/web/dashboard/catalogs/some_catalog/collections/some_coll/{suffix}"
    )
    assert response.status_code in (401, 403)


@pytest.mark.parametrize("suffix", _COLLECTION_DATA_SUFFIXES)
async def test_collection_scoped_endpoints_catalog_admin_owned(
    catalog_admin_dashboard_ctx, suffix: str
):
    """Catalog admin can read collection-scoped data for an owned catalog."""
    ctx = catalog_admin_dashboard_ctx
    response = await ctx["client"].get(
        f"/web/dashboard/catalogs/{ctx['catalog_a']}/collections/any_coll/{suffix}"
    )
    assert response.status_code == 200


@pytest.mark.parametrize("suffix", _COLLECTION_DATA_SUFFIXES)
async def test_collection_scoped_endpoints_catalog_admin_other_catalog_403(
    catalog_admin_dashboard_ctx, suffix: str
):
    """Per-collection paths inherit the catalog gate — non-owned catalog 403s."""
    ctx = catalog_admin_dashboard_ctx
    response = await ctx["client"].get(
        f"/web/dashboard/catalogs/{ctx['catalog_b']}/collections/any_coll/{suffix}"
    )
    assert response.status_code == 403
