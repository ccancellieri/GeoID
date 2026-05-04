"""Architectural invariants enforced by the IAM façade + capability-protocol
refactor. Pure static analysis — no app boot required.

If a future change violates one of these rules, the relevant test fails
with a direct pointer to the forbidden symbol. The allowlist lives here;
grow it deliberately when adding legitimate exceptions.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Iterator

import pytest

SRC_ROOT = Path(__file__).resolve().parent.parent / "src" / "dynastore"


def _iter_py(root: Path) -> Iterator[Path]:
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        yield path


def _imports(path: Path) -> list[tuple[int, str]]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return []
    out: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.append((node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.append((node.lineno, node.module))
    return out


# ---- Rule 1 ------------------------------------------------------------
# modules/iam/authorization/ must remain always-importable. It MUST NOT
# import any symbol that pulls the IAM-only extras (pydantic[email], PyJWT)
# or ties it to FastAPI.

_IAM_AUTHZ = SRC_ROOT / "modules" / "iam" / "authorization"

_FORBIDDEN_IAM_AUTHZ_MODULES = {
    "jwt",
    "fastapi",
    "starlette",
}

_FORBIDDEN_IAM_AUTHZ_SYMBOL_PATTERNS = [
    # `from pydantic import EmailStr` or `from pydantic.networks import EmailStr`
    re.compile(r"\bEmailStr\b"),
]


def test_authorization_submodule_has_no_heavy_deps() -> None:
    violations: list[str] = []
    for path in _iter_py(_IAM_AUTHZ):
        for lineno, module in _imports(path):
            root = module.split(".")[0]
            if root in _FORBIDDEN_IAM_AUTHZ_MODULES:
                violations.append(f"{path}:{lineno} imports '{module}'")
        text = path.read_text(encoding="utf-8")
        for pat in _FORBIDDEN_IAM_AUTHZ_SYMBOL_PATTERNS:
            for m in pat.finditer(text):
                line = text[: m.start()].count("\n") + 1
                violations.append(f"{path}:{line} references '{m.group(0)}'")
    assert not violations, (
        "modules/iam/authorization/ must remain framework-free and importable "
        "without IAM extras. Violations:\n  " + "\n  ".join(violations)
    )


# ---- Rule 2 ------------------------------------------------------------
# Extensions MUST NOT top-level-import sibling extensions. Cross-extension
# composition goes through capability protocols in models/protocols/.
#
# Allowed cross-imports:
#   * extensions/tools/** — shared infra, everyone uses it
#   * extensions/ogc_base.py — OGCServiceMixin
#   * extensions/protocols.py / registry.py — framework base
#   * extensions/iam/guards.py — framework-free FastAPI wrappers
#   * extensions/documentation.py — opt-in metadata helper
#   * extensions/XXX/ → extensions/XXX/** (self-imports always ok)

_EXTENSIONS_ROOT = SRC_ROOT / "extensions"

_ALLOWED_CROSS_EXTENSION_PREFIXES = (
    # Framework / shared infra
    "dynastore.extensions.tools",
    "dynastore.extensions.ogc_base",
    "dynastore.extensions.protocols",
    "dynastore.extensions.registry",
    "dynastore.extensions.documentation",
    # Shared OGC multi-item ingestion response models (BulkCreationResponse,
    # IngestionReport, SidecarRejection) used by all OGCTransactionMixin extensions.
    "dynastore.extensions.ogc_models_shared",
    # Framework-free IAM FastAPI wrappers (always-importable)
    "dynastore.extensions.iam.guards",
    # De-facto shared infra: the @expose_web_page decorator and static
    # helpers live inside extensions/web/. Every UI-bearing extension uses
    # them. This is the one path every extension has the decorator on.
    "dynastore.extensions.web.decorators",
    # Auth dependencies are the shared JWT + IAM context helpers.
    "dynastore.extensions.auth.dependencies",
)

# Ratchet: known cross-extension imports that predate the capability-protocol
# refactor. New entries must not be added. Fix an entry by replacing the
# import with a get_protocols(<CapabilityProtocol>) call and remove it here.
_WAIVED_CROSS_EXTENSION_IMPORTS: frozenset[str] = frozenset(
    {
        # stac → dimensions: STAC virtual items inspect the dimensions ext.
        "extensions/stac/stac_virtual.py:dynastore.extensions.dimensions.dimensions_extension",
        # records → features: shares OGC feature rendering.
        "extensions/records/records_service.py:dynastore.extensions.features.features_service",
        # features ↔ stac: items sidecar straddles both protocols.
        "extensions/features/ogc_generator.py:dynastore.extensions.stac.stac_items_sidecar",
        "extensions/features/features_service.py:dynastore.extensions.stac.stac_items_sidecar",
        # stac → search: STAC API search dispatches to the platform-tier
        # SearchService when ES is the resolved READ primary and the query
        # is structural-only. Slated for capability-protocol promotion
        # (ItemSearchProtocol in models/protocols/) once #222 follow-up lands.
        "extensions/stac/search.py:dynastore.extensions.search.search_models",
        "extensions/stac/search.py:dynastore.extensions.search.search_service",
        # Shared HTTP client — candidate for promotion to extensions/tools.
        "extensions/proxy/proxy_service.py:dynastore.extensions.httpx.httpx_service",
        "extensions/gdal/gdal_service.py:dynastore.extensions.httpx.httpx_service",
        "extensions/template/templating.py:dynastore.extensions.httpx.httpx_service",
        # Top-level `extensions.web` import (package side-effects, not the
        # decorator module). Candidate for narrowing to .decorators.
        "extensions/maps/maps_service.py:dynastore.extensions.web",
        "extensions/stac/stac_service.py:dynastore.extensions.web",
        "extensions/auth/authentication.py:dynastore.extensions.web",
        "extensions/tiles/tiles_service.py:dynastore.extensions.web",
        "extensions/iam/service.py:dynastore.extensions.web",
        "extensions/notebooks/notebooks_extension.py:dynastore.extensions.web",
    }
)


def _own_extension(path: Path) -> str | None:
    try:
        rel = path.relative_to(_EXTENSIONS_ROOT)
    except ValueError:
        return None
    parts = rel.parts
    # extensions/foo/bar.py → "foo"; extensions/foo.py → "foo"
    if len(parts) == 1:
        return parts[0].removesuffix(".py")
    return parts[0]


def test_extensions_do_not_cross_import() -> None:
    new_violations: list[str] = []
    stale_waivers: set[str] = set(_WAIVED_CROSS_EXTENSION_IMPORTS)
    for path in _iter_py(_EXTENSIONS_ROOT):
        own = _own_extension(path)
        if own is None:
            continue
        rel = path.relative_to(SRC_ROOT)
        for lineno, module in _imports(path):
            if not module.startswith("dynastore.extensions."):
                continue
            if module.startswith(_ALLOWED_CROSS_EXTENSION_PREFIXES):
                continue
            if module == f"dynastore.extensions.{own}" or module.startswith(
                f"dynastore.extensions.{own}."
            ):
                continue
            key = f"{rel}:{module}"
            if key in _WAIVED_CROSS_EXTENSION_IMPORTS:
                stale_waivers.discard(key)
                continue
            new_violations.append(f"{path}:{lineno} imports '{module}' (key: {key})")
    errors: list[str] = []
    if new_violations:
        errors.append(
            "New cross-extension imports detected. Use a capability Protocol "
            "in models/protocols/, or — if the import is legitimately shared "
            "infra — add it to `_ALLOWED_CROSS_EXTENSION_PREFIXES`:\n  "
            + "\n  ".join(new_violations)
        )
    if stale_waivers:
        errors.append(
            "Waivers refer to imports that no longer exist. Remove them from "
            "`_WAIVED_CROSS_EXTENSION_IMPORTS`:\n  "
            + "\n  ".join(sorted(stale_waivers))
        )
    assert not errors, "\n\n".join(errors)


# ---- Rule 3 ------------------------------------------------------------
# Legacy symbols deleted in the refactor must stay deleted. A regression
# here means a stale branch was merged or a revert snuck through.

_DELETED_SYMBOLS = {
    "IamProtocol": "split into Authenticator/Authorizer/RoleAdmin/PrincipalAdmin",
    "register_conformance_uris": "replaced by ConformanceContributor",
    "scan_and_register_providers": "replaced by WebPageContributor",
    "require_sysadmin_privileges": "replaced by Permission.SYSADMIN via require_permission",
    "require_admin_privileges": "replaced by Permission.ADMIN via require_permission",
    "require_sysadmin": "route-level guards replaced by IamMiddleware policy evaluation",
    "require_admin": "route-level guards replaced by IamMiddleware policy evaluation",
    "require_authenticated": "route-level guards replaced by IamMiddleware policy evaluation",
    "require_bearer_token": "route-level guards replaced by IamMiddleware policy evaluation",
    "ensure_sysadmin_if_targeting_admin": "replaced by ensure_privileged_role_assignment",
    "migration_runner": "versioned SQL migration framework removed; use CREATE TABLE IF NOT EXISTS",
    "register_module_migrations": "versioned SQL migration framework removed",
    "register_tenant_migrations": "versioned SQL migration framework removed",
    "SchemaEvolutionEngine": "online schema evolution framework removed",
    "StructuralMigrationTask": "migration framework removed",
    "SchemaMigrationTask": "migration framework removed",
}

_DELETED_MODULES = {
    "dynastore.extensions.tools.security": "replaced by dynastore.extensions.iam.guards",
    "dynastore.models.protocols.iam": "replaced by authentication/authorization/role_admin/principal_admin",
    "dynastore.modules.db_config.migration_runner": "versioned SQL migration framework removed",
    "dynastore.modules.catalog.schema_evolution": "online schema evolution framework removed",
    "dynastore.tasks.schema_migration": "migration framework removed",
    "dynastore.tasks.structural_migration": "migration framework removed",
    "dynastore.extensions.admin.migration_routes": "migration admin routes removed",
}

# Allow references inside this test file (naming the deleted symbols) and
# inside frozen historical plan artifacts under docs/superpowers/plans/.
_SYMBOL_SCAN_ROOT = SRC_ROOT


@pytest.mark.parametrize(
    "symbol,reason",
    sorted(_DELETED_SYMBOLS.items()),
)
def test_deleted_symbol_stays_deleted(symbol: str, reason: str) -> None:
    pat = re.compile(rf"\b{re.escape(symbol)}\b")
    hits: list[str] = []
    for path in _iter_py(_SYMBOL_SCAN_ROOT):
        text = path.read_text(encoding="utf-8")
        for m in pat.finditer(text):
            line = text[: m.start()].count("\n") + 1
            hits.append(f"{path}:{line}")
    assert not hits, f"'{symbol}' — {reason}. Found at:\n  " + "\n  ".join(hits)


@pytest.mark.parametrize(
    "module,reason",
    sorted(_DELETED_MODULES.items()),
)
def test_deleted_module_stays_deleted(module: str, reason: str) -> None:
    hits: list[str] = []
    for path in _iter_py(_SYMBOL_SCAN_ROOT):
        for lineno, mod in _imports(path):
            if mod == module or mod.startswith(module + "."):
                hits.append(f"{path}:{lineno}")
    assert not hits, f"'{module}' — {reason}. Found at:\n  " + "\n  ".join(hits)
