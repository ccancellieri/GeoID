"""DDL and CRUD operations for notebooks.platform_notebooks (global, cross-tenant).

Read operations use @cached for performance across multi-instance deployments.
The database is the single source of truth; cache is invalidated on writes.
"""
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, ResultHandler, DbResource
from dynastore.modules.db_config.exceptions import ResourceNotFoundError
from dynastore.tools.cache import cached
from .models import PlatformNotebookCreate, OwnerType

logger = logging.getLogger(__name__)

PLATFORM_NOTEBOOKS_DDL = """
CREATE TABLE IF NOT EXISTS notebooks.platform_notebooks (
    notebook_id    VARCHAR NOT NULL PRIMARY KEY,
    title          JSONB NOT NULL,
    description    JSONB,
    tags           JSONB DEFAULT '[]'::jsonb,
    content        JSONB NOT NULL,
    metadata       JSONB DEFAULT '{}'::jsonb,
    registered_by  VARCHAR NOT NULL,
    owner_type     VARCHAR NOT NULL CHECK (owner_type IN ('module', 'sysadmin')),
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    deleted_at     TIMESTAMPTZ DEFAULT NULL
);
"""


def _serialize_localized(value) -> Optional[str]:
    """Serialize LocalizedText or plain str to JSON for DB insert."""
    if value is None:
        return None
    if isinstance(value, str):
        return json.dumps({"en": value})
    if isinstance(value, dict):
        return json.dumps(value)
    if hasattr(value, "model_dump"):
        return json.dumps({k: v for k, v in value.model_dump().items() if v is not None})
    return json.dumps(str(value))


async def seed_platform_notebook(conn: DbResource, notebook: PlatformNotebookCreate) -> None:
    """Insert or update a platform notebook (always syncs content from package on deploy)."""
    query = text("""
        INSERT INTO notebooks.platform_notebooks
            (notebook_id, title, description, tags, content, metadata, registered_by, owner_type)
        VALUES
            (:notebook_id, :title, :description, :tags, :content, :metadata, :registered_by, :owner_type)
        ON CONFLICT (notebook_id) DO UPDATE SET
            title       = EXCLUDED.title,
            description = EXCLUDED.description,
            tags        = EXCLUDED.tags,
            content     = EXCLUDED.content,
            metadata    = EXCLUDED.metadata
    """)
    params = {
        "notebook_id": notebook.notebook_id,
        "title": _serialize_localized(notebook.title),
        "description": _serialize_localized(notebook.description),
        "tags": json.dumps(notebook.tags),
        "content": json.dumps(notebook.content),
        "metadata": json.dumps(notebook.metadata),
        "registered_by": notebook.registered_by,
        "owner_type": notebook.owner_type.value,
    }
    await DQLQuery(query, result_handler=ResultHandler.NONE).execute(conn, **params)


async def seed_platform_notebooks(conn: DbResource) -> int:
    """Seed all registered platform notebooks. Returns count of entries processed."""
    from .example_registry import get_registered_notebooks

    entries = get_registered_notebooks()
    for entry in entries:
        await seed_platform_notebook(conn, entry)
    if entries:
        logger.info("Seeded/updated %d platform notebook(s).", len(entries))
    return len(entries)


async def list_platform_notebooks(
    conn: DbResource,
    *,
    q: Optional[str] = None,
    tags: Optional[List[str]] = None,
    limit: int = 20,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    """List active platform notebooks with optional filtering and pagination.

    Args:
        q: Case-insensitive substring match across all language values in title + description.
        tags: Filter to notebooks containing ALL provided tags.
        limit: Max results per page.
        offset: Number of results to skip.

    Returns:
        ``(items, total_count)`` for pagination.
    """
    where_clauses = ["deleted_at IS NULL"]
    params: Dict[str, Any] = {"limit": limit, "offset": offset}

    if q:
        where_clauses.append(
            "(EXISTS (SELECT 1 FROM jsonb_each_text(title) WHERE value ILIKE :q_pat) "
            " OR EXISTS (SELECT 1 FROM jsonb_each_text(description) WHERE value ILIKE :q_pat))"
        )
        params["q_pat"] = f"%{q}%"

    if tags:
        where_clauses.append("tags @> :tags_json")
        params["tags_json"] = json.dumps(tags)

    where_sql = " AND ".join(where_clauses)

    count_query = text(
        f"SELECT COUNT(*) FROM notebooks.platform_notebooks WHERE {where_sql}"
    )
    total_count = await DQLQuery(
        count_query, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
    ).execute(conn, **{k: v for k, v in params.items() if k not in ("limit", "offset")})
    total_count = total_count or 0

    list_query = text(f"""
        SELECT notebook_id, title, description, tags, metadata, registered_by, owner_type,
               created_at, updated_at
        FROM notebooks.platform_notebooks
        WHERE {where_sql}
        ORDER BY updated_at DESC
        LIMIT :limit OFFSET :offset
    """)
    rows = await DQLQuery(list_query, result_handler=ResultHandler.ALL_DICTS).execute(
        conn, **params
    )
    return rows, total_count


@cached(maxsize=256, ttl=300, jitter=30, namespace="platform_notebooks_get", ignore=["conn"])
async def get_platform_notebook(conn: DbResource, notebook_id: str) -> Dict[str, Any]:
    """Retrieve a platform notebook by ID. Cached for 5 min."""
    query = text("""
        SELECT notebook_id, title, description, content, metadata, registered_by,
               owner_type, created_at, updated_at, deleted_at
        FROM notebooks.platform_notebooks
        WHERE notebook_id = :notebook_id AND deleted_at IS NULL
    """)
    row = await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(
        conn, notebook_id=notebook_id
    )
    if not row:
        raise ResourceNotFoundError(f"Platform notebook '{notebook_id}' not found")
    return row


async def save_platform_notebook(
    conn: DbResource, notebook: PlatformNotebookCreate
) -> Dict[str, Any]:
    """Upsert a platform notebook (sysadmin use). Invalidates cache."""
    query = text("""
        INSERT INTO notebooks.platform_notebooks
            (notebook_id, title, description, tags, content, metadata, registered_by, owner_type, updated_at)
        VALUES
            (:notebook_id, :title, :description, :tags, :content, :metadata, :registered_by, :owner_type, NOW())
        ON CONFLICT (notebook_id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            tags = EXCLUDED.tags,
            content = EXCLUDED.content,
            metadata = EXCLUDED.metadata,
            registered_by = EXCLUDED.registered_by,
            owner_type = EXCLUDED.owner_type,
            updated_at = NOW(),
            deleted_at = NULL
        RETURNING notebook_id, title, description, tags, content, metadata, registered_by,
                  owner_type, created_at, updated_at, deleted_at
    """)
    params = {
        "notebook_id": notebook.notebook_id,
        "title": _serialize_localized(notebook.title),
        "description": _serialize_localized(notebook.description),
        "tags": json.dumps(notebook.tags),
        "content": json.dumps(notebook.content),
        "metadata": json.dumps(notebook.metadata),
        "registered_by": notebook.registered_by,
        "owner_type": notebook.owner_type.value,
    }
    result = await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(conn, **params)
    # Invalidate per-notebook cache after write. list_platform_notebooks is not cached.
    get_platform_notebook.cache_invalidate(conn, notebook.notebook_id)
    return result


async def soft_delete_platform_notebook(conn: DbResource, notebook_id: str) -> None:
    """Soft-delete a platform notebook. Invalidates cache."""
    query = text("""
        UPDATE notebooks.platform_notebooks
        SET deleted_at = NOW()
        WHERE notebook_id = :notebook_id AND deleted_at IS NULL
        RETURNING notebook_id
    """)
    result = await DQLQuery(
        query, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
    ).execute(conn, notebook_id=notebook_id)
    if result is None:
        raise ResourceNotFoundError(f"Platform notebook '{notebook_id}' not found")
    # Invalidate per-notebook cache after delete. list_platform_notebooks is not cached.
    get_platform_notebook.cache_invalidate(conn, notebook_id)
