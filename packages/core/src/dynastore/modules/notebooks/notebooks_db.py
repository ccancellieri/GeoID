# src/dynastore/modules/notebooks/notebooks_db.py
"""DDL and CRUD for {tenant_schema}.notebooks (per-catalog, tenant-isolated).

title and description are stored as JSONB (LocalizedText — multilanguage).
tags is JSONB array for searchable tag filtering.
"""
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from dynastore.modules.db_config.exceptions import ResourceNotFoundError
from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, ResultHandler

from .models import NotebookCreate

logger = logging.getLogger(__name__)

NOTEBOOKS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.notebooks (
    notebook_id VARCHAR NOT NULL,
    catalog_id VARCHAR NOT NULL,
    title JSONB,
    description JSONB,
    tags JSONB DEFAULT '[]'::jsonb,
    content JSONB NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ DEFAULT NULL,
    owner_id VARCHAR,
    copied_from VARCHAR DEFAULT NULL,
    PRIMARY KEY (notebook_id)
);
"""


async def init_notebooks_storage(conn, schema: str, catalog_id: str) -> None:
    """Initialize notebooks storage for a tenant schema (idempotent)."""
    await DDLQuery(NOTEBOOKS_DDL).execute(conn, schema=schema)


async def get_notebook(conn, schema: str, notebook_id: str) -> Dict[str, Any]:
    """Retrieve a notebook by notebook_id (active only)."""
    query = text(f"""
        SELECT notebook_id, catalog_id, title, description, tags, content, metadata,
               created_at, updated_at, deleted_at, owner_id, copied_from
        FROM {schema}.notebooks
        WHERE notebook_id = :notebook_id AND deleted_at IS NULL
    """)
    row = await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(
        conn, notebook_id=notebook_id
    )
    if not row:
        raise ResourceNotFoundError(f"Notebook '{notebook_id}' not found")
    return row


async def list_notebooks(
    conn,
    schema: str,
    *,
    q: Optional[str] = None,
    tags: Optional[List[str]] = None,
    limit: int = 20,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    """List active notebooks with optional full-text and tag filtering.

    Args:
        q: Search string — case-insensitive substring match across all language
           values in ``title`` and ``description`` JSONB fields.
        tags: Filter to notebooks whose ``tags`` array contains ALL provided tags.
        limit: Max results per page.
        offset: Number of results to skip.

    Returns:
        ``(items, total_count)`` tuple for pagination.
    """
    where_clauses = ["deleted_at IS NULL"]
    params: Dict[str, Any] = {"limit": limit, "offset": offset}

    if q:
        # ILIKE search across all language values in title and description JSONB
        where_clauses.append(
            "(EXISTS (SELECT 1 FROM jsonb_each_text(title) WHERE value ILIKE :q_pat) "
            " OR EXISTS (SELECT 1 FROM jsonb_each_text(description) WHERE value ILIKE :q_pat))"
        )
        params["q_pat"] = f"%{q}%"

    if tags:
        where_clauses.append("tags @> CAST(:tags_json AS jsonb)")
        params["tags_json"] = json.dumps(tags)

    where_sql = " AND ".join(where_clauses)

    count_query = text(f"SELECT COUNT(*) FROM {schema}.notebooks WHERE {where_sql}")
    total_count = await DQLQuery(
        count_query, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
    ).execute(conn, **{k: v for k, v in params.items() if k not in ("limit", "offset")})
    total_count = total_count or 0

    list_query = text(f"""
        SELECT notebook_id, catalog_id, title, description, tags, metadata,
               created_at, updated_at, owner_id, copied_from
        FROM {schema}.notebooks
        WHERE {where_sql}
        ORDER BY updated_at DESC
        LIMIT :limit OFFSET :offset
    """)
    rows = await DQLQuery(list_query, result_handler=ResultHandler.ALL_DICTS).execute(
        conn, **params
    )
    return rows, total_count


async def soft_delete_notebook(conn, schema: str, notebook_id: str) -> None:
    """Soft-delete a notebook."""
    rowcount = await DQLQuery(
        f"UPDATE {schema}.notebooks SET deleted_at = NOW() WHERE notebook_id = :notebook_id AND deleted_at IS NULL",
        result_handler=ResultHandler.ROWCOUNT,
    ).execute(conn, notebook_id=notebook_id)
    if not rowcount:
        raise ResourceNotFoundError(f"Notebook '{notebook_id}' not found")


def _serialize_localized(value) -> Optional[str]:
    """Serialize LocalizedText or dict to JSON string for DB insert."""
    if value is None:
        return None
    if isinstance(value, dict):
        return json.dumps(value)
    if hasattr(value, "model_dump"):
        return json.dumps({k: v for k, v in value.model_dump().items() if v is not None})
    return json.dumps(str(value))


async def save_notebook(
    conn,
    schema: str,
    catalog_id: str,
    notebook: NotebookCreate,
    owner_id: Optional[str] = None,
    copied_from: Optional[str] = None,
) -> Dict[str, Any]:
    """Save or update a notebook."""
    query = text(f"""
        INSERT INTO {schema}.notebooks
            (notebook_id, catalog_id, title, description, tags, content, metadata,
             owner_id, copied_from, updated_at)
        VALUES
            (:notebook_id, :catalog_id, :title, :description, :tags, :content,
             :metadata, :owner_id, :copied_from, NOW())
        ON CONFLICT (notebook_id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            tags = EXCLUDED.tags,
            content = EXCLUDED.content,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING notebook_id, catalog_id, title, description, tags, content, metadata,
                  created_at, updated_at, deleted_at, owner_id, copied_from
    """)
    params = {
        "notebook_id": notebook.notebook_id,
        "catalog_id": catalog_id,
        "title": _serialize_localized(notebook.title),
        "description": _serialize_localized(notebook.description),
        "tags": json.dumps(notebook.tags),
        "content": json.dumps(notebook.content),
        "metadata": json.dumps(notebook.metadata),
        "owner_id": owner_id,
        "copied_from": copied_from,
    }
    return await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(conn, **params)


async def copy_from_platform(
    conn,
    schema: str,
    catalog_id: str,
    platform_notebook: Dict[str, Any],
    owner_id: str,
) -> Dict[str, Any]:
    """Copy a platform notebook into a tenant catalog (no-op if already exists)."""
    query = text(f"""
        INSERT INTO {schema}.notebooks
            (notebook_id, catalog_id, title, description, tags, content, metadata,
             owner_id, copied_from)
        VALUES
            (:notebook_id, :catalog_id, :title, :description, :tags, :content,
             :metadata, :owner_id, :copied_from)
        ON CONFLICT (notebook_id) DO NOTHING
        RETURNING notebook_id, catalog_id, title, description, tags, content, metadata,
                  created_at, updated_at, deleted_at, owner_id, copied_from
    """)
    title = platform_notebook.get("title")
    description = platform_notebook.get("description")
    tags = platform_notebook.get("tags", [])
    content = platform_notebook["content"]
    metadata = platform_notebook.get("metadata", {})
    params = {
        "notebook_id": platform_notebook["notebook_id"],
        "catalog_id": catalog_id,
        "title": _serialize_localized(title) if not isinstance(title, str) else json.dumps({"en": title}),
        "description": _serialize_localized(description),
        "tags": json.dumps(tags) if isinstance(tags, list) else tags,
        "content": json.dumps(content) if isinstance(content, dict) else content,
        "metadata": json.dumps(metadata) if isinstance(metadata, dict) else metadata,
        "owner_id": owner_id,
        "copied_from": platform_notebook["notebook_id"],
    }
    row = await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(conn, **params)
    if not row:
        return await get_notebook(conn, schema, platform_notebook["notebook_id"])
    return row
