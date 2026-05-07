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

# dynastore/modules/styles/db.py

import uuid
import logging
from typing import List, Optional
import json

from sqlalchemy import text

from dynastore.modules.db_config.query_executor import DbResource, DQLQuery, ResultHandler
from .models import Style, StyleCreate, StyleUpdate, Link, StyleSheet

logger = logging.getLogger(__name__)

# --- Query Definitions ---

_create_style_query = DQLQuery(
    """
    INSERT INTO styles.styles (catalog_id, collection_id, style_id, title, description, keywords, stylesheets)
    VALUES (:catalog_id, :collection_id, :style_id, :title, :description, :keywords, :stylesheets)
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT,
)

_get_style_by_id_query = DQLQuery(
    "SELECT * FROM styles.styles WHERE catalog_id = :catalog_id AND style_id = :style_id;",
    result_handler=ResultHandler.ONE_DICT,
)

_get_style_by_id_and_collection_query = DQLQuery(
    "SELECT * FROM styles.styles WHERE catalog_id = :catalog_id AND collection_id = :collection_id AND style_id = :style_id;",
    result_handler=ResultHandler.ONE_DICT,
)

_list_styles_query = DQLQuery(
    """
    SELECT * FROM styles.styles
    WHERE catalog_id = :catalog_id AND collection_id = :collection_id
    ORDER BY style_id
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

_list_all_styles_query = DQLQuery(
    """
    SELECT * FROM styles.styles
    ORDER BY catalog_id, collection_id, style_id
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

_delete_style_query = DQLQuery(
    "DELETE FROM styles.styles WHERE catalog_id = :catalog_id AND id = :style_uuid;",
    result_handler=ResultHandler.ROWCOUNT,
)

# --- Helper Functions ---

def _enrich_style_from_row(row: dict, root_url: str = "") -> Optional[Style]:
    """Constructs a Style from a DB row, injecting dynamic links."""
    if not row:
        return None

    base_path = (
        f"{root_url}/styles/catalogs/{row['catalog_id']}"
        f"/collections/{row['collection_id']}/styles/{row['style_id']}"
    )

    enriched_stylesheets = []
    for ss_data in row.get("stylesheets", []):
        # All encodings share the single /stylesheet endpoint (content-negotiated).
        ss_link = Link(
            href=f"{base_path}/stylesheet",
            rel="stylesheet",
            type=ss_data.get("content", {}).get("format"),
        )
        enriched_stylesheets.append(StyleSheet(content=ss_data["content"], link=ss_link))

    row["stylesheets"] = enriched_stylesheets
    row["links"] = [Link(href=base_path, rel="self", type="application/json")]
    return Style.model_validate(row)

# --- Public Functions ---

async def create_style(
    conn: DbResource, catalog_id: str, collection_id: str, style_data: StyleCreate
) -> Style:
    """Creates a new style record."""
    style_dict = style_data.model_dump(exclude={"links"})
    style_dict["stylesheets"] = json.dumps(
        [{"content": ss["content"]} for ss in style_dict["stylesheets"]]
    )
    params = {"catalog_id": catalog_id, "collection_id": collection_id, **style_dict}
    raw_row = await _create_style_query.execute(conn, **params)
    enriched = _enrich_style_from_row(raw_row)
    if enriched is None:
        # The INSERT … RETURNING just succeeded — a None enrichment means the
        # row failed to validate against the Style model, which is a code bug,
        # not a runtime "missing row" condition.
        raise RuntimeError(
            f"create_style: row enrichment returned None for "
            f"catalog={catalog_id} collection={collection_id} "
            f"style_id={style_data.style_id}"
        )
    return enriched


async def get_style_by_id(
    conn: DbResource, catalog_id: str, style_id: str
) -> Optional[Style]:
    """Retrieves a style by catalog + style_id (no collection filter)."""
    raw_row = await _get_style_by_id_query.execute(
        conn, catalog_id=catalog_id, style_id=style_id
    )
    return _enrich_style_from_row(raw_row) if raw_row else None


async def get_style_by_id_and_collection(
    conn: DbResource, catalog_id: str, collection_id: str, style_id: str
) -> Optional[Style]:
    """Retrieves a style by its unique (catalog, collection, style_id) triple."""
    raw_row = await _get_style_by_id_and_collection_query.execute(
        conn,
        catalog_id=catalog_id,
        collection_id=collection_id,
        style_id=style_id,
    )
    return _enrich_style_from_row(raw_row) if raw_row else None


async def list_styles_for_collection(
    conn: DbResource,
    catalog_id: str,
    collection_id: str,
    limit: int = 100,
    offset: int = 0,
) -> List[Style]:
    """Lists styles for a specific collection (paginated)."""
    raw_rows = await _list_styles_query.execute(
        conn,
        catalog_id=catalog_id,
        collection_id=collection_id,
        limit=limit,
        offset=offset,
    )
    return [s for s in (_enrich_style_from_row(row) for row in raw_rows) if s is not None]


async def list_all_styles(
    conn: DbResource,
    limit: int = 100,
    offset: int = 0,
) -> List[Optional[Style]]:
    """Lists styles across all catalogs and collections (cross-partition, paginated)."""
    raw_rows = await _list_all_styles_query.execute(conn, limit=limit, offset=offset)
    return [_enrich_style_from_row(row) for row in raw_rows]



async def update_style(
    conn: DbResource, catalog_id: str, style_id: str, style_data: StyleUpdate
) -> Optional[Style]:
    """Updates an existing style record identified by (catalog_id, style_id)."""
    update_values = style_data.model_dump(exclude_unset=True)
    if not update_values:
        logger.warning("update_style called with no values to update.")
        return await get_style_by_id(conn, catalog_id, style_id)

    if "stylesheets" in update_values:
        update_values["stylesheets"] = json.dumps(
            [{"content": ss["content"]} for ss in update_values["stylesheets"]]
        )

    async def _update_builder(db_resource, raw_params):
        set_clause_keys = [k for k in raw_params if k not in ("catalog_id", "style_id")]
        set_clause = ", ".join([f'"{k}" = :{k}' for k in set_clause_keys])
        query_str = f"""
            UPDATE styles.styles
            SET {set_clause}, updated_at = NOW()
            WHERE catalog_id = :catalog_id AND style_id = :style_id
            RETURNING *;
        """
        return text(query_str), {**raw_params}

    update_executor = DQLQuery.from_builder(_update_builder, result_handler=ResultHandler.ONE_DICT)
    raw_row = await update_executor.execute(
        conn, catalog_id=catalog_id, style_id=style_id, **update_values
    )
    return _enrich_style_from_row(raw_row) if raw_row else None


async def delete_style(conn: DbResource, catalog_id: str, style_uuid: uuid.UUID) -> bool:
    """Deletes a style record by its internal UUID."""
    rows_affected = await _delete_style_query.execute(
        conn, catalog_id=catalog_id, style_uuid=style_uuid
    )
    return rows_affected > 0
