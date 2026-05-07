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

from dynastore.modules.db_config.query_executor import DQLQuery, DDLQuery, ResultHandler

# --- DDL: Schema Definition ---

# Create the partitioned table to store CRS definitions.
# We use JSONB for the 'definition' column to query metadata like 'name' or 'scope' efficiently.
CREATE_CUSTOM_CRS_TABLE = DDLQuery(
    """
    CREATE TABLE IF NOT EXISTS crs.crs_definitions (
        catalog_id VARCHAR NOT NULL, 
        crs_uri VARCHAR NOT NULL,    
        definition JSONB NOT NULL,   
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (catalog_id, crs_uri)
    ) PARTITION BY LIST (catalog_id);
    """
)

# --- DML: Data Manipulation ---

# Insert a new CRS. Note: The :definition parameter is passed as a JSON string 
# and cast to JSONB by the database.
insert_custom_crs_query = DQLQuery(
    """
    INSERT INTO crs.crs_definitions (catalog_id, crs_uri, definition) 
    VALUES (:catalog_id, :crs_uri, CAST(:definition AS JSONB)) 
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_DICT
)

# Update an existing CRS definition
update_custom_crs_query = DQLQuery(
    """
    UPDATE crs.crs_definitions 
    SET definition = CAST(:definition AS JSONB) 
    WHERE catalog_id = :catalog_id AND crs_uri = :crs_uri 
    RETURNING *;
    """,
    result_handler=ResultHandler.ONE_OR_NONE
)

# Delete a CRS
delete_custom_crs_query = DQLQuery(
    "DELETE FROM crs.crs_definitions WHERE catalog_id = :catalog_id AND crs_uri = :crs_uri;",
    result_handler=ResultHandler.ROWCOUNT
)

# --- DQL: Data Retrieval ---

# Get a specific CRS by URI
get_custom_crs_by_uri_query = DQLQuery(
    "SELECT * FROM crs.crs_definitions WHERE catalog_id = :catalog_id AND crs_uri = :crs_uri;",
    result_handler=ResultHandler.ONE_OR_NONE
)

# Get a CRS by its human-readable name (stored inside the JSONB definition)
get_custom_crs_by_name_query = DQLQuery(
    """
    SELECT * FROM crs.crs_definitions 
    WHERE catalog_id = :catalog_id AND definition->>'name' = :crs_name 
    LIMIT 1;
    """,
    result_handler=ResultHandler.ONE_OR_NONE
)

# List all CRS for a catalog
list_custom_crs_query = DQLQuery(
    """
    SELECT * FROM crs.crs_definitions 
    WHERE catalog_id = :catalog_id 
    ORDER BY created_at DESC 
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS
)

# Search functionality: Looks inside the JSONB structure for matches in 'name', 'description', or 'scope'
search_custom_crs_query = DQLQuery(
    """
    SELECT * FROM crs.crs_definitions
    WHERE
        catalog_id = :catalog_id AND (
            definition->>'name' ILIKE :search_term OR
            definition->>'description' ILIKE :search_term OR
            definition->>'scope' ILIKE :search_term
        )
    ORDER BY created_at DESC
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS
)