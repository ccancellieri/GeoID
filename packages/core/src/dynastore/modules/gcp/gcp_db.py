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

# File: src/dynastore/modules/gcp/gcp_db.py

from dynastore.modules.db_config.query_executor import DQLQuery, DDLQuery, ResultHandler

# --- Schema Definition ---
# --- Schema Definition ---
CATALOG_BUCKETS_SCHEMA = """
CREATE TABLE IF NOT EXISTS gcp.catalog_buckets (
    catalog_id VARCHAR PRIMARY KEY,
    bucket_name VARCHAR NOT NULL UNIQUE,
    status VARCHAR(50) NOT NULL DEFAULT 'ready', -- ready, provisioning, failed
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_catalog
        FOREIGN KEY(catalog_id)
        REFERENCES catalog.catalogs(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS gcp.reconciliation_events (
    event_id UUID PRIMARY KEY,
    target_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    locked_at TIMESTAMPTZ,
    locked_by UUID,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_recon_status_locked ON gcp.reconciliation_events (status, locked_at) WHERE status IN ('pending', 'processing');
"""

# --- Query Objects ---

link_bucket_to_catalog_query = DQLQuery(
    """
    INSERT INTO gcp.catalog_buckets (catalog_id, bucket_name) 
    VALUES (:catalog_id, :bucket_name) 
    ON CONFLICT (catalog_id) DO UPDATE SET bucket_name = EXCLUDED.bucket_name
    RETURNING bucket_name;
    """,
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE
)

get_bucket_for_catalog_query = DQLQuery(
    "SELECT bucket_name FROM gcp.catalog_buckets WHERE catalog_id = :catalog_id;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE
)

delete_bucket_link_query = DDLQuery(
    "DELETE FROM gcp.catalog_buckets WHERE catalog_id = :catalog_id;"
)

delete_bucket_by_name_query = DDLQuery(
    "DELETE FROM gcp.catalog_buckets WHERE bucket_name = :bucket_name;"
)
