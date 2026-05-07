-- #    Copyright 2025 FAO
-- #
-- #    Licensed under the Apache License, Version 2.0 (the "License");
-- #    you may not use this file except in compliance with the License.
-- #    You may obtain a copy of the License at
-- #
-- #        http://www.apache.org/licenses/LICENSE-2.0
-- #
-- #    Unless required by applicable law or agreed to in writing, software
-- #    distributed under the License is distributed on an "AS IS" BASIS,
-- #    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- #    See the License for the specific language governing permissions and
-- #    limitations under the License.
-- #
-- #    Author: Carlo Cancellieri (ccancellieri@gmail.com)
-- #    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
-- #    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

-- =============================================================================
--  DynaStore Core Database Functions v1.3
--  This is a pure SQL script designed to be run programmatically.
--  It is idempotent and can be run safely multiple times.
-- =============================================================================

-- Tries to enable PostGIS, which is fundamental for all geospatial operations.
CREATE EXTENSION IF NOT EXISTS postgis;

-- Tries to enable btree_gist, required for advanced indexing capabilities.
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Tries to enable btree_gist, required for advanced indexing capabilities.
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- SELECT extname, extversion FROM pg_extension WHERE extname = 'btree_gin';

-- SHOW search_path;

-- SELECT n.nspname AS schema_name
-- FROM pg_opclass opc
-- JOIN pg_namespace n ON n.oid = opc.opcnamespace
-- WHERE opc.opcname = 'gin_bigint_ops';

-- SELECT opcname, opc.opcnamespace::regnamespace AS schema_name, am.amname AS access_method
-- FROM pg_opclass opc
-- JOIN pg_am am ON opc.opcmethod = am.oid
-- WHERE am.amname = 'gin'
-- ORDER BY opcname;

