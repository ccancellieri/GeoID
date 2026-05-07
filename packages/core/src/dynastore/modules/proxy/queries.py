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

from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler, DDLQuery
import os

from .models import ShortURL


# --- Schema Creation ---


CREATE_SHORT_URLS_TABLE = DDLQuery(
    sql_template="""
    CREATE TABLE IF NOT EXISTS {schema}.short_urls (
        id BIGINT NOT NULL DEFAULT nextval('{schema}.short_url_id_seq'),
        short_key VARCHAR(20) NOT NULL,
        long_url TEXT NOT NULL,
        collection_id VARCHAR(255) NOT NULL DEFAULT '_catalog_',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        comment TEXT,
        PRIMARY KEY (collection_id, short_key)
    ) PARTITION BY LIST (collection_id);
    """
)

CREATE_BASE62_FUNCTION = DDLQuery(
    sql_template="""
    CREATE OR REPLACE FUNCTION {schema}.base62(n BIGINT) RETURNS TEXT AS $$
    DECLARE
        chars TEXT[] := ARRAY['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'];
        result TEXT := '';
        num NUMERIC;
        remainder_int INT;
    BEGIN
        IF n = 0 THEN RETURN '0'; END IF;
        num := n;
        WHILE num > 0 LOOP
            remainder_int := (num % 62)::INT;
            result := chars[remainder_int + 1] || result;
            num := trunc(num / 62);
        END LOOP;
        RETURN result;
    END;
    $$ LANGUAGE plpgsql IMMUTABLE;
    """
)

CREATE_OBFUSCATE_FUNCTION = DDLQuery(
    sql_template="""
    CREATE OR REPLACE FUNCTION {schema}.obfuscate_id(n BIGINT) RETURNS BIGINT AS $$
    DECLARE
        PRIME_CONSTANT BIGINT := 1299709::BIGINT;
        POSITIVE_MASK BIGINT := 9223372036854775807::BIGINT;
    BEGIN
        RETURN (n # PRIME_CONSTANT) & POSITIVE_MASK;
    END;
    $$ LANGUAGE plpgsql IMMUTABLE;
    """
)

CREATE_SHORT_URL_SEQUENCE = DDLQuery(
    sql_template="""CREATE SEQUENCE IF NOT EXISTS {schema}.short_url_id_seq;"""
)

INSERT_SHORT_URL_WITH_GENERATED_KEY = DQLQuery(
    sql_template="""
    WITH new_vals AS (
        SELECT
            nextval('proxy.short_url_id_seq') as id,
            proxy.base62(proxy.obfuscate_id(currval('proxy.short_url_id_seq'))) as s_key
    )
    INSERT INTO {schema}.short_urls (id, short_key, long_url, collection_id, comment)
    SELECT id, s_key, :long_url, :collection_id, :comment FROM new_vals
    RETURNING id, short_key, long_url, created_at, comment;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda d: ShortURL(**d) if d else None
)

INSERT_SHORT_URL_WITH_CUSTOM_KEY = DQLQuery(
    sql_template="""
    INSERT INTO {schema}.short_urls (short_key, long_url, collection_id, comment)
    VALUES (:custom_key, :long_url, :collection_id, :comment)
    RETURNING id, short_key, long_url, created_at, comment;
    """,
    result_handler=ResultHandler.ONE_DICT,
    post_processor=lambda d: ShortURL(**d) if d else None
)

GET_LONG_URL = DQLQuery(
    sql_template="SELECT long_url FROM {schema}.short_urls WHERE short_key = :short_key;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE
)

DELETE_SHORT_URL = DQLQuery(
    sql_template="DELETE FROM {schema}.short_urls WHERE short_key = :short_key RETURNING short_key;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE
)

GET_URLS_BY_COLLECTION = DQLQuery(
    sql_template="""
    SELECT id, short_key, long_url, collection_id, created_at, comment
    FROM {schema}.short_urls
    WHERE collection_id = :collection_id
    ORDER BY created_at DESC
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS,
    post_processor=lambda rows: [ShortURL(**r) for r in rows]
)
