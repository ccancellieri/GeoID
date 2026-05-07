Based on your master context document, README_detailed.md, your task is to design and generate the complete Python code for a new, self-contained URL proxy service.

This new service must strictly adhere to the "Three Pillars" architecture (Module, Extension, Task) and all codified design patterns (pluggable interfaces, query_executor, lifespan management, etc.).

Critical Mandate: System Agnosticism
This service MUST be entirely agnostic of the OGC, catalog, and collection system. It is a standalone, global utility. It MUST NOT use catalog_id for partitioning or tenancy. All database tables you create for this service MUST be global, standard PostgreSQL tables.

You will generate all necessary components, separated into the following three pillars:

Pillar I: The proxy Module (The Foundational Layer)

This module is the foundational core. It will own the business logic, database schemas, and data models. It MUST NOT contain any API-specific logic.

1. Pluggable Storage Interface (Constraints 2, 4):

Define an abstract base class (ABC) named AbstractProxyStorage.

This interface must define all necessary data persistence methods, including:

async def initialize(self): (Called by the module's lifespan to set up base schemas and functions).

async def initialize_partitions(self, conn, for_date: datetime.date): (A separate method to be called by a maintenance task to create partitions for a given time period, e.g., the next month).

async def create_short_url(self, conn, long_url: str, custom_key: str = None, owner_id: str = None) -> ShortURL: (Generates a unique key and saves the mapping).

async def get_long_url(self, conn, short_key: str) -> str | None: (Retrieves a long URL for redirection).

async def log_redirect(self, conn, short_key: str, ip_address: str, user_agent: str, referrer: str, timestamp: datetime.datetime): (Logs a click event into the correct time-based partition).

async def get_analytics(self, conn, short_key: str, cursor: str | None = None, page_size: int = 100) -> AnalyticsPage: (Retrieves a paginated list of click analytics using keyset pagination).

async def delete_short_url(self, conn, short_key: str) -> bool: (Deletes a short URL).

2. Default PostgreSQL Implementation (Constraint 3):

Create a concrete class PostgresProxyStorage(AbstractProxyStorage) that implements the interface.

This implementation MUST use the query_executor pattern (DQLQuery, DDLExecutor) for all database operations.

Key Generation: Use a reliable method for generating short, unique keys (e.g., base62 encoding of a PostgreSQL BIGSERIAL sequence).

3. Database Schemas & Hyperscale Partitioning Strategy (Constraint 3, Scalability):
The PostgresProxyStorage.initialize method must set up the parent tables. The partitioning strategy must be configurable via environment variables.

short_urls Table (Scalability Mandate):

This table must be partitioned BY HASH (short_key) to evenly distribute writes and primary key lookups across many smaller tables.

The number of hash partitions must be configurable (e.g., env var PROXY_URL_HASH_PARTITIONS, default 64).

id: BIGSERIAL

short_key: VARCHAR(20) NOT NULL (The partitioning key)

long_url: TEXT NOT NULL

created_at: TIMESTAMPTZ NOT NULL DEFAULT NOW()

owner_id: VARCHAR(255)

PRIMARY KEY (short_key, id) (The partition key must be part of the primary key).

UNIQUE (short_key) (This will be enforced by the PRIMARY KEY).

url_analytics Table (Scalability Mandate):

This table will store trillions of rows and requires a multi-level partitioning strategy.

Level 1 (Primary): PARTITION BY RANGE (timestamp)

The partition interval must be configurable (e.g., env var PROXY_ANALYTICS_PARTITION_INTERVAL, default MONTHLY).

Level 2 (Sub-partition): PARTITION BY HASH (short_key_ref)

Within each time range (e.g., 2025_10), the data must be sub-partitioned by a hash of the short_key_ref.

The number of hash sub-partitions must be configurable (e.g., env var PROXY_ANALYTICS_HASH_PARTITIONS, default 16).

Parent Table Schema:

id: BIGSERIAL

short_key_ref: VARCHAR(20) NOT NULL (The HASH partition key)

timestamp: TIMESTAMPTZ NOT NULL DEFAULT NOW() (The RANGE partition key)

ip_address: INET

user_agent: TEXT

referrer: TEXT

geoip_info: JSONB DEFAULT NULL

geoip_processed: BOOLEAN NOT NULL DEFAULT FALSE

PRIMARY KEY (timestamp, short_key_ref, id) (All partition keys must be part of the PK).

Partition Management: The on-demand trigger pattern from README_detailed.md is not suitable for this complex multi-level setup. The initialize_partitions method will contain the DDLExecutor logic to create a specific RANGE partition (e.g., for '2025-11-01') and all its HASH sub-partitions. This method is designed to be called by a scheduled task.

4. Driver Registry (Constraint 4):

The proxy module must implement a simple driver registry.

This registry will allow different storage drivers to be registered with a name (e.g., 'proxy_storage_driver') and a priority.

The module's lifespan function must:

Initialize this registry.

Register the PostgresProxyStorage with a default priority (e.g., 10).

Select the storage driver with the highest priority (or one specified by an env var PROXY_STORAGE_DRIVER) as the active driver.

Store this active driver instance on the module (e.g., proxy_module.storage_driver).

Call await proxy_module.storage_driver.initialize() (to create parent tables).

5. Public Module API:

The proxy module will expose simple, async pass-through functions (e.g., async def create_short_url(...)) that call the corresponding methods on the active proxy_module.storage_driver.

6. Models:

ShortURL: Maps to the short_urls table.

URLAnalytics: Maps to the url_analytics table.

AnalyticsPage (Scalability Mandate):

data: list[URLAnalytics]

next_cursor: str | None (This will be the id or timestamp of the last item, to be used in the next query's WHERE clause).

Pillar II: The proxy_api Extension (The API Layer)

This extension is the stateless API layer. It MUST consume the proxy module and contain no business logic.

1. API Endpoints (Constraint 1):

POST /proxy:

Takes a JSON body with long_url and optional custom_key / owner_id.

Calls await proxy_module.create_short_url(...).

Returns a JSON response with the new ShortURL object.

GET /r/{short_key}:

This is the core redirection endpoint.

It MUST first check a high-speed cache (e.g., async-lru-cache) for the short_key.

On a cache miss, it calls await proxy_module.get_long_url(short_key).

It MUST store the result in the cache.

It MUST return an HTTP 307 (Temporary Redirect) response.

It MUST use FastAPI's BackgroundTasks to asynchronously call await proxy_module.log_redirect(...), passing the request's IP, User-Agent, Referrer, and the current timestamp. This is critical for not blocking the redirect.

GET /proxy/stats/{short_key}:

Scalability Mandate (Keyset Pagination):

Must accept an optional query parameter: ?cursor: str = None.

Calls await proxy_module.get_analytics(short_key, cursor=cursor, page_size=100).

The get_analytics implementation MUST use the cursor in a WHERE clause (e.g., WHERE short_key_ref = :key AND id > :cursor ORDER BY id LIMIT :page_size). It MUST NOT use OFFSET.

Returns the AnalyticsPage object (with data and next_cursor) as JSON.

DELETE /proxy/{short_key}:

Calls await proxy_module.delete_short_url(...).

Returns 204 No Content on success.

Final Instruction:
Generate the complete, runnable Python code for all files required for these three components (proxy_module.py, proxy_api_extension.py, geoip_processor_task.py, and any necessary models.py or queries.py), ensuring every line respects the strict separation of concerns and architectural patterns from README_detailed.md, especially the new hyperscale partitioning and keyset pagination requirements.