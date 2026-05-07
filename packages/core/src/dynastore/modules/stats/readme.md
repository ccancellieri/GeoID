# Stats Module (Developer Documentation)

The `Stats` module provides a high-performance, tenant-isolated logging and analytics engine for the DynaStore platform. It is designed to handle high-velocity access logs and metrics without contending with core business transactions.

## Architecture

The module adheres to the **Cellular Multi-Tenant Isolation** principle, ensuring that statistics are stored within the tenant's specific schema, preventing noisy neighbors and global locks.

### Core Components

- **`StatsService`**: The entry point for logging access events and querying statistics.
- **`StatsDriver` (SPI)**: Pluggable storage backend. The default `PostgresStatsDriver` handles buffering and batch writing.
- **`AccessRecord`**: Standardized data model for access logs.
- **`AsyncBufferAggregator`**: Background worker that batches writes to reduce database round-trips.

## Schema Strategy

Statistics are partitioned and isolated:

- **`{tenant}.access_logs`**: Detailed request logs, partitioned by time (Monthly).
- **`{tenant}.hourly_aggregates`**: Pre-aggregated metrics for fast dashboarding.

### Key Features

1.  **Tenant Isolation**: All data resides in `catalog_id` schemas (or `catalog` for system logs).
2.  **JIT Partitioning**: Partitions for `access_logs` are created Just-In-Time based on the incoming data stream.
3.  **Sharded Aggregates**: Counters in `hourly_aggregates` are sharded (default 16 shards) to prevent row-lock contention during high concurrency.
4.  **Hierarchical Context**: Logs capture `collection_id` and `asset_id` to allow granular reporting.

## Configuration

| Variable | Default | Description |
| :--- | :--- | :--- |
| `STATS_DRIVER` | `postgres` | The storage backend to use. |
| `STATS_FLUSH_THRESHOLD` | `1000` | Number of logs to buffer before flushing. |
| `STATS_FLUSH_INTERVAL` | `5.0` | Max seconds to wait before flushing buffer. |

## Usage

### Logging Access (FastAPI)

```python
from dynastore.modules.stats.service import STATS_SERVICE

# Inside a middleware or route
STATS_SERVICE.log_access(
    request=request,
    background_tasks=background_tasks,
    status_code=200,
    processing_time_ms=45.2,
    details={"query_params": "..."}
)
```

### Querying Stats

```python
summary = await STATS_SERVICE.get_summary(
    catalog_id="my_tenant",
    start_date=datetime(2024, 1, 1),
    collection_id="sentinel-2"
)
```

## Performance & Maintenance

- **Buffering**: Logs are held in memory and flushed in batches.
- **Pruning**: `access_logs` are automatically pruned based on the retention policy (default: 3 months).
- **Aggregates**: Retained longer for historical trend analysis.
