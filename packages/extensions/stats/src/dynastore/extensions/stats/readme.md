# Stats Extension (Administrator & User Guide)

The `Stats` extension exposes the powerful analytics capabilities of DynaStore. It provides endpoints for retrieving detailed access logs and aggregated metrics, enabling administrators and tenants to monitor usage, performance, and security.

## Key Use Cases

1.  **Usage Auditing**: Track "Who accessed What and When" for compliance and security.
2.  **Performance Monitoring**: Analyze latency trends (p50, p95) for specific collections or API keys.
3.  **Quota Tracking**: Monitor consumption against allocated limits.
4.  **Billing & Reporting**: Generate usage reports for chargeback or billing purposes.

## Core Concepts

### 1. Access Logs
Detailed, row-level records of every request processed by the system.
- **Timestamp**: Exact time of the request.
- **Principal**: The user or service identity.
- **Resource**: The catalog, collection, or asset accessed.
- **Status**: HTTP status code (200, 403, 500, etc.).
- **Latency**: Processing time in milliseconds.

### 2. Aggregates
Pre-calculated metrics rolled up by time windows (Hour, Day, Month).
- **Dimensions**: Time, Principal, API Key, Status Code.
- **Metrics**: Total Request Count, Total Latency.

### 3. Isolation
Stats are strictly tenant-isolated. An administrator for `Catalog A` cannot see the logs of `Catalog B`.

## API Discovery

The extension functionality is available under `/stats`:

### Reporting (`/stats/reports`)
- `GET /summary`: High-level dashboard metrics (Total requests, Error rates).
- `GET /logs`: Paginated list of raw access logs with filtering.

### Contextual Stats
- `GET /stats/catalog/{catalog_id}`: specific stats for a catalog.
- `GET /stats/collection/{collection_id}`: specific stats for a collection.

## Configuration

This extension requires the `Stats` module to be active and configured.

| Variable | Description |
| :--- | :--- |
| `STATS_RETENTION_DAYS` | How long detailed logs are kept (default: 90). |

## Examples

### Fetching Daily Usage Summary
```bash
curl -X GET "http://localhost/stats/reports/summary?start=2024-01-01&end=2024-01-02" \
     -H "Authorization: Bearer <token>"
```

### Auditing Access to a Sensitive Collection
```bash
curl -X GET "http://localhost/stats/reports/logs?path_pattern=*/sensitive-data/*" \
     -H "Authorization: Bearer <token>"
```
