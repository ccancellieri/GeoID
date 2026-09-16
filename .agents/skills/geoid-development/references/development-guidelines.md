# GeoID / DynaStore development guidelines

These are engineering requirements distilled from recurring failure patterns, not a claim that every mechanism below is implemented or every historical defect still exists. Verify the current revision before selecting an implementation. Public issue history can supply a problem statement, but closed does not mean fixed in this checkout.

## 1. Keep the system maintainable

- Give each capability one owner and a narrow protocol. Separate storage, indexing, orchestration, authorization, and external OGC representations.
- Reuse the current protocol/discovery boundary instead of constructing concrete managers across modules. Verify its symbols and call sites before editing.
- Keep optional modules independently installable and test missing optional dependencies. Do not add an import-time dependency to core merely to support one extension.
- Read the current workspace manifest before using old source paths or test commands. Update the smallest relevant documentation layer with behavior changes.

## 2. Feature identity is a contract

- Preserve the distinction between a stable logical feature identifier and backend-specific row, document, table, or object identifiers. GeoID's UUIDv7 identity design must be verified in the applicable creation path; do not reissue identity during indexing or projection.
- Define who generates and validates IDs, the uniqueness scope, and duplicate create/upsert behavior. Time ordering is not authorization, transaction ordering, or collision handling.
- Test client-supplied IDs, repeated writes, duplicate geometry with different IDs, reindexing, joins, and read/write round trips across supported routes. Geometry equality is not feature identity.

## 3. Dynamic routing must preserve semantics

- Describe read, exact-read, search, indexing, and write routes separately. An indexed search result is not automatically an authoritative exact read.
- Declare each backend's actual capabilities. PostgreSQL, Elasticsearch, BigQuery, DuckDB, Iceberg, Parquet, and GDAL integration points need not support identical operations. Never silently reinterpret an unsupported operation.
- Preserve IDs, types, null/missing distinctions, time ranges, geometry/CRS, filters, stable sorting, and pagination across the routes that claim parity.
- Track durable primary writes separately from secondary indexing. Define whether API acceptance promises primary durability or index visibility, and report partial completion accordingly. Preserve identity across client retries, define idempotency/conflict semantics, and prevent delayed secondary updates from overwriting newer versions. Test failure after primary commit and before the response reaches the client.
- Treat mapping/schema upgrades and backfills as explicit resumable operations. Test old records, new records, interrupted upgrades, and rollback/forward-recovery constraints.

## 4. Lifecycle and concurrency

- Capture immutable resource incarnation and cleanup ownership when scheduling deletion. Resolve neither identity nor destructive scope from a reused logical name at execution time.
- A stale task must not modify or delete a recreated resource. Make the ownership/generation guard effective at the destructive operation, not merely in a preceding unprotected check. Retain cleanup obligations for the old generation without targeting the new one.
- Gate writes and lazy activation on authoritative lifecycle state. Missing storage alone is not permission to recreate it.
- Use the current cross-process coordination mechanism for critical operations; an in-process lock cannot protect multiple workers. Specify lease ownership, expiration, fencing, cancellation, and retries where applicable.
- Make terminal task state durable and reconcile it with worker exit status. Idempotent retries must distinguish success, failure, cancellation, and work owned by another active lease.
- Keep asynchronous I/O, bounded pools, short transactions, cancellation, and shutdown cleanup explicit. Do not place live connections in memoized/cache keys or retain them in long-running tasks.

## 5. Authorization and public contracts

- Authorize at the service boundary and preserve policy through joins, materializations, exports, indexing, and caches. Check isolation on direct reads and alternate access paths, not only search.
- Separate internal envelopes from external representations. Do not leak physical names, query diagnostics, sidecars, or storage metadata into standard responses.
- For OGC/STAC changes, record the exact claimed class, requirement IDs, response examples, and applicable tests. Distinguish Records from STAC, and CRS84 from other axis-order conventions.
- Do not make private data anonymous merely to satisfy a conformance test. Select permitted test fixtures and align declared capabilities with actual access behavior.

## 6. Evidence required by change type

| Change | Minimum relevant evidence |
|---|---|
| Lifecycle | Delete/recreate race, stale retry, competing workers, interrupted cleanup |
| Routing/indexing | Supported-backend contract fixtures, exact read vs search, partial failure/recovery |
| Schema/identity | Existing/new data, duplicate IDs, migration interruption, identity-preserving round trip |
| OGC API | Normative requirement mapping, real serialized responses, negative and pagination cases |
| IAM/cache | Cross-principal isolation, policy changes, invalidation, alternate routes |
| Deployment/composition | Minimal optional-module composition and affected cloud/on-premises configuration |

Prefer real integration boundaries with isolated disposable resources. External paid or shared infrastructure requires explicit authorization; never test against a private deployment just because a reference contains its address. Use focused unit tests for deterministic logic and fault injection for rare failures.

Report executed commands and results, skipped checks, configuration, and revision. A demo is not a benchmark or proof of production capacity. Never label untested backend support or draft-standard behavior as verified.
