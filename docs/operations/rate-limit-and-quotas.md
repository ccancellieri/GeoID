# Rate limits and quotas — operator playbook

GeoID enforces request limits as **policy conditions**. A policy carrying a
`rate_limit` or `max_count` condition gates every request that matches its
`actions`/`resources` scope. Enforcement is atomic across all Cloud Run pods
via Valkey; PostgreSQL holds the durable counter row (table
`iam.usage_counters`).

This document is the operator copy-paste reference for the two most common
shapes: **anonymous rate-limit per client IP** and **ad-hoc per-user quota
role**. Both can be created from the admin UI (Policies tab → "+ Add
Condition") or with the REST calls below.

---

## Prerequisites

- A sysadmin token (`DYNASTORE_SYSADMIN_TOKEN`).
- The deployment runs behind a proxy/LB (Cloud Run + LB is the canonical
  topology) — `scope=client_ip` reads `X-Forwarded-For` leftmost-token and
  falls back to `request.client.host`. Do not use `scope=client_ip` in a
  dev environment without a fronting proxy: every caller will share the
  same loopback IP.
- Counter cardinality cap: keep the cross product
  `policies × principal_keys × live_windows` under ~10⁵ rows for a single
  Valkey/PG pair. The default reaper prunes expired window rows nightly via
  pg_cron.

```bash
export BASE=https://data.example.org/geospatial/v2/api  # adjust to your deployment
export TOKEN="$DYNASTORE_SYSADMIN_TOKEN"
```

---

## Recipe 1 — Anonymous rate-limit per client IP

Goal: cap unauthenticated tile traffic to 10 requests per minute per IP,
return 429 with `Retry-After` once the cap is hit, recover automatically on
the next minute boundary.

### 1. Create the policy

```bash
curl -X POST "$BASE/admin/policies" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "tile-anon-rate",
    "description": "10 req/min per client IP on /tiles/*",
    "actions": ["READ"],
    "resources": ["tile:*"],
    "effect": "ALLOW",
    "conditions": [{
      "type": "rate_limit",
      "config": {
        "limit": 10,
        "window_seconds": 60,
        "path_pattern": "^/tiles/",
        "methods": ["GET"],
        "scope": "client_ip"
      }
    }]
  }'
```

### 2. Bind it to anonymous traffic via a role

The anonymous principal already exists in the seeded `roles` table as
`anonymous`. Attach the policy:

```bash
curl -X PUT "$BASE/admin/roles/anonymous" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"policies": ["tile-anon-rate"]}'
```

### 3. Verify

```bash
for i in $(seq 1 12); do
  curl -s -o /dev/null -w "%{http_code}\n" "$BASE/tiles/example/1/0/0.pbf"
done
# expected: 200 ×10, then 429 ×2
```

The 429 response carries `Retry-After: 60` and the live snapshot in
`X-RateLimit-Limit` / `X-RateLimit-Remaining` / `X-RateLimit-Reset`. Once
the window rolls over, the counter resets automatically — no operator
intervention needed.

---

## Recipe 2 — Ad-hoc per-user quota role

Goal: hand a named user a fixed budget of 500 lifetime requests against an
expensive endpoint. Renewal is an explicit operator action.

### 1. Create the policy

```bash
curl -X POST "$BASE/admin/policies" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "exports-tier-2-quota",
    "description": "500 lifetime exports per principal",
    "actions": ["EXECUTE"],
    "resources": ["process:exports/*"],
    "effect": "ALLOW",
    "conditions": [{
      "type": "max_count",
      "config": {
        "limit": 500,
        "path_pattern": "^/processes/exports/",
        "scope": "principal"
      }
    }]
  }'
```

`max_count` has no `window_seconds` — the counter is lifetime, written
through to PG synchronously so it survives a Valkey restart.

### 2. Create a role that bundles it

```bash
curl -X POST "$BASE/admin/roles" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "exports-tier-2",
    "description": "Quota tier — 500 lifetime exports",
    "policies": ["exports-tier-2-quota"]
  }'
```

### 3. Assign the role to a user

```bash
curl -X PUT "$BASE/admin/principals/$PRINCIPAL_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"roles": ["exports-tier-2"]}'
```

### 4. Inspect usage

```bash
curl "$BASE/admin/policies/exports-tier-2-quota/usage?limit=50" \
  -H "Authorization: Bearer $TOKEN"
```

Returns a page of `{principal_key, count, window_start, expires_at,
last_seen_at}` rows. Each row corresponds to one (policy, principal) pair.

### 5. Renew (reset) a single principal's counter

```bash
curl -X DELETE \
  "$BASE/admin/policies/exports-tier-2-quota/usage/$PRINCIPAL_ID" \
  -H "Authorization: Bearer $TOKEN"
```

The row is deleted from `iam.usage_counters` and (if the layered driver is
active) from Valkey. The next request starts at zero. The reset event is
written to the IAM audit log as `event_type="usage_counter_reset"`.

For `rate_limit` rows, pass the originating window width so the resolver
targets the live bucket:

```bash
curl -X DELETE \
  "$BASE/admin/policies/tile-anon-rate/usage/ip:203.0.113.7?window_seconds=60" \
  -H "Authorization: Bearer $TOKEN"
```

---

## How a request is gated

1. `IamMiddleware` resolves the caller's role chain and the matching policies.
2. Each `rate_limit` / `max_count` condition calls
   `UsageCounterProtocol.incr_if_below(policy_id, principal_key, limit,
   window_seconds)`. The Valkey-backed driver runs an `EVAL` (or `EVALSHA`)
   Lua script that does the cap check and the increment atomically.
3. If any condition denies, the middleware returns 429 with `Retry-After`
   set to the window remainder (`rate_limit`) or omitted (`max_count` —
   lifetime caps don't auto-recover).
4. On allow, the response carries `X-RateLimit-Limit`,
   `X-RateLimit-Remaining`, `X-RateLimit-Reset` from the most restrictive
   matched policy.

---

## Notes

- **One counter driver is registered at a time.** `LayeredUsageCounter`
  (Valkey hot tier + PG durable write-through) wins when a counting cache
  backend is up; `PostgresUsageCounter` is the fallback. Both implement the
  same protocol — operator behavior is identical.
- **Lifetime caps write through to PG synchronously.** Rate-window counters
  batch through `AsyncBufferAggregator`; losing the last few increments on
  a pod crash is acceptable for windowed rate limits but not for a
  lifetime quota.
- **`principal_key` shape:** for `scope=principal` it is the principal ID;
  for `scope=client_ip` it is `"ip:" + leftmost(X-Forwarded-For)`; for
  `scope=role` it is the role name; for `scope=catalog` it is the
  catalog ID. The admin UI surfaces this string verbatim in the Usage
  modal — reset by key.
- **Conditions are AND.** A policy with two `rate_limit` conditions is
  denied if either trips. Use separate policies if you want OR.
- **No env vars.** Driver selection comes from the live cache module
  state, not from configuration. To disable rate-limit enforcement,
  delete the conditions from the policy (or delete the policy).
