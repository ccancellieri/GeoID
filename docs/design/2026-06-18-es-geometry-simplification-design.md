# ES geometry simplification — accept-and-simplify by default

**Date:** 2026-06-18
**Status:** Design (approved, pending implementation)
**Area:** `packages/core/src/dynastore` — storage drivers (Elasticsearch), catalog item service, geometry tool

## Problem

Ingesting a large-geometry shapefile (GAUL admin level 1, `gaul_dmgr.zip`) fails. One feature serializes to ~14.9 MB of GeoJSON geometry and is rejected before any write:

> Item '1582' geometry is 14943635 bytes, exceeding the 10000000-byte (10 MB)
> Elasticsearch per-document limit. The collection routes an Elasticsearch items
> index with geometry simplification disabled, so the item is rejected before any
> write. Reduce the geometry resolution, or enable 'simplify_geometry' on the
> Elasticsearch items driver to index a simplified copy.

A colleague tried "adding simplification using 0.01, 50 and 100" and was still rejected.

### Root cause

Those tolerance numbers are read **nowhere**. The only simplification knob the
system exposes is a single boolean, `simplify_geometry`, which defaults to
`False` (the "exact-by-default" policy introduced in #1248). When it is off:

- `maybe_simplify_for_es(doc, simplify=False, ...)` short-circuits and returns the
  geometry untouched (`geometry_simplify.py:189`).
- The pre-write guard `_enforce_es_geometry_size_limit` in `item_service.py`
  rejects any geometry over 10 MB with HTTP 422 before the PG primary row is
  created (PG is primary, ES is an async secondary — rejecting post-commit would
  desync them).

So the rejection means simplification never ran. Critically, the capability to
accept any geometry **already exists**: when `simplify=True`, `simplify_to_fit`
runs an adaptive-tolerance loop and, as a guaranteed floor, replaces the geometry
with its bounding box (`box(*geom.bounds)`, 5 coordinate pairs) which always fits
under 10 MB (`geometry_simplify.py:159-161`). The feature is correct — it is just
**off by default**, **confusingly named** (users expect a tolerance, not a bool),
and **undiscoverable**.

## Goal

> Accept all geometries and automatically simplify them to fit under the
> Elasticsearch 10 MB per-document limit. Ingestion must not fail on geometry
> size. PG keeps full resolution; ES gets a degraded copy.

Secondary concern raised by the user: ES index growth — storing near-10 MB
geometries across millions of collections and trillions of items is wasteful, so
operators should be able to opt into a *smaller* ES geometry budget.

Constraint: the config API is already used by live users. We may **add**
parameters with safe defaults, but must not rename or remove existing ones, and
must not break existing collection configs.

## Decisions (locked)

1. **Default policy = accept + auto-simplify.** Ingestion never fails on geometry
   size. ES indexes a simplified copy; PG keeps full resolution.
2. **Engine = improve the existing iterative tolerance + bbox floor (Option 1).**
   Two further engines (H3/S2 grid-snap; tiered ladder) are documented as future
   follow-ups but are **not** built here.
3. **Back-compat = new default applies to all.** Every collection that has not
   explicitly set `simplify_geometry` now auto-simplifies. A collection that
   explicitly set `simplify_geometry: false` keeps the strict reject behavior.

## Design — Option 1 (implement now)

### 1. Flip the driver-config default to simplify

`simplify_geometry` flips from `default=False` to `default=True` on the three ES
items driver configs in `modules/storage/driver_config.py`:

- `ItemsElasticsearchDriverConfig`
- `ItemsElasticsearchPrivateDriverConfig`
- `ItemsElasticsearchEnvelopeDriverConfig`

It stays a `Mutable[bool]` — **no rename, no type change, no API break**. Existing
configs that carry an explicit value keep it; configs that omit it switch from
exact-reject to accept-and-simplify.

### 2. Make the guard honor the new default everywhere

The pre-write guard reads the flag through
`_es_items_driver_simplify_enabled` (`item_service.py:746-778`). That helper
currently returns `False` on **every** fallback path:

- resolver raises → `return False` (line 768)
- config read raises → `return False` (line 777)
- neither resolver nor `get_driver_config` present → `return False` (line 778)

Flipping the Pydantic field default fixes the normal path (`getattr(cfg,
"simplify_geometry", True)` will read the new default), but those hard-coded
`False` fallbacks would still reject on a transient config-read failure. Under an
"ingestion never fails on geometry size" policy that is wrong: a config hiccup
must fail **open** (simplify), not closed (reject). So:

- Change all three fallback returns to `True`.
- Change the `getattr` default at line 775 to `True`.

The guard's structure is otherwise unchanged: it still rejects when an ES items
secondary is routed **and** simplification is explicitly disabled — that path now
only fires for `simplify_geometry: false` collections, which is the intended
escape hatch.

`elasticsearch.py:_resolve_simplify_geometry` (≈line 1185) must be re-verified to
default `True` consistently with the config field.

### 3. Harden `simplify_to_fit` convergence

Correctness is already guaranteed by the bbox floor, so this step improves
**fidelity** (fewer geometries collapsing all the way to a bbox):

- Raise `DEFAULT_MAX_ITERATIONS` from `3` to `8` (`geometry_simplify.py:62`). More
  binary-search steps between the seed tolerance and the bbox floor means a
  closer-to-budget tolerance result is found before the floor is hit.
- Keep the adaptive seed and bbox floor as-is. (Optionally tune the seed comment;
  no algorithm change required for correctness.)

The extra iterations cost a few more `geom.simplify` + serialize passes only for
geometries that actually exceed budget — bounded and off the hot path for the
common (small-geometry) case, which returns at the first size check
(`geometry_simplify.py:101`).

### 4. Optional aggression knob — `simplify_target_bytes`

Add one optional field to the three ES items driver configs:

```python
simplify_target_bytes: Mutable[int] | None = Field(
    default=None,
    description=(
        "Target byte budget for ES geometry simplification. When set, geometry "
        "is simplified to fit under this size instead of the 10 MB ES per-document "
        "limit — lower values keep ES indices smaller at the cost of geometry "
        "fidelity. Defaults to the 10 MB Elasticsearch limit when unset."
    ),
)
```

Resolution: when set, the ES driver passes it as `max_bytes` into
`maybe_simplify_for_es(...)`; when `None`, the existing `DEFAULT_MAX_BYTES`
(10 MB) is used — so default behavior is unchanged. This directly answers the ES
index-growth concern: an operator worried about storage can set, e.g.,
`simplify_target_bytes: 1_000_000` and keep every collection's ES geometry under
1 MB. PG is untouched and keeps full resolution regardless.

`simplify_target_bytes` must never exceed the hard 10 MB ES ceiling; clamp on the
high side so a misconfiguration can't reintroduce oversize docs.

### 5. Improve the explicit-disable rejection message

For the remaining reject path (collections with `simplify_geometry: false`), the
message should show the exact config change to flip, so the next person doesn't
repeat the "0.01 / 50 / 100" dead end. The current text already names the flag;
extend it to make explicit that the *default is now to simplify* and that this
collection has opted out.

## Data flow (after change)

```
ingest item
  │
  ├─ item_service.upsert
  │    └─ _enforce_es_geometry_size_limit
  │         simplify enabled (now the default) ──► no reject, item proceeds
  │         simplify explicitly false ──────────► reject (422 single / 207 bulk)
  │
  ├─ PG driver  ──► full-resolution geometry (never simplified)
  │
  └─ ES items driver (async secondary)
       └─ maybe_simplify_for_es(doc, simplify=True, max_bytes=<target or 10MB>)
            └─ simplify_to_fit: adaptive tolerance loop (≤8 iters) → bbox floor
                 stamps system.geometry_simplification = {factor, mode}
```

## Backward compatibility

| Existing collection state            | Before        | After                       |
|--------------------------------------|---------------|-----------------------------|
| No `simplify_geometry` set           | reject >10 MB | accept + auto-simplify      |
| `simplify_geometry: true`            | auto-simplify | auto-simplify (unchanged)   |
| `simplify_geometry: false`           | reject >10 MB | reject >10 MB (unchanged)   |

No config migration runs. No schema/DDL change. The only behavioral shift is the
intended one: unset collections move from reject → accept. `simplify_geometry`
stays a boolean; `simplify_target_bytes` is additive and optional.

This reverses the user-facing default of #1248 (exact-by-default). The #1248
rationale — never silently truncate without the operator asking — is now served
by the explicit `simplify_geometry: false` opt-out plus the
`system.geometry_simplification` provenance stamp on every simplified doc, so a
consumer can always tell a geometry was degraded and by how much.

## Testing

Existing suites to extend:

- `tests/dynastore/tools/test_geometry_simplify.py` — add cases for the raised
  iteration cap and for a custom `max_bytes` target (e.g. 1 MB) converging via
  tolerance, then falling to bbox when tolerance can't reach the smaller budget.
- `tests/dynastore/modules/catalog/test_item_service_geometry_guard.py` — flip the
  expected default: an ES-routed collection with no `simplify_geometry` set must
  now **accept** a >10 MB geometry; only `simplify_geometry: false` rejects.
  Add a fallback-path test: config-read failure fails **open** (no reject).
- `tests/dynastore/modules/elasticsearch/test_private_geometry_simplification.py`
  — assert the private driver default is now simplify-on and that
  `simplify_target_bytes` is honored.

End-to-end verification (the originating failure):

- Ingest `gaul_dmgr.zip` into a collection routing ES + PG. Item 1582 (14.9 MB)
  must land. ES geometry ≤ budget with `system.geometry_simplification` stamped;
  PG geometry full-resolution. Run against the `dev` environment only.

## Out of scope — documented future follow-ups (do NOT build now)

These reuse the same `maybe_simplify_for_es` seam and the `factor`/`mode` return
contract, so they slot in without touching call sites:

- **Option 2 — H3/S2 grid-snap mode.** Add `simplify_mode: tolerance | grid | bbox`
  where `grid` snaps geometry vertices to the finest H3/S2 cell resolution that
  fits the budget. Gives predictable, quantized output well-suited to aggregation
  and tiling. No H3/S2/geohash dependency exists in the codebase today — this
  would introduce one.
- **Option 3 — Tiered ladder.** A single escalation pipeline `tolerance → grid →
  bbox`, with a config cap on maximum allowed aggression, so an operator can say
  "simplify by tolerance, but never coarser than grid level N."

Both are deferred by explicit decision; this spec ships Option 1 only.

## Files touched (Option 1)

- `packages/core/src/dynastore/modules/storage/driver_config.py` — flip
  `simplify_geometry` default to `True` on 3 ES items configs; add optional
  `simplify_target_bytes`.
- `packages/core/src/dynastore/modules/catalog/item_service.py` — flip the
  `_es_items_driver_simplify_enabled` fallbacks to fail open; refine reject
  message.
- `packages/core/src/dynastore/modules/storage/drivers/elasticsearch.py` —
  resolve/pass `simplify_target_bytes` as `max_bytes`; verify
  `_resolve_simplify_geometry` default.
- `packages/core/src/dynastore/tools/geometry_simplify.py` — raise
  `DEFAULT_MAX_ITERATIONS` to 8; update the #1248 policy docstring to reflect
  simplify-by-default.
- Tests as listed above.
