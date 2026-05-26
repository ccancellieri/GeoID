# IAM decoupling via platform presets — design

**Status:** draft for review
**Date:** 2026-05-26
**Tracking issue:** to be filed against `un-fao/GeoID`

## Problem

Today every extension (`stac`, `features`, `records`, `maps`, `edr`, `dggs`, `events`, `logs`, `stats`, `web`, `admin`, `volumes`, …) implements the structural `PolicyContributor` protocol. At extension lifespan, `IamExtension.lifespan` calls `get_protocols(PolicyContributor)` and replays every contributor's `get_policies()` / `get_role_bindings()` on every boot.

Three pain points:

1. **Module coupling.** Non-IAM modules depend on IAM-shaped types just to declare their seed policies and role bindings.
2. **Reboot overwrite.** Although `provision_default_policies` is `force=False` by default and preserves operator edits for most rows, the contributor sweep still runs every boot, the `_ALWAYS_REFRESH = {"public_access"}` set genuinely overwrites operator edits, and obsolete contributor entries are never reaped. The perceived "impossible to override at runtime" reflects this.
3. **Lifecycle clutter.** Predefined roles and policies pile up at startup with no atomic way to enable/disable a service's permissions surface.

There is also a parallel, untapped opportunity: the existing `/admin/presets` registry (six routing presets: `public_catalog`, `private_catalog`, `defaults_postgres`, `private_collection`, `geoid`, `items_es_private`) already gives us POST/DELETE per name, `tier` + `catalog_scopable` scoping, and self-registration on import — but it is hard-bound to routing-only changes, has no search/pagination, no keywords, no flexible parameters, and no audit trail of what was applied where. The URL surface (`/admin/presets` and its three scope levels) stays exactly as it is today.

## Goals

1. Move every existing `PolicyContributor` body out of extension lifespan and into composable **platform presets** exposed via `/admin/presets`.
2. Reduce the cold-start path to the minimum required for a sysadmin to authenticate: schema DDL + a single sysadmin role row.
3. Make presets a general-purpose **platform action plugin** abstraction — not just IAM, not just routing. A preset can grant policies, write config, seed data, spawn long-running tasks, register cron jobs, call any library exposed through the preset context.
4. Preserve the existing tier-scoped admin API model (`platform` / `catalog` / `collection`) so delegated admins keep applying presets within their authorised scope.
5. Add the search / pagination / keyword / typed-params features the current preset surface lacks.
6. Make every apply reversible with a clear audit trail; every revoke scoped to exactly what *this* application introduced.

## Non-goals (this spec)

- A GitOps manifest-driven reconcile loop. Presets are explicitly operator-triggered.
- Replacing Keycloak realm-role mapping logic; the OIDC sync stays as is, but its hardcoded defaults migrate into a preset.
- Cross-cluster preset distribution; presets are local to one deployment.
- Authoring presets in a non-Python DSL. Presets are Python modules.

## Decisions locked during brainstorming

- **Unified Preset abstraction.** One protocol, one registry, one admin surface. Existing `RoutingPreset` is adapted to the new protocol with zero behavioural change for its six members.
- **No auto-apply on first boot.** Lifespan does DDL + a sysadmin-role survival row + registry registration. Nothing else. A fresh install is non-functional until a sysadmin authenticates and POSTs presets — by design, to ensure the lifecycle never overwrites operator state again.
- **Granularity: both atomic and curated.** One preset per extension (`stac_enable`, `features_enable`, etc.) plus a small set of curated composites (`platform_demo` for parity with today's all-extensions-on behaviour; `public_open_data`, `private_tenant` for common shapes).
- **DELETE revokes exactly what this apply introduced.** An `applied_presets` audit row stores the concrete revoke descriptor produced by the apply call; DELETE replays the reversal.
- **Self-lockout: hard refuse with override.** If a DELETE would revoke the caller's effective admin at the request scope, return 409 with a `force_self_revoke=true` override.
- **Kinds are dropped; keywords are the classifier.** A preset declares free-form keywords (`("iam", "config", "destructive", "async")`); there is no closed enum constraining what a preset may do.
- **Scope model preserved as-is.** Three URL shapes — `/admin/presets`, `/admin/catalogs/{cat}/presets`, `/admin/catalogs/{cat}/collections/{coll}/presets` — and the existing `tier: ClassVar[PresetTier]` + `catalog_scopable: ClassVar[bool]` fields on the preset are kept verbatim from today's `RoutingPreset`. No new tier-field abstraction.
- **Authorisation uses the existing IAM policy model, no new vocabulary.** Today's policies already match on HTTP method + path + param/payload conditions. `iam_baseline.apply()` installs ordinary policies that gate the catalog-scoped preset URLs for delegated roles (e.g. `effect=ALLOW, methods=[POST,DELETE], paths=["/admin/catalogs/{cat}/presets/{name}"], condition: name IN [...]`). Nothing in the platform hardcodes who may call which preset where, and nothing invents new permission strings.

## Design

### The Preset protocol

```python
class Preset(Protocol):
    name: ClassVar[str]                          # unique across all presets
    description: ClassVar[str]
    keywords: ClassVar[Tuple[str, ...]]          # free-form classifier (search-indexed)
    tier: ClassVar[PresetTier]                   # PLATFORM | CATALOG | COLLECTION | ITEMS  (unchanged from RoutingPreset)
    catalog_scopable: ClassVar[bool]             # platform-tier preset also callable at catalog scope  (unchanged from RoutingPreset)
    params_model: ClassVar[Type[BaseModel]]      # typed; defaults to NoParams when unused
    is_async: ClassVar[bool]                     # controls audit lifecycle

    async def dry_run(params, scope, ctx) -> PresetPlan: ...
    async def apply(params, scope, ctx)   -> AppliedDescriptor | TaskHandle: ...
    async def revoke(applied_descriptor, ctx) -> None | TaskHandle: ...


class CompositePreset(Preset):
    """Subclass for presets that delegate to other presets.

    Composite presets do not own apply/revoke logic — the base implementation
    iterates children forward on apply and reverse on revoke. Child failures
    during apply roll back prior children in reverse order.
    """
    compose: ClassVar[Tuple[str, ...]]           # ordered child preset names
```

The self-lockout guard is invoked on DELETE when `"iam" in preset.keywords` — no separate flag. The registry validates `compose` references at registration time. `version` and explicit drift handling are out of v1 (will be added when the first preset evolves; the audit row's `params_snapshot` is enough to detect "same call" idempotency for now).

### The PresetContext

The narrow surface a preset is permitted to touch. Passed to every `dry_run` / `apply` / `revoke` call; no preset reaches into globals.

| Field | Purpose | Real method names (verified) |
|---|---|---|
| `ctx.db` | `managed_transaction` handle; queries via `DQLQuery` / `DDLQuery` wrappers (per the standard adopted in #1404 / #1406). | `managed_transaction(engine)` |
| `ctx.iam` | `IamService` — roles + role hierarchy. | `create_role(role, catalog_id)` raises on conflict; `update_role(role, catalog_id)` is the upsert; `delete_role(name, cascade, catalog_id)`; `add_role_hierarchy(parent, child, catalog_id)` |
| `ctx.policy` | `PolicyService` — policy CRUD. | `create_policy(policy, catalog_id)`; `update_policy(policy, catalog_id)` is the upsert; `delete_policy(policy_id, catalog_id)` |
| `ctx.config` | `PluginConfig` reader+writer. | `get_persisted_config(config_cls, **scope)` reads tier-local; `set_config(config_cls, config, **scope)` writes. **No built-in snapshot/restore** — preset apply must explicitly snapshot prior config into the audit row's `revoke_descriptor` and restore by re-`set_config(prior)` (or delete if it never existed). Mirror the divergence-check pattern already used in `admin_service.py:390–419`. |
| `ctx.tasks` | Task creation. | `create_task(engine, task_data: TaskCreate, schema, initial_status, owner_id, locked_until)`. **`caller_id` is not auto-stamped** — `ctx.tasks` is a thin wrapper that auto-fills `caller_id=f"preset:{name}@{scope_key}"` into the `TaskCreate` payload before calling `create_task`. |
| `ctx.cron` | `register_cron_job(conn, job_name, schedule, command)` / `unregister_cron_job(conn, job_name)`. Both require an active conn from `ctx.db`. |
| `ctx.libs` | Curated library handles (gdal, valkey, gcs, …) — opt-in per preset; not used by core IAM presets. |
| `ctx.principal` | The caller's principal — used for audit and the self-lockout check. |
| `ctx.scope` | Resolved scope (`platform` / `catalog:{id}` / `catalog:{id}/collection:{id}`). |

This boundary is what makes presets testable and auditable: the context is mocked in unit tests; in production it is the only path through which a preset may mutate state.

### Transactional model — v1 best-effort, not strictly atomic across subsystems

Validation confirmed there is no existing pattern in geoid for "one request mutates multiple subsystems in a single transaction." `PolicyService.create_policy`, `create_task`, and `set_config` each wrap their own `managed_transaction`. Retrofitting `conn` parameter threading across all mutators is its own multi-PR refactor and out of scope here.

For v1 the preset apply is **best-effort with incremental rollback**:

- `apply()` writes incrementally; the preset records every successful write into the in-progress descriptor.
- If a sub-call fails, the preset's `apply()` walks the in-progress descriptor in reverse, calling the inverse of each successful step (e.g. `delete_policy` for each `create_policy` that succeeded), then returns failure.
- The audit row reaches `state=failed` with `last_error` populated and `revoke_descriptor=NULL` (because the system is back to pre-apply state).
- If the cleanup walk itself fails (rare; usually means the DB went away), the audit row keeps `revoke_descriptor` populated with whatever partial state survived, so a later DELETE can finish the cleanup.

This trades strict atomicity for an existing-pattern-compatible implementation. The cost is bounded: every IAM and config preset apply is at most ~10 operations; the incremental rollback list is tiny; failure during apply is rare on a healthy DB.

### The audit table

```
iam.applied_presets
─────────────────────────────────────────────────────────────────────────────
preset_name        TEXT          \\ PK part 1
scope_key          TEXT          /  PK part 2 (e.g. "catalog:cat-7")
state              TEXT          -- pending | in_progress | applied
                                 --   | revoke_pending | revoke_in_progress
                                 --   | revoked | failed | revoke_failed
applied_at         TIMESTAMPTZ
applied_by         UUID          -- principal_id (NULL for backfill)
params_snapshot    JSONB         -- exactly what was passed in
revoke_descriptor  JSONB         -- preset-defined opaque payload; for IAM presets it
                                 -- partitions by catalog_schema when scope is catalog-tier
                                 -- so revoke restricts itself to the right tenant rows
apply_task_id      UUID          -- async apply only
revoke_task_id     UUID          -- async revoke only
last_error         TEXT
updated_at         TIMESTAMPTZ
```

Row uniqueness is `(preset_name, scope_key)`. The row survives revoke (`state=revoked`) for forensic history; a fresh apply at the same `(name, scope)` upserts in place.

### Lifecycle

```
POST /admin/presets/{name}                                          (platform scope)
POST /admin/catalogs/{cat}/presets/{name}                           (catalog scope)
POST /admin/catalogs/{cat}/collections/{coll}/presets/{name}        (collection scope)
  -- IamMiddleware evaluates existing policies on (method, path, params) -- no preset-specific authz code
  ├ validate URL tier vs preset.tier / preset.catalog_scopable → 400 if mismatch
  ├ validate body against preset.params_model
  ├ acquire row-lock on (preset_name, scope_key) — prevents concurrent operator races
  ├ existing row, same params_snapshot → 200, noop
  ├ existing row, different params_snapshot, no ?force → 409 with diff
  ├ insert pending row
  ├ preset.apply(params, scope, ctx)
  │   sync  → revoke_descriptor populated, state=applied
  │   async → apply_task_id set, state=in_progress; worker callback finalises
  └ return audit row

DELETE /admin/presets/{name}                                        (and scoped variants)
  -- IamMiddleware evaluates existing policies on (method, path, params)
  ├ look up audit row for (preset_name, scope_key); 404 if absent
  ├ if "iam" in preset.keywords: simulate post-revoke effective rights for the caller
  │   if caller loses authorisation for the current request scope → 409
  │   ?force_self_revoke=true overrides
  ├ acquire row-lock
  ├ preset.revoke(revoke_descriptor, ctx)
  │   sync  → state=revoked
  │   async → revoke_task_id set, state=revoke_pending; worker callback finalises
  └ return audit row
```

Failed apply leaves `state=failed` with `last_error` populated; the audit row is preserved (no orphan grants — apply must clean up its own partial state before returning failure). Failed revoke leaves `state=revoke_failed` with the partially-reversed descriptor still in `revoke_descriptor`; operator can retry the DELETE or clean up manually.

### Composite presets

A composite declares `compose = ("child_a", "child_b", …)`. Apply iterates forward; revoke iterates reverse. Each child gets its own audit row; the composite gets a row marked `keywords=("composite",)` whose `revoke_descriptor` lists the child `(name, scope_key)` pairs that succeeded. If any **synchronous** child apply fails, prior children are revoked in reverse — no half-applied composite reaches `state=applied`.

**Async children in composites — v1 limitation.** When a composite contains an async child (one whose apply returns `TaskHandle`), the composite's `apply` call returns before the async child has finished. If that async child later fails, the composite cannot synchronously roll back prior children. The v1 behaviour: the composite's audit row transitions to `state=partial` with `last_error` populated. The platform-side worker callback updates the composite row when a child's task terminates. Operators see the `partial` state in `GET /admin/presets/{name}` and must explicitly DELETE the composite to trigger reverse rollback of every child that reached `applied`. This trades convenience for predictability; auto-rolling back chains of async revokes is novel for the codebase and is deferred until a real use case demands it. Curated composites in v1 (`platform_demo`, `public_open_data`, `private_tenant`) compose only synchronous IAM and routing presets, so the limitation does not bite the supported set; it is a constraint authors of new composites must respect.

This is how `platform_demo` reproduces today's all-on behaviour: it composes `iam_baseline` + `default_roles_baseline` + per-extension `{ext}_enable` + `oidc_sysadmin_binding` + the routing presets currently auto-applied.

### Search / discovery API

`GET /admin/presets` (and its catalog- / collection-scoped variants) returns a paginated, policy-filtered list:

| Query param | Purpose |
|---|---|
| `q` | Full-text on `name`, `description`, `keywords`. |
| `name` | Exact or prefix match. |
| `tier` | One or more `PresetTier` values. |
| `keywords` | Comma-separated AND match. |
| `applied` | `any` / `here` / `nowhere` — filter by audit presence at the requested scope. |
| `limit`, `cursor` | Keyset pagination on `name`. |

Visibility is determined by the existing IAM policies on the list endpoint itself (so a catalog admin GETting the catalog-scoped list path sees what policy admits them to see at that scope). Each result entry carries:

```
{
  "name": "stac_enable",
  "description": "...",
  "keywords": ["iam", "stac"],
  "tier": "catalog",
  "catalog_scopable": false,
  "is_async": false,
  "params_schema": { ... JSONSchema export of params_model ... },
  "currently_applied_to": ["catalog:cat-7"]
}
```

`GET /admin/presets/{name}` returns the same shape plus the last N audit events at scopes the caller can see. `POST /admin/presets/{name}/dry-run` accepts a params body and returns a `PresetPlan` (what would change) without writes.

### Scope and authorisation

This is intentionally thin: the existing IAM policy model (per `geoid/CLAUDE.md`: "ALL authz lives in `Policy` / `Role` / `ConditionHandler` declarations registered via `PermissionProtocol`; the existing `IamMiddleware` evaluates them") already covers what's needed. Presets neither introduce new permission strings nor a parallel authorisation evaluator.

The three URL shapes (`/admin/presets`, `/admin/catalogs/{cat}/presets`, `/admin/catalogs/{cat}/collections/{coll}/presets`) are normal IAM-gated endpoints. Each is matched by ordinary policies that condition on method + path + (optionally) the `{name}` path param or request payload. The preset handler does only two things on top of `IamMiddleware`:

1. Validate URL tier vs `preset.tier` / `preset.catalog_scopable` (400 if mismatch — a `tier=platform`, `catalog_scopable=false` preset POSTed at a catalog URL returns 400; nothing IAM-related).
2. Run the self-lockout runtime guard on DELETE when `"iam" in preset.keywords`.

Delegation is itself an `iam_baseline` outcome: that preset's `apply()` calls `ctx.policy.upsert_policy(...)` to install policies of the shape "role `admin`, methods `[POST, DELETE]`, path `/admin/catalogs/{cat}/presets/{name}`, condition `name IN ['stac_enable', 'features_enable', ...]`". Catalog admins inherit `admin` via the existing role hierarchy; everything downstream is the platform's normal authz path.

The self-lockout guard is scope-local: a platform sysadmin revoking a catalog-tier preset never trips it (their authorisation lives at a different scope).

### Cold-start path after the refactor

Module lifespan in the steady state does only:

1. IAM schema DDL — unchanged.
2. Sysadmin-role survival row — the existing `module.py:149` guard, kept verbatim. This is the only seed; nothing else.
3. Register every preset module into the `PresetRegistry` (no application — registration is a directory walk that imports `presets/` packages from each extension; importing has no side effects beyond appending to the registry).

A fresh deployment:

1. Operator with the `geoid.sysadmin` Keycloak realm role logs in.
2. `OidcRoleSyncConfig.role_mapping` maps `geoid.sysadmin → sysadmin` and the auto-register flow grants them the internal role at first login. (This mapping itself becomes `oidc_sysadmin_binding`, applied by `platform_demo`; on a true first login the hardcoded default in `oidc_role_sync_config.py` remains the fallback until that preset runs.)
3. Sysadmin browses `GET /admin/presets`, POSTs `platform_demo` (or selects atomic presets one by one).
4. Composite applies children: `iam_baseline`, `default_roles_baseline`, per-extension presets, routing presets.
5. Platform is now in the same state today's automatic seed produces.

An existing deployment after upgrade:

1. Migration in PR-2 backfills `applied_presets` rows reflecting the current DB state (`state=applied`, `revoke_descriptor` reconstructed from the present grants/policies). Nothing user-visible changes.
2. Lifespan stops calling `provision_default_policies` and the contributor loop. Boot becomes faster.
3. If operators want to alter what today's contributors install, they DELETE the audit row (revoke) and re-apply with different params, or apply a different preset.

### Decoupling sequencing

| PR | Scope | Risk | Reversible by |
|----|-------|------|----------------|
| **PR-1** | `Preset` protocol + `PresetRegistry` + `applied_presets` table + admin endpoints (search, dry-run, scoped POST/DELETE, self-lockout) + `RoutingPreset` adapted to new protocol with no behaviour change. Adapter supports the existing `on_applied` hook plus a new symmetric optional `on_revoked` hook (needed because `geoid` routing preset's `on_applied` calls IAM via `register_geoid_policies_for_catalog`; without the symmetric hook, revoke would orphan those IAM rows). | Low — no IAM lifecycle change yet. | Revert. |
| **PR-2** | `iam_baseline` preset built from today's `IamExtension.get_policies()` + `get_role_bindings()`. Stop calling `provision_default_policies` for IAM's own contributions in lifespan. Backfill migration. | Medium — first removal from lifespan; backfill must be exact. | Revert (migration is forward-only but lifespan re-seeds on re-enable). |
| **PR-3** | Per-extension presets. Validation against the codebase confirmed every existing `PolicyContributor` is pure-data and uniformly reversible, so a single generic `PolicyContributorPreset(contributor=…)` wrapper handles all of them. Per-extension declaration becomes one line each: `register_preset(PolicyContributorPreset(StacPolicyContributor(), name="stac_enable", keywords=("iam","stac"), …))`. Each removes the contributor's runtime self-registration. Backfill row per applied preset. AppliedDescriptor shape: `{contributor_name, policy_ids: [...], role_names: [...]}` — revoke deletes policies by id and removes role bindings; never deletes shared roles (e.g. `sysadmin`, `anonymous`) since multiple contributors bind to them. | Low per-extension; we can ship them in one PR. | Revert per extension. |
| **PR-4** | Curated composites: `platform_demo`, `public_open_data`, `private_tenant`. | Low — pure composition. | Revert. |
| **PR-5** | Delete `PolicyContributor` protocol, the `get_protocols(PolicyContributor)` discovery loop, `_ALWAYS_REFRESH`, and `provision_default_policies` itself. Lifespan reaches its target minimum. **Includes a one-shot `public_access` normalization migration**: explicit `UPDATE iam.policies SET ... WHERE id='public_access'` (or DELETE+INSERT) to ensure the narrowed shape from commit `eaa8cbf89` is persisted in every deployment. Without this, pre-2026-04-29 deployments could silently retain the over-broad `/web/.*` regex once `_ALWAYS_REFRESH` is removed. The migration also includes a pre-check that alerts (does not block) if the policy still contains the unanchored pattern. | Med — public_access normalization is load-bearing for prod safety. | Revert + re-introduce protocol if needed. |
| **PR-6** | Admin frontend: paginated preset list with keyword/scope filters, params form generated from `params_schema`, apply / dry-run / rollback buttons, applied-presets history per scope. | Low — purely additive UI on top of REST. | Revert. |
| **PR-7** | `default_roles_baseline` preset (admin / editor / user / unauthenticated + hierarchy), decoupled from `authorization.py` defaults. | Medium — operator override stops being lifecycle-overwritten. | Revert. |

PR ordering allows pausing after any number — the system remains functional with mixed contributor/preset behaviour up to PR-5.

### Backfill migration (load-bearing)

After PR-2 the existing prod state must be representable as `applied_presets` rows so DELETE works. The migration:

1. Snapshots the present `policies` and grants/bindings.
2. For each contributor that's about to stop running, computes the `revoke_descriptor` it would produce.
3. Inserts one `applied_presets` row per `(preset_name, scope_key)` with `state=applied`, `applied_by=NULL` (system backfill), `params_snapshot={}`, and `revoke_descriptor` populated from the computed snapshot. A subsequent operator POST with empty params is a no-op (snapshots match); a POST with non-empty params returns 409 with the diff and requires `?force=true` to replace.

If backfill diverges from reality, DELETE either no-ops (descriptor refers to absent rows) or partially reverses; either case is recoverable. The migration is reversible (drop the rows it added).

### Example: an async, destructive preset

`purge_orphan_assets` (illustrative; not in the v1 scope but proves the abstraction):

```python
class PurgeOrphanAssets(Preset):
    name = "purge_orphan_assets"
    keywords = ("data", "destructive", "async")
    tier = CATALOG
    catalog_scopable = False
    params_model = PurgeOrphanAssetsParams
    is_async = True
    # no "iam" in keywords → self-lockout guard does not run

    async def apply(self, params, scope, ctx):
        task_id = await ctx.tasks.create(
            kind="purge_orphan_assets",
            payload={"scope": scope.key, "max_age_days": params.max_age_days},
            caller_id=f"preset:{self.name}@{scope.key}",
        )
        return TaskHandle(task_id=task_id, revoke_descriptor=None)

    async def revoke(self, applied_descriptor, ctx):
        raise IrreversibleRevokeError(
            "purge_orphan_assets is irreversible; pass force_destructive_revoke=true "
            "to mark un-applied without rollback"
        )
```

A DELETE on this returns 409 unless `force_destructive_revoke=true` — the `destructive` keyword is the human-readable hint, the `IrreversibleRevokeError` is the enforcement.

## Risks and open items

- **Extension presence at apply time.** `platform_demo` references `stac_enable` etc.; the composite must validate every child is registered when apply runs (clear 409 if an extension is not present in this deployment). Children registered later become available without code change.
- **OIDC role mapping at first boot.** The hardcoded default in `oidc_role_sync_config.py` must remain as a fallback for the very first sysadmin login, because no preset can have been applied yet. The `oidc_sysadmin_binding` preset only takes over the second time around. Symmetric design call for revoke: if an operator DELETEs `oidc_sysadmin_binding`, the system falls back to the hardcoded default (which restores the same mapping that existed before any preset was applied). An operator who genuinely wants no fallback (i.e. wants the OIDC mapping fully removed and not auto-restored) sets `IDP_DISABLE_HARDCODED_FALLBACK=true`; in that case the hardcoded default is ignored and revoke leaves the platform with no OIDC role mapping (potentially locking out everyone).
- **Per-tenant catalog isolation.** `revoke_descriptor` for catalog-scoped IAM presets must partition by `catalog_schema`; the partition shape is set at apply time and validated at revoke. Tested per #1337 / #1341 / #1344 tenancy work.
- **Concurrency vs long-running async apply.** Row lock on `(preset_name, scope_key)` is held only across the synchronous `apply` call; for async, the audit row's `state=in_progress` acts as the lock. A second POST while a row is `in_progress` returns 409.
- **Removal of `_ALWAYS_REFRESH`.** The `public_access` policy was force-refreshed every boot to self-heal a pre-2026-04-29 overly-broad version. After removal, deployments that still hold the old shape will keep it forever unless explicitly re-applied. A one-shot migration in PR-5 normalises the row.
- **The "tasks" preset namespace.** Async preset apply spawns rows in the global `tasks.tasks` table (per #1404 architecture). Naming convention `preset:{name}@{scope_key}` for `caller_id` keeps the existing attribution and delete-attribution chains (#1407) coherent.

## Test plan

Per preset (unit, via mocked `PresetContext`):

- `apply` idempotency — second call with same params is a no-op returning the existing audit row.
- `apply` then `revoke` returns the system to its pre-apply state for every field the preset touches.
- `dry_run` matches `apply` in scope; never writes.
- `params_model` rejects malformed input.

Cross-preset (integration, real DB):

- Backfill migration on a representative seed produces audit rows whose DELETE cleanly reverses to the present state.
- A composite preset with a deliberately failing child rolls back prior children and returns 5xx with the failure cause; the audit table has no `applied` rows for that composite.
- Self-lockout: a fresh sysadmin POSTs `iam_baseline`, then DELETEs `iam_baseline` → 409. Same DELETE with `?force_self_revoke=true` succeeds and the principal loses sysadmin.
- Scope: a catalog-scoped admin can POST/DELETE catalog-tier presets at their catalog and is 403'd elsewhere.

End-to-end (full stack):

- Fresh empty DB → sysadmin first login → POST `platform_demo` → application reaches functional parity with today's auto-seeded boot.
- Existing prod-shaped DB → backfill migration → no functional change → DELETE+re-apply of one atomic preset round-trips cleanly.

## Out of scope

- GitOps reconcile from a manifest file.
- Preset versioning that auto-migrates `applied_versions` to the current code's `version` without operator action.
- A preset-authoring SDK distinct from "implement the protocol in Python".
- Federated multi-cluster preset distribution.
