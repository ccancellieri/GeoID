# STAC API — Asset Transactions Extension (proposed)

**Audience**: STAC API Extensions SWG, OGC API SWG (for longer-term track).
**Status**: Draft — pilot implementation in
[un-fao/geoid](https://github.com/un-fao/geoid) (`extensions/assets`,
`modules/gcp/asset_processes.py`, `models/protocols/asset_process.py`).
**Companion of**: [STAC Transaction Extension][txn], [OGC API — Features Part 4][feat4].

[txn]: https://github.com/stac-api-extensions/transaction
[feat4]: https://docs.ogc.org/DRAFTS/20-002.html

---

## 1. Problem

STAC defines **Assets** as a data model — key-value descriptors attached to
Items or Collections with `href`, `type`, `roles`, `title`. Nothing in the
STAC or OGC stack standardises **asset lifecycle operations**:

| Operation | STAC | OGC API - Features | OGC API - Processes |
|---|---|---|---|
| Create Item/Feature | Transaction Ext | Part 4 (CRUD) | n/a |
| Upload a blob behind an asset | — | — | — |
| Pre-signed PUT for direct client → object-store upload | — | — | — |
| Pre-signed GET (short-lived download) | — | — | — |
| Register an external-URL asset and later trigger ingestion | — | — | partial |
| Delete blob + asset + derived items atomically | — | — | — |
| Search assets by metadata independently of parent Item | — | — | — |

Every provider of raster/vector/data-lake platforms therefore re-invents these
operations with bespoke REST surfaces:

- **GeoServer REST** — `/workspaces/{ws}/coveragestores/{cs}/file.{ext}` (PUT raw)
- **Mapbox Upload API** — temp S3 creds + multi-step upload
- **ArcGIS Online Items API** — multipart upload + item publishing
- **pygeoapi** — no upload; read-only providers
- **GeoNetwork** — file attachments, XML-centric, no signed URLs
- **DynaStore (this repo)** — `POST /assets/catalogs/{cid}/upload` → `UploadTicket`

The absence of a shared contract means:

1. **Client fragmentation** — every desktop/web tool writes per-vendor code.
2. **No interoperable download story** — signed URLs are ad-hoc headers
   everywhere (e.g. STAC `assets[].alternate` only hints at mirrors).
3. **No discovery of asset-scoped operations** — a client cannot ask "what
   can I do with *this* asset?" (download, ingest, validate, convert).

## 2. Scope

This proposal targets **Asset Transactions** — CRUD and execution operations
scoped to a Catalog / Collection / Item **asset** — as a STAC API extension.
It is deliberately narrower than a full "OGC API — Assets" spec (§ 7).

Out of scope:

- Extending the asset data model itself (covered by existing STAC extensions).
- Ingestion semantics for the derived Items (provider-specific).
- Access-control model (reuses the host API's authn/z).

## 3. Ingredients already standardised

1. **STAC Assets** — data model, `href`, `type`, `roles`, `alternate`.
2. **STAC Transaction Extension** — Item/Collection CRUD.
3. **OGC API - Features Part 4** — Create/Replace/Update/Delete at the Feature level.
4. **OGC API - Processes Part 1** — typed process execution with job handles.
5. **RFC 6750 + cloud-vendor V4 signing** — pre-signed URL contracts.

What is missing is a **process surface for asset-scoped operations** that
reuses OGC Processes-style `process_id` routing, but is anchored at the asset
rather than at a standalone process catalogue.

## 4. Proposed resource model

The extension adds these paths under any STAC API (or sibling asset-serving
API) host:

```
GET    /catalogs/{cid}/assets/{aid}/processes
GET    /catalogs/{cid}/collections/{colid}/assets/{aid}/processes
GET    /catalogs/{cid}/assets/{aid}/{process_id}           # idempotent (download, inspect)
POST   /catalogs/{cid}/assets/{aid}/{process_id}           # state-changing (ingest, convert)
POST   /catalogs/{cid}/assets/{aid}/upload                 # init signed-PUT session
GET    /catalogs/{cid}/assets/{aid}/upload/{ticket_id}/status
```

For STAC-native hosts, the equivalent anchors are:

```
/collections/{cid}/items/{iid}/assets/{aid}/processes
/collections/{cid}/items/{iid}/assets/{aid}/{process_id}
```

### 4.1 Process discovery

```http
GET /catalogs/landsat/assets/LC09_198030_20251225/processes
→ 200 [
  {
    "process_id": "download",
    "title": "Download",
    "description": "Short-lived signed GET URL.",
    "http_method": "GET",
    "applicable": true,
    "parameters_schema": { "type":"object", "properties": { "ttl": {"type":"integer","minimum":60,"maximum":604800,"default":86400} } }
  },
  {
    "process_id": "ingest",
    "title": "Ingest",
    "description": "Parse the blob and produce Items in the linked collection.",
    "http_method": "POST",
    "applicable": true,
    "parameters_schema": { "$ref": "#/components/schemas/IngestionParams" }
  }
]
```

Each entry carries `applicable: false` with a short `reason` when registered
but inapplicable (e.g. `download` is not applicable to a driver-native asset
that cannot be represented as a standalone blob).

### 4.2 Process invocation output shape

All invocations return a single polymorphic envelope:

```json
{
  "type": "signed_url" | "redirect" | "job" | "inline",
  "url": "https://storage.googleapis.com/...?X-Goog-Signature=...",
  "method": "GET",
  "expires_at": "2026-04-18T12:34:56Z",
  "headers": {},
  "job_id": "ogc-process-a1b2",
  "data": { ... }
}
```

- `signed_url` — client follows `url` with `method` + `headers`.
- `redirect` — the server MAY instead return HTTP 302; equivalent.
- `job` — async execution; poll the OGC Processes `jobs/{job_id}` endpoint.
- `inline` — result body small enough to return inline (validation reports).

### 4.3 Upload ticket (symmetric to download)

The upload surface — already shipped in many providers — is folded into the
same model: `initiate_upload` returns a `signed_url`-shaped result with
`method: "PUT"` and backend-specific headers; clients reuse the same
client-side code path as `download`.

## 5. Conformance classes

The namespace base is
`https://stac-extensions.github.io/asset-transactions/1.0/conf` (reserved-pending
until the extension is accepted; see §6.3). The reference implementation emits
these exact URIs from `extensions/assets/conformance.py`, so code and this
document stay aligned.

### 5.1 Core classes

| Class | URI | Requires |
|---|---|---|
| Core | `…/asset-transactions/1.0/conf/core` | Landing + `/conformance` + asset GET/list + process discovery |
| Upload | `…/asset-transactions/1.0/conf/upload` | `POST .../upload` + ticket polling, OR `upload` AssetProcess at `/processes/upload/execution` |
| Process execution | `…/asset-transactions/1.0/conf/processes` | Arbitrary `{process_id}` with `parameters_schema` |
| Async | `…/asset-transactions/1.0/conf/async` | `type:job` + OGC Processes integration |
| Search | `…/asset-transactions/1.0/conf/search` | `POST /assets-search` with `AssetFilter` (scalar operator set) |
| Sync | `…/asset-transactions/1.0/conf/sync` | Asset CRUD MUST emit a sync-event consumable by downstream subscribers; reverse cascade via `DELETE .../assets/{id}?force=true&propagate=true` deletes items linked through the asset. Backends advertise per-event delivery semantics via `Asset.sync.delivery: at-least-once \| exactly-once`. Bucket-side metadata mirroring (e.g. `x-goog-meta-*`) is **advisory**, not part of this class. |

### 5.2 Implementation-profile classes

The pilot enforces additional capabilities beyond the core set above and
advertises them as separate classes so a client can probe each independently.
These are candidates for inclusion in the core set as the proposal matures.

| Class | URI | Requires |
|---|---|---|
| Write policies | `…/asset-transactions/1.0/conf/write-policies` | Per-collection/per-catalog `AssetsWritePolicy`: identity matcher chain + conflict-action vocabulary |
| Versioning | `…/asset-transactions/1.0/conf/versioning` | `new_version` on conflict (driver-capability-gated) |
| Virtual assets | `…/asset-transactions/1.0/conf/virtual-assets` | `kind=virtual` external-href registration without bucket presence |
| References | `…/asset-transactions/1.0/conf/references` | `asset_references` cascade-delete safety across driver-owned referencing entities |

A server advertises all of these in its `/conformance` document; clients can
probe capability without trial-and-error.

### Sync conformance — semantics

The reference implementation (DynaStore) realises the `Sync` class via:

- **Single-writer outbox**: `AssetService` writes the PG row and inserts an `ASSET_*` row into the `events` table in the same transaction. Downstream subscribers (`AssetEntitySyncSubscriber`, `BucketAnnotationPatcher`, `ItemReverseCascadeSubscriber`) consume from the outbox, never from a parallel listener bus — eliminating dual-writer races.
- **Advisory mirroring**: `BucketAnnotationPatcher` patches GCS object metadata (`x-goog-meta-*`) on a best-effort basis; failures log and never raise. The asset row + outbox remain the source of truth.
- **Reverse cascade**: `ItemReverseCascadeSubscriber` listens for `ASSET_HARD_DELETION` with `propagate=true` in the payload. It enumerates items whose `extra_metadata->'assets' ? :asset_id` and deletes them via the items API. Forward cascade (item delete → asset cleanup) is enforced by a PG trigger on the items sidecar.
- **Disaster recovery**: an out-of-band `asset reconcile` CLI walks PG asset rows, diffs against the routed index driver, and replays missing `ASSET_UPDATE` events through the outbox so subscribers re-mirror.

## 6. STAC extension vs. new OGC API — recommendation

Two viable tracks exist; I recommend **both, staged**:

### 6.1 Track A — STAC API extension (ship first)

**Why**: STAC's extension process is lightweight, receptive, and already
covers Item-level Transactions. Assets are first-class in STAC; adding
asset-scoped process invocation is the narrowest interoperable step.

**Process** (per [STAC extensions README][stac-ext]):

1. Open issue at `stac-api-extensions/*` proposing a new repo.
2. Bootstrap from the extension template; publish the JSON-Schema
   fragments (`parameters_schema` per process) and OpenAPI snippets.
3. Reference the pilot implementation (this repo).
4. Seek two additional implementations for "Community" stage.

[stac-ext]: https://github.com/radiantearth/stac-spec/blob/master/extensions/README.md#proposing-new-extensions

### 6.2 Track B — OGC API - Assets (long-term)

**Why**: OGC API - Assets as a standalone specification would bind Features,
Processes, Records, and STAC together with a shared asset lifecycle. Higher
adoption ceiling, wider client reach, but needs a SWG and a 12–18 month
cycle. The STAC extension's conformance classes map directly to OGC API
requirement classes, so the migration is mechanical.

**Process**:

1. Present the STAC extension to the OGC GDC SWG (already has coverage SWG
   overlap) as evidence of demand.
2. Propose a charter: "OGC API - Assets", reusing `AssetProcessProtocol`
   semantics and the `UploadTicket` / `AssetProcessOutput` models.
3. Align with OGC API - Common landing / conformance scaffolding.

### 6.3 Decision

**Start with the STAC extension.** Reasons:

1. **Faster shipping** — the STAC extension registry accepts new entries
   in weeks, not OGC's 12–18 months.
2. **Natural anchor** — STAC already has Assets as first-class; the
   extension slots in without inventing a new resource root.
3. **Community ready** — STAC client projects (stac-browser, stac-fastapi,
   pystac-client) actively extend via these patterns.
4. **Escape hatch** — conformance classes are drafted to be cleanly liftable
   into an OGC API spec later, without renaming URIs or reshaping payloads.

The OGC track runs in parallel once two independent STAC implementations
exist.

## 7. Boundary with OGC API - Processes

The extension does **not** replace OGC Processes. Rather:

- `GET .../{process_id}` is equivalent to a side-effect-free Process call
  with the asset as the implicit first input.
- `POST .../{process_id}` with `type: job` response is a thin wrapper over
  OGC Processes `execute`; the `job_id` refers to the host's Processes API.
- `GET .../processes` is a per-asset projection of the global Process catalog
  filtered by `applicable(asset)`.

A host that already implements OGC Processes should expose the same processes
through this surface by implementing the `AssetProcessProtocol` adapter — a
one-line delegation in most cases.

## 8. Reference implementation

This proposal is backed by a working implementation in
[un-fao/geoid][geoid]:

| Artefact | Path |
|---|---|
| Protocol | `src/dynastore/models/protocols/asset_process.py` |
| Router | `src/dynastore/extensions/assets/assets_service.py` (`list_*_processes`, `invoke_*_process`) |
| Process (GCS download) | `src/dynastore/modules/gcp/asset_processes.py::GcsDownloadAssetProcess` |
| Shared GCS signing | `src/dynastore/modules/gcp/tools/signed_urls.py` |
| Upload surface (existing) | `src/dynastore/modules/gcp/gcp_storage_ops.py::GCPModule.initiate_upload` |

Runtime registration uses DynaStore's capability-protocol pattern
(`get_protocols(AssetProcessProtocol)`) so new processes — `ingest`,
`validate`, `convert`, domain-specific — plug in without modifying the router.

[geoid]: https://github.com/un-fao/geoid

## 9. Open questions

1. **Anchor naming** — STAC uses `/collections/{cid}/items/{iid}/assets/{aid}`;
   DynaStore uses `/catalogs/{cid}/assets/{aid}` (assets can be catalog-level,
   not just item-level). The extension should allow both; the conformance
   class nominates one as canonical.
2. **Process ID namespacing** — should `ingest` be `dynastore:ingest` to
   avoid collisions once multiple implementations ship custom processes?
   Precedent: OGC Processes uses provider-prefixed IDs in practice.
3. **Auth model** — signed URLs bypass the host's authn/z on follow-up; the
   spec must mandate that `ttl` be bounded and expose the cap via
   `parameters_schema.properties.ttl.maximum`.
4. **Relationship to OGC Coverages — Part 2 (Transactions)** — if/when that
   draft matures, parts of this extension overlap. The conformance classes
   are designed to be subsumed cleanly.

## 10. Next steps

- [ ] File issue at `stac-api-extensions/` proposing a new `asset-transactions`
      extension repo.
- [ ] Publish the OpenAPI fragments and JSON-Schema parameter schemas as
      standalone artefacts.
- [ ] Add pilot implementations: `validate` (geospatial format check),
      `ingest` (trigger existing `modules/tasks/ingestion` pipeline),
      `inspect` (return GDAL/OGR metadata inline).
- [ ] Present at the next OGC GDC SWG meeting as a companion to the
      ogc-dimensions and STAC-datacube work already on the agenda.
