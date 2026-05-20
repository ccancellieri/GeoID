# Asset upload — review-env smoke runbook

This runbook validates the asset upload lifecycle end-to-end on the **review** environment after a deploy that touches any of:

- `extensions/assets/assets_service.py` (REST surface, write-policy gate)
- `modules/catalog/asset_distributed.py` (chain runner)
- `modules/catalog/drivers/pg_asset_driver.py` (DDL)
- `modules/gcp/gcp_finalize_activator.py` (Pub/Sub activator)
- `extensions/gcp/gcp_events.py` (Pub/Sub HTTP handler)

GCP Pub/Sub push subscriptions cannot reach `localhost`, so end-to-end finalize verification only works against a deployed environment. This runbook covers the manual smoke flow on review.

---

## Prerequisites

- Deploy succeeded for `geospatial-catalog`, `geospatial-geoid`, and `geospatial-tools` services.
- Review DB has been reset (`Reset database (review only)` workflow with `mode=full, dry_run=false`) — required after schema-touching deploys.
- A sysadmin token for review (`DYNASTORE_SYSADMIN_TOKEN`).
- A small test file (`data.tif` or any binary, ~1 MB).

```bash
export REVIEW_BASE=https://data.review.fao.org/geospatial/v2/api
export TOKEN="$DYNASTORE_SYSADMIN_TOKEN"
export CAT=smoke-$(date +%s)
export COL=images
```

---

## Flow

### 1. Create the catalog and a collection

```bash
curl -X POST "$REVIEW_BASE/catalogs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"id\": \"$CAT\", \"title\": \"Smoke catalog\"}"

curl -X POST "$REVIEW_BASE/catalogs/$CAT/collections" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"id\": \"$COL\", \"title\": \"Images\"}"
```

Expected: 201 Created on each. The catalog readiness extension provisions the bucket + IAM in the background; wait until the catalog's `provisioning_status=ready` (poll once or twice over ~30s):

```bash
until curl -s "$REVIEW_BASE/catalogs/$CAT" -H "Authorization: Bearer $TOKEN" \
  | jq -e '.provisioning_status == "ready"' > /dev/null; do
  sleep 5
done
```

### 2. Confirm the OGC surface

```bash
curl -s "$REVIEW_BASE/assets/" -H "Authorization: Bearer $TOKEN" | jq '.links[] | .rel'
# Expected: "self", "conformance", "data"

curl -s "$REVIEW_BASE/assets/conformance" -H "Authorization: Bearer $TOKEN" \
  | jq '.conformsTo[] | select(test("asset-transactions"))'
# Expected: ten URIs ending in /core /upload /processes /async /search /sync
#           /write-policies /versioning /virtual-assets /references
```

### 3. Initiate upload — happy path

```bash
curl -s -X POST \
  "$REVIEW_BASE/assets/catalogs/$CAT/collections/$COL/upload" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "filename": "data.tif",
    "content_type": "image/tiff",
    "asset": {
      "asset_id": "smoke-asset-1",
      "asset_type": "RASTER",
      "metadata": {"smoke": true}
    }
  }' | tee /tmp/ticket.json | jq '.ticket_id, .upload_url[:80], .method, .backend'
```

Expected: 200 OK with a `ticket_id`, a GCS resumable signed URL (begins with `https://storage.googleapis.com/...`), `method=PUT`, `backend=gcs`.

**Server-side invariant — verify the born-claimed PENDING row exists immediately:**

```bash
curl -s "$REVIEW_BASE/assets/catalogs/$CAT/collections/$COL/assets-search" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"filters": [{"field": "asset_id", "op": "eq", "value": "smoke-asset-1"}], "limit": 5}' \
  | jq '.[] | {asset_id, status, kind, filename, uri, content_hash}'
```

Expected: one row with `status="pending"`, `kind="physical"`, `filename="data.tif"`, `uri=null`, `content_hash=null`.

### 4. Upload bytes to the signed URL

```bash
UPLOAD_URL=$(jq -r '.upload_url' /tmp/ticket.json)
curl -X PUT "$UPLOAD_URL" \
  -H "Content-Type: image/tiff" \
  --data-binary @data.tif \
  -w "HTTP %{http_code}\n"
```

Expected: HTTP 200 from GCS.

### 5. Watch the activator transition the row to ACTIVE

GCS fires `OBJECT_FINALIZE` → Pub/Sub push → review's `/gcp/events/pubsub-push` handler → inline activator UPDATEs the row. Poll until status transitions:

```bash
for i in 1 2 3 4 5 6; do
  STATE=$(curl -s "$REVIEW_BASE/assets/catalogs/$CAT/collections/$COL/assets/smoke-asset-1" \
    -H "Authorization: Bearer $TOKEN" \
    | jq -r '.status + "|" + (.content_hash // "null") + "|" + ((.size_bytes // 0)|tostring)')
  echo "[t=$((i*5))s] $STATE"
  case "$STATE" in active\|*) break ;; esac
  sleep 5
done
```

Expected: within ~10–30s the row reads `active|md5:<base64>|<bytes>`. If after 60s it's still `pending`:
- Check Pub/Sub delivery: `gcloud pubsub subscriptions describe ds-${CAT}-default-sub` for ack count.
- Check the catalog service logs in Cloud Run for `finalize` log lines.
- A truly orphan finalize will appear in the index_failure_log: `GET /catalogs/$CAT/index-failures?reason=orphan_finalize`.

### 6. Verify policy gate — refuse on filename collision (default REFUSE_FAIL)

```bash
curl -s -X POST \
  "$REVIEW_BASE/assets/catalogs/$CAT/collections/$COL/upload" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "filename": "data.tif",
    "content_type": "image/tiff",
    "asset": {
      "asset_id": "smoke-asset-2",
      "asset_type": "RASTER",
      "metadata": {}
    }
  }' -w "\nHTTP %{http_code}\n" | jq '.matcher, .reason, .existing_id'
```

Expected: HTTP 409, body `matcher="filename"`, `reason="conflict"`, `existing_id="smoke-asset-1"`. **No** `upload_url` in the response, **no** new asset row.

### 7. Verify bulk POST — 207 IngestionReport on partial rejection

```bash
curl -s -X POST \
  "$REVIEW_BASE/assets/catalogs/$CAT/collections/$COL/assets:bulk" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '[
    {"asset_id": "smoke-asset-1", "kind": "physical", "filename": "data.tif", "asset_type": "RASTER", "metadata": {}},
    {"asset_id": "smoke-asset-3", "kind": "physical", "filename": "fresh.tif", "asset_type": "RASTER", "metadata": {}}
  ]' -w "\nHTTP %{http_code}\n" | jq '.accepted_ids, .rejections, .total'
```

Expected: HTTP 207, `accepted_ids=["smoke-asset-3"]`, `rejections[0].asset_id="smoke-asset-1"`, `rejections[0].matcher="filename"|"asset_id"`, `total=2`.

### 8. Drift endpoint — should be empty after a clean smoke run

```bash
curl -s "$REVIEW_BASE/assets/catalogs/$CAT/collections/$COL/assets:drift" \
  -H "Authorization: Bearer $TOKEN" \
  | jq '{orphans_imported, ghosts_marked_failed, stuck_pending_failed}'
```

Expected: all three counts = 0. Orphan blobs left from a previous test → re-run the bucket-reconcile task with `apply=true`:

```bash
curl -X POST "$REVIEW_BASE/assets/catalogs/$CAT/collections/$COL/tasks/bucket-reconcile/execute" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"apply": true, "pending_ttl_minutes": 60}'
```

### 9. Cleanup

```bash
curl -X DELETE "$REVIEW_BASE/catalogs/$CAT" \
  -H "Authorization: Bearer $TOKEN" -w "HTTP %{http_code}\n"
# Expected: 204 No Content (cascades collections + assets + asset_references + bucket).
```

---

## Pass criteria

- [ ] Step 3 returns a PENDING row with `uri=null` (born-claimed contract).
- [ ] Step 5 transitions to ACTIVE within 60s with `content_hash` carrying the `md5:` prefix and `size_bytes` populated.
- [ ] Step 6 returns 409 with structured rejection body — **no signed URL minted**.
- [ ] Step 7 returns 207 with structured `IngestionReport`.
- [ ] Step 8 reports 0 drift across all three categories.
- [ ] Step 9 leaves the bucket prefix empty (`gsutil ls -r gs://<bucket>/$CAT/`).

If any step fails, capture the catalog service Cloud Run logs and the Pub/Sub subscription state, attach to the deploy PR's smoke-evidence comment, and fix forward (per `feedback_ship_fix_during_merge.md`).
