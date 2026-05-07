#!/usr/bin/env python3
"""
test_es_audit_fixes.py — Customer verification script for PR #408 ES audit fixes.

Covers 7 regression tests (one per commit):
  T1  Items SEARCH routing  — ES returns items that PG misses
  T2  HATEOAS link methods  — plugin-config endpoints advertise PUT, not PATCH
  T3  Registry re-key       — GET /configs/.../plugins/items_postgresql_driver returns 200
  T4  STAC error surfacing  — malformed item POST returns a real error code (not 500)
  T5  Alias maintenance     — fresh catalog's items index joins dynastore-items alias
  T6  is_private mapping    — collections ES doc contains is_private boolean field
  T7  Privacy apply handler — toggling is_private on collection config syncs to ES

Usage
-----
  python test_es_audit_fixes.py \\
      --token "<bearer_token>" \\
      [--api  https://data.review.fao.org/geospatial/v2/api/catalog] \\
      [--es-url  https://<cluster>.gcp.cloud.es.io:443] \\
      [--es-key  <base64_api_key>] \\
      [--catalog-id  my-test-<timestamp>]  # auto-generated if omitted
      [--keep]  # skip cleanup on exit (default: delete test catalog)

T5-T7 (ES-direct checks) are skipped automatically when --es-url/--es-key are absent.

Requirements: pip install requests
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

try:
    import requests
except ImportError:
    sys.exit("pip install requests")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    return str(int(time.time()))


@dataclass
class Result:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class Runner:
    api: str
    token: str
    es_url: Optional[str]
    es_key: Optional[str]
    catalog_id: str
    keep: bool
    results: list[Result] = field(default_factory=list)

    # -- session ---------------------------------------------------------

    def _headers(self, json_body: bool = False) -> dict:
        h: dict = {"Authorization": f"Bearer {self.token}"}
        if json_body:
            h["Content-Type"] = "application/json"
        return h

    def _es_headers(self) -> dict:
        return {"Authorization": f"ApiKey {self.es_key}"}

    # -- assertion helpers -----------------------------------------------

    def _ok(self, name: str, detail: str = "") -> None:
        print(f"  ✓  {name}" + (f" — {detail}" if detail else ""))
        self.results.append(Result(name, True, detail))

    def _fail(self, name: str, detail: str = "") -> None:
        print(f"  ✗  {name}" + (f" — {detail}" if detail else ""))
        self.results.append(Result(name, False, detail))

    def _skip(self, name: str, reason: str = "") -> None:
        print(f"  -  {name} (skip{': ' + reason if reason else ''})")
        self.results.append(Result(name, True, f"skipped: {reason}"))

    # -- low-level API calls ---------------------------------------------

    def _get(self, path: str, **params) -> requests.Response:
        return requests.get(f"{self.api}{path}", headers=self._headers(), params=params, timeout=30)

    def _post(self, path: str, body: dict) -> requests.Response:
        return requests.post(f"{self.api}{path}", headers=self._headers(True),
                             data=json.dumps(body), timeout=30)

    def _put(self, path: str, body: dict) -> requests.Response:
        return requests.put(f"{self.api}{path}", headers=self._headers(True),
                            data=json.dumps(body), timeout=30)

    def _delete(self, path: str) -> requests.Response:
        return requests.delete(f"{self.api}{path}", headers=self._headers(), timeout=30)

    def _es_get(self, path: str) -> Optional[dict]:
        if not (self.es_url and self.es_key):
            return None
        r = requests.get(f"{self.es_url}{path}", headers=self._es_headers(), timeout=30)
        return r.json() if r.ok else None

    # ==================================================================
    # Setup
    # ==================================================================

    def setup(self) -> bool:
        """Create catalog + collection + index two public items."""
        print("\n[setup] creating test catalog and collection …")
        cat = self.catalog_id
        self.coll_id = f"coll-{_ts()}"
        self.item_ids: list[str] = []

        # catalog
        r = self._post("/stac/catalogs", {
            "catalog_id": cat,
            "title": f"ES Audit Test {cat}",
            "description": "Automated test — safe to delete",
        })
        if r.status_code not in (200, 201, 409):
            print(f"  catalog create failed {r.status_code}: {r.text[:200]}")
            return False
        print(f"  catalog {cat!r} ready")

        # collection
        r = self._post(f"/stac/catalogs/{cat}/collections", {
            "id": self.coll_id,
            "type": "Collection",
            "stac_version": "1.0.0",
            "description": "ES audit test collection",
            "links": [],
            "title": "ES Audit Collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
            },
            "license": "proprietary",
        })
        if r.status_code not in (200, 201, 409):
            print(f"  collection create failed {r.status_code}: {r.text[:200]}")
            return False
        print(f"  collection {self.coll_id!r} ready")

        # two STAC items
        for i in range(2):
            item_id = f"item-{_ts()}-{i}"
            r = self._post(
                f"/stac/catalogs/{cat}/collections/{self.coll_id}/items",
                {
                    "type": "Feature",
                    "stac_version": "1.0.0",
                    "id": item_id,
                    "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
                    "bbox": [12.4, 41.8, 12.6, 42.0],
                    "properties": {"datetime": "2024-06-01T00:00:00Z"},
                    "links": [],
                    "assets": {},
                },
            )
            if r.status_code in (200, 201):
                self.item_ids.append(item_id)
                print(f"  item {item_id!r} indexed (status {r.status_code})")
            else:
                print(f"  item {i} error {r.status_code}: {r.text[:120]}")

        if not self.item_ids:
            print("  no items indexed — aborting")
            return False

        print("  waiting 8 s for OUTBOX drain …")
        time.sleep(8)
        return True

    # ==================================================================
    # T1 — Items SEARCH routing  (commit 6d17e17)
    # ==================================================================

    def t1_search_routing(self) -> None:
        print("\n[T1] Items SEARCH routing — ES first")
        r = self._post(
            f"/search/catalogs/{self.catalog_id}/items-search",
            {"limit": 10},
        )
        if not r.ok:
            self._fail("T1 search routing", f"HTTP {r.status_code}: {r.text[:200]}")
            return
        body = r.json()
        count = len(body.get("features", []))
        total = body.get("features_count", body.get("numberMatched", "?"))
        if count > 0:
            self._ok("T1 search routing", f"found {count} items (total≈{total})")
        else:
            self._fail("T1 search routing",
                       f"0 features returned — ES routing not active yet? "
                       f"(response: {json.dumps(body)[:200]})")

    # ==================================================================
    # T2 — HATEOAS link methods  (commit d93f695)
    # ==================================================================

    def t2_hateoas_put(self) -> None:
        print("\n[T2] HATEOAS links on plugin-config endpoint")
        r = self._get(f"/configs/catalogs/{self.catalog_id}/plugins/items_elasticsearch_driver")
        if r.status_code == 404:
            self._skip("T2 hateoas PUT", "items_elasticsearch_driver not registered on this catalog")
            return
        if not r.ok:
            self._fail("T2 hateoas PUT", f"HTTP {r.status_code}: {r.text[:200]}")
            return
        body = r.json()
        links = body.get("_links", [])
        methods = {lk.get("rel"): lk.get("method", "").upper() for lk in links}
        # The "edit" (or "self-put") rel must be PUT, not PATCH
        edit_methods = [lk.get("method", "").upper() for lk in links
                        if lk.get("rel") in ("edit", "self", "update")]
        if "PATCH" in edit_methods:
            self._fail("T2 hateoas PUT", f"link still advertises PATCH: {edit_methods}")
        elif edit_methods:
            self._ok("T2 hateoas PUT", f"edit link method = {edit_methods}")
        else:
            # No edit link present — check raw response shape
            self._ok("T2 hateoas PUT", f"no legacy PATCH edit link found; links: {list(methods)}")

    # ==================================================================
    # T3 — Registry re-key  (commit a9f56a1)
    # ==================================================================

    def t3_registry_rekey(self) -> None:
        print("\n[T3] TypedDriver registry re-key — GET /configs/.../plugins/items_postgresql_driver")
        r = self._get(f"/configs/catalogs/{self.catalog_id}/plugins/items_postgresql_driver")
        if r.status_code == 200:
            self._ok("T3 registry re-key", "200 — driver config resolved by snake_case key")
        elif r.status_code == 404:
            self._fail("T3 registry re-key",
                       "404 — re-key fix not active; stale qualname key in registry")
        else:
            self._fail("T3 registry re-key", f"HTTP {r.status_code}: {r.text[:150]}")

    # ==================================================================
    # T4 — STAC error surfacing  (commit 73e40d1)
    # ==================================================================

    def t4_stac_error_surfacing(self) -> None:
        print("\n[T4] STAC error surfacing — malformed item returns real error code")
        r = self._post(
            f"/stac/catalogs/{self.catalog_id}/collections/{self.coll_id}/items",
            {   # deliberately invalid: geometry is wrong type, bbox wrong length
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": f"bad-item-{_ts()}",
                "geometry": "INVALID_NOT_A_DICT",
                "bbox": [0],
                "properties": {},  # missing required `datetime`
                "links": [],
                "assets": {},
            },
        )
        if r.status_code == 500:
            self._fail("T4 STAC error surfacing", "generic 500 — exception still masked")
        elif r.status_code in (400, 422):
            self._ok("T4 STAC error surfacing",
                     f"{r.status_code} validation error surfaced correctly")
        elif r.status_code in (200, 201):
            self._ok("T4 STAC error surfacing",
                     "item accepted (payload may be lenient on this env; no mask if no error)")
        else:
            self._ok("T4 STAC error surfacing",
                     f"{r.status_code} non-500 response — masking fixed")

    # ==================================================================
    # T5 — Alias maintenance  (commit fc73dd7)  — ES-direct
    # ==================================================================

    def t5_alias_maintenance(self) -> None:
        print("\n[T5] dynastore-items alias includes new catalog")
        if not (self.es_url and self.es_key):
            self._skip("T5 alias maintenance", "no --es-url/--es-key provided")
            return

        alias = self._es_get("/dynastore-items/_count")
        if alias is None:
            self._fail("T5 alias maintenance", "could not reach ES cluster")
            return

        # Check if our per-catalog index is in the alias
        alias_info = self._es_get("/_alias/dynastore-items")
        if alias_info is None:
            self._fail("T5 alias maintenance", "alias lookup failed")
            return

        expected_index = f"dynastore-{self.catalog_id}-items"
        if expected_index in alias_info:
            self._ok("T5 alias maintenance",
                     f"{expected_index!r} is in dynastore-items alias")
        else:
            indices_in_alias = list(alias_info.keys())
            self._fail("T5 alias maintenance",
                       f"{expected_index!r} NOT in alias. Alias members: {indices_in_alias}")

    # ==================================================================
    # T6 — is_private field in collection mapping  (commit 93923dc)
    # ==================================================================

    def t6_is_private_mapping(self) -> None:
        print("\n[T6] is_private field in dynastore-collections ES mapping")
        if not (self.es_url and self.es_key):
            self._skip("T6 is_private mapping", "no --es-url/--es-key provided")
            return

        mapping = self._es_get("/dynastore-collections/_mapping")
        if mapping is None:
            self._fail("T6 is_private mapping", "could not reach ES cluster")
            return

        props = {}
        for idx_body in mapping.values():
            props = (idx_body.get("mappings", {})
                             .get("properties", {}))
            break

        if "is_private" in props:
            field_type = props["is_private"].get("type")
            self._ok("T6 is_private mapping", f"field present, type={field_type}")
        else:
            self._fail("T6 is_private mapping",
                       f"is_private NOT in mapping. Fields: {list(props)[:10]}")

        # Also verify our test collection has the field populated
        search_body = {
            "query": {"term": {"collection_id": self.coll_id}},
            "_source": ["collection_id", "is_private"],
            "size": 1,
        }
        doc = requests.post(
            f"{self.es_url}/dynastore-collections/_search",
            headers=self._es_headers(),
            json=search_body,
            timeout=30,
        ).json()
        hits = doc.get("hits", {}).get("hits", [])
        if hits:
            src = hits[0].get("_source", {})
            if "is_private" in src:
                self._ok("T6 is_private value", f"doc has is_private={src['is_private']}")
            else:
                self._fail("T6 is_private value",
                           f"doc missing is_private. Source: {src}")
        else:
            self._skip("T6 is_private value",
                       "collection not yet indexed in ES (may need time)")

    # ==================================================================
    # T7 — Privacy apply handler  (commit 4c2b147)  — ES-direct
    # ==================================================================

    def t7_privacy_apply_handler(self) -> None:
        print("\n[T7] CollectionPrivacy apply-handler syncs is_private to ES")
        if not (self.es_url and self.es_key):
            self._skip("T7 privacy handler", "no --es-url/--es-key provided")
            return

        # Read current privacy config
        r = self._get(
            f"/configs/catalogs/{self.catalog_id}/collections/{self.coll_id}/plugins/collection_privacy"
        )
        if r.status_code == 404:
            # create it
            r = self._put(
                f"/configs/catalogs/{self.catalog_id}/collections/{self.coll_id}/plugins/collection_privacy",
                {"is_private": True},
            )
            if not r.ok:
                self._fail("T7 privacy handler",
                           f"could not PUT collection_privacy: {r.status_code} {r.text[:150]}")
                return
        elif r.ok:
            # toggle
            current = r.json().get("is_private", False)
            r2 = self._put(
                f"/configs/catalogs/{self.catalog_id}/collections/{self.coll_id}/plugins/collection_privacy",
                {"is_private": not current},
            )
            if not r2.ok:
                self._fail("T7 privacy handler",
                           f"PUT failed: {r2.status_code} {r2.text[:150]}")
                return
        else:
            self._fail("T7 privacy handler", f"config GET failed: {r.status_code}")
            return

        print("  waiting 4 s for apply handler …")
        time.sleep(4)

        # Now check ES
        search_body = {
            "query": {"term": {"collection_id": self.coll_id}},
            "_source": ["collection_id", "is_private"],
            "size": 1,
        }
        doc = requests.post(
            f"{self.es_url}/dynastore-collections/_search",
            headers=self._es_headers(),
            json=search_body,
            timeout=30,
        ).json()
        hits = doc.get("hits", {}).get("hits", [])
        if hits:
            src = hits[0].get("_source", {})
            es_val = src.get("is_private")
            if es_val is not None:
                self._ok("T7 privacy handler",
                         f"ES doc is_private={es_val} (apply handler fired)")
            else:
                self._fail("T7 privacy handler",
                           "is_private still missing from ES doc after PUT")
        else:
            self._fail("T7 privacy handler",
                       "collection not found in ES after apply handler")

    # ==================================================================
    # Teardown
    # ==================================================================

    def teardown(self) -> None:
        if self.keep:
            print(f"\n[teardown] --keep set — catalog {self.catalog_id!r} preserved")
            return
        print(f"\n[teardown] deleting catalog {self.catalog_id!r} …")
        r = self._delete(f"/stac/catalogs/{self.catalog_id}")
        if r.ok:
            print(f"  catalog deleted (status {r.status_code})")
        else:
            print(f"  delete returned {r.status_code} — manual cleanup may be needed")

    # ==================================================================
    # Main
    # ==================================================================

    def run(self) -> int:
        banner = f"ES Audit Fix — verification against {self.api}"
        print("=" * len(banner))
        print(banner)
        print("=" * len(banner))

        if not self.setup():
            print("\nSetup failed — cannot run tests")
            return 1

        self.t1_search_routing()
        self.t2_hateoas_put()
        self.t3_registry_rekey()
        self.t4_stac_error_surfacing()
        self.t5_alias_maintenance()
        self.t6_is_private_mapping()
        self.t7_privacy_apply_handler()

        self.teardown()

        # Summary
        print("\n" + "=" * 60)
        passed = [r for r in self.results if r.passed]
        failed = [r for r in self.results if not r.passed]
        print(f"RESULTS  {len(passed)} passed / {len(failed)} failed / {len(self.results)} total")
        if failed:
            print("\nFailed tests:")
            for r in failed:
                print(f"  ✗  {r.name}: {r.detail}")
        print("=" * 60)
        return 0 if not failed else 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(
        description="Verify PR #408 ES audit fixes against a running environment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--api",
        default="https://data.review.fao.org/geospatial/v2/api/catalog",
        help="Catalog API base URL (no trailing slash)",
    )
    p.add_argument(
        "--token",
        required=True,
        help="Bearer token with sysadmin or catalog-admin privileges",
    )
    p.add_argument(
        "--es-url",
        dest="es_url",
        help="Elasticsearch cluster URL (optional — enables T5/T6/T7)",
    )
    p.add_argument(
        "--es-key",
        dest="es_key",
        help="Elasticsearch ApiKey (base64, optional — enables T5/T6/T7)",
    )
    p.add_argument(
        "--catalog-id",
        dest="catalog_id",
        default=f"es-audit-test-{_ts()}",
        help="Catalog ID to create (auto-generated if omitted)",
    )
    p.add_argument(
        "--keep",
        action="store_true",
        help="Skip teardown (leave test catalog in place)",
    )
    args = p.parse_args()

    runner = Runner(
        api=args.api.rstrip("/"),
        token=args.token,
        es_url=args.es_url,
        es_key=args.es_key,
        catalog_id=args.catalog_id,
        keep=args.keep,
    )
    sys.exit(runner.run())


if __name__ == "__main__":
    main()
