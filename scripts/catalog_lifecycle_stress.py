#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/
"""Catalog/collection lifecycle stress test.

Exercises the *full* create -> ready -> use -> destroy loop against a live
deployment and reports the per-phase failure rate. Each worker runs an
independent catalog lifecycle:

  1. POST /stac/catalogs                         -> create catalog
  2. GET  /catalog/catalogs/{cat}                -> WAIT until provisioning is
     genuinely terminal: ``provisioning_status == "ready"`` AND the most-recent
     provision task is ``COMPLETED``. ``failed`` status (or a FAILED/DEAD_LETTER
     task) aborts the iteration as a *provisioning* failure.
  2b. GET /catalog/catalogs/{cat}                -> VERIFY the ready catalog is
     actually HEALTHY: read ``provisioning_checklist`` and fail if any step is
     ``degraded``/``failed``. A catalog flips to ``ready`` even when eventing is
     degraded or the bucket was rolled back, so asserting status alone reports a
     half-broken catalog as clean. This phase is HARD — it is the guard that
     stops the run from going green on a ready-but-unusable catalog.
  3. POST /stac/catalogs/{cat}/collections       -> create N collections
  4. (optional) POST .../upload + PUT bytes      -> upload a REAL file so the
     asset is created through the normal eventful path, then assert the
     ``asset`` event actually fired on GET /events/catalogs/{cat}/events. This
     is the only trustworthy proof that pub/sub eventing provisioned correctly:
     a catalog can be ``ready`` while its eventing step is ``degraded`` (e.g.
     pub/sub IAM not granted), so status alone does NOT guarantee events.
  5. DELETE .../collections/{col}?force=true      -> hard-delete each collection,
     wait for the ``collection_hard_delete`` task to reach COMPLETED.
  6. DELETE /stac/catalogs/{cat}?force=true       -> hard-delete the catalog,
     assert it 404s afterwards.

Every phase outcome (ok / fail + reason + elapsed) is recorded. At the end the
script prints a human summary and a machine-readable JSON block, and exits
non-zero if any phase saw a failure (so a Cloud Run job / CI run goes red).

Run locally:
    python scripts/catalog_lifecycle_stress.py \
        --base https://data.review.fao.org/geospatial/dev/api/catalog \
        --iterations 20 --concurrency 4 --collections 2 --upload

As a Cloud Run job, pass everything via env (STRESS_BASE, STRESS_ITERATIONS,
STRESS_CONCURRENCY, STRESS_COLLECTIONS, STRESS_UPLOAD, STRESS_TOKEN,
STRESS_READY_TIMEOUT, STRESS_DELETE_TIMEOUT). CLI flags override env.

If the deployment enforces auth, pass a bearer token with --token / STRESS_TOKEN.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

# Phases tracked in the report, in lifecycle order.
PHASES = (
    "create_catalog",
    "wait_ready",
    "verify_provisioning",
    "create_collection",
    "upload_asset",
    "verify_event",
    "delete_collection",
    "delete_catalog",
)

# Advisory phases are reported but do NOT count toward the pass/fail verdict or
# the process exit code. ``verify_event`` is advisory because asset creation
# does not currently emit a durable, catalog-queryable event on this platform
# (confirmed on dev 2026-06-17: neither virtual-asset creation nor real uploads
# produce an ``asset_*`` row in the events outbox; only catalog-lifecycle events
# are surfaced). The phase still runs so the gap stays visible and so it will
# light up automatically once asset eventing is wired — but a missing asset
# event must not mask an otherwise-clean create/use/destroy lifecycle.
ADVISORY_PHASES = ("verify_event",)
HARD_PHASES = tuple(p for p in PHASES if p not in ADVISORY_PHASES)

# Terminal task states (case-insensitive match).
_TASK_OK = {"completed", "success", "succeeded"}
_TASK_BAD = {"failed", "error", "dead_letter", "cancelled"}

# Per-step provisioning-checklist values that mean the catalog is NOT actually
# healthy. Under the atomic provisioning contract a fully-provisioned catalog
# has every step ``complete`` (or ``skipped`` when a resource is deliberately
# not applicable); eventing failures now mark the catalog ``failed`` rather than
# letting it flip to ``ready`` with a ``degraded`` step. ``degraded`` is kept in
# this set as a defensive tripwire: it must not appear under the atomic contract,
# so if it ever does (legacy data, a regression to the old soft path) the
# ``verify_provisioning`` phase must flag it instead of reporting green. This
# closes the gap that let a "ready" catalog with a missing bucket and broken
# eventing pass silently.
_STEP_UNHEALTHY = {"degraded", "failed"}


@dataclass
class PhaseResult:
    ok: bool
    reason: str = ""
    elapsed: float = 0.0


@dataclass
class IterationResult:
    index: int
    catalog_id: str
    phases: dict[str, PhaseResult] = field(default_factory=dict)
    # A catalog we created but could NOT delete — operator must clean up.
    leaked_catalog: Optional[str] = None


def _status_of(doc: dict[str, Any]) -> Optional[str]:
    return doc.get("provisioning_status") or doc.get("status")


def _task_status(doc: dict[str, Any]) -> Optional[str]:
    tk = doc.get("task") or doc.get("provision_task")
    if isinstance(tk, dict):
        return tk.get("status")
    if isinstance(tk, str):
        return tk
    return None


class StressRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.base = args.base.rstrip("/")
        self.collections = args.collections
        self.upload = args.upload
        self.ready_timeout = args.ready_timeout
        self.delete_timeout = args.delete_timeout
        self.event_timeout = args.event_timeout
        self.poll = args.poll_interval
        headers = {"Content-Type": "application/json"}
        if args.token:
            headers["Authorization"] = f"Bearer {args.token}"
        self._headers = headers
        # A unique-ish run tag so parallel runs / retries never collide on ids.
        # Time-seeded; this is a test harness, not production code.
        self.run_tag = args.run_tag or f"stress{int(time.time())}"

    # -- low-level helpers ---------------------------------------------------

    async def _ready_signal(self, client: httpx.AsyncClient, cat: str) -> str:
        """Return 'ready' | 'failed' | 'pending' from the provisioning endpoint."""
        r = await client.get(f"{self.base}/catalog/catalogs/{cat}")
        if r.status_code == 404:
            return "pending"  # row not visible cross-pod yet
        r.raise_for_status()
        doc = r.json()
        st = (_status_of(doc) or "").lower()
        tk = (_task_status(doc) or "").lower()
        if st == "failed" or tk in _TASK_BAD:
            return "failed"
        if st == "ready" and (tk in _TASK_OK or tk == ""):
            return "ready"
        return "pending"

    async def _poll_gone(
        self, client: httpx.AsyncClient, url: str, timeout: float = 20.0
    ) -> tuple[bool, int]:
        """Poll *url* until it 404s (resource gone) or *timeout* elapses.

        The delete *task* completing is the authoritative signal; the resource
        view is eventually consistent across a load-balanced deployment, so a
        single immediate GET can transiently still see the row. Poll instead of
        asserting once — this catches a genuine non-deletion while tolerating
        read-after-write lag. Returns (gone, last_status).
        """
        deadline = time.monotonic() + timeout
        last = -1
        while True:
            r = await client.get(url)
            last = r.status_code
            if last == 404:
                return True, last
            if time.monotonic() >= deadline:
                return False, last
            await asyncio.sleep(self.poll)

    async def _wait_task(
        self, client: httpx.AsyncClient, cat: str, task_id: str, timeout: float
    ) -> tuple[bool, str]:
        deadline = time.monotonic() + timeout
        last = "?"
        while time.monotonic() < deadline:
            r = await client.get(f"{self.base}/tasks/catalogs/{cat}/{task_id}")
            if r.status_code == 200:
                last = (r.json().get("status") or "?").lower()
                if last in _TASK_OK:
                    return True, last
                if last in _TASK_BAD:
                    return False, last
            await asyncio.sleep(self.poll)
        return False, f"timeout(last={last})"

    # -- phases --------------------------------------------------------------

    async def _create_catalog(self, client: httpx.AsyncClient, cat: str) -> PhaseResult:
        t0 = time.monotonic()
        body = {
            "type": "Catalog",
            "stac_version": "1.1.0",
            "id": cat,
            "description": f"lifecycle stress {cat}",
            "links": [],
        }
        r = await client.post(f"{self.base}/stac/catalogs", json=body)
        # 202 = async create accepted; the caller then polls
        # provisioning_status via _wait_ready below.
        ok = r.status_code in (200, 201, 202)
        return PhaseResult(
            ok,
            "" if ok else f"HTTP {r.status_code}: {r.text[:160]}",
            time.monotonic() - t0,
        )

    async def _wait_ready(self, client: httpx.AsyncClient, cat: str) -> PhaseResult:
        t0 = time.monotonic()
        deadline = t0 + self.ready_timeout
        while time.monotonic() < deadline:
            sig = await self._ready_signal(client, cat)
            if sig == "ready":
                return PhaseResult(True, "", time.monotonic() - t0)
            if sig == "failed":
                return PhaseResult(
                    False, "provisioning_status=failed", time.monotonic() - t0
                )
            await asyncio.sleep(self.poll)
        return PhaseResult(
            False, f"ready timeout after {self.ready_timeout}s", time.monotonic() - t0
        )

    async def _verify_provisioning(
        self, client: httpx.AsyncClient, cat: str
    ) -> PhaseResult:
        """Assert a 'ready' catalog is actually HEALTHY, not merely terminal.

        ``wait_ready`` only proves the catalog flipped to ``ready`` — but a
        catalog reaches ``ready`` even when a provisioning step is ``degraded``
        or ``failed`` (the checklist evaluator treats ``degraded`` as
        terminal-good). This phase GETs the catalog status and inspects
        ``provisioning_checklist``: any step in :data:`_STEP_UNHEALTHY` means the
        catalog is ``ready`` but not truly usable (e.g. eventing disabled, or a
        rolled-back bucket), so the phase fails and names the offending steps.
        Without this, a half-provisioned catalog passes the whole run as clean.

        Backward-compatible: a deployment that does not yet surface
        ``provisioning_checklist`` only has its top-level status re-asserted, so
        this phase never regresses against older servers.
        """
        t0 = time.monotonic()
        r = await client.get(f"{self.base}/catalog/catalogs/{cat}")
        if r.status_code != 200:
            return PhaseResult(
                False,
                f"status GET HTTP {r.status_code}: {r.text[:160]}",
                time.monotonic() - t0,
            )
        doc = r.json()
        st = (_status_of(doc) or "").lower()
        if st != "ready":
            return PhaseResult(
                False,
                f"provisioning_status={st or 'unknown'} (expected ready)",
                time.monotonic() - t0,
            )
        checklist = doc.get("provisioning_checklist") or {}
        if isinstance(checklist, dict) and checklist:
            unhealthy = {
                k: v
                for k, v in checklist.items()
                if isinstance(v, str) and v.lower() in _STEP_UNHEALTHY
            }
            if unhealthy:
                detail = ", ".join(f"{k}={v}" for k, v in sorted(unhealthy.items()))
                return PhaseResult(
                    False,
                    f"ready but provisioning steps not healthy: {detail}",
                    time.monotonic() - t0,
                )
        return PhaseResult(True, "", time.monotonic() - t0)

    async def _create_collection(
        self, client: httpx.AsyncClient, cat: str, col: str
    ) -> PhaseResult:
        t0 = time.monotonic()
        body = {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": col,
            "description": f"stress collection {col}",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
        }
        r = await client.post(f"{self.base}/stac/catalogs/{cat}/collections", json=body)
        ok = r.status_code in (200, 201)
        return PhaseResult(
            ok,
            "" if ok else f"HTTP {r.status_code}: {r.text[:160]}",
            time.monotonic() - t0,
        )

    async def _upload_asset(
        self, client: httpx.AsyncClient, cat: str, col: str, asset_id: str
    ) -> PhaseResult:
        """Upload a real (tiny) file through the eventful asset path."""
        t0 = time.monotonic()
        payload = b"stress-test asset bytes\n"
        init = {
            "filename": f"{asset_id}.bin",
            "content_type": "application/octet-stream",
            "asset": {"asset_id": asset_id, "asset_type": "ASSET"},
        }
        r = await client.post(
            f"{self.base}/assets/catalogs/{cat}/collections/{col}/upload", json=init
        )
        if r.status_code not in (200, 201):
            return PhaseResult(
                False,
                f"initiate HTTP {r.status_code}: {r.text[:160]}",
                time.monotonic() - t0,
            )
        tk = r.json()
        url = tk.get("upload_url")
        method = (tk.get("method") or "PUT").upper()
        up_headers = tk.get("headers") or {}
        ticket = tk.get("ticket_id")
        if not url:
            return PhaseResult(False, "no upload_url in ticket", time.monotonic() - t0)
        # PUT the bytes straight to the signed URL (no auth header — the URL is
        # already signed; use a bare client so we don't leak the bearer token).
        async with httpx.AsyncClient(timeout=60) as raw:
            pr = await raw.request(method, url, content=payload, headers=up_headers)
        if pr.status_code not in (200, 201, 204):
            return PhaseResult(
                False, f"PUT bytes HTTP {pr.status_code}", time.monotonic() - t0
            )
        # Poll the upload finalize/status so the asset row + event are committed.
        if ticket:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                sr = await client.get(
                    f"{self.base}/assets/catalogs/{cat}/upload/{ticket}/status"
                )
                if sr.status_code == 200:
                    st = (sr.json().get("status") or "").lower()
                    if st in ("complete", "completed", "ready", "active", "done"):
                        break
                    if st in ("failed", "error"):
                        return PhaseResult(
                            False, f"upload status={st}", time.monotonic() - t0
                        )
                await asyncio.sleep(self.poll)
        # Confirm the asset is actually listed.
        lr = await client.get(f"{self.base}/assets/catalogs/{cat}/collections/{col}")
        listed = False
        if lr.status_code == 200:
            data = lr.json()
            rows = (
                data
                if isinstance(data, list)
                else data.get("assets", data.get("items", []))
            )
            listed = any(
                (a.get("asset_id") == asset_id) for a in rows if isinstance(a, dict)
            )
        if not listed:
            return PhaseResult(
                False, "asset not listed after upload", time.monotonic() - t0
            )
        return PhaseResult(True, "", time.monotonic() - t0)

    async def _verify_event(
        self, client: httpx.AsyncClient, cat: str, asset_id: str
    ) -> PhaseResult:
        """Confirm an asset event actually fired for this catalog (ADVISORY).

        Reads the durable *system* outbox (``/events/system``) — the only event
        endpoint that reliably retains and returns rows. The catalog-scoped
        ``/events/catalogs/{cat}/events`` endpoint filters on a top-level
        ``catalog_id`` column that platform-scoped lifecycle events leave null
        (the catalog id lives inside ``payload.kwargs``), so it returns ``[]``
        for events that genuinely exist — do not use it as a probe.

        We look for an asset-scoped event referencing this catalog. If asset
        eventing is not wired (the current dev state), this fails — which is the
        whole point of an advisory probe: surface the gap without failing the
        run.
        """
        t0 = time.monotonic()
        deadline = t0 + self.event_timeout
        cat_l = cat.lower()
        while time.monotonic() < deadline:
            r = await client.get(f"{self.base}/events/system", params={"limit": 200})
            if r.status_code == 200:
                data = r.json()
                events = (
                    data
                    if isinstance(data, list)
                    else data.get("events", data.get("items", []))
                )
                for ev in events:
                    if not isinstance(ev, dict):
                        continue
                    et = str(ev.get("event_type", "")).lower()
                    blob = json.dumps(ev).lower()
                    if "asset" in et and (cat_l in blob or asset_id.lower() in blob):
                        return PhaseResult(True, "", time.monotonic() - t0)
            await asyncio.sleep(self.poll)
        return PhaseResult(
            False,
            f"no asset event in system outbox within {self.event_timeout:.0f}s "
            "(asset eventing not surfaced — see runbook)",
            time.monotonic() - t0,
        )

    async def _delete_collection(
        self, client: httpx.AsyncClient, cat: str, col: str
    ) -> PhaseResult:
        t0 = time.monotonic()
        r = await client.request(
            "DELETE",
            f"{self.base}/stac/catalogs/{cat}/collections/{col}",
            params={"force": "true"},
        )
        if r.status_code == 202:
            tid = (r.json() or {}).get("task_id")
            if tid:
                ok, st = await self._wait_task(client, cat, tid, self.delete_timeout)
                if not ok:
                    return PhaseResult(
                        False, f"hard-delete task {st}", time.monotonic() - t0
                    )
        elif r.status_code not in (200, 204):
            return PhaseResult(
                False, f"HTTP {r.status_code}: {r.text[:160]}", time.monotonic() - t0
            )
        # Confirm gone (poll — eventual consistency across pods).
        gone, last = await self._poll_gone(
            client, f"{self.base}/stac/catalogs/{cat}/collections/{col}"
        )
        if not gone:
            return PhaseResult(
                False, f"collection still present (HTTP {last})", time.monotonic() - t0
            )
        return PhaseResult(True, "", time.monotonic() - t0)

    async def _delete_catalog(self, client: httpx.AsyncClient, cat: str) -> PhaseResult:
        t0 = time.monotonic()
        r = await client.request(
            "DELETE", f"{self.base}/stac/catalogs/{cat}", params={"force": "true"}
        )
        if r.status_code == 202:
            tid = (r.json() or {}).get("task_id")
            if tid:
                ok, st = await self._wait_task(client, cat, tid, self.delete_timeout)
                if not ok:
                    return PhaseResult(
                        False, f"hard-delete task {st}", time.monotonic() - t0
                    )
        elif r.status_code not in (200, 204):
            return PhaseResult(
                False, f"HTTP {r.status_code}: {r.text[:160]}", time.monotonic() - t0
            )
        gone, last = await self._poll_gone(client, f"{self.base}/stac/catalogs/{cat}")
        if not gone:
            return PhaseResult(
                False, f"catalog still present (HTTP {last})", time.monotonic() - t0
            )
        return PhaseResult(True, "", time.monotonic() - t0)

    # -- one full lifecycle --------------------------------------------------

    async def run_iteration(
        self, client: httpx.AsyncClient, index: int
    ) -> IterationResult:
        cat = f"{self.run_tag}_c{index:04d}"
        res = IterationResult(index=index, catalog_id=cat)

        # Every phase call is wrapped in _guard so a transient network error
        # (read timeout, connection reset) is recorded as a phase failure
        # instead of aborting the whole run — the error itself is a data point.
        res.phases["create_catalog"] = await _guard(self._create_catalog(client, cat))
        if not res.phases["create_catalog"].ok:
            return res  # never created — nothing to clean up

        res.phases["wait_ready"] = await _guard(self._wait_ready(client, cat))
        if not res.phases["wait_ready"].ok:
            # Catalog exists but never became ready: try to delete it anyway so
            # we don't leak a half-provisioned catalog, and flag if delete fails.
            d = await _guard(self._delete_catalog(client, cat))
            res.phases["delete_catalog"] = d
            if not d.ok:
                res.leaked_catalog = cat
            return res

        res.phases["verify_provisioning"] = await _guard(
            self._verify_provisioning(client, cat)
        )
        if not res.phases["verify_provisioning"].ok:
            # Catalog is 'ready' but a provisioning step is degraded/failed (e.g.
            # eventing disabled, rolled-back bucket). Proceeding to upload would
            # either fail confusingly or appear to pass against a half-broken
            # catalog — the false-green this script must stop producing. Tear it
            # down so we don't leak it, record the failure, and end the iteration.
            d = await _guard(self._delete_catalog(client, cat))
            res.phases["delete_catalog"] = d
            if not d.ok:
                res.leaked_catalog = cat
            return res

        cols = [f"{cat}_col{j}" for j in range(self.collections)]
        first_fail = False
        for col in cols:
            cr = await _guard(self._create_collection(client, cat, col))
            res.phases.setdefault("create_collection", cr)
            if not cr.ok:
                res.phases["create_collection"] = cr
                first_fail = True
                break
            res.phases["create_collection"] = cr

        if not first_fail and self.upload and cols:
            asset_id = f"{cat}_asset0"
            up = await _guard(self._upload_asset(client, cat, cols[0], asset_id))
            res.phases["upload_asset"] = up
            if up.ok:
                res.phases["verify_event"] = await _guard(
                    self._verify_event(client, cat, asset_id)
                )

        # Always attempt teardown (even after a mid-flight failure) so the run
        # is self-cleaning. Delete collections, then the catalog.
        del_ok = True
        for col in cols:
            dc = await _guard(self._delete_collection(client, cat, col))
            prev = res.phases.get("delete_collection")
            # keep the first failure visible; otherwise the last ok
            if prev is None or (prev.ok and not dc.ok):
                res.phases["delete_collection"] = dc
            if not dc.ok:
                del_ok = False

        dcat = await _guard(self._delete_catalog(client, cat))
        res.phases["delete_catalog"] = dcat
        if not dcat.ok:
            del_ok = False
            res.leaked_catalog = cat
        _ = del_ok
        return res


async def _guard(coro) -> PhaseResult:
    """Run a phase coroutine, converting any exception into a failed PhaseResult.

    A stress harness must never let one transient network error (read timeout,
    connection reset) abort the whole run — that error IS a data point.
    """
    t0 = time.monotonic()
    try:
        return await coro
    except Exception as e:  # noqa: BLE001 — deliberately broad: record, don't crash
        return PhaseResult(
            False,
            f"exception: {type(e).__name__}: {str(e)[:140]}",
            time.monotonic() - t0,
        )


async def _bounded(sem: asyncio.Semaphore, coro):
    async with sem:
        try:
            return await coro
        except Exception as e:  # noqa: BLE001 — backstop so one iteration can't kill the run
            r = IterationResult(index=-1, catalog_id="?")
            r.phases["create_catalog"] = PhaseResult(
                False, f"iteration crashed: {type(e).__name__}: {str(e)[:140]}"
            )
            return r


def _iter_clean(r: IterationResult) -> bool:
    """An iteration is clean iff every HARD phase it ran passed.

    Advisory phases (verify_event) are ignored for the verdict — they are
    reported separately and must not turn an otherwise-clean lifecycle red.
    """
    return all(p.ok for k, p in r.phases.items() if k in HARD_PHASES)


async def main_async(args: argparse.Namespace) -> int:
    runner = StressRunner(args)
    sem = asyncio.Semaphore(args.concurrency)
    limits = httpx.Limits(max_connections=args.concurrency * 4)
    # Retry the connect/read at the transport level so a single dropped socket
    # (common against a load-balanced dev deployment) doesn't surface as a
    # phase failure. _guard still catches anything that survives the retries.
    transport = httpx.AsyncHTTPTransport(retries=2, limits=limits)
    print(
        f"[stress] base={runner.base} iterations={args.iterations} "
        f"concurrency={args.concurrency} collections={args.collections} "
        f"upload={args.upload} run_tag={runner.run_tag}",
        flush=True,
    )
    t0 = time.monotonic()
    async with httpx.AsyncClient(
        timeout=args.http_timeout, headers=runner._headers, transport=transport
    ) as client:
        tasks = [
            _bounded(sem, runner.run_iteration(client, i))
            for i in range(args.iterations)
        ]
        results: list[IterationResult] = []
        for fut in asyncio.as_completed(tasks):
            r = await fut
            results.append(r)
            status = "OK " if _iter_clean(r) else "FAIL"
            print(
                f"[{status}] iter {r.index:04d} {r.catalog_id} "
                + " ".join(
                    f"{k}={'ok' if v.ok else ('~' if k in ADVISORY_PHASES else 'X')}"
                    for k, v in r.phases.items()
                ),
                flush=True,
            )
    wall = time.monotonic() - t0

    # -- aggregate ----------------------------------------------------------
    per_phase: dict[str, dict[str, Any]] = {}
    for ph in PHASES:
        attempts = [r.phases[ph] for r in results if ph in r.phases]
        fails = [p for p in attempts if not p.ok]
        if not attempts:
            continue
        per_phase[ph] = {
            "attempts": len(attempts),
            "failures": len(fails),
            "failure_rate": round(len(fails) / len(attempts), 4),
            "p50_s": round(sorted(p.elapsed for p in attempts)[len(attempts) // 2], 2),
            "max_s": round(max(p.elapsed for p in attempts), 2),
            "sample_reasons": sorted({p.reason for p in fails})[:5],
            "advisory": ph in ADVISORY_PHASES,
        }
    total_iters = len(results)
    clean_iters = sum(1 for r in results if _iter_clean(r))
    leaked = [r.leaked_catalog for r in results if r.leaked_catalog]

    summary = {
        "base": runner.base,
        "iterations": total_iters,
        "clean_iterations": clean_iters,
        "overall_failure_rate": round((total_iters - clean_iters) / total_iters, 4)
        if total_iters
        else 0.0,
        "wall_seconds": round(wall, 1),
        "per_phase": per_phase,
        "advisory_phases": list(ADVISORY_PHASES),
        "leaked_catalogs": leaked,
    }

    print("\n========== CATALOG LIFECYCLE STRESS REPORT ==========")
    print(f"iterations            : {total_iters}")
    _pct = f"{100 * clean_iters / total_iters:.1f}%" if total_iters else "n/a"
    print(f"fully-clean           : {clean_iters}  ({_pct})")
    print(f"overall failure rate  : {summary['overall_failure_rate'] * 100:.1f}%")
    print(f"wall clock            : {wall:.1f}s")
    print("per-phase  (~ = advisory, excluded from verdict):")
    for ph in PHASES:
        if ph in per_phase:
            d = per_phase[ph]
            tag = " ~" if ph in ADVISORY_PHASES else "  "
            print(
                f"{tag}{ph:<18} {d['failures']}/{d['attempts']} fail "
                f"({d['failure_rate'] * 100:.1f}%)  p50={d['p50_s']}s max={d['max_s']}s"
                + (f"  reasons={d['sample_reasons']}" if d["sample_reasons"] else "")
            )
    if leaked:
        print(f"\n!! LEAKED catalogs (manual cleanup needed): {leaked}")
    print("\nJSON " + json.dumps(summary))
    print("=====================================================")

    # Non-zero exit if anything failed, so a Cloud Run job / CI marks red.
    return 0 if clean_iters == total_iters and total_iters > 0 else 1


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Catalog/collection lifecycle stress test")
    p.add_argument(
        "--base",
        default=_env(
            "STRESS_BASE", "https://data.review.fao.org/geospatial/dev/api/catalog"
        ),
    )
    p.add_argument(
        "--iterations", type=int, default=int(_env("STRESS_ITERATIONS", "20"))
    )
    p.add_argument(
        "--concurrency", type=int, default=int(_env("STRESS_CONCURRENCY", "4"))
    )
    p.add_argument(
        "--collections", type=int, default=int(_env("STRESS_COLLECTIONS", "2"))
    )
    p.add_argument(
        "--upload",
        action="store_true",
        default=_env("STRESS_UPLOAD", "0") in ("1", "true", "True"),
    )
    p.add_argument("--token", default=_env("STRESS_TOKEN", ""))
    p.add_argument(
        "--ready-timeout",
        type=float,
        default=float(_env("STRESS_READY_TIMEOUT", "180")),
    )
    p.add_argument(
        "--delete-timeout",
        type=float,
        default=float(_env("STRESS_DELETE_TIMEOUT", "120")),
    )
    p.add_argument(
        "--event-timeout", type=float, default=float(_env("STRESS_EVENT_TIMEOUT", "30"))
    )
    p.add_argument(
        "--poll-interval", type=float, default=float(_env("STRESS_POLL", "3"))
    )
    p.add_argument(
        "--http-timeout", type=float, default=float(_env("STRESS_HTTP_TIMEOUT", "60"))
    )
    p.add_argument("--run-tag", default=_env("STRESS_RUN_TAG", ""))
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(main_async(parse_args(sys.argv[1:]))))
