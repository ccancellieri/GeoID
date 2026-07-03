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
"""Partition a preseed extent into a uniform bbox grid and fan it out.

A single ``tiles_preseed`` process execution renders its whole bbox list
sequentially inside ONE Cloud Run Job, so a dense layer either runs for hours
or is killed at the Job task-timeout ceiling (Cloud Run's hard max is 24h — a
days-to-weeks extent cannot fit in one Job no matter the timeout). Preseed is
idempotent (a rerun re-renders and overwrites; disjoint bboxes touch disjoint
tiles), so the fix is to split the extent into a grid of sub-bboxes and submit
one execution per cell. The dispatcher routes each to its own concurrent Job;
wall-clock collapses to the slowest single cell.

This is the operational, no-deploy path: it drives the PUBLIC process API and
sets ``execution_overrides.timeout_seconds`` per execution — a request-body
lever that already works on the live deployment, independent of the routing-
config default. Use it to preseed a large layer today; the in-repo coordinator
(auto-partition + progress aggregation) is the durable follow-up.

Uniform grid caveat: cells are equal-area in DEGREES, not in tile/feature
count, so a dense region becomes a hotspot cell that dominates wall-clock. For
badly skewed layers, pass a finer --nx/--ny (more, smaller cells) so the
hotspot is subdivided; a density-balanced quadtree is the coordinator's job.

Examples
--------
Dry-run a 4x4 grid over a bbox (prints the 16 payloads, sends nothing):

    python scripts/preseed_fanout.py \
        --catalog my_cat --collection my_col \
        --bbox " -180 -90 180 90" --nx 4 --ny 4 --dry-run

Fire an 8x8 fan-out against dev with a 24h per-Job ceiling, 0.5s apart:

    python scripts/preseed_fanout.py \
        --catalog my_cat --collection my_col \
        --bbox "60 5 100 40" --nx 8 --ny 8 \
        --timeout-seconds 86400 --stagger-seconds 0.5

Notes
-----
* The dev/review catalog API is open (no token). Cloudflare bans the default
  ``python-urllib`` User-Agent with a 403 "error code: 1010" — this script
  sends a browser UA so requests pass. Override with --user-agent if needed.
* --bbox that starts with a negative number confuses argparse; quote it with a
  leading space, e.g. ``--bbox " -180 -90 180 90"``.
* Antimeridian-crossing extents (minx > maxx) are NOT handled — split them into
  two runs (… to 180 and -180 to …).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from typing import List, Optional, Tuple

BoundingBox = Tuple[float, float, float, float]

DEFAULT_BASE_URL = "https://data.review.fao.org/geospatial/dev/api/"
# curl's default UA passes Cloudflare where python-urllib is banned (1010).
DEFAULT_USER_AGENT = "curl/8.4.0"


def partition_bbox(bbox: BoundingBox, nx: int, ny: int) -> List[BoundingBox]:
    """Split ``bbox`` into an ``nx`` (lon) by ``ny`` (lat) uniform grid.

    Row-major, south-to-north then west-to-east. Cells are equal-area in
    degrees; each is a disjoint sub-extent of the input.
    """
    minx, miny, maxx, maxy = bbox
    if maxx <= minx or maxy <= miny:
        raise ValueError(f"degenerate/inverted bbox {bbox!r} (need minx<maxx, miny<maxy)")
    if nx < 1 or ny < 1:
        raise ValueError("--nx and --ny must be >= 1")
    dx = (maxx - minx) / nx
    dy = (maxy - miny) / ny
    cells: List[BoundingBox] = []
    for j in range(ny):
        y0 = miny + j * dy
        y1 = maxy if j == ny - 1 else y0 + dy
        for i in range(nx):
            x0 = minx + i * dx
            x1 = maxx if i == nx - 1 else x0 + dx
            cells.append((round(x0, 8), round(y0, 8), round(x1, 8), round(y1, 8)))
    return cells


def build_payload(
    *,
    catalog_id: str,
    collection_id: str,
    cell: BoundingBox,
    output_format: str,
    operation: str,
    tms_ids: Optional[List[str]],
    formats: Optional[List[str]],
    timeout_seconds: Optional[int],
    max_retries: Optional[int],
) -> dict:
    """Build one OGC ExecuteRequest body scoped to a single grid cell."""
    inputs: dict = {
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "update_bbox": [list(cell)],
        "output_format": output_format,
        "operation": operation,
    }
    if tms_ids:
        inputs["tms_ids"] = tms_ids
    if formats:
        inputs["formats"] = formats

    body: dict = {"inputs": inputs, "response": "document"}

    overrides: dict = {}
    if timeout_seconds:
        overrides["timeout_seconds"] = timeout_seconds
    if max_retries is not None:
        overrides["max_retries"] = max_retries
    if overrides:
        body["execution_overrides"] = overrides
    return body


def execution_url(base_url: str, catalog_id: str, collection_id: str) -> str:
    base = base_url.rstrip("/")
    return (
        f"{base}/processes/catalogs/{catalog_id}"
        f"/collections/{collection_id}/processes/tiles_preseed/execution"
    )


def submit(
    url: str, body: dict, *, user_agent: str, token: Optional[str], timeout: float
) -> Tuple[int, str]:
    """POST one execution. Returns (http_status, location_or_error)."""
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", user_agent)
    req.add_header("Prefer", "respond-async")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            location = resp.headers.get("Location", "")
            if not location:
                try:
                    payload = json.loads(resp.read().decode("utf-8"))
                    location = str(payload.get("jobID") or payload.get("id") or "")
                except Exception:  # noqa: BLE001 — body is best-effort context only
                    location = ""
            return resp.status, location or "(accepted, no Location)"
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8")[:300]
        except Exception:  # noqa: BLE001
            pass
        return e.code, f"HTTPError: {detail}"
    except urllib.error.URLError as e:
        return 0, f"URLError: {e.reason}"


def _parse_bbox(raw: str) -> BoundingBox:
    parts = raw.replace(",", " ").split()
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            f"--bbox needs 4 numbers 'minx miny maxx maxy', got {raw!r}"
        )
    minx, miny, maxx, maxy = (float(p) for p in parts)
    return (minx, miny, maxx, maxy)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Fan a tiles_preseed extent out into a uniform bbox grid.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Catalog API base URL.")
    p.add_argument("--catalog", required=True, help="Catalog id.")
    p.add_argument("--collection", required=True, help="Collection id.")
    p.add_argument(
        "--bbox", required=True, type=_parse_bbox,
        help="Overall extent 'minx miny maxx maxy' (quote a leading '-': \" -180 ...\").",
    )
    p.add_argument("--nx", type=int, default=4, help="Grid columns (longitude splits).")
    p.add_argument("--ny", type=int, default=4, help="Grid rows (latitude splits).")
    p.add_argument(
        "--output-format", default="mvt", choices=["mvt", "pmtiles"],
        help="mvt = disjoint tiles, no merge (parallel-safe). pmtiles = one "
             "archive per cell, NOT merged by this script.",
    )
    p.add_argument("--operation", default="seed", choices=["seed", "renew"])
    p.add_argument(
        "--tms", action="append", default=None,
        help="TMS id (repeatable). Omit to use the collection's preseed config.",
    )
    p.add_argument(
        "--formats", action="append", default=None,
        help="Tile format (repeatable). Omit to use the preseed config.",
    )
    p.add_argument(
        "--timeout-seconds", type=int, default=86400,
        help="Per-execution Cloud Run Job timeout ceiling (86400 = 24h max). "
             "0 to leave the platform default.",
    )
    p.add_argument(
        "--max-retries", type=int, default=0,
        help="Per-execution retry cap. 0 avoids a duplicate multi-hour rerun on "
             "transient failure; raise if you want the platform to auto-retry.",
    )
    p.add_argument("--stagger-seconds", type=float, default=0.5,
                   help="Delay between submissions to spare the dispatcher.")
    p.add_argument("--http-timeout", type=float, default=30.0,
                   help="Per-request HTTP timeout (submission is async; small is fine).")
    p.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    p.add_argument("--token", default=None, help="Optional Bearer token (dev is open).")
    p.add_argument("--dry-run", action="store_true",
                   help="Print partitions + payloads; send nothing.")
    args = p.parse_args(argv)

    try:
        cells = partition_bbox(args.bbox, args.nx, args.ny)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if args.output_format == "pmtiles":
        print(
            "WARNING: --output-format pmtiles produces one archive PER CELL; this "
            "script does NOT merge them. Use mvt for parallel fan-out, or merge "
            "the per-cell PMTiles yourself.",
            file=sys.stderr,
        )

    url = execution_url(args.base_url, args.catalog, args.collection)
    timeout_s = args.timeout_seconds if args.timeout_seconds > 0 else None

    print(
        f"Fan-out: {len(cells)} cells ({args.nx}x{args.ny}) over {args.bbox}\n"
        f"  -> {url}\n"
        f"  output_format={args.output_format} operation={args.operation} "
        f"timeout={timeout_s or 'platform-default'}s max_retries={args.max_retries}\n"
    )

    submitted = 0
    failed: List[Tuple[BoundingBox, int, str]] = []
    for idx, cell in enumerate(cells):
        body = build_payload(
            catalog_id=args.catalog,
            collection_id=args.collection,
            cell=cell,
            output_format=args.output_format,
            operation=args.operation,
            tms_ids=args.tms,
            formats=args.formats,
            timeout_seconds=timeout_s,
            max_retries=args.max_retries,
        )
        if args.dry_run:
            print(f"[{idx + 1}/{len(cells)}] cell={cell}\n{json.dumps(body)}")
            continue

        status, info = submit(
            url, body, user_agent=args.user_agent, token=args.token,
            timeout=args.http_timeout,
        )
        ok = status in (200, 201, 202)
        marker = "OK " if ok else "ERR"
        print(f"[{idx + 1}/{len(cells)}] {marker} {status} cell={cell} -> {info}")
        if ok:
            submitted += 1
        else:
            failed.append((cell, status, info))
        if args.stagger_seconds > 0 and idx < len(cells) - 1:
            time.sleep(args.stagger_seconds)

    if args.dry_run:
        print(f"\nDry-run: {len(cells)} payloads printed, nothing sent.")
        return 0

    print(f"\nSubmitted {submitted}/{len(cells)} executions; {len(failed)} failed.")
    if failed:
        print("Failed cells (rerun just these — preseed is idempotent):")
        for cell, status, info in failed:
            print(f"  status={status} cell={cell} :: {info}")
    print(
        "\nPoll a job with:  GET <Location>  (each execution is an independent, "
        "resumable tiles_preseed Job)."
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
