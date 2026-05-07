#    Copyright 2025 FAO
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

"""
Auto-provision the logs dashboard into an OpenSearch Dashboards / Kibana
upstream via the saved-objects REST API.

Works identically against OpenSearch Dashboards 2.x and Elastic-Cloud Kibana
7+/8+/9+: both expose ``POST /api/saved_objects/_import`` with the same
multipart contract. The only differences are header / auth conventions, which
are env-configurable.

Failures never raise: the ES module must continue startup even if Dashboards
is unreachable or misconfigured.
"""

import asyncio
import logging
import os
from importlib.resources import files as _pkg_files
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_NDJSON_PACKAGE = "dynastore.extensions.logs.kibana"
_NDJSON_FILENAME = "dynastore-logs-dashboard.ndjson"


def _upstream_url() -> Optional[str]:
    """Return the configured upstream URL, stripped of trailing slash, or None."""
    raw = os.environ.get("KIBANA_UPSTREAM_URL", "").strip().rstrip("/")
    return raw or None


def _api_key() -> Optional[str]:
    """Return the configured Elastic/OpenSearch API key, or None in dev."""
    key = os.environ.get("KIBANA_UPSTREAM_API_KEY", "").strip()
    return key or None


def _forward_headers() -> dict:
    """Headers to send on every upstream call (XSRF + optional auth)."""
    headers = {
        # OpenSearch Dashboards accepts osd-xsrf; Kibana accepts kbn-xsrf.
        # Sending both is harmless and makes the same code work on both.
        "osd-xsrf": "true",
        "kbn-xsrf": "true",
    }
    key = _api_key()
    if key:
        headers["Authorization"] = f"ApiKey {key}"
    return headers


async def _wait_until_ready(
    client: httpx.AsyncClient, base_url: str, timeout_s: float = 30.0
) -> bool:
    """Poll ``/api/status`` until the upstream reports overall state available."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    last_err: Optional[str] = None
    while asyncio.get_event_loop().time() < deadline:
        try:
            resp = await client.get(f"{base_url}/api/status", timeout=5.0)
            if resp.status_code == 200:
                body = resp.json()
                # OpenSearch Dashboards: body["status"]["overall"]["state"] == "green"
                # Kibana 8+:            body["status"]["overall"]["level"] == "available"
                overall = (body.get("status") or {}).get("overall") or {}
                state = overall.get("state") or overall.get("level")
                if state in ("green", "available"):
                    return True
                last_err = f"overall state={state!r}"
            elif resp.status_code in (401, 403):
                # Auth problem — no point in retrying
                last_err = f"HTTP {resp.status_code} from /api/status (check KIBANA_UPSTREAM_API_KEY)"
                break
            else:
                last_err = f"HTTP {resp.status_code}"
        except Exception as exc:
            last_err = str(exc)
        await asyncio.sleep(2.0)
    logger.warning(
        "dashboards_provisioner: upstream %s not ready within %ds (%s)",
        base_url, int(timeout_s), last_err or "unknown",
    )
    return False


async def provision_dashboards() -> None:
    """
    Import the bundled logs dashboard NDJSON into the configured Dashboards
    upstream. No-op (with INFO log) when ``KIBANA_UPSTREAM_URL`` is unset.

    Idempotent: uses ``?overwrite=true`` so reruns replace existing objects
    cleanly, matching the ES-module behaviour of auto-creating the log index
    on every startup.
    """
    base_url = _upstream_url()
    if not base_url:
        logger.info(
            "dashboards_provisioner: KIBANA_UPSTREAM_URL not set — skipping "
            "dashboard auto-import."
        )
        return

    try:
        ndjson_bytes = (_pkg_files(_NDJSON_PACKAGE) / _NDJSON_FILENAME).read_bytes()
    except Exception as exc:
        logger.warning(
            "dashboards_provisioner: could not read bundled NDJSON %s/%s: %s",
            _NDJSON_PACKAGE, _NDJSON_FILENAME, exc,
        )
        return

    async with httpx.AsyncClient(headers=_forward_headers()) as client:
        if not await _wait_until_ready(client, base_url):
            return

        try:
            resp = await client.post(
                f"{base_url}/api/saved_objects/_import",
                params={"overwrite": "true"},
                files={"file": (_NDJSON_FILENAME, ndjson_bytes, "application/ndjson")},
                timeout=30.0,
            )
        except Exception as exc:
            logger.warning(
                "dashboards_provisioner: POST _import failed (%s): %s",
                base_url, exc,
            )
            return

        if resp.status_code >= 400:
            logger.warning(
                "dashboards_provisioner: upstream returned HTTP %d: %s",
                resp.status_code, resp.text[:400],
            )
            return

        try:
            body = resp.json()
            success_count = body.get("successCount")
            errors = body.get("errors") or []
            if errors:
                logger.warning(
                    "dashboards_provisioner: imported %s objects with %d errors: %s",
                    success_count, len(errors),
                    [e.get("error", {}).get("type") for e in errors[:5]],
                )
            else:
                logger.info(
                    "dashboards_provisioner: provisioned %s saved objects into %s",
                    success_count, base_url,
                )
        except Exception:
            logger.info(
                "dashboards_provisioner: POST _import OK (%d) — unparsed body",
                resp.status_code,
            )
