# ES Geometry Accept-and-Simplify — Implementation Plan

> **Execution:** Implement this plan task-by-task with TDD — each task ends with a green test run and a commit. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Elasticsearch-backed collections accept any geometry by auto-simplifying oversized geometry to fit the 10 MB ES per-document limit by default, instead of rejecting it.

**Architecture:** Flip the existing (already-implemented but off-by-default) `simplify_geometry` capability to default-on across the three ES items driver configs, make every config-resolution path fail *open* (simplify) rather than closed (reject), raise the simplification iteration cap for better fidelity, and add an optional `simplify_target_bytes` budget so operators can keep ES indices small. PG always keeps full-resolution geometry.

**Tech Stack:** Python 3.12, Pydantic v2, shapely, orjson, pytest. Companion spec: `docs/design/2026-06-18-es-geometry-simplification-design.md`.

## Global Constraints

- NO AI attribution in commits, comments, or any file.
- NO config migration, NO runtime DDL — this is a code-default change only.
- `simplify_geometry` stays `Mutable[bool]` — no rename, no type change, no removal (live config-API users).
- `simplify_target_bytes` is additive and optional; default `None` ⇒ behavior identical to today's 10 MB.
- `simplify_target_bytes` must never exceed `DEFAULT_MAX_BYTES` (10 MB ES ceiling) — clamp high side.
- PG geometry is NEVER simplified (only the ES seam transforms geometry).
- Run tests locally to green before any push. Test runner: from repo root, shared venv with `PYTHONPATH` covering `packages/*/src` and `packages/extensions/*/src`. Command form: `python -m pytest <path>::<test> -v`.
- Deploy/verify ONLY to the `dev` GitHub environment, never `review`/v2.

---

### Task 1: Raise simplification iteration cap & refresh policy docstring

**Files:**
- Modify: `packages/core/src/dynastore/tools/geometry_simplify.py:62` (constant) and the module docstring (lines 32-51).
- Test: `tests/dynastore/tools/test_geometry_simplify.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `DEFAULT_MAX_ITERATIONS == 8` (relied on by callers that use the default). `simplify_to_fit`/`maybe_simplify_for_es` signatures unchanged.

- [ ] **Step 1: Write the failing test**

Add to `tests/dynastore/tools/test_geometry_simplify.py`:

```python
def test_default_max_iterations_is_eight():
    assert DEFAULT_MAX_ITERATIONS == 8


def test_custom_smaller_budget_shrinks_geometry():
    # A dense ring that serializes well over 1 MB.
    poly = Polygon(_ring(60_000))
    doc = {"id": "x", "geometry": mapping(poly)}
    out, factor, mode = simplify_to_fit(doc, max_bytes=1_000_000)
    assert geometry_geojson_size(out["geometry"]) <= 1_000_000
    assert mode in (MODE_TOLERANCE, MODE_BBOX)
    assert factor < 1.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/dynastore/tools/test_geometry_simplify.py::test_default_max_iterations_is_eight -v`
Expected: FAIL — `assert 3 == 8`.

- [ ] **Step 3: Raise the constant**

In `packages/core/src/dynastore/tools/geometry_simplify.py`, line 62:

```python
DEFAULT_MAX_ITERATIONS = 8
```

Also update the module docstring "Geometry policy (issue #1248)" block (lines 32-51) so it states the new default. Replace the first sentence of that section:

```
Geometry policy (issue #1248, revised 2026-06-18)
=================================================

Simplification is **on by default** for Elasticsearch items drivers. ES indexes
a simplified copy of any geometry that exceeds the byte budget; the PostgreSQL
primary always keeps full resolution. A collection may opt out by setting
``simplify_geometry: false`` on its ES items driver config, in which case an
oversized geometry is rejected up-front by the ``item_service.upsert`` pre-write
guard (HTTP 422) rather than truncated.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/dynastore/tools/test_geometry_simplify.py -v`
Expected: PASS (all, including the two new tests).

- [ ] **Step 5: Commit**

```bash
git add packages/core/src/dynastore/tools/geometry_simplify.py tests/dynastore/tools/test_geometry_simplify.py
git commit -m "feat(geometry): raise ES simplify iteration cap to 8 and document simplify-by-default"
```

---

### Task 2: Flip driver-config defaults to simplify + add `simplify_target_bytes`

**Files:**
- Modify: `packages/core/src/dynastore/modules/storage/driver_config.py` — `ItemsElasticsearchDriverConfig` (field at 1134-1143), `ItemsElasticsearchPrivateDriverConfig` (field at 1207-1216), `ItemsElasticsearchEnvelopeDriverConfig` (field at 1261-1270).
- Test: `tests/dynastore/modules/storage/test_es_items_driver_config_defaults.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: each of the three configs has `simplify_geometry` default `True` and a new `simplify_target_bytes: Mutable[Optional[int]]` default `None`. Resolvers in later tasks read `getattr(cfg, "simplify_target_bytes", None)`.

- [ ] **Step 1: Write the failing test**

Create `tests/dynastore/modules/storage/test_es_items_driver_config_defaults.py` (copy the FAO license header from any sibling test file verbatim), then:

```python
"""ES items driver configs simplify geometry by default (revised 2026-06-18)."""
import pytest

from dynastore.modules.storage.driver_config import (
    ItemsElasticsearchDriverConfig,
    ItemsElasticsearchPrivateDriverConfig,
    ItemsElasticsearchEnvelopeDriverConfig,
)

_CONFIGS = [
    ItemsElasticsearchDriverConfig,
    ItemsElasticsearchPrivateDriverConfig,
    ItemsElasticsearchEnvelopeDriverConfig,
]


@pytest.mark.parametrize("cls", _CONFIGS)
def test_simplify_geometry_defaults_on(cls):
    assert cls().simplify_geometry is True


@pytest.mark.parametrize("cls", _CONFIGS)
def test_simplify_target_bytes_defaults_none(cls):
    assert cls().simplify_target_bytes is None


@pytest.mark.parametrize("cls", _CONFIGS)
def test_explicit_disable_is_respected(cls):
    assert cls(simplify_geometry=False).simplify_geometry is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/dynastore/modules/storage/test_es_items_driver_config_defaults.py -v`
Expected: FAIL — `test_simplify_geometry_defaults_on` asserts `False is True`; `test_simplify_target_bytes_defaults_none` errors with unknown attribute.

- [ ] **Step 3: Flip defaults and add the field (all three classes)**

For EACH of the three field blocks, change `default=False` to `default=True`, rewrite the description, and append the new field. The replacement block (apply the same edit at 1134-1143, 1207-1216, 1261-1270):

```python
    simplify_geometry: Mutable[bool] = Field(
        default=True,
        description=(
            "When True (default), oversized geometries are simplified to fit "
            "the Elasticsearch per-document byte budget before indexing "
            "(lossy; the PostgreSQL primary always keeps full resolution). "
            "Set False to index exact geometry and reject items whose geometry "
            "exceeds the budget with HTTP 422 before any write."
        ),
    )
    simplify_target_bytes: Mutable[Optional[int]] = Field(
        default=None,
        ge=1,
        examples=[None, 1_000_000],
        description=(
            "Target byte budget for ES geometry simplification. When set, "
            "geometry is simplified to fit under this size instead of the "
            "10 MB Elasticsearch per-document limit — lower values keep ES "
            "indices smaller at the cost of geometry fidelity. Values above "
            "the 10 MB ES ceiling are clamped down. Defaults to the 10 MB "
            "limit when unset. Does not affect the PostgreSQL primary."
        ),
    )
```

(`Optional` and `Field` are already imported — see `driver_config.py:42,47`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/dynastore/modules/storage/test_es_items_driver_config_defaults.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/core/src/dynastore/modules/storage/driver_config.py tests/dynastore/modules/storage/test_es_items_driver_config_defaults.py
git commit -m "feat(storage): default ES items drivers to simplify geometry; add simplify_target_bytes"
```

---

### Task 3: Make the pre-write guard fail open + sharpen the reject message

**Files:**
- Modify: `packages/core/src/dynastore/modules/catalog/item_service.py` — `_es_items_driver_simplify_enabled` (746-778) and the `_reason` closure inside `_enforce_es_geometry_size_limit` (848-857).
- Test: `tests/dynastore/modules/catalog/test_item_service_geometry_guard.py` (extend; update existing default-off assumptions).

**Interfaces:**
- Consumes: the guard reads each resolved driver's `is_es_items_driver` attr and awaits `_resolve_simplify_geometry(catalog_id, collection_id, db_resource=...)`.
- Produces: `_es_items_driver_simplify_enabled` returns `True` on every fallback/error path (fail open). Guard rejects ONLY when an ES items secondary is routed AND simplify resolves explicitly `False`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/dynastore/modules/catalog/test_item_service_geometry_guard.py` (reuse the file's existing `_big_geometry()` helper):

```python
class _StubESDriver:
    is_es_items_driver = True

    def __init__(self, *, simplify=None, raise_on_resolve=False):
        self._simplify = simplify
        self._raise = raise_on_resolve

    async def _resolve_simplify_geometry(self, catalog_id, collection_id, *, db_resource=None):
        if self._raise:
            raise RuntimeError("configs protocol unavailable")
        return self._simplify


class _Resolved:
    def __init__(self, driver):
        self.driver = driver


@pytest.mark.asyncio
async def test_guard_accepts_when_simplify_enabled_by_default():
    svc = ItemService()
    item = {"id": "1582", "geometry": _big_geometry()}
    kept = await svc._enforce_es_geometry_size_limit(
        "cat", "coll", [item], [_Resolved(_StubESDriver(simplify=True))],
        is_single=True,
    )
    assert kept == [item]


@pytest.mark.asyncio
async def test_guard_fails_open_when_resolution_errors():
    svc = ItemService()
    item = {"id": "1582", "geometry": _big_geometry()}
    kept = await svc._enforce_es_geometry_size_limit(
        "cat", "coll", [item], [_Resolved(_StubESDriver(raise_on_resolve=True))],
        is_single=True,
    )
    assert kept == [item]


@pytest.mark.asyncio
async def test_guard_rejects_when_simplify_explicitly_disabled():
    svc = ItemService()
    item = {"id": "1582", "geometry": _big_geometry()}
    with pytest.raises(ValueError) as exc:
        await svc._enforce_es_geometry_size_limit(
            "cat", "coll", [item], [_Resolved(_StubESDriver(simplify=False))],
            is_single=True,
        )
    assert "simplify_geometry" in str(exc.value)
```

Then UPDATE any existing test in this file that asserts rejection using a stub whose simplify resolves to `None`/missing — those now expect acceptance. (Search the file for existing `_enforce_es_geometry_size_limit` calls and `pytest.raises(ValueError)`; any that relied on the old default-off must set `simplify=False` explicitly to keep asserting rejection.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/dynastore/modules/catalog/test_item_service_geometry_guard.py::test_guard_fails_open_when_resolution_errors -v`
Expected: FAIL — guard currently returns `False` on resolver error → raises ValueError instead of keeping the item.

- [ ] **Step 3: Flip the fallbacks to fail open**

In `packages/core/src/dynastore/modules/catalog/item_service.py`, `_es_items_driver_simplify_enabled` (746-778). Change the docstring line "Defaults to False (exact-by-default, #1248)" to "Defaults to True (simplify-by-default, revised 2026-06-18) — a config-read failure must fail OPEN (simplify), never reject." Then change the four fallback returns:

```python
        resolver = getattr(driver, "_resolve_simplify_geometry", None)
        if resolver is not None:
            try:
                return bool(await resolver(
                    catalog_id, collection_id, db_resource=db_resource,
                ))
            except Exception:
                return True
        get_config = getattr(driver, "get_driver_config", None)
        if get_config is not None:
            try:
                cfg = await get_config(
                    catalog_id, collection_id, db_resource=db_resource,
                )
                return bool(getattr(cfg, "simplify_geometry", True))
            except Exception:
                return True
        return True
```

- [ ] **Step 4: Sharpen the reject message**

In the same file, replace the `_reason` closure body (848-857):

```python
        def _reason(item_id: Any, size: int) -> str:
            return (
                f"Item '{item_id}' geometry is {size} bytes, exceeding the "
                f"{DEFAULT_MAX_BYTES}-byte (10 MB) Elasticsearch "
                f"per-document limit. ES geometry simplification is ON by "
                f"default, but this collection has explicitly set "
                f"'simplify_geometry: false' on its Elasticsearch items driver, "
                f"so the oversized item is rejected before any write. To accept "
                f"and auto-simplify it, remove that override or set "
                f"'simplify_geometry: true' (PostgreSQL keeps full resolution)."
            )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/dynastore/modules/catalog/test_item_service_geometry_guard.py -v`
Expected: PASS (all, including updated existing tests).

- [ ] **Step 6: Commit**

```bash
git add packages/core/src/dynastore/modules/catalog/item_service.py tests/dynastore/modules/catalog/test_item_service_geometry_guard.py
git commit -m "feat(catalog): geometry guard fails open to simplify-by-default; clearer opt-out reject message"
```

---

### Task 4: ES driver — fail-open resolver + apply `simplify_target_bytes` budget

**Files:**
- Modify: `packages/core/src/dynastore/modules/storage/drivers/elasticsearch.py` — `_resolve_simplify_geometry` (498-538), call sites (1185-1187 resolves; transforms at 1394-1397, 1997-1999, 2108-2109). Add a sibling resolver `_resolve_simplify_max_bytes`.
- Test: `tests/dynastore/modules/elasticsearch/test_es_geometry_budget.py` (create)

**Interfaces:**
- Consumes: `DEFAULT_MAX_BYTES` from `dynastore.tools.geometry_simplify`; config field `simplify_target_bytes` from Task 2.
- Produces: `_resolve_simplify_geometry` returns `True` on degrade paths (fail open). New `async _resolve_simplify_max_bytes(catalog_id, collection_id, *, db_resource=None) -> int` returns the effective ES geometry budget: the config's `simplify_target_bytes` clamped to `(1, DEFAULT_MAX_BYTES)`, or `DEFAULT_MAX_BYTES` when unset/unavailable. Every `maybe_simplify_for_es(...)` call passes `max_bytes=<resolved budget>`.

- [ ] **Step 1: Write the failing tests**

Create `tests/dynastore/modules/elasticsearch/test_es_geometry_budget.py` (FAO header verbatim from a sibling), then:

```python
"""ES driver resolves a clamped geometry byte budget and fails open."""
import pytest

from dynastore.tools.geometry_simplify import DEFAULT_MAX_BYTES


def _clamp_budget(target):
    # Mirror of the driver's clamp logic for a pure unit check.
    if target is None:
        return DEFAULT_MAX_BYTES
    return max(1, min(int(target), DEFAULT_MAX_BYTES))


def test_budget_none_is_default():
    assert _clamp_budget(None) == DEFAULT_MAX_BYTES


def test_budget_clamped_to_ceiling():
    assert _clamp_budget(50_000_000) == DEFAULT_MAX_BYTES


def test_budget_small_value_passes_through():
    assert _clamp_budget(1_000_000) == 1_000_000
```

Note: the clamp helper above documents the required arithmetic. The implementer must wire the *same* clamp into the driver method in Step 3; if the driver exposes a static clamp helper, import and assert against it instead of the local copy.

- [ ] **Step 2: Run tests to verify they pass-as-spec**

Run: `python -m pytest tests/dynastore/modules/elasticsearch/test_es_geometry_budget.py -v`
Expected: PASS (this locks the clamp contract before wiring).

- [ ] **Step 3: Flip resolver fallbacks and add the budget resolver**

In `elasticsearch.py`, `_resolve_simplify_geometry` (498-538): update the docstring line "Degrade-safe: returns ``False``" to "Degrade-safe: returns ``True`` (fail open to simplify) when the configs protocol is unavailable or the row is missing." Then change the three `False` outcomes to `True`:

```python
            return bool(getattr(driver_config, "simplify_geometry", True))
        ...
        if configs is None:
            return True
        ...
        return bool(getattr(config, "simplify_geometry", True))
```

Immediately after `_resolve_simplify_geometry`, add the budget resolver:

```python
    async def _resolve_simplify_max_bytes(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        db_resource: Optional[Any] = None,
    ) -> int:
        """Effective ES geometry byte budget for simplification.

        Reads ``simplify_target_bytes`` from this driver's config and clamps it
        to ``(1, DEFAULT_MAX_BYTES)``. Returns ``DEFAULT_MAX_BYTES`` (the 10 MB
        ES ceiling) when unset or when the config is unavailable.
        """
        from dynastore.tools.geometry_simplify import DEFAULT_MAX_BYTES

        config_cls = self.__class__._driver_config_class
        try:
            if config_cls is None:
                cfg = await self.get_driver_config(
                    catalog_id, collection_id, db_resource=db_resource,
                )
            else:
                from dynastore.models.protocols.configs import ConfigsProtocol
                from dynastore.models.driver_context import DriverContext
                from dynastore.tools.discovery import get_protocol

                configs = get_protocol(ConfigsProtocol)
                if configs is None:
                    return DEFAULT_MAX_BYTES
                cfg = await configs.get_config(
                    config_cls,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    ctx=DriverContext(db_resource=db_resource),
                )
        except Exception:
            return DEFAULT_MAX_BYTES
        target = getattr(cfg, "simplify_target_bytes", None)
        if target is None:
            return DEFAULT_MAX_BYTES
        return max(1, min(int(target), DEFAULT_MAX_BYTES))
```

- [ ] **Step 4: Pass the budget at the three transform call sites**

At 1185-1187 (resolves `simplify_geometry` before the public index transform), add the budget resolution right after:

```python
        simplify_geometry = await self._resolve_simplify_geometry(
            catalog_id, collection_id, db_resource=db_resource,
        )
        simplify_max_bytes = await self._resolve_simplify_max_bytes(
            catalog_id, collection_id, db_resource=db_resource,
        )
```

Then update the transform at 1394-1397:

```python
            es_doc, factor, mode = maybe_simplify_for_es(
                es_doc, simplify=simplify_geometry, max_bytes=simplify_max_bytes,
            )
            _apply_geometry_simplification(es_doc, factor, mode)
```

At the context call site (1997-1999), resolve the budget next to the existing `simplify_geometry` line and pass it:

```python
        simplify_geometry = await self._resolve_simplify_geometry(ctx.catalog, ctx.collection)
        simplify_max_bytes = await self._resolve_simplify_max_bytes(ctx.catalog, ctx.collection)
        doc, factor, mode = maybe_simplify_for_es(
            doc, simplify=simplify_geometry, max_bytes=simplify_max_bytes,
        )
        _apply_geometry_simplification(doc, factor, mode)
```

At the bulk call site (2108-2109), find where `simplify_geometry` is resolved before the loop and resolve `simplify_max_bytes` once in the same place, then pass it:

```python
            doc, factor, mode = maybe_simplify_for_es(
                doc, simplify=simplify_geometry, max_bytes=simplify_max_bytes,
            )
            _apply_geometry_simplification(doc, factor, mode)
```

(Resolve `simplify_max_bytes` ONCE outside the per-item loop, mirroring `simplify_geometry`.)

- [ ] **Step 5: Run the full ES + tools + guard suites**

Run:
```
python -m pytest tests/dynastore/modules/elasticsearch/ tests/dynastore/tools/test_geometry_simplify.py tests/dynastore/modules/catalog/test_item_service_geometry_guard.py -v
```
Expected: PASS. Update any existing ES test that asserted the old default-off resolver behavior (search `tests/dynastore/modules/elasticsearch/test_private_geometry_simplification.py` for assumptions that simplification is off by default; flip to expect on, or set `simplify_geometry=False` explicitly where a no-op was intended).

- [ ] **Step 6: Commit**

```bash
git add packages/core/src/dynastore/modules/storage/drivers/elasticsearch.py tests/dynastore/modules/elasticsearch/test_es_geometry_budget.py
git commit -m "feat(es): fail open to simplify-by-default and honor clamped simplify_target_bytes budget"
```

---

### Task 5: Repo-wide default-flip sweep + green gate

**Files:**
- Modify: any remaining test/fixture asserting the old `simplify_geometry` default-off or reject-by-default.

**Interfaces:**
- Consumes: all prior tasks.
- Produces: a fully green test run with the new defaults.

- [ ] **Step 1: Find stragglers**

Run: `git grep -n "simplify_geometry" -- '*.py' | grep -i test`
For each hit, confirm whether it assumes the OLD default (off/reject). Any that do must either set `simplify_geometry=False` explicitly (if testing the opt-out) or be updated to expect simplify-on.

- [ ] **Step 2: Run the targeted core suites**

Run:
```
python -m pytest tests/dynastore/tools/test_geometry_simplify.py tests/dynastore/modules/storage/test_es_items_driver_config_defaults.py tests/dynastore/modules/catalog/test_item_service_geometry_guard.py tests/dynastore/modules/elasticsearch/ -v
```
Expected: PASS, no failures, no errors.

- [ ] **Step 3: Lint changed files**

Run: `uv tool run ruff check packages/core/src/dynastore/tools/geometry_simplify.py packages/core/src/dynastore/modules/storage/driver_config.py packages/core/src/dynastore/modules/catalog/item_service.py packages/core/src/dynastore/modules/storage/drivers/elasticsearch.py`
Expected: no errors (ruff uses bugbear; B904 etc.).

- [ ] **Step 4: Commit any test fixes**

```bash
git add -A
git commit -m "test: align geometry-simplify suites with simplify-by-default"
```

---

### Task 6: End-to-end verification on dev (manual, post-merge gate)

**Files:** none (operational).

**Interfaces:** consumes the merged change.

- [ ] **Step 1: Deploy the branch to dev**

Run (per project deploy convention):
```
gh workflow run deploy.yml -R un-fao/dynastore -f environment=dev -f geoid_ref=<branch> -f services_all=true -f force_deploy=true
```

- [ ] **Step 2: Ingest the originating failure case**

Ingest `gaul_dmgr.zip` into a collection routing ES + PG. Confirm item `1582` (≈14.9 MB geometry) is ACCEPTED (no 422).

- [ ] **Step 3: Verify both surfaces**

- ES doc for item 1582: geometry serializes ≤ budget; `system.geometry_simplification.{factor,mode}` is stamped.
- PG row for item 1582: geometry is full-resolution (unchanged).

- [ ] **Step 4: Verify the opt-out still rejects**

On a collection with `simplify_geometry: false`, re-ingest the same feature and confirm the HTTP 422 with the sharpened reject message.

---

## Self-Review

**Spec coverage:**
- Decision 1 (accept+auto-simplify default) → Tasks 2 + 3 + 4 (config default, guard fail-open, resolver fail-open).
- Decision 2 (improve Option 1 engine) → Task 1 (iteration cap).
- Decision 3 (new default applies to all) → Task 2 default flip + Task 5 sweep.
- `simplify_target_bytes` knob → Task 2 (field) + Task 4 (resolve/clamp/apply).
- Sharper reject message → Task 3 Step 4.
- Back-compat (explicit false still rejects) → Task 2 `test_explicit_disable_is_respected`, Task 3 `test_guard_rejects_when_simplify_explicitly_disabled`.
- Testing section → Tasks 1-5; E2E gaul_dmgr → Task 6.
- Out-of-scope (H3/S2, tiered ladder) → not in any task (correct; documented as future in spec).

**Placeholder scan:** No TBD/TODO; every code step shows real code. Task 4 Step 1 uses a documented local clamp mirror with an explicit instruction to assert against the driver's own helper if exposed — acceptable, not a placeholder.

**Type consistency:** `simplify_geometry: Mutable[bool]`, `simplify_target_bytes: Mutable[Optional[int]]`, `_resolve_simplify_max_bytes(...) -> int`, `maybe_simplify_for_es(doc, *, simplify, max_bytes)` — names match across Tasks 2/4 and the verified current signature in `geometry_simplify.py`.
