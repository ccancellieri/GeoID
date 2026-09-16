# DynaStore Agent Guidelines

## Required development skills

Before project work, read `.agents/skills/geoid-development/SKILL.md`
and its development guidelines. Always also load
the installed `ogc-api-standards` skill; follow its relevant
reference routing, including the GeoID implementation map. For documentation or
behavior changes, also load the local `geoid-docs` skill when available.

These instructions and project skills are intentionally versioned. Keep private
runtime configuration, credentials, and personal notes untracked. Verify current manifests,
symbols, commands, and repository visibility: older paths and capability claims
below or in references may be stale. Private endpoints in local references are
not permission to access them or to publish them. `un-fao/GeoID` remains read-only
under the user's current instruction. Check explicit targets before remote writes.

This document provides essential information for AI agents working on the DynaStore codebase.

## 1. Build, Lint, and Test Commands

**Environment:**
- The project uses a `manage.sh` script to handle Docker-based development.
- Local development often uses a virtual environment (`.venv`).

**Test:**
- **Run all tests:** `pytest tests/`
- **Run a specific test file:** `pytest tests/path/to/test_file.py`
- **Run a specific test function:** `pytest tests/path/to/test_file.py::test_function_name`
- **Run with coverage:** `pytest tests/ --cov=dynastore --cov-report=html`
- **Note:** Tests are configured in `pytest.ini`.

**Lint & Format:**
- **Lint:** `ruff check src/ tests/`
- **Format:** `black src/ tests/`
- **Type Check:** `mypy src/`

**Run Application:**
- **Dev Mode:** `./manage.sh dev` (Runs with hot-reload and debugpy)
- **Prod Mode:** `./manage.sh prod`

## 2. Code Style & Standards

**General:**
- **Style:** PEP 8.
- **Formatting:** Use `black` for code formatting.
- **Imports:** Standard library first, then third-party, then local application imports. Use absolute imports for local modules (e.g., `from dynastore.core import ...`).

**Type Safety:**
- **Type Hints:** Mandatory for function signatures (arguments and return types).
- **Generic Types:** Use `list[str]` instead of `List[str]` (Python 3.9+ syntax).

**Async/Await:**
- **I/O:** All I/O operations (DB, Network, File) MUST be `async`.
- **Resource Management:** Use `async with` context managers.
- **Blocking Code:** Avoid blocking calls in async functions.

**Logging:**
- Use the project's logger:
  ```python
  from dynastore.core.logger import get_logger
  logger = get_logger(__name__)
  ```
- Log at appropriate levels (DEBUG, INFO, WARNING, ERROR).

**Error Handling:**
- Use specific exception types.
- Provide meaningful error messages.
- Log errors with stack traces when appropriate (`logger.exception(...)`).

## 3. Project-Specific Architecture & Patterns

**Protocols & Discovery:**
- **Protocol-First:** Eliminate direct usage of concrete `*Manager` classes.
- **Access:** Always access functionality via `dynastore.tools.discovery.get_protocol(ProtocolName)`.
  ```python
  # CORRECT
  db = get_protocol(DatabaseProtocol)
  storage = get_protocol(StorageProtocol)

  # INCORRECT
  db = DatabaseManager()
  ```

**Database Access:**
- **Engine:** Access the engine via `get_protocol(DatabaseProtocol).engine`.
- **Legacy:** DO NOT use `get_any_engine()`.
- **Connections:** Always use `DBResource` context managers.
- **Transactions:** Ensure `db_resource` passed to managers is a connection/engine object, not an ID.
- **Concurrency:** Use distributed advisory locks (via `db_config` module) for DDL and critical control paths to support Cloud Run scaling.

**Service Layer:**
- **Abstraction:** Public service methods MUST accept Logical IDs (`catalog_id`, `collection_id`) and resolve physical storage internally.
- **Encapsulation:** NEVER expose physical schema/table names in public API signatures.
- **Caching:** Do NOT pass active `db_resource` connections to cached resolution methods (e.g., `resolve_physical_schema`) as it breaks `alru_cache`.

**Testing Strategy:**
- **Integration over Unit:** Prioritize real-world integration tests (`tests/dynastore/modules/*/integration/`) over mocks for database and external services.
- **GCP/External:** Prefer real resources for tests (e.g., actual buckets). Ensure generated IDs are short and compliant.
- **Module Access:** Retrieve modules/protocols via `get_protocol(StorageProtocol)` instead of `get_module_instance_by_class`.
- **Environment:** Background tasks must check `os.environ.get("PYTEST_CURRENT_TEST")` to avoid running during test teardown.

**JSON & Data:**
- **Serialization:** Use `orjson` for high-performance JSON handling.
- **Streaming:** When manually constructing JSON (e.g., WFS), ensure valid syntax (`None` -> `null`).
- **Validity:** For `TSTZRANGE` validity checks, handle open-ended ranges: `upper(validity) IS NULL OR upper(validity) > NOW()`.

**Localization:**
- **Strictness:** When creating localized resources with dicts (e.g., `{"en": "val"}`), use `lang="*"` to bypass single-language validation if necessary.

## 4. Documentation

- Update `README.md` for user-facing changes.
- Update docstrings for API changes.
- Add examples for new features.
