# Contributing to DynaStore

Thank you for your interest in contributing to DynaStore! This document provides guidelines for contributing to the project.

## Code of Conduct

Be respectful, inclusive, and professional in all interactions.

## Getting Started

### Development Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/your-username/dynastore.git
   cd dynastore
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -e '.[all]'
   ```

4. **Start the development environment**
   ```bash
   src/dynastore/scripts/rebuild.sh dev
   ```

## Development Workflow

### Creating a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

### Making Changes

1. Write your code following the project's coding standards
2. Add tests for new functionality
3. Update documentation as needed
4. Ensure all tests pass

### Running Tests

First start the dev database (port 54320, separate from production on 5432):

```bash
docker compose -f src/dynastore/docker/docker-compose.yml -f src/dynastore/docker/docker-compose.dev.yml up -d db
```

```bash
# Run all tests (parallel, default)
.venv/bin/pytest tests/

# Run sequentially (required for some integration tests)
.venv/bin/pytest tests/ -p no:xdist -o "addopts="

# Run specific test file
.venv/bin/pytest tests/path/to/test_file.py

# Run with coverage report
.venv/bin/pytest tests/ -p no:xdist -o "addopts=" \
    --cov=src/dynastore --cov-report=term --cov-report=html

# Run only unit tests (no DB required — note: wait_for_db fixture still needs DB)
.venv/bin/pytest tests/dynastore/modules/storage/unit \
                 tests/dynastore/extensions/tools/unit \
                 tests/dynastore/tools/
```

Current baseline coverage: **~50%** (stable) / **~34%** (refactor in progress).  
See the [Coverage Report](testing/coverage-report.md) for per-module breakdown and improvement priorities.

### Code Quality

We use the following tools for code quality:

```bash
# Format code (if using black)
black src/ tests/

# Lint code (if using ruff)
ruff check src/ tests/

# Type checking (if using mypy)
mypy src/
```

## Pull Request Process

1. **Update your fork**
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Push your changes**
   ```bash
   git push origin feature/your-feature-name
   ```

3. **Create a Pull Request**
   - Go to the GitHub repository
   - Click "New Pull Request"
   - Select your branch
   - Fill in the PR template with:
     - Description of changes
     - Related issues
     - Testing performed
     - Screenshots (if UI changes)

4. **PR Requirements**
   - All tests must pass
   - Code coverage should not decrease
   - Documentation updated if needed
   - Commit messages are clear and descriptive

## Coding Standards

### Python Style

- Follow PEP 8 guidelines
- Use type hints for function signatures
- Write docstrings for all public APIs
- Keep functions focused and concise

### Async/Await

- All I/O operations must be async
- Use `async with` for resource management
- Avoid blocking operations in async code

### Database Operations

- Always use `DBResource` for database connections
- Use parameterized queries to prevent SQL injection
- Close connections properly using context managers

### Logging

- Use DynaStore's logger: `from dynastore.core.logger import get_logger`
- Log at appropriate levels (DEBUG, INFO, WARNING, ERROR)
- Include context in log messages

### Error Handling

- Use specific exception types
- Provide meaningful error messages
- Log errors with stack traces when appropriate

## Plugin Naming Convention

DynaStore uses a consistent **underscore-everywhere** convention for naming
modules, extensions, tasks, and extras.  This ensures that the name a developer
writes in `pyproject.toml` matches the folder on disk and the entry-point key —
no mental translation required.

### Rules

| Artifact | Naming | Example |
|----------|--------|---------|
| Python folder | `_` (language requirement) | `src/dynastore/extensions/catalog_events/` |
| Entry-point key | `_` (matches folder) | `catalog_events = "…:CatalogEventHandler"` |
| Optional-dependency key | `_` (matches entry-point) | `extension_catalog_events = []` |
| `SCOPE` env var value | `_` | `SCOPE=api_core,catalog_api` |

### Why not hyphens?

Python module names **must** use underscores (`import catalog_events`).
Pip normalizes extras names to hyphens internally (PEP 685), but the
**source** `pyproject.toml` can use either character — pip treats them as
equivalent.  By writing underscores everywhere we get one convention from
folder to entry-point to extras to SCOPE, with zero `.replace()` ambiguity.

### Prefixes

Optional-dependency keys use a prefix to indicate the plugin type:

| Prefix | Plugin type | Example |
|--------|-------------|---------|
| `module_` | Module | `module_catalog = [...]` |
| `extension_` | Extension | `extension_web = [...]` |
| `catalog_task_` | In-process task hosted by the catalog (FastAPI BackgroundTasks) | `catalog_task_gcp_provision = [...]` |
| `worker_task_` | Heavy task packaged for a Cloud Run Job (sync-db) | `worker_task_gdal = [...]` |

The discovery system strips `module_` / `extension_` prefixes automatically
when matching against entry-point names (e.g. `extension_catalog_events`
matches entry-point `catalog_events`).

Task extras use a richer `<location>_task_<name>` shape so the **deployment
topology** (in-process vs Cloud Run Job) is readable from the extras name
alone. Each Cloud Run Job is deployed with a single `SCOPE=worker_task_<name>`
env var; `gcp_cloud_runner` strips the `worker_task_` prefix to resolve the
Cloud Run Job back to its task entry-point.

### Downstream projects

External projects that register dynastore entry-points should follow the same
convention.  Example `pyproject.toml`:

```toml
[project.optional-dependencies]
module_spanner = []
extension_permissions = []
extension_catalog_events = []

my_app = [
    "my-project[module_spanner]",
    "my-project[extension_permissions]",
    "my-project[extension_catalog_events]",
]

[project.entry-points."dynastore.modules"]
spanner = "my_project.modules.spanner:SpannerModule"

[project.entry-points."dynastore.extensions"]
permissions    = "my_project.extensions.permissions:PermissionsExtension"
catalog_events = "my_project.extensions.catalog_events:CatalogEventHandler"
```

## Creating External Modules

See the [example project template](../examples/my-project/) for a ready-to-use
starting point when creating modules, extensions, or tasks in a separate repository.

## Documentation

- Update README.md for user-facing changes
- Update docstrings for API changes
- Add examples for new features
- Keep documentation concise and clear

## Commit Messages

Use clear, descriptive commit messages:

```
feat: Add support for external modules
fix: Resolve database connection leak
docs: Update installation instructions
test: Add integration tests for catalog API
refactor: Simplify event handling logic
```

## CI/CD & Multi-Repository Architecture

GeoID uses a multi-repository architecture separating the public framework from
private deployment configurations.

### Repository Roles

| Repository | Visibility | Role |
|---|---|---|
| **GeoID** (this repo) | Public | Framework source code, tests, reusable test workflow |
| **Deployment repos** (e.g. dynastore) | Private | Service configs, deployment workflows, GCP infrastructure |
| **Extension repos** (e.g. fao-aip-catalog) | Private | Custom modules/extensions + own deployment configs |

### Test Workflow (`test.yml`)

GeoID's test workflow is **reusable** via `workflow_call`. Downstream repos call
it as a test gate before deployment:

```
Extension repo (deploy) ──► Dynastore deploy.yml ──► GeoID test.yml
                                                        │
                                                        ├─ Unit tests
                                                        └─ Integration tests
```

The workflow:
1. Builds the test-runner image (`tester` Dockerfile stage) and the canonical compose's `db` service
2. Brings up `db` from the canonical `docker-compose.yml` and runs unit tests in a one-off `docker run` joined to the compose network
3. For integration: brings up `db + elasticsearch + keycloak + catalog + worker` and runs the integration suite, scoped to `*/integration/*` paths
4. Generates JUnit XML reports, SVG badges, and JSON test summaries
5. Uploads artifacts for downstream deployment workflows

### Running Tests Locally

```bash
# Start the dev db (host-bound to 127.0.0.1:54320 for psql access)
docker compose -f src/dynastore/docker/docker-compose.yml -f src/dynastore/docker/docker-compose.dev.yml up -d db

# Run all unit tests on the host venv against that db
.venv/bin/pytest tests/ --ignore-glob="**/integration/*"

# Or, mirror CI: run inside the `tester` Dockerfile stage joined to the compose network
docker compose -f packages/core/src/dynastore/docker/docker-compose.yml up -d db
NETWORK=$(docker network ls --filter "name=dynastore_net" --format "{{.Name}}" | head -n1)
docker run --rm --network "$NETWORK" \
  -v "$PWD":/dynastore/workspace \
  -v "$PWD/tests":/dynastore/tests -v "$PWD/pytest.ini":/dynastore/pytest.ini \
  -e DATABASE_URL=postgresql://postgres:postgres@db:5432/gis_dev \
  geoid-test-runner:ci /opt/venv/bin/pytest tests/ --ignore-glob="**/integration/*"
```

### Test Configuration

- `pytest.ini` — root config with xdist parallel execution, custom markers
- `tests/conftest.py` — shared fixtures (`app_lifespan`, `db_engine`, clients)
- `tests/dynastore/conftest.py` — session-scoped DB cleanup, model fixtures
- Custom markers: `@pytest.mark.enable_modules(...)`, `@pytest.mark.gcp`, `@pytest.mark.local_only`

### Building on GeoID (Downstream Projects)

GeoID is consumed as a library: a downstream project declares a dependency on
`dynastore[<extras>]` and registers its own modules/extensions through Python
entry points — no fork required. The [example project template](../examples/my-project/)
shows the minimal layout (a `pyproject.toml` with the dependency and entry-point
declarations, a custom module/extension, a `docker/` build, and tests).

Packaging and deployment of a downstream service (CI/CD, container build, and
infrastructure templates) are environment-specific and live in each operator's
own deployment repository, which should be kept private.

## Questions or Issues?

- Check existing issues and discussions
- Create a new issue with:
  - Clear description
  - Steps to reproduce (for bugs)
  - Expected vs actual behavior
  - Environment details

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
