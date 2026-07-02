COMPOSE := docker compose -f packages/core/src/dynastore/docker/docker-compose.yml -f packages/core/src/dynastore/docker/docker-compose.dev.yml
PYTEST  := .venv/bin/pytest

.PHONY: help db-up db-down db-wait test test-unit test-integration test-coverage clean dev-sync

help:
	@echo "DynaStore developer targets:"
	@echo "  make dev-sync          Install all extensions for local dev/testing"
	@echo "  make db-up             Start the test database (host-bound 127.0.0.1:54320)"
	@echo "  make db-down           Stop and remove the test database (drops volumes)"
	@echo "  make test              Run unit + integration tests (requires db-up)"
	@echo "  make test-unit         Run unit tests only (no database required for most)"
	@echo "  make test-integration  Run integration tests only (requires db-up)"
	@echo "  make test-coverage     Run full suite with coverage report"
	@echo "  make clean             Remove pytest cache and coverage artefacts"

# Derive the install set from the filesystem so added/retired extensions
# never drift out of sync with this list.
EXTENSION_DIRS := $(dir $(wildcard packages/extensions/*/pyproject.toml))

dev-sync:
	uv pip install -e packages/core/ $(addprefix -e ,$(EXTENSION_DIRS))

db-up:
	$(COMPOSE) up -d db
	@$(MAKE) --no-print-directory db-wait

db-down:
	$(COMPOSE) down -v

db-wait:
	@echo "Waiting for postgres on 127.0.0.1:54320..."
	@for i in $$(seq 1 30); do \
	  if docker compose -f packages/core/src/dynastore/docker/docker-compose.yml exec -T db pg_isready -U testuser -d gis_dev >/dev/null 2>&1; then \
	    echo "Database ready."; exit 0; \
	  fi; sleep 1; \
	done; \
	echo "Database did not become ready in 30s." >&2; exit 1

test: db-up
	$(PYTEST) tests -vv

test-unit:
	$(PYTEST) tests --ignore-glob="**/integration/*" -vv

test-integration: db-up
	$(PYTEST) tests/dynastore -k integration -vv

test-coverage: db-up
	$(PYTEST) tests --cov=packages/core/src/dynastore --cov-report=term --cov-report=html

clean:
	rm -rf .pytest_cache htmlcov .coverage
