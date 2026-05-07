#!/bin/bash
#
# rebuild.sh — wipe volumes + rebuild + start a dev docker composition.
#
# Shared across geoid / dynastore / fao-aip-catalog — shipped as package data at
# `dynastore/scripts/rebuild.sh`. Downstream wrappers may invoke this from the
# pip-installed path:
#
#   exec "$(python -c 'import dynastore,os;print(os.path.join(os.path.dirname(dynastore.__file__),"scripts/rebuild.sh"))')" "$@"
#
# SAFETY: hard-wired to `docker-compose.dev.yml`. Refuses local/prod/on-prem;
# refuses if COMPOSE_FILE is pre-set. The previous `test` mode pointed at a
# `docker-compose.test.yml` that no longer exists in geoid (tests now run
# against the canonical `docker-compose.yml` via CI). Downstream repos that
# still ship their own `docker-compose.test.yml` (e.g. fao-aip-catalog) should
# invoke `docker compose -f docker/docker-compose.test.yml ...` directly
# instead of going through this script.
#
# Repo root: resolved from (first match wins)
#   1. $REPO_ROOT env var
#   2. $PWD if it contains `${COMPOSE_SUBDIR}/docker-compose.dev.yml`
#   3. error
#
# Compose subdir: `${COMPOSE_SUBDIR}` env var overrides the default
# `src/dynastore/docker`. Downstream wrappers (e.g. fao-aip-catalog whose
# composes live at `docker/`) export `COMPOSE_SUBDIR=docker` before delegating.
#
# Project name: `${PROJECT_PREFIX}_dev`. PROJECT_PREFIX defaults to the
# basename of the repo root (keeps volumes isolated per-repo without forking).
#
# Usage:
#   cd <repo> && .../rebuild.sh dev            # wipe + rebuild + up dev stack
#   cd <repo> && .../rebuild.sh dev --no-wipe  # rebuild images only, keep volumes

set -euo pipefail

env="${1:-}"
flag="${2:-}"

case "$env" in
    dev) ;;
    test)
        echo "REFUSED: 'test' mode was removed (docker-compose.test.yml no longer exists in geoid)."
        echo "Tests now run against the canonical docker-compose.yml via CI."
        echo "Downstream repos with their own docker-compose.test.yml should invoke"
        echo "  docker compose -f docker/docker-compose.test.yml ..."
        echo "directly."
        exit 2
        ;;
    local|prod|production|on-prem|onprem)
        echo "REFUSED: '$env' is a persistent-data composition."
        echo "This script only rebuilds dev. For $env, use 'docker compose' directly."
        exit 2
        ;;
    *)
        echo "Usage: $0 dev [--no-wipe]"
        echo "Refuses to run against local/prod/on-prem compositions (data preserved)."
        exit 2
        ;;
esac

if [[ -n "${COMPOSE_FILE:-}" ]]; then
    echo "Refusing to run: \$COMPOSE_FILE is set ($COMPOSE_FILE)."
    echo "Unset it — this script picks the compose file explicitly."
    exit 2
fi

# Resolve repo root: env override, else CWD if it has ${COMPOSE_SUBDIR}/docker-compose.<env>.yml.
COMPOSE_SUBDIR="${COMPOSE_SUBDIR:-src/dynastore/docker}"
if [[ -n "${REPO_ROOT:-}" ]]; then
    DIR="$REPO_ROOT"
elif [[ -f "${PWD}/${COMPOSE_SUBDIR}/docker-compose.${env}.yml" ]]; then
    DIR="$PWD"
else
    echo "Cannot locate repo root. Run from a repo containing ${COMPOSE_SUBDIR}/docker-compose.${env}.yml,"
    echo "or set REPO_ROOT=/path/to/repo."
    exit 1
fi

compose_file="${DIR}/${COMPOSE_SUBDIR}/docker-compose.${env}.yml"
project="${PROJECT_PREFIX:-$(basename "$DIR")}_${env}"

if [[ ! -f "$compose_file" ]]; then
    echo "Compose file not found: $compose_file"; exit 1
fi

cd "${DIR}/${COMPOSE_SUBDIR}"

echo "Environment: $env"
echo "Compose file: $compose_file"
echo "Project name: $project"
echo

if [[ "$flag" == "--no-wipe" ]]; then
    echo "=== [1/3] stop (keeping volumes) ==="
    docker compose -p "$project" -f "docker-compose.${env}.yml" down --remove-orphans
else
    echo "=== [1/3] stop + wipe volumes ==="
    docker compose -p "$project" -f "docker-compose.${env}.yml" down -v --remove-orphans
fi

echo "=== [2/3] build ==="
docker compose -p "$project" -f "docker-compose.${env}.yml" build

echo "=== [3/3] up -d ==="
docker compose -p "$project" -f "docker-compose.${env}.yml" up -d

echo
echo "Done. Tail logs with:"
echo "  docker compose -p $project -f docker-compose.${env}.yml logs -f"
