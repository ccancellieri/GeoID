#!/bin/bash
#
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

# start.sh: Entrypoint for Cloud Run Service (API or Worker mode).
# The same application image is used for both API and Worker.
# The MODE is controlled by the CMD passed to the container (api|worker).

set -e

# --- Default Configuration ---
: "${APP:=dynastore}"
: "${APP_DIR:=/${APP}}"
: "${GUNICORN_WORKERS:=4}"
: "${GUNICORN_THREADS:=1}"
: "${GUNICORN_TIMEOUT:=30}"
: "${KEEP_ALIVE:=5}"
: "${LOG_LEVEL:=info}"
: "${ACCESS_LOG:=-}"
: "${ERROR_LOG:=-}"
: "${VENV_PATH:=/opt/venv}"
# Worker concurrency: number of parallel worker processes for the Procrastinate worker.
# Multiple processes prevent deadlocks when tasks write to shared resources (e.g. static files).
: "${WORKER_CONCURRENCY:=4}"
# --- Durable Task Environment ---
: "${RUNNER_ID:=$(hostname)}"
: "${NAME:=$(hostname)}"
: "${MAX_RETRIES:=3}"

# --- Environment Loading ---
# Load env vars from baked .env file if it exists, respecting existing vars.
# The .env file uses shell-safe quoting (shlex.quote), so we use eval to let
# bash handle quote removal natively instead of manual pattern-matching.
if [ "$IGNORE_BAKED_ENV" != "true" ] && [ -f "${APP_DIR}/env/.env" ]; then
    echo "Loading baked environment variables from ${APP_DIR}/env/.env..."
    while IFS= read -r line || [ -n "$line" ]; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ -z "${line//[[:space:]]/}" ]] && continue
        line="${line#export }"
        key="${line%%=*}"
        if [[ "$key" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]] && [ -z "${!key}" ]; then
            eval "export ${line}"
        fi
    done < "${APP_DIR}/env/.env"
fi

# --- Virtual Environment ---
if [ -f "${VENV_PATH}/bin/activate" ]; then
    source "${VENV_PATH}/bin/activate"
else
    echo "Warning: Virtual environment not found at ${VENV_PATH}"
fi

# --- Mode Selection ---
MODE="${1:-api}"
shift || true

case "$MODE" in
    api)
        echo "Starting API (Gunicorn/Uvicorn, ${GUNICORN_WORKERS} workers)..."

        if [ "$ACCESS_LOG" != "-" ] && [ -n "$ACCESS_LOG" ]; then
            mkdir -p "$(dirname "$ACCESS_LOG")"
        fi
        if [ "$ERROR_LOG" != "-" ] && [ -n "$ERROR_LOG" ]; then
            mkdir -p "$(dirname "$ERROR_LOG")"
        fi

        exec gunicorn \
          --worker-class "uvicorn.workers.UvicornWorker" \
          "${APP}.main:app" \
          --bind "0.0.0.0:${PORT:-${TCP_PORT:-80}}" \
          --workers "${GUNICORN_WORKERS}" \
          --threads "${GUNICORN_THREADS}" \
          --timeout "${GUNICORN_TIMEOUT}" \
          --keep-alive "${KEEP_ALIVE}" \
          --log-level "${LOG_LEVEL}" \
          --access-logfile "${ACCESS_LOG}" \
          --error-logfile "${ERROR_LOG}" \
          "$@"
        ;;

    worker)
        echo "Starting Worker (${WORKER_CONCURRENCY} concurrent processes)..."
        # Optionally expose a minimal HTTP health check endpoint for Cloud Run liveness probes.
        if [ -n "$TCP_PORT" ]; then
            python -m http.server "$TCP_PORT" &>/dev/null &
        fi

        # Run with --concurrency to spawn multiple worker processes.
        # This prevents deadlocks when tasks write to shared resources (e.g. static files)
        # while other tasks are waiting for I/O.
        exec python -m "${APP}.main" --worker --concurrency "${WORKER_CONCURRENCY}" "$@"
        ;;

    *)
        echo "Usage: $0 {api|worker} [args...]"
        echo "Executing custom command: $MODE $*"
        exec "$MODE" "$@"
        ;;
esac
