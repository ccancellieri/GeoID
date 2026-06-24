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
        echo "Starting Worker..."
        # Expose a minimal HTTP probe for Cloud Run liveness checks.
        # The probe returns 200 only while the worker process is alive.
        #
        # Implementation note: `exec` below replaces this shell with the Python
        # process, so the shell PID ($$) becomes the worker PID.  We write it
        # to a well-known file before exec so the probe server can verify the
        # process is still running via os.kill(pid, 0).
        if [ -n "$TCP_PORT" ]; then
            _PROBE_PID_FILE="/tmp/dynastore-worker.pid"
            echo "$$" > "$_PROBE_PID_FILE"
            python - "$TCP_PORT" "$_PROBE_PID_FILE" &>/dev/null <<'PROBE_EOF' &
import http.server
import os
import sys


class _WorkerProbeHandler(http.server.BaseHTTPRequestHandler):
    pid_file = ""

    def do_GET(self):
        alive = False
        try:
            pid = int(open(self.pid_file).read().strip())
            os.kill(pid, 0)
            alive = True
        except Exception:
            pass
        code = 200 if alive else 503
        self.send_response(code)
        self.end_headers()
        self.wfile.write(b"ok\n" if alive else b"worker dead\n")

    def log_message(self, *args):
        pass  # suppress access log noise


if __name__ == "__main__":
    port = int(sys.argv[1])
    _WorkerProbeHandler.pid_file = sys.argv[2]
    server = http.server.HTTPServer(("0.0.0.0", port), _WorkerProbeHandler)
    server.serve_forever()
PROBE_EOF
        fi

        exec python -m "${APP}.main" --worker "$@"
        ;;

    *)
        echo "Usage: $0 {api|worker} [args...]"
        echo "Executing custom command: $MODE $*"
        exec "$MODE" "$@"
        ;;
esac
