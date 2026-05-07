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

# start-task.sh: Entrypoint for Cloud Run Jobs (one-off task execution)

set -e

# --- Default Configuration ---
: "${APP:=dynastore}"
: "${APP_DIR:=/${APP}}"
: "${LOG_LEVEL:=info}"
: "${VENV_PATH:=/opt/venv}"
# --- Durable Task Environment ---
: "${NAME:=$(hostname)}"
: "${RUNNER_ID:=$(hostname)}"

# --- Environment Loading ---
# Load env vars from baked .env file if it exists, respecting existing vars.
if [ "$IGNORE_BAKED_ENV" != "true" ] && [ -f "${APP_DIR}/env/.env" ]; then
    echo "Loading baked environment variables from ${APP_DIR}/env/.env..."
    while IFS='=' read -r key value || [ -n "$key" ]; do
        [[ "$key" =~ ^#.*$ ]] && continue
        [[ -z "$key" ]] && continue
        key="${key#export }"
        if [ -z "${!key}" ]; then
            export "$key=$value"
        fi
    done < "${APP_DIR}/env/.env"
fi

# --- Virtual Environment ---
if [ -f "${VENV_PATH}/bin/activate" ]; then
    source "${VENV_PATH}/bin/activate"
else
    echo "Warning: Virtual environment not found at ${VENV_PATH}"
fi

# --- Execute One-off Task ---
# Invoke as a module so we work whether the source is mounted at
# ${APP_DIR}/src/${APP} (geoid dev image COPYs src/) or installed only
# into site-packages (dynastore deployment image installs via pip + git ref).
echo "Starting one-off task (Cloud Run Job)..."
exec python -m "${APP}.main_task" "$@"
