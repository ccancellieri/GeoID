# STAGE 0: JupyterLite Builder — pre-builds browser-based Python kernel (Pyodide)
# Skipped cheaply when BUILD_JUPYTERLITE=false (Cloud Run jobs/workers that
# never serve /web/pages/notebooks).
FROM python:3.12-slim AS jupyterlite-builder

ARG BUILD_JUPYTERLITE=false

WORKDIR /build

RUN if [ "${BUILD_JUPYTERLITE}" = "true" ]; then \
        pip install --no-cache-dir jupyterlite-core jupyterlite-pyodide-kernel; \
    else \
        echo "JupyterLite builder: skipped (BUILD_JUPYTERLITE=false)"; \
    fi

COPY src/dynastore/extensions/notebooks/jupyterlite/ ./lite-config/

# Pre-download piplite wheels so the built site is fully offline-capable
# (on-prem docker compose / air-gapped Cloud Run). `pip download` emits pure-Python
# wheels for the requirements — skipped when BUILD_JUPYTERLITE=false.
RUN mkdir -p ./lite-output ./lite-config/wheels && \
    if [ "${BUILD_JUPYTERLITE}" = "true" ] && [ -s ./lite-config/requirements.txt ]; then \
        pip download --no-deps --only-binary=:all: --platform=pyodide_2024_0_wasm32 \
            --dest ./lite-config/wheels -r ./lite-config/requirements.txt \
        || pip download --no-deps --only-binary=:all: \
            --dest ./lite-config/wheels -r ./lite-config/requirements.txt \
        || echo "JupyterLite: wheel pre-download skipped (packages will fall back to piplite CDN)"; \
    fi

RUN if [ "${BUILD_JUPYTERLITE}" = "true" ]; then \
        WHEEL_ARGS="" && \
        for w in /build/lite-config/wheels/*.whl; do \
            [ -e "$w" ] && WHEEL_ARGS="$WHEEL_ARGS --piplite-wheels=$w"; \
        done && \
        jupyter lite build --lite-dir /build/lite-config --output-dir /build/lite-output $WHEEL_ARGS; \
    else \
        echo "JupyterLite: stage noop"; \
    fi

# STAGE 1: Builder (Production dependencies)
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.12.3 AS builder

LABEL maintainer="carlo.cancellieri@fao.org"

ARG APP_DIR=/dynastore
ARG VENV_PATH=/opt/venv

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git build-essential libpq-dev python3-full python3-pip \
    python3.12-venv python3.12-dev sudo curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR ${APP_DIR}

# Virtual Env Setup
RUN python3.12 -m venv ${VENV_PATH}
ENV PATH="${VENV_PATH}/bin:$PATH"
ENV PYTHONPATH=${APP_DIR}/src

# Build-time commit hash for version info (no .git in runtime image)
ARG BUILD_COMMIT=unknown
ENV BUILD_COMMIT=${BUILD_COMMIT}

# Build-time version (resolved by CI from `git describe --tags` or by the
# downstream wrapper from a pinned tag). setuptools-scm has no .git/ here,
# so we hand it the version explicitly via SETUPTOOLS_SCM_PRETEND_VERSION.
# Empty / unset → setuptools-scm falls back to "0.0.0+unknown" (configured
# in pyproject.toml), which is fine for ad-hoc local builds.
ARG APP_VERSION=

# Install Production Dependencies — split into two layers so source-only
# commits don't bust the (~3 min) deps-install cache.
#
# Layer 1: project metadata. The pip-install layer below caches on
# pyproject.toml + the stubbed src tree alone; source-only commits hit
# the cache and skip the heavy GDAL / pyiceberg / etc. install.
COPY pyproject.toml LICENSE README.md requirements*.txt pytest.ini* setup.cfg* .coveragerc* ./

# Copy .env file if it exists (CI creates it in root, local has it in env/)
# We use pyproject.toml as a sentinel to prevent COPY from failing if .env is missing.
RUN mkdir -p ./env
COPY pyproject.toml .en[v]* ./env/
# Also copy to root and src to be safe for discovery logic
COPY pyproject.toml .en[v]* ./
COPY pyproject.toml .en[v]* ./src/

# Layer 2: stub the dynastore package so setuptools can build a wheel
# without bundling real source. Layer 3 replaces the stub with real files
# and rebuilds the wheel --no-deps, picking up the real APP_VERSION.
RUN mkdir -p src/dynastore/scripts && \
    : > src/dynastore/__init__.py && \
    : > src/dynastore/scripts/__init__.py && \
    : > src/dynastore/scripts/start.sh && \
    : > src/dynastore/scripts/start-task.sh

# SCOPE selects which pyproject.toml extras to install.
# Set per-service via docker-compose.yml build args (from .env) or CI.
ARG SCOPE=all
# Set BUILD_JUPYTERLITE=false for services that do not include the notebooks scope (e.g. worker).
ARG BUILD_JUPYTERLITE=false
# Use buildkit cache for pip
#
# fiona ships a manylinux wheel with its own bundled libgdal — that bundled
# build does NOT include the Arrow/Parquet driver.  Reading GeoParquet over
# /vsigs/ from the ingestion task therefore failed with
# ``CPLE_OpenFailedError: not recognized as being in a supported file format``
# even though the system GDAL from ``ghcr.io/osgeo/gdal:ubuntu-full-3.12.3``
# has the Parquet driver (the maps service confirms via /maps/gdal/vector/info
# → driverShortName='Parquet').  Force fiona to rebuild from sdist against
# the system libgdal so it inherits every driver the base image ships.
RUN --mount=type=cache,target=/root/.cache/pip \
    SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DYNASTORE=0.0.0 \
    ${VENV_PATH}/bin/pip install --upgrade pip setuptools setuptools-scm wheel && \
    SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DYNASTORE=0.0.0 \
    ${VENV_PATH}/bin/pip install --no-binary fiona ".[${SCOPE}]" uvicorn gunicorn

# Layer 3: real source overlay. Fast layer — busts when src/, tests/, or
# APP_VERSION change. Re-installs dynastore --no-deps so the wheel metadata
# (and therefore importlib.metadata.version("DynaStore")) reflects the real
# version baked in via SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DYNASTORE.
COPY src ./src
COPY tests ./tests
RUN --mount=type=cache,target=/root/.cache/pip \
    SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DYNASTORE="${APP_VERSION:-0.0.0+unknown}" \
    ${VENV_PATH}/bin/pip install --no-deps --force-reinstall --no-build-isolation .

# Startup scripts ship as package data under dynastore/scripts/ (SSOT).
# Read them from the local source tree (Layer 3 has overlaid the real files);
# the deployment-wrapper Dockerfile reads the equivalent files out of
# ${VENV_PATH}/lib/.../site-packages/dynastore/scripts/. Both reach identical
# canonical content; only the dispatch path differs.
RUN install -m 755 /dynastore/src/dynastore/scripts/start.sh /usr/local/bin/start.sh && \
    install -m 755 /dynastore/src/dynastore/scripts/start-task.sh /usr/local/bin/start-task.sh

# Overlay JupyterLite assets when scope includes notebooks; bridge.js (committed) is preserved.
# After copy, inject bridge.js into lab/index.html so the host can postMessage
# LOAD_NOTEBOOK / INIT_CONTEXT into the iframe.
# Install JupyterLite assets into /opt/jupyterlite (outside ${APP_DIR}/src so the
# dev hot-reload bind mount does not mask them). The notebooks extension reads
# JUPYTERLITE_DIR and serves this path via @expose_static("lite"). bridge.js is
# copied from the committed source tree alongside lab/, then injected into
# lab/index.html as <script src="../bridge.js"> so the host can postMessage
# LOAD_NOTEBOOK / INIT_CONTEXT into the iframe.
ENV JUPYTERLITE_DIR=/opt/jupyterlite
# Ensure the dir exists unconditionally so the runtime-stage COPY --from=builder
# does not fail when BUILD_JUPYTERLITE=false (worker / job images).
RUN mkdir -p ${JUPYTERLITE_DIR}
RUN --mount=type=bind,from=jupyterlite-builder,source=/build/lite-output,target=/tmp/lite-output \
    if [ "${BUILD_JUPYTERLITE}" = "true" ]; then \
        cp -r /tmp/lite-output/. "${JUPYTERLITE_DIR}/" && \
        cp "${APP_DIR}/src/dynastore/extensions/notebooks/static/lite/bridge.js" "${JUPYTERLITE_DIR}/bridge.js" && \
        for sub in lab notebooks tree edit; do \
            if [ -f "${JUPYTERLITE_DIR}/${sub}/index.html" ]; then \
                if ! grep -q "bridge.js" "${JUPYTERLITE_DIR}/${sub}/index.html"; then \
                    sed -i 's#</head>#<script src="../bridge.js"></script></head>#' "${JUPYTERLITE_DIR}/${sub}/index.html"; \
                fi; \
                python3 -c "import re,sys; p='${JUPYTERLITE_DIR}/${sub}/index.html'; s=open(p).read(); new=re.sub(r'(id=\"jupyter-config-data\"[^>]*>)\s*\{\s*\}\s*(<)', r'\1{\"exposeAppInBrowser\": \"true\"}\2', s); open(p,'w').write(new) if new!=s else None"; \
            fi; \
        done && \
        echo "JupyterLite: assets installed at ${JUPYTERLITE_DIR}"; \
    else \
        echo "JupyterLite: skipped (BUILD_JUPYTERLITE=false)"; \
    fi

EXPOSE 80

# Default: start as API service. Override CMD to "worker" for worker services.
ENTRYPOINT ["/usr/local/bin/start.sh"]
CMD ["api"]

# STAGE 2: Tester (Inherit from builder and add test/dev deps)
FROM builder AS tester

ARG APP_DIR=/dynastore
WORKDIR ${APP_DIR}

ARG SCOPE=test,all
RUN --mount=type=cache,target=/root/.cache/pip \
    ${VENV_PATH}/bin/pip install ".[${SCOPE},dev]"

# STAGE 3: Development (Specific for local development with debugging)
FROM tester AS development

ARG APP_DIR=/dynastore
WORKDIR ${APP_DIR}

# debugpy is already installed via .[dev] in tester stage
EXPOSE 5678 5679

# STAGE 4: Runtime (Production image)
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.12.3 AS runtime

ARG APP_USER=dynastore
ARG APP_UID=1001
ARG APP_GID=1001
ARG APP_DIR=/dynastore
ENV VENV_PATH=/opt/venv

# Install runtime dependencies (curl for healthchecks)
RUN apt-get update && \
    apt-get install -y --no-install-recommends sudo curl && \
    rm -rf /var/lib/apt/lists/*

# Environment setup
ENV APP_DIR=${APP_DIR}
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=${APP_DIR}/src
ENV PATH="${VENV_PATH}/bin:${HOME}/.local/bin:$PATH"
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# User Creation
RUN groupadd --gid $APP_GID $APP_USER && \
    useradd --uid $APP_UID --gid $APP_GID --create-home --shell /bin/bash $APP_USER

WORKDIR ${APP_DIR}

# Copy venv and source from builder (PROD ONLY)
COPY --from=builder ${VENV_PATH} ${VENV_PATH}
COPY --from=builder ${APP_DIR} ${APP_DIR}
# JupyterLite assets live outside APP_DIR so the dev hot-reload mount cannot
# mask them; missing in worker/job images where BUILD_JUPYTERLITE=false.
ENV JUPYTERLITE_DIR=/opt/jupyterlite
COPY --from=builder /opt/ /opt/
# Scripts are already in /usr/local/bin from builder stage if we inherit,
# but runtime inherits from gdal, so we still need to copy them from builder.
COPY --from=builder /usr/local/bin/start.sh /usr/local/bin/start.sh
COPY --from=builder /usr/local/bin/start-task.sh /usr/local/bin/start-task.sh

# Ownership
RUN chown -R $APP_USER:$APP_USER ${APP_DIR} ${VENV_PATH}

USER $APP_USER

# Default: start as API service. Override CMD to "worker" for worker services.
# For Cloud Run Jobs, use start-task.sh as the entrypoint in the job spec.
ENTRYPOINT ["/usr/local/bin/start.sh"]
CMD ["api"]