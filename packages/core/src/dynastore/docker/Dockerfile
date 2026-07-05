# GDAL version is pinned in 6 places that MUST stay in lockstep:
#   1-2. this Dockerfile — two `FROM ghcr.io/osgeo/gdal:ubuntu-full-<X>` stages
#   3.   pyproject.toml (root) — `module_gdal` extra `GDAL==<X>`
#   4.   pyproject.toml (root) — `extension_maps` extra `GDAL==<X>`
#   5-6. packages/extensions/gdal/pyproject.toml + packages/extensions/maps/pyproject.toml — `GDAL==<X>`
# Bump all 6 together; CI guards drift via scripts/check_gdal_pin_consistency.py.
#
# The base image is pinned by DIGEST, not just by the ubuntu-full-<X> tag.
# osgeo rebuilds those tags in place onto newer Ubuntu releases, so the digest
# freezes the exact OS/Python/GDAL triple regardless of later tag rebases.
# Current digest resolves `ubuntu-full-3.13.1` = GDAL 3.13.1 on Ubuntu 26.04 /
# Python 3.14. The full geospatial+columnar stack now ships cp314 wheels
# (numpy 2, pyarrow 24, shapely, rasterio, pyproj; fiona was retired in favour
# of pyogrio) so the build no longer falls back to compiling from source.
# GDAL 3.13.0 carries the CVE-2026-8087/8088 fixes (no 3.11/3.12 backport);
# 3.13.1 is a bugfix release closing the netCDF driver
# scanForGeometryContainers stack-overflow flagged against 3.13.0.

# STAGE 0: JupyterLite Builder — pre-builds browser-based Python kernel (Pyodide)
# Skipped cheaply when BUILD_JUPYTERLITE=false (Cloud Run jobs/workers that
# never serve /web/pages/notebooks).
FROM python:3.13-slim AS jupyterlite-builder

ARG BUILD_JUPYTERLITE=false

WORKDIR /build

RUN if [ "${BUILD_JUPYTERLITE}" = "true" ]; then \
        pip install --no-cache-dir jupyterlite-core jupyterlite-pyodide-kernel; \
    else \
        echo "JupyterLite builder: skipped (BUILD_JUPYTERLITE=false)"; \
    fi

COPY packages/extensions/notebooks/src/dynastore/extensions/notebooks/jupyterlite/ ./lite-config/

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
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.13.1@sha256:aff1d5515aa0e9b50be34ab11d6c0c2cfabc23cdcb7a2e0bc5748101eedb3e4a AS builder

LABEL maintainer="carlo.cancellieri@fao.org"

ARG APP_DIR=/dynastore
ARG VENV_PATH=/opt/venv

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git build-essential libpq-dev python3-full python3-pip \
    python3-venv python3-dev sudo curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR ${APP_DIR}

# Virtual Env Setup
RUN python3 -m venv ${VENV_PATH}
ENV PATH="${VENV_PATH}/bin:$PATH"
# No PYTHONPATH — `dynastore` and `dynastore.extensions` are PEP 420 namespace
# packages, contributed to by dynastore-core (modules, tools, models, tasks)
# AND every dynastore-ext-* wheel (one extension subpackage each). Pointing
# PYTHONPATH at any single source dir would shadow site-packages and break
# the namespace merge — Python's `dynastore.__path__` would only see the
# source tree, missing the ext wheels in site-packages. Letting pip install
# all wheels into the same site-packages directory is the correct invariant.

# Install uv for workspace-aware package resolution (pip cannot resolve
# workspace-local packages declared in [tool.uv.sources]).
RUN ${VENV_PATH}/bin/pip install uv

# Build-time commit hash for version info (no .git in runtime image)
ARG BUILD_COMMIT=unknown
ENV BUILD_COMMIT=${BUILD_COMMIT}

# Build-time timestamp for version info (no .git in runtime image; without
# this the /health endpoint would fall back to reporting request time as if
# it were the build time)
ARG BUILD_TIME=
ENV BUILD_TIME=${BUILD_TIME}

# Build-time version (resolved by CI from `git describe --tags` or by the
# downstream wrapper from a pinned tag). setuptools-scm has no .git/ here,
# so we hand it the version explicitly via SETUPTOOLS_SCM_PRETEND_VERSION.
# Empty / unset → setuptools-scm falls back to "0.0.0+unknown" (configured
# in pyproject.toml), which is fine for ad-hoc local builds.
ARG APP_VERSION=

# Install Production Dependencies — split into two layers so source-only
# commits don't bust the (~3 min) deps-install cache.
#
# Layer 1: project metadata + workspace packages stub tree. The deps-install
# layer below caches on pyproject.toml + the stubbed packages tree alone;
# source-only commits hit the cache and skip the heavy GDAL / pyiceberg /
# etc. install. Real source lands in Layer 3.
COPY pyproject.toml LICENSE README.md requirements*.txt pytest.ini* setup.cfg* .coveragerc* ./
# Workspace members — pyproject.toml files only, so uv can read them and
# resolve workspace sources. Source files come in Layer 3 (cache-bust on src
# change is fine; a fresh pyproject.toml authorship is rare).
COPY packages/core/pyproject.toml ./packages/core/pyproject.toml
COPY packages/extensions/ ./packages/extensions-meta/
RUN mkdir -p ./packages/extensions && \
    for d in ./packages/extensions-meta/*/; do \
        name=$(basename "$d"); \
        mkdir -p "./packages/extensions/${name}"; \
        cp "${d}pyproject.toml" "./packages/extensions/${name}/pyproject.toml"; \
    done && rm -rf ./packages/extensions-meta
# Module wheels (packages/modules/*) are workspace members too, so uv must see
# their pyproject.toml at resolve time — otherwise `.[${SCOPE}]` chains that pull
# a module extra (e.g. module_datastore via the db_sync/worker chain) fail with
# "references a workspace in tool.uv.sources but is not a workspace member".
# Unlike extensions, a module wheel carries NO source of its own: the impl lives
# in dynastore-core under dynastore/modules/<name>/ (a regular package owned by
# core), and the module wheel is an entry-point-only stub (packages = []). So we
# stage ONLY the pyproject.toml — and deliberately create no src stub, since the
# wheel declares no packages to build. Source arrives with `COPY packages` in
# Layer 3; keeping just the pyproject here preserves the deps-install cache.
COPY packages/modules/ ./packages/modules-meta/
RUN mkdir -p ./packages/modules && \
    for d in ./packages/modules-meta/*/; do \
        name=$(basename "$d"); \
        mkdir -p "./packages/modules/${name}"; \
        cp "${d}pyproject.toml" "./packages/modules/${name}/pyproject.toml"; \
    done && rm -rf ./packages/modules-meta

# Copy .env file if it exists (CI creates it in root, local has it in env/)
# We use pyproject.toml as a sentinel to prevent COPY from failing if .env is missing.
RUN mkdir -p ./env
COPY pyproject.toml .en[v]* ./env/
COPY pyproject.toml .en[v]* ./

# Layer 2 prep: stub every workspace member so uv pip can build wheel
# metadata without real source (real source lands in Layer 3 and triggers
# --force-reinstall there). Each wheel needs at least its declared package
# directory to exist.
#
# IMPORTANT: do NOT create `dynastore/__init__.py` or
# `dynastore/extensions/__init__.py` — those are PEP 420 namespace packages
# (multiple wheels contribute subpackages). Stubbing them with regular
# `__init__.py` files would make Python treat the namespace as a regular
# package, blocking the merge with ext-wheel subdirs at runtime. The leaf
# subpackages (e.g. `dynastore.extensions.web`, `dynastore.scripts`) do
# need `__init__.py` since they're regular packages owned by exactly one
# wheel.
RUN mkdir -p packages/core/src/dynastore/extensions packages/core/src/dynastore/scripts && \
    : > packages/core/src/dynastore/scripts/__init__.py && \
    : > packages/core/src/dynastore/scripts/start.sh && \
    : > packages/core/src/dynastore/scripts/start-task.sh && \
    for d in packages/extensions/*/; do \
        name=$(basename "$d"); \
        mkdir -p "${d}src/dynastore/extensions/${name}" && \
        : > "${d}src/dynastore/extensions/${name}/__init__.py"; \
    done

# SCOPE selects which pyproject.toml extras to install.
# Set per-service via docker-compose.yml build args (from .env) or CI.
ARG SCOPE=all
# Set BUILD_JUPYTERLITE=false for services that do not include the notebooks scope (e.g. worker).
ARG BUILD_JUPYTERLITE=false
# Use buildkit cache for pip
#
# pyogrio ships a manylinux wheel with its own bundled libgdal. Two problems
# with the bundled build: (1) it may lag the system GDAL and omit drivers the
# base image carries, and (2) loading pyogrio's libgdal alongside the osgeo
# bindings' system libgdal puts two libgdal copies in one process — a known
# crash hazard in the ingestion worker, which imports both the osgeo reader
# and PyogrioReader. Force pyogrio to rebuild from sdist against the system
# libgdal (the ubuntu-full image ships GDAL dev headers) so it links the one
# system libgdal — inheriting every driver and eliminating the dual-libgdal
# conflict. (Previously applied to fiona, retired in favour of pyogrio.)
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 \
    ${VENV_PATH}/bin/pip install --upgrade pip setuptools setuptools-scm wheel uv && \
    SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 \
    ${VENV_PATH}/bin/uv pip install --no-binary pyogrio ".[${SCOPE}]" uvicorn gunicorn

# Layer 3: real source overlay. Fast layer — busts when packages/ or
# APP_VERSION change. Re-installs dynastore-core + every workspace ext wheel
# --no-deps so the wheel metadata (and therefore
# importlib.metadata.version("dynastore-core") / version("dynastore-ext-*"))
# reflects the real version baked in via SETUPTOOLS_SCM_PRETEND_VERSION.
# Layer 2 used "0.0.0" as a cache-stable placeholder; this layer overwrites
# the wheel metadata with APP_VERSION (CI passes the real git-describe value).
#
# Critical: uv installs workspace members editable by default (`.pth` file
# in site-packages pointing at the source dir). Editable wins in sys.path,
# so `dynastore.__path__` would only see the source tree and miss the
# ext-wheel subdirectories under site-packages. We explicitly uninstall the
# editable install first, then reinstall as a regular wheel — this puts
# dynastore-core's source physically in site-packages where it shares one
# directory with every dynastore-ext-* sub-package and the namespace
# `dynastore.extensions` resolves correctly across all wheels.
#
# SCOPE invariant: the ext-wheel reinstall loop honors whatever Layer 2
# selected via `.[${SCOPE}]`. We list the dynastore-ext-* distributions pip
# already installed and only refresh those — re-installing every directory
# under packages/extensions/ here would silently inject wheels that the
# SCOPE chain explicitly excludes (e.g. ext-iam onto scope_catalog), making
# entry-point discovery wire up middleware (PermissionProtocol) on services
# that should not gate.
COPY packages ./packages
COPY tests ./tests
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    ${VENV_PATH}/bin/pip uninstall -y dynastore-core DynaStore 2>/dev/null || true && \
    ${VENV_PATH}/bin/pip uninstall -y dynastore-core DynaStore 2>/dev/null || true && \
    rm -f ${VENV_PATH}/lib/python*/site-packages/__editable__.dynastore_core-*.pth && \
    rm -rf ${VENV_PATH}/lib/python*/site-packages/dynastore_core-0.0.0.dist-info && \
    SETUPTOOLS_SCM_PRETEND_VERSION="${APP_VERSION:-0.0.0+unknown}" \
    ${VENV_PATH}/bin/pip install --no-deps --force-reinstall --no-build-isolation packages/core && \
    installed_exts=$(${VENV_PATH}/bin/pip list --format=freeze 2>/dev/null \
        | awk -F'==' '/^dynastore-ext-/ {print $1}') && \
    echo "Layer 3: SCOPE-installed extensions to refresh:" && echo "$installed_exts" && \
    for pkg in packages/extensions/*/; do \
        dist=$(${VENV_PATH}/bin/python -c "import tomllib,sys;print(tomllib.load(open(sys.argv[1],'rb'))['project']['name'])" "$pkg/pyproject.toml" 2>/dev/null) || \
            { echo "ERROR: cannot read project name from $pkg/pyproject.toml"; exit 1; }; \
        if ! echo "$installed_exts" | grep -qx "$dist"; then \
            echo "Layer 3: SCOPE skip $pkg (dist '$dist' not selected by .[${SCOPE}])"; \
            continue; \
        fi; \
        SETUPTOOLS_SCM_PRETEND_VERSION="${APP_VERSION:-0.0.0+unknown}" \
        ${VENV_PATH}/bin/pip install --no-deps --force-reinstall --no-build-isolation "$pkg" \
            || { echo "ERROR: re-install failed for $pkg"; exit 1; }; \
    done && \
    SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DYNASTORE="${APP_VERSION:-0.0.0+unknown}" \
    ${VENV_PATH}/bin/pip install --no-deps --force-reinstall --no-build-isolation .

# SCOPE manifest: snapshot the exact dynastore-ext-* set this SCOPE resolved to
# (the clean, pruned builder set). The tester stage re-runs `uv pip install`,
# which re-pulls workspace members the SCOPE excluded; it reads this manifest to
# prune them back out so the dev/test image matches the service SCOPE instead of
# silently activating excluded middleware (e.g. ext-iam on scope_catalog).
RUN ${VENV_PATH}/bin/pip list 2>/dev/null \
        | awk '/^dynastore-ext-/ {print $1}' | sort > ${VENV_PATH}/scope-exts.txt && \
    echo "Builder SCOPE manifest ($(wc -l < ${VENV_PATH}/scope-exts.txt) exts):" && \
    cat ${VENV_PATH}/scope-exts.txt

# Startup scripts ship as package data under dynastore/scripts/ (SSOT).
# Read them from the local source tree (Layer 3 has overlaid the real files);
# the deployment-wrapper Dockerfile reads the equivalent files out of
# ${VENV_PATH}/lib/.../site-packages/dynastore/scripts/. Both reach identical
# canonical content; only the dispatch path differs.
RUN install -m 755 /dynastore/packages/core/src/dynastore/scripts/start.sh /usr/local/bin/start.sh && \
    install -m 755 /dynastore/packages/core/src/dynastore/scripts/start-task.sh /usr/local/bin/start-task.sh

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
        cp "${APP_DIR}/packages/extensions/notebooks/src/dynastore/extensions/notebooks/static/lite/bridge.js" "${JUPYTERLITE_DIR}/bridge.js" && \
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
ARG APP_VERSION=
# `uv pip install ".[…,dev]"` re-pulls workspace members the SCOPE excluded
# (the in-scope set becomes editable; excluded exts land as plain wheels). The
# builder's Layer 3 already guards its own venv; replay that guard here using the
# builder SCOPE manifest so the dev/test image carries the same ext set as the
# service it represents. SCOPE=all (CI test image) prunes nothing.
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked \
    SETUPTOOLS_SCM_PRETEND_VERSION="${APP_VERSION:-0.0.0+unknown}" \
    ${VENV_PATH}/bin/uv pip install ".[${SCOPE},dev]" && \
    keep=$(cat ${VENV_PATH}/scope-exts.txt) && \
    for pkg in $(${VENV_PATH}/bin/pip list 2>/dev/null | awk '/^dynastore-ext-/ {print $1}'); do \
        if ! echo "$keep" | grep -qx "$pkg"; then \
            echo "tester: SCOPE-prune out-of-scope ext '$pkg' (not in builder SCOPE manifest)"; \
            ${VENV_PATH}/bin/pip uninstall -y "$pkg" >/dev/null 2>&1 || true; \
        fi; \
    done && \
    echo "tester: ext set after prune ($(${VENV_PATH}/bin/pip list 2>/dev/null | grep -c '^dynastore-ext-') exts)"

# STAGE 3: Development (Specific for local development with debugging)
FROM tester AS development

ARG APP_DIR=/dynastore
WORKDIR ${APP_DIR}

# debugpy is already installed via .[dev] in tester stage
EXPOSE 5678 5679

# STAGE 4: Runtime (Production image)
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.13.1@sha256:aff1d5515aa0e9b50be34ab11d6c0c2cfabc23cdcb7a2e0bc5748101eedb3e4a AS runtime

ARG APP_USER=dynastore
ARG APP_UID=1001
ARG APP_GID=1001
ARG APP_DIR=/dynastore
ENV VENV_PATH=/opt/venv

# Re-declared here (also set in the builder stage): this stage is a fresh
# `FROM`, not `FROM builder`, so ENV values from the builder stage do not
# carry over — only COPY --from brings filesystem content across stages.
ARG BUILD_COMMIT=unknown
ENV BUILD_COMMIT=${BUILD_COMMIT}
ARG BUILD_TIME=
ENV BUILD_TIME=${BUILD_TIME}

# Install runtime dependencies (curl for healthchecks)
RUN apt-get update && \
    apt-get install -y --no-install-recommends sudo curl && \
    rm -rf /var/lib/apt/lists/*

# Environment setup
ENV APP_DIR=${APP_DIR}
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Runtime: no PYTHONPATH — wheels are installed into site-packages and
# `dynastore`/`dynastore.extensions` are PEP 420 namespace packages.
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

# Base on-prem defaults layer — consumed by config_seeder.py on first boot.
# Files under docker/config/defaults/ supply initial values for PluginConfig
# classes (task_routing_config, oidc_role_sync_config, …) when no DB row
# exists yet (override=false).  On-prem installs that run the geoid image
# directly need no deployment wrapper to bootstrap sensible defaults.
#
# dynastore's per-environment Dockerfile overlays its env-specific folder
# on top of this base so the seeder finds a single merged defaults/ path.
# The overlay pattern and documented delta live in dynastore/docker/Dockerfile.
COPY packages/core/src/dynastore/docker/config/defaults/ ${APP_DIR}/config/defaults/

# Ownership
RUN chown -R $APP_USER:$APP_USER ${APP_DIR} ${VENV_PATH}

USER $APP_USER

# Default: start as API service. Override CMD to "worker" for worker services.
# For Cloud Run Jobs, use start-task.sh as the entrypoint in the job spec.
ENTRYPOINT ["/usr/local/bin/start.sh"]
CMD ["api"]