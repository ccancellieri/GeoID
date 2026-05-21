# STAGE 1: Builder — extends the dynastore base image
# Replace the dynastore base image tag with the version you want to pin.
ARG DYNASTORE_IMAGE=ghcr.io/un-fao/dynastore:latest
FROM ${DYNASTORE_IMAGE} AS builder

ARG APP=my_project
ARG APP_DIR=/my_project
ARG VENV_PATH=/opt/venv

WORKDIR ${APP_DIR}

# Copy your project source
COPY pyproject.toml setup.py LICENSE README.md ./
COPY src ./src

# Install your project on top of the dynastore venv
RUN --mount=type=cache,target=/root/.cache/pip \
    ${VENV_PATH}/bin/pip install ".[my_project_modules]"

# The start.sh and start-task.sh scripts are already installed in
# /usr/local/bin/ from the dynastore base image — no need to copy them again.

EXPOSE 80

ENTRYPOINT ["/usr/local/bin/start.sh"]
CMD ["api"]

# STAGE 2: Runtime
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.13.0@sha256:83d114a6891ae18aa175d81a14c92a96d23944667b36fde9c15c4a10215d8939 AS runtime

ARG APP_USER=my_project
ARG APP_UID=1001
ARG APP_GID=1001
ARG APP_DIR=/my_project
ENV VENV_PATH=/opt/venv

RUN apt-get update && \
    apt-get install -y --no-install-recommends sudo curl && \
    rm -rf /var/lib/apt/lists/*

ENV APP_DIR=${APP_DIR}
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=${APP_DIR}/src
ENV PATH="${VENV_PATH}/bin:${HOME}/.local/bin:$PATH"
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN groupadd --gid $APP_GID $APP_USER && \
    useradd --uid $APP_UID --gid $APP_GID --create-home --shell /bin/bash $APP_USER

WORKDIR ${APP_DIR}

COPY --from=builder ${VENV_PATH} ${VENV_PATH}
COPY --from=builder ${APP_DIR} ${APP_DIR}
COPY --from=builder /usr/local/bin/start.sh /usr/local/bin/start.sh
COPY --from=builder /usr/local/bin/start-task.sh /usr/local/bin/start-task.sh

RUN chown -R $APP_USER:$APP_USER ${APP_DIR} ${VENV_PATH}

USER $APP_USER

ENTRYPOINT ["/usr/local/bin/start.sh"]
CMD ["api"]
