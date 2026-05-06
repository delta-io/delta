#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# From 2022-10-19
FROM ubuntu:focal-20221019@sha256:450e066588f42ebe1551f3b1a535034b6aa46cd936fe7f2c6b0d72997ec61dbd

ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true

# SECURITY: Temporal lockdown — refuse any package version published after this date.
ENV UV_EXCLUDE_NEWER="2026-02-10T00:00:00Z"

# Optional: pass --build-arg UV_INDEX_URL=https://your-proxy/simple/ to use a PyPI mirror
ARG UV_INDEX_URL
ENV UV_INDEX_URL=${UV_INDEX_URL}

RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    curl \
    wget \
    openjdk-8-jdk \
    python3.8 \
    python3-distutils \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv (SHA-verified; version+checksum defined in project/scripts/install-uv.sh)
COPY project/scripts/install-uv.sh /tmp/install-uv.sh
RUN bash /tmp/install-uv.sh && rm /tmp/install-uv.sh

# Create venv with Python 3.8 and install hash-verified locked dependencies
COPY docker-build-requirements.lock /tmp/requirements.lock
RUN uv venv /opt/venv --python python3.8 && \
    uv pip install --python /opt/venv/bin/python --require-hashes -r /tmp/requirements.lock && \
    rm /tmp/requirements.lock

ENV PATH="/opt/venv/bin:$PATH"
ENV VIRTUAL_ENV="/opt/venv"

# Do not add any non-deterministic changes (e.g., copy from files
# from repo) in this Dockerfile, so that the docker image
# generated from this can be reused across builds.
