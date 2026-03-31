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

# Install uv (SHA-verified, statically-linked musl build for portability)
RUN UV_VERSION="0.10.1" && \
    UV_SHA256="d1a3b08dd9abf9e500541cadd0e2f4b144c99b9265fb00e500c2b5c82a3b4ee8" && \
    curl -fsSL -o uv.tar.gz \
      "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-x86_64-unknown-linux-musl.tar.gz" && \
    echo "${UV_SHA256}  uv.tar.gz" | sha256sum -c - && \
    tar -xzf uv.tar.gz && \
    install -m 755 uv-x86_64-unknown-linux-musl/uv /usr/local/bin/uv && \
    install -m 755 uv-x86_64-unknown-linux-musl/uvx /usr/local/bin/uvx && \
    rm -rf uv-x86_64-unknown-linux-musl uv.tar.gz

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
