#!/bin/sh
# Install uv — SHA256-verified, statically-linked musl build.
# Single source of truth for version + checksum across CI workflows and Docker.
#
# Usage:
#   bash project/scripts/install-uv.sh          # from repo root (CI)
#   COPY project/scripts/install-uv.sh /tmp/    # in Dockerfile
#   RUN bash /tmp/install-uv.sh
set -eu

UV_VERSION="0.10.1"
# SHA from official release asset:
# https://github.com/astral-sh/uv/releases/download/0.10.1/uv-x86_64-unknown-linux-musl.tar.gz
UV_SHA256="d1a3b08dd9abf9e500541cadd0e2f4b144c99b9265fb00e500c2b5c82a3b4ee8"

TARBALL="uv-x86_64-unknown-linux-musl.tar.gz"
EXTRACT_DIR="uv-x86_64-unknown-linux-musl"

curl -fsSL -o "$TARBALL" \
  "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/${TARBALL}"

echo "${UV_SHA256}  ${TARBALL}" | sha256sum -c -

tar -xzf "$TARBALL"
install -m 755 "${EXTRACT_DIR}/uv" /usr/local/bin/uv
install -m 755 "${EXTRACT_DIR}/uvx" /usr/local/bin/uvx
rm -rf "$EXTRACT_DIR" "$TARBALL"

uv --version
