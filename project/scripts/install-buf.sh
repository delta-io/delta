#!/bin/sh
# Install buf — SHA256-verified.
# Single source of truth for version + checksum across CI workflows.
#
# Installs to ~/buf/bin/buf, which is the path expected by
# dev/delta-connect-gen-protos.sh (export PATH="$PATH:~/buf/bin").
#
# Usage:
#   bash project/scripts/install-buf.sh          # from repo root (CI)
set -eu

BUF_VERSION="v1.28.1"
# SHA from official release asset:
# https://github.com/bufbuild/buf/releases/download/v1.28.1/sha256.txt
BUF_SHA256="870cf492d381a967d36636fdee9da44b524ea62aad163659b8dbf16a7da56987"

TARBALL="buf-Linux-x86_64.tar.gz"

curl -fsSL -o "$TARBALL" \
  "https://github.com/bufbuild/buf/releases/download/${BUF_VERSION}/${TARBALL}"

echo "${BUF_SHA256}  ${TARBALL}" | sha256sum -c -

mkdir -p ~/buf
tar -xzf "$TARBALL" -C ~/buf --strip-components 1
rm "$TARBALL"

~/buf/bin/buf --version
