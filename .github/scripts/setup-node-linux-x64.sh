#!/usr/bin/env bash
set -euo pipefail

: "${NODE_VERSION:?NODE_VERSION must be set}"
: "${NODE_SHA256:?NODE_SHA256 must be set}"
: "${RUNNER_TEMP:?RUNNER_TEMP must be set}"
: "${GITHUB_PATH:?GITHUB_PATH must be set}"

archive="node-v${NODE_VERSION}-linux-x64.tar.xz"
archive_path="${RUNNER_TEMP}/${archive}"
install_dir="${RUNNER_TEMP}/node-v${NODE_VERSION}-linux-x64"

curl -fsSL -o "${archive_path}" "https://nodejs.org/dist/v${NODE_VERSION}/${archive}"
echo "${NODE_SHA256}  ${archive_path}" | sha256sum -c -
tar -xJf "${archive_path}" -C "${RUNNER_TEMP}"
echo "${install_dir}/bin" >> "${GITHUB_PATH}"

"${install_dir}/bin/node" --version
"${install_dir}/bin/npm" --version
