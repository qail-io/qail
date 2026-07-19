#!/usr/bin/env bash
set -euo pipefail

: "${DART_VERSION:?DART_VERSION must be set}"
: "${DART_SHA256:?DART_SHA256 must be set}"
: "${RUNNER_TEMP:?RUNNER_TEMP must be set}"
: "${GITHUB_PATH:?GITHUB_PATH must be set}"

archive="dartsdk-linux-x64-release.zip"
archive_path="${RUNNER_TEMP}/${archive}"
install_dir="${RUNNER_TEMP}/dart-sdk"
url="https://storage.googleapis.com/dart-archive/channels/stable/release/${DART_VERSION}/sdk/${archive}"

curl -fsSL -o "${archive_path}" "${url}"
echo "${DART_SHA256}  ${archive_path}" | sha256sum -c -
unzip -q "${archive_path}" -d "${RUNNER_TEMP}"
echo "${install_dir}/bin" >> "${GITHUB_PATH}"

"${install_dir}/bin/dart" --version
