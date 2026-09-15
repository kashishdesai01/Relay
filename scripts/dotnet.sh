#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export DOTNET_CLI_TELEMETRY_OPTOUT=1
if [[ -x .review-tools/dotnet/dotnet ]]; then
  export DOTNET_CLI_HOME="$PWD/.review-tools/cli-home"
  export NUGET_PACKAGES="$PWD/.review-tools/nuget"
  exec .review-tools/dotnet/dotnet "$@"
fi
exec dotnet "$@"
