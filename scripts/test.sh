#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -z ${RELAY_TEST_DATABASE:-} ]]; then
  echo 'Set RELAY_TEST_DATABASE to an isolated database named relay_test*.' >&2
  exit 1
fi
bash scripts/dotnet.sh restore Relay.sln --locked-mode
bash scripts/dotnet.sh test tests/Relay.Tests --no-restore -m:1 -nr:false -p:UseSharedCompilation=false --filter 'Category!=Broker'
if [[ -n ${RELAY_TEST_SERVICEBUS:-} ]]; then
  bash scripts/dotnet.sh test tests/Relay.Tests --no-build --filter 'Category=Broker'
fi
bash scripts/dotnet.sh build Relay.sln -c FaultTests --no-restore -m:1 -nr:false -p:UseSharedCompilation=false
python3 scripts/fault-tests.py
