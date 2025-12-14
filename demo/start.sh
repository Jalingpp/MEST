#!/usr/bin/env bash
set -euo pipefail

# Enter MEST root (script resides in MEST/demo)
cd "$(dirname "$0")/.."

echo "Building demo server into demo/demo-server ..."
# build the demo server binary inside demo/ to keep repo root clean
go build -o ./demo/demo-server ./demo

echo "Starting demo server on :8080"
./demo/demo-server
