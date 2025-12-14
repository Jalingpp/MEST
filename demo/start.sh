#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."  # enter MEST root

echo "Building demo server..."
go build -o demo-server ./demo

echo "Starting demo server on :8080"
./demo-server

