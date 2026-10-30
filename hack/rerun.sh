#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
trap '"$ROOT/hack/undeploy.sh"' EXIT
"$ROOT/hack/undeploy.sh"
"$ROOT/hack/deploy.sh" --clean-on-start
"$ROOT/hack/e2e.sh"
