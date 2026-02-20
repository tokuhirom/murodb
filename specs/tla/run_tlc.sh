#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODULE="$ROOT_DIR/CrashResilience.tla"
CFG="${1:-$ROOT_DIR/CrashResilience.cfg}"

if [[ -z "${TLA2TOOLS_JAR:-}" ]]; then
  echo "TLA2TOOLS_JAR is not set."
  echo "Example: export TLA2TOOLS_JAR=/path/to/tla2tools.jar"
  exit 1
fi

if [[ ! -f "$TLA2TOOLS_JAR" ]]; then
  echo "tla2tools.jar not found: $TLA2TOOLS_JAR"
  exit 1
fi

exec java -cp "$TLA2TOOLS_JAR" tlc2.TLC -deadlock -cleanup -config "$CFG" "$MODULE"
