#!/bin/sh
# Read-only release gate. Never source/eval the supplied environment file.
set -eu
SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$SCRIPT_DIR/release_preflight.py" "$@"
