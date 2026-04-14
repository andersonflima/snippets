#!/bin/sh
set -eu
# contract: file_contract
# candidate: file_contract
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SOURCE_FILE="$SCRIPT_DIR/../kms.sh"
test -f "$SOURCE_FILE"
sh -n "$SOURCE_FILE"
grep -Eq '^(#!|[[:space:]]*[A-Za-z_][A-Za-z0-9_]*\(\)|[[:space:]]*(set|if|for|while|case|printf|echo)[[:space:]])' "$SOURCE_FILE"