#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

if [ "${1:-}" = "" ]; then
  printf 'Uso: sh scripts/configure_restricted_dev_env.sh <bucket> [opções extras do setup]\n' >&2
  exit 1
fi

if [ "${1#-}" != "$1" ]; then
  exec sh "${SCRIPT_DIR}/setup_restricted_dev_env.sh" "$@"
fi

S3_BUCKET="$1"
shift

exec sh "${SCRIPT_DIR}/setup_restricted_dev_env.sh" --s3-bucket "${S3_BUCKET}" "$@"
