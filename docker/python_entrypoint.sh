#!/bin/sh
set -e
if [ -n "$PG_PASS_FILE" ] && [ -f "$PG_PASS_FILE" ]; then
  export PG_PASS="$(cat "$PG_PASS_FILE")"
  export POSTGRES_PASSWORD="$PG_PASS"
  export ARB_PG_PASS="$PG_PASS"
fi
exec "$@"
