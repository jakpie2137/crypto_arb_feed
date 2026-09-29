#!/usr/bin/env sh
set -e
cd "$(dirname "$0")/.."
[ -f demo-config.json ] || cp demo-config.example.json demo-config.json
mkdir -p secrets
[ -f secrets/pg_password.txt ] || echo postgres > secrets/pg_password.txt
python3 scripts/render_env_from_config.py --docker-env-file .env.generated
docker compose up -d --build "$@"
