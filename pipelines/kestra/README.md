# Kestra flows (optional orchestrator profile)

Place YAML flows here, e.g.:

- `retention_prune.yaml` — cron `*/5 * * * *` → `python /app/pipelines/retention/prune_db.py`
- `compute_global_rankings.yaml` — cron `*/15 * * * *` → ranking script
- `export_to_gcs.yaml` — when `cloud.export_enabled=true`

The default `docker-compose.yml` runs ranking and retention via lightweight loop containers
(`ranking-worker`, `retention-worker`) without Kestra, to keep RAM ~4–5 GB.

Enable Kestra UI: `docker compose --profile orchestrator up -d kestra` (port 8088).
