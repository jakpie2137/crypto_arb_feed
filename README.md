# crypto_arb_feed

Public demo stack: **multi-exchange WebSocket order book feeds**, **web OB viewer** (Swing-like, read-only), **rolling gain analytics**, and optional **Data Engineering** export (GCS / BigQuery / dbt).

No market making, no executors, no reporter, **no API keys** — only public market-data WebSockets.

Derived from a private production trading platform repo.

---

## Quick start (Docker)

Requirements: Docker with Compose v2, Python 3.9+ (standard library only, used to render the env file).

### One command

```bash
./scripts/start.sh            # Linux / macOS / WSL
```
```bat
scripts\start.cmd             :: Windows (cmd or PowerShell)
```

Both scripts do the steps below, skip anything that already exists, and pass extra arguments to `docker compose up` (e.g. `./scripts/start.sh --profile orchestrator`).

### Step by step

Linux / macOS / WSL:

```bash
cd crypto_arb_feed
cp demo-config.example.json demo-config.json
mkdir -p secrets && echo postgres > secrets/pg_password.txt
python3 scripts/render_env_from_config.py --docker-env-file .env.generated
docker compose up -d --build
```

Windows PowerShell:

```powershell
cd crypto_arb_feed
Copy-Item demo-config.example.json demo-config.json
New-Item -ItemType Directory -Force secrets | Out-Null
Set-Content -NoNewline secrets\pg_password.txt postgres
python scripts\render_env_from_config.py --docker-env-file .env.generated
docker compose up -d --build
```

### Files involved

| File | In git? | Created by | What it holds |
|------|---------|------------|---------------|
| `demo-config.example.json` | yes | — | Template with working defaults. Don't edit; copy it. |
| `demo-config.json` | no | you (copy of the example) | Your settings: features, retention, intervals, cloud export. The defaults work locally as-is. |
| `secrets/pg_password.txt` | no | you | Postgres password, plain text, single line. Mounted as a Docker secret into every service. |
| `.env.generated` | no | `render_env_from_config.py` | `KEY=value` lines derived from `demo-config.json`, loaded by compose via `env_file`. Never edit by hand. |

After changing `demo-config.json`, re-run the render script and `docker compose up -d` so containers pick up the new values.

Notes:

- The Postgres password is applied only when the `postgres_data` volume is first created. To change it later, run `docker compose down -v` (this **deletes the DB data**) or change it inside Postgres.
- Postgres host/port/db/user are fixed in `docker-compose.yml` (`environment:` overrides `env_file`), so the `postgres` section of `demo-config.json` has no effect under Docker.
- `DISABLED_FEEDS` for `bootstrap-symbols` is also fixed in compose; `feeds.disabled_feeds` in config does not reach it.
- The first `--build` compiles the Java feed with Gradle and takes a few minutes. The feed healthcheck allows 120 s to start; `docker compose ps` shows when it is `healthy`.
- Logs: `docker compose logs -f feed`. Stop: `docker compose down`.

| URL | Service |
|-----|---------|
| http://localhost:3000 | Web OB viewer |
| http://localhost:8050 | Rolling gains dashboard (auto-refresh ≥60s) |
| http://localhost:8051 | Global rankings dashboard |
| http://localhost:8082/orderbook?ex=BITGET&sym=BTCUSDT | Feed proxy (internal/debug) |

---

## Architecture & data flow

```
Exchanges (public WS)
    → feed (Java FeedFilterApp, headless)
        → in-memory OB + 1s history ring
        → RollingGainDetector (30s/60s) → market_data.ob_arb_rolling_gain_30s
        → (optional) dense snapshots → market_data.orderbook_snapshots (15 min retention)
    → feed-proxy (HTTP /orderbook, /prices)
        → ob-viewer-api (FastAPI, gain math)
            → ob-viewer-web (nginx, auto-refresh 100ms — interval only in demo-config.json)
    → Postgres (manager.symbol + market_data.*)
        → rolling / ranking Dash dashboards
        → ranking-worker (every 15 min)
        → retention-worker (every 5 min)
        → (optional) pg_to_gcs.py → GCS → BigQuery → dbt
```

### Retention (defaults)

| Data | Table | Retention |
|------|-------|-----------|
| Dense OB snapshots | `orderbook_snapshots` | **15 min** (if enabled) |
| Rolled gains | `ob_arb_rolling_gain_30s` | **24 h** |
| Global rankings | `global_gain_rankings` | **48 h** |

Controlled in `demo-config.json` → `.env.generated` via `scripts/render_env_from_config.py`.

---

## Configuration (`demo-config.json`)

**Gitignored** — copy from `demo-config.example.json`.

| Section | Purpose |
|---------|---------|
| `deployment.public_read_only` | Cloud demo: no admin/write UI |
| `features.save_rolled_gains` | Headless `RollingGainDetector` in feed container |
| `features.save_dense_snapshots` | Dense DB snapshots (off by default in cloud) |
| `detectors.rolling_interval_sec` | **30 or 60** — rolled gain compute interval |
| `viewer.ob_refresh_interval_ms` | Web OB poll interval (**100** default, not exposed in UI) |
| `dashboards.rolling_refresh_interval_sec` | Rolling dashboard refresh (**≥60**) |
| `cloud.*` | GCS/BQ paths — see below |

Symbols come from `conf/feeds/*.yaml` (trimmed demo lists). `bootstrap-symbols` upserts `manager.symbol` on first start.

**Disabled feeds (no API keys):** `bitvavo`, `bitkub`, `kraken`.

---

## Web OB viewer vs Swing GUI

Web viewer includes almost full Swing parity:

- Symbol filter, source/ref exchange, volume, fee display  
- Side-by-side OB with heatmap bars and per-level gains  
- BUY_GAIN / SELL_GAIN summary  
- **Refresh** and **Refresh FR** buttons  
- **Always-on auto-refresh** (interval from config only)

**Not in web UI (by design):** Start Big gain detector, Start snapshots→DB, auto-refresh toggle, refresh interval dropdown.

Local Swing GUI: run feed with `OB_GUI=true` (see Gradle `runGui` / native dev).

---

## Data Engineering (optional)

### GCS + BigQuery + dbt

When you want cloud analytics:

1. Create GCP project, bucket, BigQuery dataset.
2. Create service account with `storage.objectAdmin` + BigQuery data editor.
3. Save JSON key **outside git**, e.g. `secrets/gcp_sa.json` (gitignored).
4. In `demo-config.json`:

```json
"cloud": {
  "export_enabled": true,
  "gcs_bucket": "your-bucket",
  "gcs_prefix": "rolled_gains/",
  "bigquery_project": "your-project",
  "bigquery_dataset": "arb_demo",
  "gcp_credentials_file": "/run/secrets/gcp_sa.json"
}
```

5. Mount secret in compose and set `GOOGLE_APPLICATION_CREDENTIALS`.
6. Run `pipelines/export/pg_to_gcs.py` (or Kestra profile `orchestrator`).
7. dbt models in `pipelines/dbt/` — point sources at BQ external tables over GCS Parquet.

**Nothing in this repo should contain credentials.** Project IDs and bucket names in config are fine; service account JSON never goes into git.

### Kestra

Optional profile: `docker compose --profile orchestrator up -d kestra` (port 8088). Flows in `pipelines/kestra/` (retention, ranking, export).

### Not included in cloud runtime

- **Kafka / PySpark** — documented as local-only extensions; WebSocket→Postgres is enough at demo scale.

---

## Local development (without Docker)

```powershell
# Postgres on :5437, schemas from ops/sql/018,019,030
python ops/scripts/bootstrap_symbols_from_feeds.py

# Terminal 1 — feed headless
$env:OB_GUI="false"; $env:SAVE_ROLLED_GAINS="true"
$env:ARB_PG_URL="jdbc:postgresql://localhost:5437/arb_demo"
.\gradlew.bat :java-platform:runFeedHeadless

# Terminal 2 — feed proxy
.\gradlew.bat :java-platform:runFeedProxy

# Terminal 3 — API + web
uvicorn web.ob_viewer_api.main:app --port 8090
# open web/ob-viewer/index.html with API proxy or use docker web container
```

---

## Project layout

```
crypto_arb_feed/
├── java-platform/       # feed_filter + feed_proxy
├── analytics/ob_analytics/
├── web/ob-viewer/       # static web UI
├── web/ob_viewer_api/   # FastAPI
├── conf/feeds/          # symbol allowlists
├── pipelines/           # ranking, retention, export, dbt, kestra
├── ops/sql/             # Postgres schema
├── scripts/             # render_env_from_config.py, start.sh, start.cmd
├── demo-config.json     # gitignored, copy of demo-config.example.json
├── .env.generated       # gitignored, rendered from demo-config.json
├── secrets/             # gitignored, pg_password.txt
└── docker-compose.yml
```

---

## License / attribution

Feed and analytics code derived from the private trading platform repo. Use public exchange WebSocket endpoints only; no authenticated trading APIs in this project.
