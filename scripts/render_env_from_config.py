#!/usr/bin/env python3
"""Read demo-config.json and print/export env vars for Docker services."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def load_config(path: Path) -> dict:
    if not path.is_file():
        print(f"Config not found: {path}", file=sys.stderr)
        sys.exit(1)
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def config_to_env(cfg: dict) -> dict[str, str]:
    env: dict[str, str] = {}

    features = cfg.get("features", {})
    env["SAVE_DENSE_SNAPSHOTS"] = str(features.get("save_dense_snapshots", False)).lower()
    env["SAVE_ROLLED_GAINS"] = str(features.get("save_rolled_gains", True)).lower()
    env["SAVE_BIG_GAIN_STATS"] = str(features.get("save_big_gain_stats", False)).lower()
    env["PUBLIC_READ_ONLY"] = str(cfg.get("deployment", {}).get("public_read_only", False)).lower()

    retention = cfg.get("retention", {})
    env["DENSE_SNAPSHOT_RETENTION_MINUTES"] = str(retention.get("dense_snapshots_minutes", 15))
    env["ROLLED_GAINS_RETENTION_HOURS"] = str(retention.get("rolled_gains_hours", 24))

    detectors = cfg.get("detectors", {})
    env["ARB_ROLLING_INTERVAL_SEC"] = str(detectors.get("rolling_interval_sec", 30))
    env["DENSE_SNAPSHOT_INTERVAL_MS"] = str(detectors.get("dense_snapshot_interval_ms", 2000))
    env["ARB_ROLLING_VOLUME_USD"] = str(detectors.get("target_volume_usd", 1000))
    env["ARB_DETECTOR_TOTAL_FEES_PCT"] = str(detectors.get("total_fees_pct", 0.0008))

    viewer = cfg.get("viewer", {})
    env["OB_VIEWER_REFRESH_MS"] = str(viewer.get("ob_refresh_interval_ms", 100))
    env["OB_VIEWER_DISPLAY_DEPTH"] = str(viewer.get("display_depth", 20))
    env["OB_VIEWER_TARGET_VOLUME_USD"] = str(viewer.get("default_target_volume_usd", 5000))

    dashboards = cfg.get("dashboards", {})
    env["ROLLING_DASHBOARD_REFRESH_SEC"] = str(dashboards.get("rolling_refresh_interval_sec", 60))
    env["GLOBAL_RANKING_REFRESH_SEC"] = str(dashboards.get("global_ranking_refresh_interval_sec", 900))

    feeds = cfg.get("feeds", {})
    env["OB_GUI_SYMBOL_LIMIT"] = str(feeds.get("symbol_limit", 100))

    pg = cfg.get("postgres", {})
    env["PG_HOST"] = pg.get("host", "postgres")
    env["PG_PORT"] = str(pg.get("port", 5432))
    env["PG_DB"] = pg.get("database", "arb_demo")
    env["PG_USER"] = pg.get("user", "postgres")

    cloud = cfg.get("cloud", {})
    env["GCS_EXPORT_ENABLED"] = str(cloud.get("export_enabled", False)).lower()
    env["GCS_BUCKET"] = cloud.get("gcs_bucket", "")
    env["GCS_PREFIX"] = cloud.get("gcs_prefix", "rolled_gains/")
    env["BQ_PROJECT"] = cloud.get("bigquery_project", "")
    env["BQ_DATASET"] = cloud.get("bigquery_dataset", "arb_demo")

    env["OB_GUI"] = "false"
    return env


def main() -> None:
    root = Path(os.environ.get("CRYPTO_ARB_FEED_ROOT", Path(__file__).resolve().parents[1]))
    cfg_path = Path(os.environ.get("DEMO_CONFIG", root / "demo-config.json"))
    cfg = load_config(cfg_path)
    env = config_to_env(cfg)

    if len(sys.argv) > 1 and sys.argv[1] == "--docker-env-file":
        out = Path(sys.argv[2]) if len(sys.argv) > 2 else root / ".env.generated"
        lines = [f"{k}={v}" for k, v in sorted(env.items())]
        out.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"Wrote {out}")
        return

    for k, v in sorted(env.items()):
        print(f"export {k}={v!r}")


if __name__ == "__main__":
    main()
