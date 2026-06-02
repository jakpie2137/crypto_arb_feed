#!/usr/bin/env python3
"""Incremental export of rolled gains from Postgres to GCS (Parquet).

Requires demo-config.json cloud.export_enabled=true and a GCP service account
JSON mounted at cloud.gcp_credentials_file (never commit credentials).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path


def main() -> None:
    if os.getenv("GCS_EXPORT_ENABLED", "false").lower() != "true":
        print("GCS export disabled (set cloud.export_enabled in demo-config.json)")
        return

    bucket = os.getenv("GCS_BUCKET", "")
    prefix = os.getenv("GCS_PREFIX", "rolled_gains/")
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not bucket:
        raise SystemExit("GCS_BUCKET not configured in demo-config.json")
    if not creds or not Path(creds).is_file():
        raise SystemExit("Mount GCP service account JSON and set GOOGLE_APPLICATION_CREDENTIALS")

    import pandas as pd
    import psycopg
    from google.cloud import storage

    conn = psycopg.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "arb_demo"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASS", os.getenv("POSTGRES_PASSWORD", "postgres")),
    )
    df = pd.read_sql(
        """
        SELECT * FROM market_data.ob_arb_rolling_gain_30s
        WHERE ts >= NOW() - INTERVAL '20 minutes'
        ORDER BY ts
        """,
        conn,
    )
    conn.close()
    if df.empty:
        print("No new rows to export")
        return

    now = datetime.now(timezone.utc)
    blob_name = f"{prefix}dt={now.strftime('%Y-%m-%d')}/hour={now.strftime('%H')}/rolled_{int(now.timestamp())}.parquet"
    local = Path("/tmp/export.parquet")
    df.to_parquet(local, index=False)

    client = storage.Client()
    blob = client.bucket(bucket).blob(blob_name)
    blob.upload_from_filename(local)
    print(f"Exported {len(df)} rows to gs://{bucket}/{blob_name}")


if __name__ == "__main__":
    main()
