from pathlib import Path
import os
import requests
import pandas as pd
from sqlalchemy import text
from scripts.db import ENGINE

DATASET_ID = "vw6y-z8j6" 
BASE_JSON = f"https://data.sfgov.org/resource/{DATASET_ID}.json"
APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None)

FIELDS = [
    "service_request_id","requested_datetime","closed_date","status_description",
    "service_name","service_subtype","neighborhoods_sffind_boundaries"
]

PAGE_SIZE = 50000  

def latest_in_db():
    with ENGINE.connect() as c:
        
        row = c.execute(text("SELECT COALESCE(MAX(created_at),'2000-01-01') FROM sf311")).scalar()
    return pd.to_datetime(row)

def fetch_since(ts: pd.Timestamp) -> pd.DataFrame:
    headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}
    out = []
    where = f"requested_datetime > '{ts.strftime('%Y-%m-%dT%H:%M:%S')}'"
    offset = 0
    while True:
        params = {
            "$select": ",".join(FIELDS),
            "$where": where,
            "$order": "requested_datetime ASC",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        r = requests.get(BASE_JSON, params=params, headers=headers, timeout=120)
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        out.extend(chunk)
        offset += PAGE_SIZE
    return pd.DataFrame(out)

def upsert(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df = df.rename(columns={
        "service_request_id":"request_id",
        "requested_datetime":"created_at",
        "closed_date":"closed_at",
        "status_description":"status",
        "service_name":"category",
        "service_subtype":"subcategory",
        "neighborhoods_sffind_boundaries":"neighborhood"
    })
    for c in ("created_at","closed_at"):
        df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)

    keep = ["request_id","created_at","closed_at","status","category","subcategory","neighborhood"]
    df = df[keep].dropna(subset=["request_id","created_at"]).drop_duplicates("request_id")

    def clean(v):
        return None if pd.isna(v) else v

    sql = text("""
        INSERT INTO sf311
        (request_id, created_at, closed_at, status, category, subcategory, neighborhood, raw)
        VALUES
        (:request_id, :created_at, :closed_at, :status, :category, :subcategory, :neighborhood, '{}'::jsonb)
        ON CONFLICT (request_id) DO UPDATE SET
          created_at = EXCLUDED.created_at,
          closed_at  = EXCLUDED.closed_at,
          status     = EXCLUDED.status,
          category   = EXCLUDED.category,
          subcategory= EXCLUDED.subcategory,
          neighborhood=EXCLUDED.neighborhood
    """)

    n = 0
    with ENGINE.begin() as conn:
        batch = []
        for _, r in df.iterrows():
            batch.append({
                "request_id": str(r.get("request_id")),
                "created_at": clean(r.get("created_at")),
                "closed_at":  clean(r.get("closed_at")),
                "status":     clean(r.get("status")),
                "category":   clean(r.get("category")),
                "subcategory":clean(r.get("subcategory")),
                "neighborhood":clean(r.get("neighborhood")),
            })
            if len(batch) >= 2000:
                conn.execute(sql, batch)
                n += len(batch)
                batch.clear()
        if batch:
            conn.execute(sql, batch)
            n += len(batch)
    return n

def main():
    ts = latest_in_db()
    print(f"[incremental] fetching rows after {ts} …")
    df = fetch_since(ts)
    print(f"[incremental] fetched: {len(df):,} rows")
    n = upsert(df)
    print(f"[incremental] upserted: {n:,} rows")

if __name__ == "__main__":
    main()
