from pathlib import Path
import json
import sys
import pandas as pd
from sqlalchemy import text
from scripts.db import ENGINE

def clean(v):
    import pandas as pd
    if pd.isna(v):
        return None
    return v

CLEAN = Path("data/clean/sf311.csv")

def main():
    print(">> loader start", flush=True)

    # 1) sanity checks
    print("DB URL:", ENGINE.url, flush=True)

    if not CLEAN.exists():
        print(f"ERR: {CLEAN} not found", file=sys.stderr, flush=True)
        sys.exit(1)

    try:
        df = pd.read_csv(CLEAN, parse_dates=["created_at","closed_at"])
    except Exception as e:
        print("ERR reading CSV:", e, file=sys.stderr, flush=True)
        sys.exit(1)

    print("Rows in cleaned file:", len(df), flush=True)
    if df.empty:
        print("ERR: cleaned file is empty", file=sys.stderr, flush=True)
        sys.exit(1)

    # 2) upsert
    sql = text("""
        INSERT INTO sf311
        (request_id, created_at, closed_at, status, category, subcategory, neighborhood, raw)
        VALUES
        (:request_id, :created_at, :closed_at, :status, :category, :subcategory, :neighborhood, :raw)
        ON CONFLICT (request_id) DO UPDATE SET
          created_at = EXCLUDED.created_at,
          closed_at  = EXCLUDED.closed_at,
          status     = EXCLUDED.status,
          category   = EXCLUDED.category,
          subcategory= EXCLUDED.subcategory,
          neighborhood=EXCLUDED.neighborhood;
    """)

    try:
        count = 0
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
                    "neighborhood": clean(r.get("neighborhood")),
                    "raw":        json.dumps({})
                })
                if len(batch) >= 2000:
                    conn.execute(sql, batch)
                    count += len(batch)
                    print(f"  inserted/upserted: {count}", flush=True)
                    batch.clear()
            if batch:
                conn.execute(sql, batch)
                count += len(batch)
                print(f"  inserted/upserted: {count}", flush=True)
    except Exception as e:
        print("ERR during upsert:", e, file=sys.stderr, flush=True)
        sys.exit(1)

    print(f">> loader done. total upserted: {count}", flush=True)

if __name__ == "__main__":
    main()