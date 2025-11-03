from pathlib import Path
import pandas as pd

RAW = Path("data/raw/sf311.csv")
CLEAN_DIR = Path("data/clean"); CLEAN_DIR.mkdir(parents=True, exist_ok=True)

def main():
    df = pd.read_csv(RAW)
    df = df.rename(columns={
        "service_request_id": "request_id",
        "requested_datetime": "created_at",
        "closed_date": "closed_at",
        "status_description": "status",
        "service_name": "category",
        "service_subtype": "subcategory",
        "neighborhoods_sffind_boundaries": "neighborhood",
    })

    for c in ("created_at", "closed_at"):
        df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)

    keep = ["request_id","created_at","closed_at","status","category","subcategory","neighborhood"]
    df = df[keep].dropna(subset=["request_id","created_at"]).drop_duplicates(subset=["request_id"])

    out = CLEAN_DIR / "sf311.csv"
    df.to_csv(out, index=False)
    print(f"cleaned -> {out} ({len(df):,} rows)")

if __name__ == "__main__":
    main()