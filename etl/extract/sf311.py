from pathlib import Path
import sys
import requests

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://data.sfgov.org/resource/vw6y-z8j6.csv"
FIELDS = [
    "service_request_id","requested_datetime","closed_date","status_description",
    "service_name","service_subtype","neighborhoods_sffind_boundaries"
]
PARAMS = {
    "$select": ",".join(FIELDS),
    "$order": "requested_datetime DESC",
    "$limit": 100000
}

def main():
    print(f"cwd: {Path.cwd()}")
    print("requesting CSV…")
    try:
        r = requests.get(URL, params=PARAMS, timeout=120)
    except Exception as e:
        print(f"request failed: {e}")
        sys.exit(1)

    print(f"status: {r.status_code}")
    if r.status_code != 200:
        print(r.text[:500])
        sys.exit(1)

    out = RAW_DIR / "sf311.csv"
    out.write_bytes(r.content)
    print(f"wrote {out} ({len(r.content):,} bytes)")

if __name__ == "__main__":
    main()