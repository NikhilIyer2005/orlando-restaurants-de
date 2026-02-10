import json
import os
import time
from pathlib import Path
import pandas as pd
import requests
from dotenv import load_dotenv

DETAILS_URL = "https://api.yelp.com/v3/businesses/{}"
STAGING_RESTAURANTS = Path("data/staging/staging_restaurants.csv")
OUT_DIR = Path("data/raw/details")


def fetch_one(api_key: str, business_id: str) -> dict:
    headers = {"Authorization": f"Bearer {api_key}"}
    url = DETAILS_URL.format(business_id)
    resp = requests.get(url, headers=headers, timeout=30)

    # Yelp rate limiting can return 429 :contentReference[oaicite:4]{index=4}
    if resp.status_code == 429:
        raise RuntimeError("429 rate limit from Yelp (Too Many Requests)")
    
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    load_dotenv()
    api_key = os.getenv("YELP_API_KEY")
    if not api_key:
        raise RuntimeError("Missing YELP_API_KEY in your .env file")
    
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(STAGING_RESTAURANTS)
    ids = df["source_id"].dropna().astype(str).unique().tolist()

    # Safety cap
    ids = ids[:200]

    pause_seconds = 0.35    # if hit 429, increase to 0.75-1.0
    max_retries_429 = 5

    saved = 0
    skipped = 0

    for i, business_id in enumerate(ids, start=1):
        out_path = OUT_DIR / f"{business_id}.json"

        # Cache: don't refresh if file exists
        if out_path.exists():
            skipped += 1
            continue

        retries = 0
        while True:
            try:
                data = fetch_one(api_key, business_id)
                out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
                saved += 1
                break
            except RuntimeError as e:
                if "429" in str(e) and retries < max_retries_429:
                    # simple backoff
                    wait = (2 ** retries) * 1.0
                    print(f"[{i}/{len(ids)}] 429 rate limit. Sleeping {wait:.1f}s then retrying...")
                    time.sleep(wait)
                    retries += 1
                    continue
                raise
        if i % 10 == 0:
            print(f"Progress: {i}/{len(ids)} | saved={saved} | skipped={skipped}")
            time.sleep(pause_seconds)
    print("\nDONE")
    print(f"Saved new detail files: {saved}")
    print(f"Already existed (cached): {skipped}")
    print(f"Details folder: {OUT_DIR}")


if __name__ == "__main__":
    main()

