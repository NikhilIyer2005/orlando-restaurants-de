import json
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

UCF_LAT = 28.6024
UCF_LON = -81.2001

SEARCH_URL = "https://api.yelp.com/v3/businesses/search"

def fetch_page(api_key: str, offset: int, radius: int = 5000) -> dict:
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "latitude": UCF_LAT,
        "longitude": UCF_LON,
        "radius": radius,
        "categories": "restaurants",
        "limit": 50,
        "offset": offset,
        "sort_by": "rating"
    }

    resp = requests.get(SEARCH_URL, headers=headers, params=params, timeout=30)

    #Rate-limit handling
    if resp.status_code == 429:
        raise RuntimeError("Rate limited (429). Try again later or slow down requests.")
    
    resp.raise_for_status()
    return resp.json()

def main() -> None:
    load_dotenv()
    api_key = os.getenv("YELP_API_KEY")
    if not api_key:
        raise RuntimeError("Missing YELP_API_KEY in your .env file")
    
    out_dir = Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)

    target_restaurants = 200    # Stop once ~200 unique businesses
    pause_sec = 1.0
    max_offset = 1000           # Safety cap

    seen_ids: set[str] = set()
    offset = 0
    page_num = 0

    while len(seen_ids) < target_restaurants and offset <= max_offset:
        out_path = out_dir / f"yelp_search_offset_{offset:03d}.json"

        # If file already exists, reuse it
        if out_path.exists():
            data = json.loads(out_path.read_text(encoding="utf-8"))
        else:
            data = fetch_page(api_key, offset)
            out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

        businesses = data.get("businesses", [])
        if not businesses:
            print(f"No more results at offset={offset}. Stopping.")
            break

        for b in businesses:
            bid = b.get("id")
            if bid:
                seen_ids.add(bid)
        
        print(
            f"Saved/loaded {out_path.name} | "
            f"page_businesses={len(businesses)} | "
            f"unique_so_far={len(seen_ids)}"
        )

        # Next page
        page_num += 1
        offset += 50
        time.sleep(pause_sec)
    
    print(f"\nDone. Unique restaurants collected: {len(seen_ids)}")
    print("Raw files saved in: data/raw/")

if __name__ == "__main__":
    main()