import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


#UCF center point
UCF_LAT = 28.6024
UCF_LON = -81.2001

def main() -> None:
    load_dotenv()
    api_key = os.getenv("YELP_API_KEY")
    if not api_key:
        raise RuntimeError("Missing YELP_API_KEY in your .env file")
    
    url = "https://api.yelp.com/v3/businesses/search"
    headers = {"Authorization": f"Bearer {api_key}"}

    params = {
        "latitude": UCF_LAT,
        "longitude": UCF_LON,
        "radius": 5000,      #meters
        "categories": "restaurants",
        "limit": 50,
        "offset": 0,
        "sort_by": "rating"
    }

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    out_dir = Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "yelp_search_offset_000.json"
    out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    print(f"Saved: {out_path}")
    print(f"Returned businesses: {len(data.get('businesses', []))}")
    print(f"Total: {data.get('total')}")

if __name__ == "__main__":
    main()