import json
from pathlib import Path
from math import radians, sin, cos, asin, sqrt
import pandas as pd

# UCF center point
UCF_LAT = 28.6024
UCF_LON = -81.2001

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/staging")
OUT_PATH = OUT_DIR / "staging_restaurants.csv"


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points (miles)."""
    # radius of Earth (miles)
    R = 3958.7613
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2- lat1
    dlon = lon2- lon1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return R * c

def load_businesses() -> list[dict]:
    files = sorted(RAW_DIR.glob("yelp_search_offset_*.json"))
    if not files:
        raise FileNotFoundError("No raw files found in data/raw (expected yelp_search_offset_*.json)")
    
    businesses: list[dict] = []
    for fp in files:
        data = json.loads(fp.read_text(encoding="utf-8"))
        businesses.extend(data.get("businesses", []))
    return businesses

def normalize(businesses: list[dict]) -> pd.DataFrame:
    rows = []
    for b in businesses:
        bid = b.get("id")
        coords = b.get("coordinates") or {}
        loc = b.get("location") or {}

        lat = coords.get("latitude")
        lon = coords.get("longitude")

        # Basic address flattening
        display_address = loc.get("display_address") or []
        address_1 = loc.get("address1")
        address_2 = loc.get("address2")
        address_3 = loc.get("address3")
        city = loc.get("city")
        state = loc.get("state")
        zip_code = loc.get("zip_code")
        full_address = ", ".join([x for x in display_address if x]) if display_address else None

        # Distance to UCF
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            distance_to_ucf = haversine_miles(UCF_LAT, UCF_LON, float(lat), float(lon))

        rows.append(
            {
                "source": "yelp",
                "source_id": bid,
                "name": b.get("name"),
                "rating": b.get("rating"),
                "review_count": b.get("review_count"),
                "price": b.get("price"),
                "is_closed": b.get("is_closed"),
                "phone": b.get("phone"),
                "display_phone": b.get("display_phone"),
                "url": b.get("url"),
                "latitude": lat,
                "longitude": lon,
                "distance_to_ucf_miles": distance_to_ucf,
                "address1": address_1,
                "address2": address_2,
                "address3": address_3,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "full_address": full_address
            }
        )
    
    df = pd.DataFrame(rows)

    # Drop rows missing a business id
    df = df.dropna(subset=['source_id'])

    # Deduplicate: keep first occurrence per source_id
    df = df.drop_duplicates(subset=['source_id'], keep= "first")

    # Sort by distance
    if "distance_to_ucf_miles" in df.columns:
        df = df.sort_values(by='distance_to_ucf_miles', ascending=True, na_position='last')
    return df


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    businesses = load_businesses()
    df = normalize(businesses)

    df.to_csv(OUT_PATH, index=False)
    print(f"Saved: {OUT_PATH}")
    print(f"Unique restaurants: {len(df)}")
    print("Top 5 closest (name, miles):")
    print(df[["name", "distance_to_ucf_miles"]].head(5).to_string(index=False))

if __name__ == "__main__":
    main()
        