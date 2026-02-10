import json
from pathlib import Path

RAW_FILE = Path("data/raw/yelp_search_offset_000.json")

def main() -> None:
    data = json.loads(RAW_FILE.read_text(encoding="utf-8"))
    businesses = data.get("businesses", [])
    print(f"Businesses in file: {len(businesses)}\n")

    b = businesses[0]

    print("=== BASIC ===")
    print("id:", b.get('id'))
    print("name:", b.get("name"))
    print("rating:", b.get("rating"), "review_count:", b.get("review_count"))
    print("price:", b.get("price"))

    print("\n=== COORDINATES ===")
    print(b.get("coordinates"))

    print("\n=== LOCATION ===")
    loc = b.get("location") or {}
    print("address1:", loc.get("address1"))
    print("city:", loc.get("city"), "state:", loc.get("state"), "zip:", loc.get("zip_code"))
    print("display_address:", loc.get("display_address"))

    print("\n=== CATEGORIES (one-to-many) ===")
    cats = b.get("categories") or []
    for c in cats:
        print("-", c.get("title"), f"(alias={c.get('alias')})")

if __name__ == "__main__":
    main()
