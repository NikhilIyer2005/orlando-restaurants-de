import json
from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/staging")
OUT_PATH = OUT_DIR / "staging_categories.csv"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    files = sorted(RAW_DIR.glob("yelp_search_offset_*.json"))
    if not files:
        raise FileNotFoundError("No raw files found in data/raw")
    
    for fp in files:
        data = json.loads(fp.read_text(encoding="utf-8"))
        businesses = data.get("businesses", [])

        for b in businesses:
            source_id = b.get("id")
            if not source_id:
                continue

            categories = b.get("categories") or []
            for c in categories:
                rows.append(
                    {
                        "source": "yelp",
                        "source_id": source_id,
                        "category_title": c.get("title"),
                        "category_alias": c.get("alias")
                    }
                )
    df = pd.DataFrame(rows)

    # Drop totally empty category from rows
    df = df.dropna(subset=["source_id", "category_alias"])

    # Deduplicate in case the same restaurant appears in multiple pages
    df = df.drop_duplicates(subset=["source_id", "category_alias"], keep='first')

    df.to_csv(OUT_PATH, index=False)
    print(f"Saved: {OUT_PATH}")
    print(f"Category rows: {len(df)}")
    print(f"Unique restaurants with categories: {df['source_id'].nunique()}")

    print("\nTop 10 categories by count:")
    print(
        df.groupby("category_title")["source_id"]
        .count()
        .sort_values(ascending=False)
        .head(10)
        .to_string()
    )

if __name__ == "__main__":
    main()