from pathlib import Path
import pandas as pd

STAGING_DIR = Path("data/staging")

RESTAURANTS_PATH = STAGING_DIR / "staging_restaurants.csv"
CUISINE_PATH = STAGING_DIR / "staging_cuisine_map.csv"
OUT_PATH = STAGING_DIR / "indian_restaurants.csv"

def main() -> None:
    restaurants = pd.read_csv(RESTAURANTS_PATH)
    cuisine = pd.read_csv(CUISINE_PATH)

    # Filter to Indian
    indian_ids = cuisine.loc[cuisine["canonical_cuisine"] == "Indian", "source_id"].unique()
    indian = restaurants[restaurants["source_id"].isin(indian_ids)].copy()

    # Keep only useful columns for now
    cols = [
        "source_id",
        "name",
        "rating",
        "review_count",
        "price",
        "distance_to_ucf_miles",
        "full_address",
        "city",
        "state",
        "zip_code",
        "url"
    ]
    indian = indian[cols]

    # Sort: best rated first, then closer
    indian = indian.sort_values(
        by=["rating", "review_count", "distance_to_ucf_miles"],
        ascending=[False, False, True],
        na_position="last"
    )

    indian.to_csv(OUT_PATH, index=False)

    print(f"Saved: {OUT_PATH}")
    print(f"Indian restaurants: {len(indian)}")
    print("\nTop 8 Indian near UCF (name | rating | reviews | miles):")
    print(
        indian[["name", "rating", "review_count", "distance_to_ucf_miles"]]
        .head(8)
        .to_string(index=False)
    )

if __name__ == "__main__":
    main()