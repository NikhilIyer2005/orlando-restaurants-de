from pathlib import Path
import pandas as pd

STAGING_DIR = Path("data/staging")
RESTAURANTS = STAGING_DIR / "staging_restaurants.csv"
HOURS = STAGING_DIR / "staging_hours.csv"
OUT = STAGING_DIR / "late_night_restaurants.csv"

def main() -> None:
    r = pd.read_csv(RESTAURANTS)
    h = pd.read_csv(HOURS)

    late_ids = (
        h.groupby('source_id')['is_late_night_11pm']
        .max()
        .reset_index()
        .query('is_late_night_11pm == 1')['source_id']
    )

    late = r[r['source_id'].isin(late_ids)].copy()

    cols = [
        "source_id",
        'name',
        'rating',
        'review_count',
        'price',
        'distance_to_ucf_miles',
        'full_address',
        'url'
    ]
    late = late[cols]

    late = late.sort_values(
        by=['rating', 'review_count', 'distance_to_ucf_miles'],
        ascending=[False, False, True],
        na_position='last'
    )

    late.to_csv(OUT, index=False)
    print(f"Saved: {OUT}")
    print(f"Late-night restaurants: {len(late)}")
    print("\nTop late-night near UCF:")
    print(
        late[['name', 'rating', 'review_count', 'distance_to_ucf_miles']]
        .head(10)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()