from pathlib import Path
import pandas as pd

STAGING_DIR = Path("data/staging")
LATE = STAGING_DIR / "late_night_restaurants.csv"
INDIAN = STAGING_DIR / "indian_restaurants.csv"
OUT = STAGING_DIR / "late_night_indian_restaurants.csv"


def main() -> None:
    late = pd.read_csv(LATE)
    indian = pd.read_csv(INDIAN)
    out = late.merge(indian[['source_id']], on='source_id', how='inner')
    out.to_csv(OUT, index=False)

    if not out.empty:
        print("\nLate_night Indian near UCF:")
        print(
            out[['name', 'rating', 'review_count', 'distance_to_ucf_miles']]
            .sort_values(
                by=['rating', 'review_count', 'distance_to_ucf_miles'],
                ascending=[False, False, True],
                na_position='last'
            )
            .to_string(index=False)
        )

if __name__ == "__main__":
    main()