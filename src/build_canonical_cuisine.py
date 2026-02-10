from pathlib import Path
import pandas as pd

STAGING_DIR = Path("data/staging")
CATEGORIES_PATH = STAGING_DIR / "staging_categories.csv"
OUT_PATH = STAGING_DIR / "staging_cuisine_map.csv"

# Canonical cuisine mapping rules
CANONICAL_MAP = {
    'indpak': "Indian",
    'pakistani': "Indian",
    'himalayan': "Indian",
    'indianfusion': "Indian"
}

def main() -> None:
    df = pd.read_csv(CATEGORIES_PATH)

    # Only keep categories that matter
    df = df[df["category_alias"].isin(CANONICAL_MAP.keys())].copy()

    # Map to canonical cuisine
    df["canonical_cuisine"] = df["category_alias"].map(CANONICAL_MAP)

    # Deduplicate: one cuisine per restaurant
    df = df.drop_duplicates(subset=["source_id", "canonical_cuisine"])

    df.to_csv(OUT_PATH, index=False)

    print(f"Saved: {OUT_PATH}")
    print(f"Indian restaurants identified: {df['source_id'].nunique()}")
    print("\nSample rows:")
    print(df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()