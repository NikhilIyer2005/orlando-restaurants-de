from pathlib import Path
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

STAGING_DIR = Path("data/staging")

TABLES = {
    "staging_restaurants": STAGING_DIR / "staging_restaurants.csv",
    "staging_categories": STAGING_DIR / "staging_categories.csv",
    "staging_cuisine_map": STAGING_DIR / "staging_cuisine_map.csv",
    "staging_hours": STAGING_DIR / "staging_hours.csv",
    "indian_restaurants": STAGING_DIR / "indian_restaurants.csv",
    "late_night_restaurants": STAGING_DIR / "late_night_restaurants.csv",
    "late_night_indian_restaurants": STAGING_DIR / "late_night_indian_restaurants.csv"
}

def main() -> None:
    pg_url = os.getenv("POSTGRES_URL")
    if not pg_url:
        raise RuntimeError("Missing POSTGRES_URL in .env")
    
    engine = create_engine(pg_url)

    for table_name, csv_path in TABLES.items():
        if not csv_path.exists():
            print(f"Skipping {table_name}: missing {csv_path}")
            continue

        df = pd.read_csv(csv_path)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Loaded {table_name} ({len(df)} rows)")
    
    print("\n✅ Postgres load complete.")


if __name__ == "__main__":
    main()