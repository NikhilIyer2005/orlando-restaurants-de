import json
from pathlib import Path
import pandas as pd


DETAILS_DIR = Path("data/raw/details")
OUT_DIR = Path("data/staging")
OUT_PATH = OUT_DIR / "staging_hours.csv"

LATE_NIGHT_CUTOFF = "23:00" # 11PM


def hhmm_from_yelp(time_str: str | None) -> str | None:
    """Convert Yelp time like '1730' -> '17:30'."""
    if not time_str or not isinstance(time_str, str):
        return None
    s = time_str.strip()
    if len(s) != 4 or not s.isdigit():
        return None
    return f"{s[:2]} : {s[2:]}"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(DETAILS_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError("No detail JSON files found in data/raw/details")
    
    rows = []
    missing_hours = 0

    for fp in files:
        data = json.loads(fp.read_text(encoding='utf-8'))
        source_id = data.get('id')
        if not source_id:
            continue

        hours_list = data.get("hours") or []
        if not hours_list:
            missing_hours += 1
            continue

        # Yelp can have multiple "hours" entries
        open_periods = hours_list[0].get('open') or []
        for p in open_periods:
            day = p.get("day")
            start = hhmm_from_yelp(p.get("start"))
            end = hhmm_from_yelp(p.get("end"))

            if start is None or end is None or day is None:
                continue

            # Overnight if it crosses midnight (18:00 -> 02:00)
            is_overnight = end < start

            # Late-night: open past 11pm or overnight
            is_late_night_llpm = is_overnight or (end >= LATE_NIGHT_CUTOFF)

            rows.append(
                {
                    "source": "yelp",
                    "source_id": source_id,
                    "day": int(day),
                    "start_time": start,
                    "end_time": end,
                    "is_overnight": int(is_overnight),
                    "is_late_night_11pm": int(is_late_night_llpm)
                }
            )
    df = pd.DataFrame(rows)

    # Dedupe in case of weird duplicates
    if not df.empty:
        df = df.drop_duplicates(subset=['source_id', 'day', 'start_time', 'end_time'], keep='first')

    df.to_csv(OUT_PATH, index=False)

    print(f"Saved: {OUT_PATH}")
    print(f"Hour rows: {len(df)}")
    print(f"Restaurants with hours: {df['source_id'].nunique() if not df.empty else 0}")
    print(f"Restaurants missing hours: {missing_hours}")

    if not df.empty:
        late = df.groupby('source_id')['is_late_night_11pm'].max()
        print(f"Late-night (11pm+) restaurants: {int(late.sum())}")

        print("\nSample late-night rows:")
        sample = df[df['is_late_night_11pm'] == 1].head(10)
        print(sample[['source_id', 'day', 'start_time', 'end_time', 'is_overnight', 'is_late_night_11pm']].to_string(index=False))


if __name__ == "__main__":
    main()