import json

import pandas as pd
import pytest

from src import build_staging_hours, build_staging_restaurants


@pytest.mark.parametrize(
    ("yelp_time", "expected"),
    [
        ("1730", "17:30"),
        ("2300", "23:00"),
        ("0200", "02:00"),
        ("", None),
        (None, None),
        ("invalid", None),
    ],
)
def test_hhmm_from_yelp(yelp_time, expected):
    result = build_staging_hours.hhmm_from_yelp(yelp_time)
    assert result == expected


def test_haversine_same_location():
    distance = build_staging_restaurants.haversine_miles(
        28.6024, -81.2001, 28.6024, -81.2001
    )
    assert distance == pytest.approx(0.0)


def test_normalize_deduplicates_restaurant():
    businesses = [
        {
            "id": "restaurant-1",
            "name": "Original Restaurant",
            "coordinates": {"latitude": 28.6024, "longitude": -81.2001},
        },
        {
            "id": "restaurant-1",
            "name": "Duplicate Restaurant",
            "coordinates": {"latitude": 28.6024, "longitude": -81.2001},
        },
    ]

    result = build_staging_restaurants.normalize(businesses)

    assert len(result) == 1
    assert result.iloc[0]["name"] == "Original Restaurant"


def test_normalize_missing_coordinates():
    businesses = [
        {
            "id": "restaurant-1",
            "name": "Restaurant With Coordinates",
            "coordinates": {"latitude": 28.6024, "longitude": -81.2001},
        },
        {
            "id": "restaurant-2",
            "name": "Restaurant Without Coordinates",
        },
    ]

    result = build_staging_restaurants.normalize(businesses)

    missing = result.loc[result["source_id"] == "restaurant-2"]
    assert pd.isna(missing.iloc[0]["distance_to_ucf_miles"])


def test_late_night_classification(tmp_path, monkeypatch):
    details_dir = tmp_path / "details"
    output_dir = tmp_path / "staging"
    details_dir.mkdir()

    restaurants = [
        ("closes-22", "1700", "2200"),
        ("closes-23", "1700", "2300"),
        ("overnight", "1800", "0200"),
    ]

    for restaurant_id, start, end in restaurants:
        data = {
            "id": restaurant_id,
            "hours": [{"open": [{"day": 0, "start": start, "end": end}]}],
        }

        (details_dir / f"{restaurant_id}.json").write_text(
            json.dumps(data), encoding="utf-8"
        )

    output_path = output_dir / "staging_hours.csv"

    monkeypatch.setattr(build_staging_hours, "DETAILS_DIR", details_dir)
    monkeypatch.setattr(build_staging_hours, "OUT_DIR", output_dir)
    monkeypatch.setattr(build_staging_hours, "OUT_PATH", output_path)

    build_staging_hours.main()

    result = pd.read_csv(output_path).set_index("source_id")

    assert result.loc["closes-22", "is_late_night_11pm"] == 0
    assert result.loc["closes-23", "is_late_night_11pm"] == 1
    assert result.loc["overnight", "is_late_night_11pm"] == 1
    assert result.loc["overnight", "is_overnight"] == 1
