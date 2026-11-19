import sqlite3
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

from src import extract_pages, load_to_sqlite


def test_fetch_page_uses_correct_parameters(monkeypatch):
    response = Mock(status_code=200)
    response.json.return_value = {"businesses": [{"id": "restaurant-1"}]}

    def fake_get(url, *, headers, params, timeout):
        assert url == extract_pages.SEARCH_URL
        assert headers["Authorization"] == "Bearer test-api-key"
        assert params["offset"] == 50
        assert params["limit"] == 50
        assert params["radius"] == 6000
        assert timeout == 30
        return response

    monkeypatch.setattr(extract_pages.requests, "get", fake_get)

    result = extract_pages.fetch_page("test-api-key", offset=50, radius=6000)

    assert result["businesses"][0]["id"] == "restaurant-1"
    response.raise_for_status.assert_called_once()


def test_fetch_page_handles_rate_limit(monkeypatch):
    response = Mock(status_code=429)

    monkeypatch.setattr(
        extract_pages.requests,
        "get",
        lambda *args, **kwargs: response,
    )

    with pytest.raises(RuntimeError, match="Rate limited"):
        extract_pages.fetch_page("test-api-key", offset=0)


def test_fetch_page_handles_server_error(monkeypatch):
    response = Mock(status_code=500)
    response.raise_for_status.side_effect = requests.HTTPError("HTTP 500")

    monkeypatch.setattr(
        extract_pages.requests,
        "get",
        lambda *args, **kwargs: response,
    )

    with pytest.raises(requests.HTTPError):
        extract_pages.fetch_page("test-api-key", offset=0)


def test_sqlite_load_and_rerun(tmp_path, monkeypatch):
    staging_dir = tmp_path / "staging"
    warehouse_dir = tmp_path / "warehouse"
    staging_dir.mkdir()

    csv_path = staging_dir / "staging_restaurants.csv"
    db_path = warehouse_dir / "restaurants.db"

    pd.DataFrame(
        [
            {"source_id": "r1", "name": "Restaurant One"},
            {"source_id": "r2", "name": "Restaurant Two"},
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.setattr(
        load_to_sqlite,
        "TABLES",
        {"staging_restaurants": csv_path},
    )
    monkeypatch.setattr(load_to_sqlite, "WAREHOUSE_DIR", warehouse_dir)
    monkeypatch.setattr(load_to_sqlite, "DB_PATH", db_path)

    load_to_sqlite.main()

    with sqlite3.connect(db_path) as connection:
        result = pd.read_sql_query("SELECT * FROM staging_restaurants", connection)

    assert len(result) == 2
    assert set(result["source_id"]) == {"r1", "r2"}

    # Running the loader again should not duplicate rows.
    load_to_sqlite.main()

    with sqlite3.connect(db_path) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM staging_restaurants"
        ).fetchone()[0]

    assert count == 2


def test_sqlite_skips_missing_csv(tmp_path, monkeypatch):
    warehouse_dir = tmp_path / "warehouse"
    db_path = warehouse_dir / "restaurants.db"

    monkeypatch.setattr(
        load_to_sqlite,
        "TABLES",
        {"missing_table": tmp_path / "missing.csv"},
    )
    monkeypatch.setattr(load_to_sqlite, "WAREHOUSE_DIR", warehouse_dir)
    monkeypatch.setattr(load_to_sqlite, "DB_PATH", db_path)

    load_to_sqlite.main()

    with sqlite3.connect(db_path) as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()

    assert tables == []
