# Late-Night Restaurant Data Pipeline (Yelp API, UCF Area)

This project implements an end-to-end **data engineering pipeline** that ingests restaurant data from the Yelp API, normalizes nested JSON into relational tables, and produces analytics-ready datasets identifying **late-night restaurants near the University of Central Florida (UCF)**.

The pipeline is designed with clear **raw → staging → warehouse** separation and supports querying in both **SQLite** and **Dockerized PostgreSQL** environments.

---

## 🚀 Project Overview

**Goal:**  
Identify and analyze restaurants near UCF that are open late at night, with a focus on cuisine-specific queries (e.g., Indian restaurants open late).

**Key Questions Answered:**
- Which restaurants near UCF are open late (past 11 PM)?
- Which Indian restaurants are open late near UCF?
- How do ratings, review counts, and proximity compare among late-night options?

---

## 🏗️ Architecture

Yelp API
├── Search Endpoint (pagination)
└── Business Details Endpoint (hours, categories)
↓
Raw Layer (JSON)
↓
Staging Layer (normalized CSVs)
↓
Warehouse Layer (SQLite for local analytics, PostgreSQL for production-style querying)
↓
SQL Analytics Queries

---

## 📂 Data Layers

### Raw Layer (`data/raw/`)
- Immutable JSON responses from Yelp API
- Cached paginated search results
- Cached per-business detail responses (hours, categories)

### Staging Layer (`data/staging/`)
Normalized relational tables:
- `staging_restaurants` – one row per restaurant
- `staging_categories` – one-to-many restaurant categories
- `staging_hours` – normalized weekly operating hours
- `staging_cuisine_map` – canonical cuisine mapping
- Derived datasets:
  - `late_night_restaurants`
  - `indian_restaurants`
  - `late_night_indian_restaurants`

### Warehouse Layer (`data/warehouse/`)
- SQLite database for local analytics
- PostgreSQL database running in Docker for production-style querying

---

## ⏰ Late-Night Definition

A restaurant is considered **late-night** if it:
- Closes **at or after 11:00 PM**, OR
- Operates overnight (closes after midnight)

Overnight detection is handled by identifying operating hours where `end_time < start_time`.

---

## 🧰 Tech Stack

- **Languages:** Python, SQL
- **Data Processing:** Pandas, SQLAlchemy
- **Databases:** SQLite, PostgreSQL
- **Infrastructure:** Docker
- **APIs:** Yelp Fusion API
- **Tooling:** VS Code, Git

---

## 📊 Example Queries

```sql
-- Top late-night restaurants near UCF
SELECT name, rating, review_count, distance_to_ucf_miles
FROM late_night_restaurants
ORDER BY rating DESC, review_count DESC, distance_to_ucf_miles ASC;

-- Late-night Indian restaurants near UCF
SELECT *
FROM late_night_indian_restaurants;

---

🔁 Pipeline Characteristics

- Idempotent transformations that can be safely re-run

- Cached raw API responses to avoid redundant ingestion

- Clear separation between raw, staging, and warehouse layers

---

🔮 Future Improvements

- Add minimum review-count credibility filters

- Schedule pipeline execution using Airflow or cron

- Add a lightweight BI or dashboard layer

---

📌 Notes

- API keys and database credentials are managed via environment variables

- Raw data and credentials are excluded from version control

- All derived tables can be regenerated from raw inputs