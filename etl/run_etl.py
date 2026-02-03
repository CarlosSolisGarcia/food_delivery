"""
Orchestrate the full ETL pipeline: extract → transform → load.

Uses data/train.csv by default. Requires tables to exist (run init_db.py first).

Usage from project root:
    python -m etl.run_etl
    python -m etl.run_etl data/train.csv
"""
import os
from pathlib import Path

from etl.extract import extract_data
from etl.load import (
    load_data,
    prepare_delivery_person_table,
    prepare_orders_table,
    prepare_restaurants_table,
)
from etl.transform import preprocess_data


def _project_root():
    """Project root (folder containing etl/, data/, .env)."""
    return Path(__file__).resolve().parent.parent


def _load_env():
    """Load KEY = value variables from .env into the environment."""
    env_path = _project_root() / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ[key.strip()] = value.strip().strip('"')


def run_etl(csv_path=None):
    """
    Run the full pipeline.

    Args:
        csv_path: Path to the CSV file. If None, uses data/train.csv.
    """
    root = _project_root()
    path = Path(csv_path) if csv_path else root / "data" / "train.csv"
    if not path.is_absolute():
        path = root / path

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    _load_env()

    print("1. Extract: reading CSV...")
    raw = extract_data(path)
    print(f"   Rows read: {len(raw):,}")

    print("2. Transform: cleaning and normalizing...")
    cleaned = preprocess_data(raw)
    print(f"   Rows after transform: {len(cleaned):,}")

    print("3. Prepare: building DataFrames per table...")
    delivery_person_df = prepare_delivery_person_table(cleaned)
    restaurants_df = prepare_restaurants_table(cleaned)
    orders_df = prepare_orders_table(cleaned, delivery_person_df, restaurants_df)
    print(f"   delivery_person: {len(delivery_person_df):,} rows")
    print(f"   restaurants: {len(restaurants_df):,} rows")
    print(f"   orders: {len(orders_df):,} rows")

    print("4. Load: inserting into PostgreSQL...")
    load_data(delivery_person_df, restaurants_df, orders_df)
    print("   Load completed.")

    print("ETL finished successfully.")


if __name__ == "__main__":
    import sys
    csv_path = sys.argv[1] if len(sys.argv) > 1 else None
    run_etl(csv_path)
