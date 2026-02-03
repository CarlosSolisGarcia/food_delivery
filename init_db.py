"""
Create the database (if it does not exist) and tables.
Reads connection variables from .env and runs sql/init.sql.

Usage: python init_db.py
"""
import os
import re
from pathlib import Path

import psycopg

def load_env():
    """Load KEY = value variables from .env into the environment."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ[key.strip()] = value.strip().strip('"')


def ensure_database_exists(conn_params, dbname):
    """Create the database if it does not exist (connects to 'postgres' to do so)."""
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", dbname):
        raise ValueError(f"Invalid database name: {dbname!r}")
    params_to_postgres = {**conn_params, "dbname": "postgres"}
    with psycopg.connect(**params_to_postgres) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                (dbname,),
            )
            if cur.fetchone() is None:
                cur.execute(psycopg.sql.SQL("CREATE DATABASE {}").format(psycopg.sql.Identifier(dbname)))
                print(f"Database '{dbname}' created.")
            else:
                print(f"Database '{dbname}' already exists.")


def main():
    load_env()
    conn_params = {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME", "delivery_db"),
        "user": os.environ.get("DB_USER", "delivery_user"),
        "password": os.environ.get("DB_PASSWORD", "delivery_password"),
    }
    dbname = conn_params["dbname"]

    # Create the database if it does not exist (connect to 'postgres')
    ensure_database_exists(conn_params, dbname)

    # Create tables in the target database
    sql_path = Path(__file__).parent / "sql" / "init.sql"
    sql = sql_path.read_text(encoding="utf-8")

    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)
        conn.commit()

    print("Tables created successfully (or already existed).")


if __name__ == "__main__":
    main()
