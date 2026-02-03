# Food Delivery — ETL Pipeline

ETL pipeline to load food delivery data from CSV into a PostgreSQL database. The project normalizes the dataset into tables (`delivery_person`, `restaurants`, `orders`) so it can be queried and used for analysis.

---

## Objective

- **Extract** delivery data from CSV (`data/train.csv`).
- **Transform** it: clean types, unify date/time columns, handle missing values (e.g. string `"NaN"`).
- **Load** it into PostgreSQL with a simple schema (delivery people, restaurants, orders with foreign keys).

The result is a small relational database ready for SQL analysis or further use in other tools.

---

## Project structure

```
food_delivery/
├── data/           # CSV sources (train.csv, test.csv)
├── etl/            # ETL pipeline
│   ├── extract.py  # Read CSV
│   ├── transform.py# Clean and normalize
│   ├── load.py     # Prepare DataFrames and insert into PostgreSQL
│   └── run_etl.py  # Run full pipeline
├── sql/
│   └── init.sql    # Table definitions
├── init_db.py      # Create database and tables (if they don't exist)
├── .env.example    # Template for connection variables
└── README.md
```

---

## Prerequisites

- **Python 3** (e.g. 3.10+)
- **PostgreSQL** (local or remote)
- Dependencies: `pandas`, `psycopg[binary]`

---

## Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/food_delivery.git
   cd food_delivery
   ```

2. **Create a virtual environment and install dependencies**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # Windows
   pip install pandas "psycopg[binary]"
   ```

3. **Configure the database**
   - Copy `.env.example` to `.env`.
   - Fill in `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT` for your PostgreSQL instance.

4. **Create the database and tables**
   ```bash
   python init_db.py
   ```

5. **Run the ETL**
   ```bash
   python -m etl.run_etl
   ```
   Or with a custom CSV path:
   ```bash
   python -m etl.run_etl data/train.csv
   ```

---

## Usage summary

| Step        | Command / file   | Description                          |
|------------|------------------|--------------------------------------|
| Create DB  | `python init_db.py` | Creates DB (if missing) and tables   |
| Full ETL   | `python -m etl.run_etl` | Extract → transform → load from CSV |

---

## Note

This repository is a **first trial** of building and maintaining a project with the help of **Cursor** (AI-assisted editor). The structure, ETL logic, and this README were developed with that workflow in mind.
