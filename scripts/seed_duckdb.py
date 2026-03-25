"""
One-time seed utility: load CSV files into DuckDB tables.

Run from project root:
    python scripts/seed_duckdb.py
"""

from pathlib import Path
import os
import duckdb


TABLES = {
    "states": "states.csv",
    "cities": "cities.csv",
    "customers": "customers.csv",
    "categories": "categories.csv",
    "products": "products.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
}


def main() -> None:
    db_path = os.getenv("DUCKDB_PATH", "Qwen_llama/motia/data/analytics.duckdb")
    root = Path(".")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(db_path)
    try:
        for table, fname in TABLES.items():
            csv_path = root / fname
            if not csv_path.exists():
                print(f"SKIP {fname} (not found)")
                continue

            # Normalize path separator for SQL string literal on Windows
            p = str(csv_path.resolve()).replace("\\", "/")
            conn.execute(
                f"""
                CREATE OR REPLACE TABLE "{table}" AS
                SELECT * FROM read_csv_auto('{p}', header=true)
                """
            )
            n = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            print(f"OK   {table:<12} {n} rows")
    finally:
        conn.close()

    print(f"\nDuckDB ready at: {db_path}")


if __name__ == "__main__":
    main()
