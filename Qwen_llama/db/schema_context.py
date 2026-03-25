"""Schema prompt builder for SQL generation."""

from db.duckdb_connection import get_read_connection

_STATIC_FALLBACK = """
Tables
------
states         : state_id (PK), state_name
cities         : city_id (PK), city_name, state_id (FK->states)
customers      : customer_id (PK), customer_name, gender, age, region_id, city_id (FK->cities)
categories     : category_id (PK), category_name
products       : product_id (PK), product_name, category_id (FK->categories), price DOUBLE
orders         : order_id (PK), customer_id (FK->customers), order_date DATE, total_amount DOUBLE
order_items    : order_item_id (PK), order_id (FK->orders), product_id (FK->products),
                 quantity INT, item_price DOUBLE

Aggregation rules
-----------------
product/category revenue    = SUM(oi.quantity * oi.item_price)
customer/city/state revenue = SUM(o.total_amount)
quantity sold               = SUM(oi.quantity)
order count                 = COUNT(DISTINCT o.order_id)

Date filter: always use o.order_date BETWEEN ? AND ?
Use ? placeholders for all params.
"""


def _live_schema_prompt() -> str:
    conn = get_read_connection()
    try:
        rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='main'
              AND table_name NOT LIKE '_raw_%'
            ORDER BY table_name
            """
        ).fetchall()
        tables = [r[0] for r in rows]
        if not tables:
            return ""

        lines = ["Tables", "------"]
        for t in tables:
            cols = conn.execute(f'DESCRIBE "{t}"').fetchall()
            col_s = ", ".join(f"{c[0]} {c[1]}" for c in cols)
            lines.append(f"{t:<14}: {col_s}")

        lines.append("")
        lines.append("SQL rules")
        lines.append("---------")
        lines.append("- SELECT only")
        lines.append("- Alias group key as name and metric as value")
        lines.append("- Use ? placeholders for all params")
        lines.append("- For ranked queries use ORDER BY value DESC/ASC LIMIT ?")
        return "\n".join(lines)
    finally:
        conn.close()


def get_schema_prompt() -> str:
    try:
        live = _live_schema_prompt().strip()
        if live:
            return live
    except Exception:
        pass
    return _STATIC_FALLBACK.strip()
