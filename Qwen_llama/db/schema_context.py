"""db/schema_context.py

Single source of truth for the database schema description used in LLM prompts.
Update this whenever a column is added or a table is renamed — the SQL generator
will pick it up automatically without touching any SQL templates.
"""

_SCHEMA = """
Tables
------
orders
  order_id      UUID        Primary key
  customer_id   UUID        FK → customers.customer_id
  order_date    DATE        Date the order was placed
  total_amount  NUMERIC     Pre-computed order total (sum of line items)

order_items
  order_item_id UUID        Primary key
  order_id      UUID        FK → orders.order_id
  product_id    UUID        FK → products.product_id
  quantity      INTEGER     Units ordered
  item_price    NUMERIC     Price per unit at time of sale

products
  product_id    UUID        Primary key
  product_name  TEXT
  category_id   UUID        FK → categories.category_id

categories
  category_id   UUID        Primary key
  category_name TEXT

customers
  customer_id   UUID        Primary key
  customer_name TEXT
  city_id       UUID        FK → cities.city_id

cities
  city_id       UUID        Primary key
  city_name     TEXT
"""

_RULES = """
Revenue rules
-------------
- Product / category revenue  = SUM(oi.quantity * oi.item_price)
- Customer / city revenue      = SUM(o.total_amount)
- Quantity                     = SUM(oi.quantity)
- Always filter by o.order_date BETWEEN %s AND %s

Parameter order rules (CRITICAL — must match exactly)
------------------------------------------------------
- Ranked queries   (top / bottom): 3 params → (start_date, end_date, limit::int)
- Aggregate queries (sum / total):  2 params → (start_date, end_date)
- Use %s for every placeholder — never hard-code dates or numbers in the SQL
- The LIMIT clause must be written as: LIMIT %s  (not LIMIT 5)

Safety rules
------------
- Output a single SELECT statement only — no CTEs that modify data, no DML
- Do NOT include semicolons
- Do NOT include comments (-- or /* */)
- Do NOT use ILIKE or regex operators — use exact column names only
- Always end with ORDER BY value DESC (top) or ASC (bottom)
"""


def get_schema_prompt() -> str:
    """Return the schema + rules block ready to drop into an LLM system prompt."""
    return f"Database schema:\n{_SCHEMA}\n{_RULES}"


def get_schema_summary() -> str:
    """One-liner for logging / debugging."""
    tables = [line.strip() for line in _SCHEMA.splitlines() if line and not line.startswith(" ")]
    return "Tables: " + ", ".join(tables)