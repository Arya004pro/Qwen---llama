"""db/schema_context.py — Exact DB schema matching the actual dataset."""

SCHEMA = """
Tables
------
states         : state_id (PK), state_name
cities         : city_id (PK), city_name, state_id (FK→states)
customers      : customer_id (PK), customer_name, gender, age, region_id, city_id (FK→cities)
categories     : category_id (PK), category_name
products       : product_id (PK), product_name, category_id (FK→categories), price NUMERIC
orders         : order_id (PK), customer_id (FK→customers), order_date DATE, total_amount NUMERIC
order_items    : order_item_id (PK), order_id (FK→orders), product_id (FK→products),
                 quantity INT, item_price NUMERIC

Key notes
---------
- customers.gender  : text (e.g. Male / Female)
- customers.age     : integer (customer age in years)
- customers.region_id : integer (not a FK to a separate table in this dataset)
- products.price    : list price per unit (item_price in order_items is actual sale price)

Aggregation rules
-----------------
product/category revenue  = SUM(oi.quantity * oi.item_price)
customer/city/state revenue = SUM(o.total_amount)
quantity sold              = SUM(oi.quantity)
order count                = COUNT(DISTINCT o.order_id)

Date filter: always use  o.order_date BETWEEN %s AND %s

SQL rules
---------
- SELECT only — no INSERT/UPDATE/DELETE/DROP/CREATE
- Use %s for ALL placeholders — never hardcode values
- No semicolons, no SQL comments
- Alias primary group-by column as "name", metric as "value"
- For ranked queries:    ORDER BY value DESC/ASC   LIMIT %s
- For aggregate:         ONE row, ONE column aliased "value"
- For threshold abs:     HAVING SUM(...) > %s
- For threshold pct:     HAVING SUM(...) > fraction * (SELECT SUM(...) WHERE dates BETWEEN %s AND %s)
- For zero-filter:       NOT EXISTS subquery
- For intersection:      entities present in BOTH period subqueries
- For growth/delta:      CTE or subquery per period → return name, value1, value2, delta
- State-level queries:   JOIN cities ci ON cu.city_id = ci.city_id
                         JOIN states s  ON ci.state_id = s.state_id
- Gender/age filters:    use customers.gender or customers.age directly in WHERE
"""


def get_schema_prompt() -> str:
    return SCHEMA.strip()