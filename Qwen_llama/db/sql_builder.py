"""db/sql_builder.py

Builds parameterized PostgreSQL SQL from structured intent.
All values come via %s placeholders — nothing hardcoded.

The LLM (Qwen) handles:
  - Natural language → intent JSON             (parse_intent_step)
  - Edge-case filters (gender, age, state, etc.) (custom_filter queries)

This module handles:
  - Converting known entity/metric/query_type combos → correct SQL structure

Entity → join path and aggregation expression
---------------------------------------------
product   → JOIN order_items oi JOIN orders o JOIN products p
              revenue  = SUM(oi.quantity * oi.item_price)
              quantity = SUM(oi.quantity)
              order_count = COUNT(DISTINCT o.order_id)

category  → + JOIN categories cat
              same aggregation via product

customer  → JOIN orders o JOIN customers c
              revenue  = SUM(o.total_amount)
              quantity = via order_items
              order_count = COUNT(DISTINCT o.order_id)

city      → + JOIN cities ci
              same via customer

state     → + JOIN cities ci JOIN states s
              same via customer
"""

from __future__ import annotations


# ── Entity configuration ──────────────────────────────────────────────────────

_ENTITY_CONFIG = {
    "product": {
        "name_col":  "p.product_name",
        "from":      "order_items oi JOIN orders o ON oi.order_id = o.order_id JOIN products p ON oi.product_id = p.product_id",
        "revenue":      "SUM(oi.quantity * oi.item_price)",
        "quantity":     "SUM(oi.quantity)",
        "order_count":  "COUNT(DISTINCT o.order_id)",
        "group_by":  "p.product_name",
        "pk":        "oi.product_id",
        "table":     "products",
        "table_pk":  "product_id",
        "table_name_col": "product_name",
    },
    "category": {
        "name_col":  "cat.category_name",
        "from":      "order_items oi JOIN orders o ON oi.order_id = o.order_id JOIN products p ON oi.product_id = p.product_id JOIN categories cat ON p.category_id = cat.category_id",
        "revenue":      "SUM(oi.quantity * oi.item_price)",
        "quantity":     "SUM(oi.quantity)",
        "order_count":  "COUNT(DISTINCT o.order_id)",
        "group_by":  "cat.category_name",
        "pk":        "p.category_id",
        "table":     "categories",
        "table_pk":  "category_id",
        "table_name_col": "category_name",
    },
    "customer": {
        "name_col":  "c.customer_name",
        "from":      "orders o JOIN customers c ON o.customer_id = c.customer_id",
        "revenue":      "SUM(o.total_amount)",
        "quantity":     "SUM(oi.quantity)",          # needs order_items join
        "order_count":  "COUNT(DISTINCT o.order_id)",
        "group_by":  "c.customer_name",
        "pk":        "o.customer_id",
        "table":     "customers",
        "table_pk":  "customer_id",
        "table_name_col": "customer_name",
        # For quantity, need extra join
        "_qty_from": "orders o JOIN customers c ON o.customer_id = c.customer_id JOIN order_items oi ON o.order_id = oi.order_id",
    },
    "city": {
        "name_col":  "ci.city_name",
        "from":      "orders o JOIN customers cu ON o.customer_id = cu.customer_id JOIN cities ci ON cu.city_id = ci.city_id",
        "revenue":      "SUM(o.total_amount)",
        "quantity":     "SUM(oi.quantity)",
        "order_count":  "COUNT(DISTINCT o.order_id)",
        "group_by":  "ci.city_name",
        "pk":        "cu.city_id",
        "table":     "cities",
        "table_pk":  "city_id",
        "table_name_col": "city_name",
        "_qty_from": "orders o JOIN customers cu ON o.customer_id = cu.customer_id JOIN cities ci ON cu.city_id = ci.city_id JOIN order_items oi ON o.order_id = oi.order_id",
    },
    "state": {
        "name_col":  "s.state_name",
        "from":      "orders o JOIN customers cu ON o.customer_id = cu.customer_id JOIN cities ci ON cu.city_id = ci.city_id JOIN states s ON ci.state_id = s.state_id",
        "revenue":      "SUM(o.total_amount)",
        "quantity":     "SUM(oi.quantity)",
        "order_count":  "COUNT(DISTINCT o.order_id)",
        "group_by":  "s.state_name",
        "pk":        "ci.state_id",
        "table":     "states",
        "table_pk":  "state_id",
        "table_name_col": "state_name",
        "_qty_from": "orders o JOIN customers cu ON o.customer_id = cu.customer_id JOIN cities ci ON cu.city_id = ci.city_id JOIN states s ON ci.state_id = s.state_id JOIN order_items oi ON o.order_id = oi.order_id",
    },
}


def _cfg(entity: str, metric: str) -> tuple[dict, str]:
    cfg  = _ENTITY_CONFIG[entity]
    from_clause = cfg.get("_qty_from", cfg["from"]) if metric == "quantity" else cfg["from"]
    agg  = cfg[metric]
    return cfg, from_clause, agg


# ── Public builders ───────────────────────────────────────────────────────────

def build_top_n(entity: str, metric: str, ascending: bool = False) -> str:
    cfg, from_c, agg = _cfg(entity, metric)
    direction = "ASC" if ascending else "DESC"
    return f"""
SELECT {cfg['name_col']} AS name,
       {agg} AS value
FROM {from_c}
WHERE o.order_date BETWEEN %s AND %s
GROUP BY {cfg['group_by']}
ORDER BY value {direction}
LIMIT %s
""".strip()


def build_aggregate(entity: str, metric: str) -> str:
    cfg, from_c, agg = _cfg(entity, metric)
    return f"""
SELECT {agg} AS value
FROM {from_c}
WHERE o.order_date BETWEEN %s AND %s
""".strip()


def build_threshold_absolute(entity: str, metric: str, operator: str = ">") -> str:
    cfg, from_c, agg = _cfg(entity, metric)
    return f"""
SELECT {cfg['name_col']} AS name,
       {agg} AS value
FROM {from_c}
WHERE o.order_date BETWEEN %s AND %s
GROUP BY {cfg['group_by']}
HAVING {agg} {operator} %s
ORDER BY value DESC
""".strip()


def build_threshold_percentage(entity: str, metric: str, operator: str = ">") -> str:
    """Params: (start, end, start, end) — dates repeated for correlated subquery."""
    cfg, from_c, agg = _cfg(entity, metric)
    # Subquery uses same FROM but without GROUP BY
    # For the subquery we need a simpler FROM (just the metric tables)
    sub_from = from_c
    return f"""
SELECT {cfg['name_col']} AS name,
       {agg} AS value
FROM {from_c}
WHERE o.order_date BETWEEN %s AND %s
GROUP BY {cfg['group_by']}
HAVING {agg} {operator} (%s / 100.0) * (
    SELECT {agg}
    FROM {sub_from}
    WHERE o.order_date BETWEEN %s AND %s
)
ORDER BY value DESC
""".strip()


def build_zero_filter(entity: str, metric: str) -> str:
    """Params: (start, end)"""
    cfg = _ENTITY_CONFIG[entity]
    # Build an existence check based on entity type
    if entity == "product":
        exists_check = f"""
    SELECT 1
    FROM order_items oi2
    JOIN orders o2 ON oi2.order_id = o2.order_id
    WHERE oi2.product_id = p.product_id
      AND o2.order_date BETWEEN %s AND %s"""
        return f"""
SELECT {cfg['table_name_col']} AS name, 0 AS value
FROM {cfg['table']} p
WHERE NOT EXISTS ({exists_check}
)
ORDER BY name
""".strip()
    elif entity == "category":
        exists_check = f"""
    SELECT 1
    FROM order_items oi2
    JOIN products p2 ON oi2.product_id = p2.product_id
    JOIN orders o2 ON oi2.order_id = o2.order_id
    WHERE p2.category_id = cat.category_id
      AND o2.order_date BETWEEN %s AND %s"""
        return f"""
SELECT {cfg['table_name_col']} AS name, 0 AS value
FROM {cfg['table']} cat
WHERE NOT EXISTS ({exists_check}
)
ORDER BY name
""".strip()
    elif entity == "customer":
        exists_check = f"""
    SELECT 1
    FROM orders o2
    WHERE o2.customer_id = c.customer_id
      AND o2.order_date BETWEEN %s AND %s"""
        return f"""
SELECT {cfg['table_name_col']} AS name, 0 AS value
FROM {cfg['table']} c
WHERE NOT EXISTS ({exists_check}
)
ORDER BY name
""".strip()
    elif entity == "city":
        exists_check = f"""
    SELECT 1
    FROM orders o2
    JOIN customers cu2 ON o2.customer_id = cu2.customer_id
    WHERE cu2.city_id = ci.city_id
      AND o2.order_date BETWEEN %s AND %s"""
        return f"""
SELECT {cfg['table_name_col']} AS name, 0 AS value
FROM {cfg['table']} ci
WHERE NOT EXISTS ({exists_check}
)
ORDER BY name
""".strip()
    else:  # state
        exists_check = f"""
    SELECT 1
    FROM orders o2
    JOIN customers cu2 ON o2.customer_id = cu2.customer_id
    JOIN cities ci2 ON cu2.city_id = ci2.city_id
    WHERE ci2.state_id = s.state_id
      AND o2.order_date BETWEEN %s AND %s"""
        return f"""
SELECT {cfg['table_name_col']} AS name, 0 AS value
FROM {cfg['table']} s
WHERE NOT EXISTS ({exists_check}
)
ORDER BY name
""".strip()


def build_growth_ranking(entity: str, metric: str, ascending: bool = False) -> str:
    """
    Params: (start1, end1, start2, end2, limit)
    Returns: name, value1, value2, delta
    """
    cfg, from_c, agg = _cfg(entity, metric)
    direction = "ASC" if ascending else "DESC"
    return f"""
WITH p1 AS (
    SELECT {cfg['group_by']} AS name,
           {agg} AS v1
    FROM {from_c}
    WHERE o.order_date BETWEEN %s AND %s
    GROUP BY {cfg['group_by']}
),
p2 AS (
    SELECT {cfg['group_by']} AS name,
           {agg} AS v2
    FROM {from_c}
    WHERE o.order_date BETWEEN %s AND %s
    GROUP BY {cfg['group_by']}
)
SELECT COALESCE(p1.name, p2.name) AS name,
       COALESCE(p1.v1, 0) AS value1,
       COALESCE(p2.v2, 0) AS value2,
       COALESCE(p2.v2, 0) - COALESCE(p1.v1, 0) AS delta
FROM p1 FULL OUTER JOIN p2 ON p1.name = p2.name
ORDER BY delta {direction}
LIMIT %s
""".strip()


def build_comparison(entity: str, metric: str) -> str:
    """
    Params: (start1, end1, start2, end2, limit)
    Returns: name, value1, value2
    """
    cfg, from_c, agg = _cfg(entity, metric)
    return f"""
WITH p1 AS (
    SELECT {cfg['group_by']} AS name,
           {agg} AS value1
    FROM {from_c}
    WHERE o.order_date BETWEEN %s AND %s
    GROUP BY {cfg['group_by']}
),
p2 AS (
    SELECT {cfg['group_by']} AS name,
           {agg} AS value2
    FROM {from_c}
    WHERE o.order_date BETWEEN %s AND %s
    GROUP BY {cfg['group_by']}
)
SELECT COALESCE(p1.name, p2.name) AS name,
       COALESCE(p1.value1, 0) AS value1,
       COALESCE(p2.value2, 0) AS value2
FROM p1 FULL OUTER JOIN p2 ON p1.name = p2.name
ORDER BY value1 DESC
LIMIT %s
""".strip()


def build_intersection(entity: str, metric: str) -> str:
    """
    Entities present in BOTH periods, ranked by combined metric.
    Params: (start1, end1, start2, end2, start1, end1, start2, end2, limit)
    """
    cfg, from_c, agg = _cfg(entity, metric)
    return f"""
SELECT {cfg['name_col']} AS name,
       {agg} AS value
FROM {from_c}
WHERE o.order_date BETWEEN %s AND %s
  AND {cfg['pk']} IN (
      SELECT {cfg['pk']}
      FROM {from_c.replace('o.', 'o2.').replace('JOIN orders o ', 'JOIN orders o2 ')}
      WHERE o2.order_date BETWEEN %s AND %s
  )
  AND {cfg['pk']} IN (
      SELECT {cfg['pk']}
      FROM {from_c.replace('o.', 'o3.').replace('JOIN orders o ', 'JOIN orders o3 ')}
      WHERE o3.order_date BETWEEN %s AND %s
  )
GROUP BY {cfg['group_by']}
ORDER BY value DESC
LIMIT %s
""".strip()


def build_sql(parsed: dict) -> str | None:
    """
    Main entry point. Returns parameterized SQL or None if not supported.
    Falls back to None for custom-filter queries (LLM handles those).
    """
    entity     = parsed.get("entity")
    metric     = parsed.get("metric", "revenue")
    qt         = parsed.get("query_type", "top_n")
    thr        = parsed.get("threshold")
    filters    = parsed.get("filters", {})

    # If there are demographic/custom filters (gender, age, region), fall through to LLM
    if filters:
        return None

    if entity not in _ENTITY_CONFIG:
        return None

    ascending = (qt == "bottom_n")

    if qt == "top_n":
        return build_top_n(entity, metric, ascending=False)
    if qt == "bottom_n":
        return build_top_n(entity, metric, ascending=True)
    if qt == "aggregate":
        return build_aggregate(entity, metric)
    if qt == "zero_filter":
        return build_zero_filter(entity, metric)
    if qt == "growth_ranking":
        user_query = parsed.get("_user_query", "")
        asc = any(w in user_query.lower() for w in ["lowest","worst","least","minimum","smallest"])
        return build_growth_ranking(entity, metric, ascending=asc)
    if qt == "comparison":
        return build_comparison(entity, metric)
    if qt == "intersection":
        return build_intersection(entity, metric)
    if qt == "threshold":
        if not thr:
            return None
        op  = ">" if thr.get("operator","gt") == "gt" else "<"
        typ = thr.get("type","absolute")
        if typ == "absolute":
            return build_threshold_absolute(entity, metric, op)
        if typ == "percentage":
            return build_threshold_percentage(entity, metric, op)

    return None