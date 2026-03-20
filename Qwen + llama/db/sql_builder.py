# ---------- PRODUCTS ----------

def product_revenue_query():
    return """
    SELECT p.product_name,
           SUM(oi.quantity * oi.item_price) AS value
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE o.order_date BETWEEN %s AND %s
    GROUP BY p.product_name
    ORDER BY value DESC
    LIMIT 5;
    """

def product_quantity_query():
    return """
    SELECT p.product_name,
           SUM(oi.quantity) AS value
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE o.order_date BETWEEN %s AND %s
    GROUP BY p.product_name
    ORDER BY value DESC
    LIMIT 5;
    """

def total_revenue_query():
    return """
    SELECT SUM(oi.quantity * oi.item_price)
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_date BETWEEN %s AND %s;
    """


# ---------- CUSTOMERS ----------

def customer_revenue_query():
    return """
    SELECT c.customer_name,
           SUM(o.total_amount) AS value
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.order_date BETWEEN %s AND %s
    GROUP BY c.customer_name
    ORDER BY value DESC
    LIMIT 5;
    """

def customer_order_count_query():
    return """
    SELECT c.customer_name,
           COUNT(o.order_id) AS value
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.order_date BETWEEN %s AND %s
    GROUP BY c.customer_name
    ORDER BY value DESC
    LIMIT 5;
    """


# ---------- CITIES ----------

def city_revenue_query():
    return """
    SELECT ci.city_name,
           SUM(o.total_amount) AS value
    FROM orders o
    JOIN customers cu ON o.customer_id = cu.customer_id
    JOIN cities ci ON cu.city_id = ci.city_id
    WHERE o.order_date BETWEEN %s AND %s
    GROUP BY ci.city_name
    ORDER BY value DESC
    LIMIT 5;
    """


# ---------- CATEGORIES ----------

def category_revenue_query():
    return """
    SELECT cat.category_name,
           SUM(oi.quantity * oi.item_price) AS value
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    JOIN categories cat ON p.category_id = cat.category_id
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_date BETWEEN %s AND %s
    GROUP BY cat.category_name
    ORDER BY value DESC
    LIMIT 5;
    """
