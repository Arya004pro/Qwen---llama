# db/sql_registry.py

SQL_REGISTRY = {

    "product": {
        "revenue": {
            "top": """
                SELECT p.product_name AS name,
                       SUM(oi.quantity * oi.item_price) AS value
                FROM order_items oi
                JOIN orders o ON oi.order_id = o.order_id
                JOIN products p ON oi.product_id = p.product_id
                WHERE o.order_date BETWEEN %s AND %s
                GROUP BY p.product_name
                ORDER BY value DESC
                LIMIT %s;
            """,
            "aggregate": """
                SELECT SUM(oi.quantity * oi.item_price) AS value
                FROM order_items oi
                JOIN orders o ON oi.order_id = o.order_id
                WHERE o.order_date BETWEEN %s AND %s;
            """
        },
        "quantity": {
            "top": """
                SELECT p.product_name AS name,
                       SUM(oi.quantity) AS value
                FROM order_items oi
                JOIN orders o ON oi.order_id = o.order_id
                JOIN products p ON oi.product_id = p.product_id
                WHERE o.order_date BETWEEN %s AND %s
                GROUP BY p.product_name
                ORDER BY value DESC
                LIMIT %s;
            """
        }
    },

    "customer": {
        "revenue": {
            "top": """
                SELECT c.customer_name AS name,
                       SUM(o.total_amount) AS value
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                WHERE o.order_date BETWEEN %s AND %s
                GROUP BY c.customer_name
                ORDER BY value DESC
                LIMIT %s;
            """,
            "aggregate": """
                SELECT SUM(o.total_amount) AS value
                FROM orders o
                WHERE o.order_date BETWEEN %s AND %s;
            """
        }
    },

    "city": {
        "revenue": {
            "top": """
                SELECT ci.city_name AS name,
                       SUM(o.total_amount) AS value
                FROM orders o
                JOIN customers cu ON o.customer_id = cu.customer_id
                JOIN cities ci ON cu.city_id = ci.city_id
                WHERE o.order_date BETWEEN %s AND %s
                GROUP BY ci.city_name
                ORDER BY value DESC
                LIMIT %s;
            """,
            "aggregate": """
                SELECT SUM(o.total_amount) AS value
                FROM orders o
                JOIN customers cu ON o.customer_id = cu.customer_id
                JOIN cities ci ON cu.city_id = ci.city_id
                WHERE o.order_date BETWEEN %s AND %s;
            """
        }
    },

    "category": {
        "revenue": {
            "top": """
                SELECT cat.category_name AS name,
                       SUM(oi.quantity * oi.item_price) AS value
                FROM order_items oi
                JOIN products p ON oi.product_id = p.product_id
                JOIN categories cat ON p.category_id = cat.category_id
                JOIN orders o ON oi.order_id = o.order_id
                WHERE o.order_date BETWEEN %s AND %s
                GROUP BY cat.category_name
                ORDER BY value DESC
                LIMIT %s;
            """,
            "aggregate": """
                SELECT SUM(oi.quantity * oi.item_price) AS value
                FROM order_items oi
                JOIN products p ON oi.product_id = p.product_id
                JOIN categories cat ON p.category_id = cat.category_id
                JOIN orders o ON oi.order_id = o.order_id
                WHERE o.order_date BETWEEN %s AND %s;
            """
        }
    }
}
