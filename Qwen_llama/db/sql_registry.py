"""db/sql_registry.py

Backward-compatible stub.  The old registry contained 460 lines of SQL
hardcoded to a 7-table e-commerce schema.  That has been removed.

Any call site that attempts ``SQL_REGISTRY[entity][metric][ranking]``
will now receive a ``KeyError`` and fall through to the LLM SQL
generation path — which is the desired behaviour for arbitrary datasets.
"""

SQL_REGISTRY: dict = {}