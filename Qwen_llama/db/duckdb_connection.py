"""DuckDB connection helpers for the analytics project."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Any

import duckdb

from config import DUCKDB_PATH

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DB_FILE = Path(DUCKDB_PATH)
if not _DB_FILE.is_absolute():
    _DB_FILE = (_PROJECT_ROOT / _DB_FILE).resolve()


def _ensure_parent_dir() -> None:
    _DB_FILE.parent.mkdir(parents=True, exist_ok=True)


def pg_to_duck(sql: str) -> str:
    """Convert psycopg2-style placeholders to DuckDB placeholders."""
    return sql.replace("%s", "?")


def get_read_connection() -> duckdb.DuckDBPyConnection:
    _ensure_parent_dir()
    return duckdb.connect(database=str(_DB_FILE), read_only=False)


def get_write_connection() -> duckdb.DuckDBPyConnection:
    _ensure_parent_dir()
    return duckdb.connect(database=str(_DB_FILE), read_only=False)


def run_query(sql: str, params: Iterable[Any] | None = None) -> list:
    q = pg_to_duck(sql)
    p = list(params or [])
    conn = get_read_connection()
    try:
        return conn.execute(q, p).fetchall()
    finally:
        conn.close()


def explain_query(sql: str, params: Iterable[Any] | None = None) -> tuple[bool, str]:
    q = pg_to_duck(sql)
    p = list(params or [])
    conn = get_read_connection()
    try:
        conn.execute(f"EXPLAIN {q}", p).fetchall()
        return True, ""
    except Exception as exc:
        return False, str(exc).split("\n")[0]
    finally:
        conn.close()
