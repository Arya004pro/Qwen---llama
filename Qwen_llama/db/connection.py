"""Compatibility shim for legacy imports."""

from db.duckdb_connection import get_read_connection as get_connection

__all__ = ["get_connection"]
