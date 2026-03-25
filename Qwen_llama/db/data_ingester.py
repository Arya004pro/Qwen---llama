"""User data ingestion helpers for DuckDB."""

from __future__ import annotations

import base64
import re
from pathlib import Path
from typing import Any

from config import DUCKDB_PATH
from db.duckdb_connection import get_write_connection

_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _safe_table_name(name: str, used: set[str]) -> str:
    stem = Path(name).stem.lower()
    stem = re.sub(r"[^a-z0-9_]+", "_", stem).strip("_") or "uploaded_data"
    table = stem
    i = 2
    while table in used:
        table = f"{stem}_{i}"
        i += 1
    used.add(table)
    return table


def _write_temp_file(name: str, b64_data: str, upload_dir: Path) -> Path:
    upload_dir.mkdir(parents=True, exist_ok=True)
    p = upload_dir / name
    p.write_bytes(base64.b64decode(b64_data))
    return p


def _sql_quote_path(file_path: Path) -> str:
    return str(file_path.resolve()).replace("\\", "/").replace("'", "''")


def _resolve_input_file(file_obj: dict[str, Any], upload_dir: Path) -> Path:
    raw_path = str(file_obj.get("path") or "").strip()
    if raw_path:
        p = Path(raw_path)
        if p.exists() and p.is_file():
            return p
    name = str(file_obj.get("name") or "").strip()
    data = str(file_obj.get("data") or "").strip()
    if name and data:
        return _write_temp_file(name, data, upload_dir)
    raise ValueError("Each file must include either a valid path or (name + data).")


def _load_file_to_table(conn, file_path: Path, table: str) -> int:
    ext = file_path.suffix.lower()
    p = _sql_quote_path(file_path)
    if ext in (".csv", ".tsv"):
        delim_clause = "delim='\\t'," if ext == ".tsv" else "delim=',',"
        try:
            conn.execute(
                f'CREATE OR REPLACE TABLE "{table}" AS '
                f"SELECT * FROM read_csv_auto('{p}', {delim_clause} header=true)"
            )
        except Exception:
            # Fallback for messy CSVs: read all columns as text and ignore malformed rows.
            conn.execute(
                f'CREATE OR REPLACE TABLE "{table}" AS '
                f"SELECT * FROM read_csv_auto('{p}', {delim_clause} header=true, sample_size=-1, "
                "all_varchar=true, ignore_errors=true, null_padding=true)"
            )
    elif ext in (".json", ".jsonl"):
        conn.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM read_json_auto(\'{p}\')')
    elif ext == ".parquet":
        conn.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM read_parquet(\'{p}\')')
    else:
        raise ValueError(f"Unsupported file format: {ext}")
    return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


def ingest_files(files: list[dict[str, Any]], reset_db: bool = False) -> dict[str, Any]:
    if not files:
        raise ValueError("No files provided")

    db_file = Path(DUCKDB_PATH)
    if not db_file.is_absolute():
        db_file = (_PROJECT_ROOT / db_file).resolve()
    upload_dir = db_file.parent / "uploads"
    conn = get_write_connection()
    used: set[str] = set()
    created: list[str] = []
    row_counts: dict[str, int] = {}
    try:
        if reset_db:
            existing = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
            for (t,) in existing:
                conn.execute(f'DROP TABLE IF EXISTS "{t}"')

        for f in files:
            fp = _resolve_input_file(f, upload_dir)
            table = _safe_table_name(fp.name, used)
            n = _load_file_to_table(conn, fp, table)
            created.append(table)
            row_counts[table] = n

        schema = []
        for t in created:
            cols = conn.execute(f'DESCRIBE "{t}"').fetchall()
            schema.append(
                {
                    "table": t,
                    "columns": [{"name": c[0], "type": c[1]} for c in cols],
                }
            )
        return {
            "tables_created": created,
            "row_counts": row_counts,
            "relationships": [],
            "schema": schema,
        }
    finally:
        conn.close()
