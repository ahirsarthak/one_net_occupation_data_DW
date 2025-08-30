import re
import sqlite3
from typing import List, Tuple


def validate_staging(conn: sqlite3.Connection) -> Tuple[List[str], List[Tuple[str, str]]]:
    """Simple validation focused on staging (extract→load), not dims/facts.
    Returns (errors, summary) where summary is a list of (label, value).
    """
    errors: List[str] = []
    summary: List[Tuple[str, str]] = []

    def add_sum(label: str, sql: str) -> None:
        cur = conn.execute(sql)
        summary.append((label, str(cur.fetchone()[0])))

    # Rows present in staging
    for table in ("stg_occupation_data", "stg_skills", "stg_knowledge", "stg_abilities"):
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if cur.fetchone():
            add_sum(f"rows_{table}", f"SELECT COUNT(*) FROM {table}")

    # Staging key checks (no duplicate check per request)
    for table in ("stg_skills", "stg_knowledge", "stg_abilities"):
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if not cur.fetchone():
            continue
        # No 'unavailable' in keys
        cur = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE onetsoc_code = 'unavailable' OR element_id = 'unavailable' OR scale_id = 'unavailable'"
        )
        if cur.fetchone()[0] > 0:
            errors.append(f"'{table}' has 'unavailable' in key columns")
        # SOC format shape (LIKE mask)
        cur = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE onetsoc_code NOT LIKE '__-____.__'")
        if cur.fetchone()[0] > 0:
            errors.append(f"'{table}' has invalid SOC format in onetsoc_code")

    return errors, summary


__all__ = ["validate_staging"]
