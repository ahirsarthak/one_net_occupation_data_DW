import re
import sqlite3
from typing import List, Tuple


def validate_preload(records: List[dict]) -> List[str]:
    """Lightweight checks: required fields present and duplicates in input."""
    errors: List[str] = []
    seen = set()
    for r in records:
        code = r.get("onetsoc_code", "")
        title = r.get("title", "")
        if not code or not title:
            errors.append(f"Missing required fields for record: {r}")
            continue
        if code in seen:
            errors.append(f"Duplicate onetsoc_code in input: {code}")
        seen.add(code)
    return errors


def validate_postload(conn: sqlite3.Connection) -> List[str]:
    """post-load checks for dim_occupation integrity."""
    errors: List[str] = []
    # Unique onetsoc_code (should be enforced by schema)
    cur = conn.execute(
        """
        SELECT onetsoc_code, COUNT(*)
        FROM dim_occupation
        GROUP BY onetsoc_code
        HAVING COUNT(*) > 1
        """
    )
    if cur.fetchall():
        errors.append("Duplicate onetsoc_code in dim_occupation")

    # Non-null required fields
    cur = conn.execute("SELECT COUNT(*) FROM dim_occupation WHERE onetsoc_code IS NULL OR title IS NULL")
    if cur.fetchone()[0] > 0:
        errors.append("Null required fields in dim_occupation")

    # Staging: no 'unavailable' in keys and SOC format check
    for table in ("stg_skills", "stg_knowledge", "stg_abilities"):
        cur = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE onetsoc_code = 'unavailable' OR element_id = 'unavailable' OR scale_id = 'unavailable'")
        if cur.fetchone()[0] > 0:
            errors.append(f"'{table}' has 'unavailable' in key columns")
        # SOC format: use LIKE shape check (SQLite lacks REGEXP)
        cur2 = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE onetsoc_code NOT LIKE '__-____.__'")
        if cur2.fetchone()[0] > 0:
            errors.append(f"'{table}' has invalid SOC format in onetsoc_code")
        # Duplicates in staging
        cur = conn.execute(f"SELECT COUNT(*) FROM (SELECT onetsoc_code, element_id, scale_id, COUNT(*) c FROM {table} GROUP BY 1,2,3 HAVING c > 1)")
        if cur.fetchone()[0] > 0:
            errors.append(f"Duplicate (onetsoc_code, element_id, scale_id) rows in {table}")

    # Fact grain uniqueness
    cur = conn.execute("SELECT COUNT(*) FROM fact_occupation_element_rating")
    total = cur.fetchone()[0]
    cur = conn.execute("SELECT COUNT(*) FROM (SELECT occupation_id, element_id, scale_id FROM fact_occupation_element_rating GROUP BY 1,2,3)")
    distincts = cur.fetchone()[0]
    if total != distincts:
        errors.append("Fact grain (occupation_id, element_id, scale_id) is not unique")

    # Fact elements join sanity
    cur = conn.execute("SELECT COUNT(*) FROM fact_occupation_element_rating f LEFT JOIN dim_element e ON e.element_id = f.element_id WHERE e.element_id IS NULL")
    if cur.fetchone()[0] > 0:
        errors.append("Fact has element_ids not present in dim_element")

    return errors


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


__all__ = ["validate_preload", "validate_postload", "validate_staging"]
