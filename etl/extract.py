import os
import sqlite3
from typing import List, Dict


def _read_sql_strip_go(path: str) -> str:
    """Read a SQL dump file and strip SQL Server GO batch separators.
    Returns the cleaned SQL text suitable for SQLite.executescript.
    """
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    lines = [ln for ln in sql.splitlines() if ln.strip().upper() != "GO"]
    return "\n".join(lines)


def _select_from_sql(path: str, select_sql: str, coerce_strip: bool = False) -> List[Dict[str, str]]:
    """Execute a SQL dump in an in-memory SQLite DB and run a SELECT.
    - Strips 'GO' statements automatically
    - Returns rows as a list of dicts
    - If coerce_strip=True, trims string fields (occupation rows)
    """
    cleaned = _read_sql_strip_go(path)
    with sqlite3.connect(":memory:") as conn:
        conn.executescript(cleaned)
        cur = conn.execute(select_sql)
        cols = [d[0] for d in cur.description]
        out: List[Dict[str, str]] = []
        for row in cur.fetchall():
            rec = {k: v for k, v in zip(cols, row)}
            if coerce_strip:
                for key in ("onetsoc_code", "title", "description"):
                    if key in rec and rec[key] is not None:
                        rec[key] = str(rec[key]).strip()
            out.append(rec)
    return out


def _path_or_none(raw_dir: str, filename: str) -> str | None:
    """Return absolute path if file exists, else None."""
    path = os.path.join(raw_dir, filename)
    return path if os.path.exists(path) else None


# Unified domain configuration for consistent behavior
def _select_occupation(raw_dir: str) -> List[Dict[str, str]]:
    path = _path_or_none(raw_dir, "03_occupation_data.sql")
    if not path:
        raise FileNotFoundError("Missing required file: 03_occupation_data.sql")
    return _select_from_sql(
        path,
        "SELECT onetsoc_code, title, description FROM occupation_data",
        coerce_strip=True,
    )


def _select_domain(raw_dir: str, filename: str, table: str) -> List[Dict[str, str]]:
    path = _path_or_none(raw_dir, filename)
    if not path:
        return []
    return _select_from_sql(
        path,
        f"""
        SELECT onetsoc_code, element_id, scale_id, data_value, n, standard_error,
               lower_ci_bound, upper_ci_bound, recommend_suppress, not_relevant,
               date_updated, domain_source
        FROM {table}
        """,
    )


def load_onet_records(raw_dir: str, domain: str) -> List[Dict[str, str]]:
    """Load rows from canonical O*NET SQL files (no fallbacks)."""
    domain = domain.lower().strip()
    if domain == "occupation":
        return _select_occupation(raw_dir)
    if domain == "skills":
        return _select_domain(raw_dir, "16_skills.sql", "skills")
    if domain == "knowledge":
        return _select_domain(raw_dir, "15_knowledge.sql", "knowledge")
    if domain == "abilities":
        return _select_domain(raw_dir, "11_abilities.sql", "abilities")
    if domain == "level_scale_anchors":
        path = _path_or_none(raw_dir, "06_level_scale_anchors.sql")
        if not path:
            return []
        return _select_from_sql(
            path,
            """
            SELECT element_id, scale_id, anchor_value, anchor_description
            FROM level_scale_anchors
            """,
        )
    if domain == "scales_reference":
        path = _path_or_none(raw_dir, "04_scales_reference.sql")
        if not path:
            return []
        return _select_from_sql(
            path,
            """
            SELECT scale_id, scale_name, minimum, maximum
            FROM scales_reference
            """,
        )
    raise ValueError(f"Unsupported domain: {domain}")


__all__ = ["load_onet_records"]
