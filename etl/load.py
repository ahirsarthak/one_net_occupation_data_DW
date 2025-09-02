import os
import csv
import sqlite3
from typing import Any, Dict, List, Optional, Sequence


def init_db(db_path: str, schema_path: str) -> None:
    """Initialize SQLite database and apply schema script."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    # Recreate DB fresh to ensure schema changes apply cleanly during development
    if os.path.exists(db_path):
        os.remove(db_path)
    with sqlite3.connect(db_path) as conn, open(schema_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())


# processed_file tracking removed for simplicity


def load_stg_occupation(conn: sqlite3.Connection, records: Sequence[Dict[str, Any]]) -> int:
    """Truncate and load staging for occupations. Returns count loaded."""
    conn.execute("DELETE FROM stg_occupation_data")
    if records:
        conn.executemany(
            """
            INSERT INTO stg_occupation_data (onetsoc_code, title, description)
            VALUES (:onetsoc_code, :title, :description)
            """,
            records,
        )
    return len(records)


def load_dim_occupation(conn: sqlite3.Connection, records: Sequence[Dict[str, Any]]) -> int:
    """Upsert occupations by onetsoc_code. Returns count processed."""
    if records:
        conn.executemany(
            """
            INSERT INTO dim_occupation (onetsoc_code, title, description, major_group_code)
            VALUES (:onetsoc_code, :title, :description, :major_group_code)
            ON CONFLICT(onetsoc_code) DO UPDATE SET
              title = excluded.title,
              description = excluded.description,
              major_group_code = excluded.major_group_code
            """,
            records,
        )
    return len(records)


def load_dim_major_group(conn: sqlite3.Connection, csv_path: str) -> int:
    """Load SOC major group lookup from CSV. Returns count loaded.
    CSV columns: code_full,name; derives 2-digit code as PK.
    """
    if not os.path.exists(csv_path):
        return 0
    rows: List[tuple] = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            full = (r.get("code_full") or "").strip()
            name = (r.get("name") or "").strip()
            if len(full) >= 2 and name:
                code = full[:2]
                rows.append((code, full, name))
    if rows:
        conn.executemany(
            """
            INSERT INTO dim_major_group (major_group_code, code_full, name)
            VALUES (?, ?, ?)
            ON CONFLICT(major_group_code) DO UPDATE SET
              code_full = excluded.code_full,
              name = excluded.name
            """,
            rows,
        )
    return len(rows)

def load_stg_skills(conn: sqlite3.Connection, records: Sequence[Dict[str, Any]]) -> int:
    """Truncate and load skills staging. Returns count loaded."""
    if not records:
        return 0
    conn.execute("DELETE FROM stg_skills")
    conn.executemany(
        """
        INSERT INTO stg_skills (
            onetsoc_code, element_id, scale_id, data_value, n, standard_error,
            lower_ci_bound, upper_ci_bound, recommend_suppress, not_relevant,
            date_updated, domain_source
        ) VALUES (
            :onetsoc_code, :element_id, :scale_id, :data_value, :n, :standard_error,
            :lower_ci_bound, :upper_ci_bound, :recommend_suppress, :not_relevant,
            :date_updated, :domain_source
        )
        """,
        records,
    )
    return len(records)


def load_stg_knowledge(conn: sqlite3.Connection, records: Sequence[Dict[str, Any]]) -> int:
    """Truncate and load knowledge staging. Returns count loaded."""
    if not records:
        return 0
    conn.execute("DELETE FROM stg_knowledge")
    conn.executemany(
        """
        INSERT INTO stg_knowledge (
            onetsoc_code, element_id, scale_id, data_value, n, standard_error,
            lower_ci_bound, upper_ci_bound, recommend_suppress, not_relevant,
            date_updated, domain_source
        ) VALUES (
            :onetsoc_code, :element_id, :scale_id, :data_value, :n, :standard_error,
            :lower_ci_bound, :upper_ci_bound, :recommend_suppress, :not_relevant,
            :date_updated, :domain_source
        )
        """,
        records,
    )
    return len(records)


def load_stg_abilities(conn: sqlite3.Connection, records: Sequence[Dict[str, Any]]) -> int:
    """Truncate and load abilities staging. Returns count loaded."""
    if not records:
        return 0
    conn.execute("DELETE FROM stg_abilities")
    conn.executemany(
        """
        INSERT INTO stg_abilities (
            onetsoc_code, element_id, scale_id, data_value, n, standard_error,
            lower_ci_bound, upper_ci_bound, recommend_suppress, not_relevant,
            date_updated, domain_source
        ) VALUES (
            :onetsoc_code, :element_id, :scale_id, :data_value, :n, :standard_error,
            :lower_ci_bound, :upper_ci_bound, :recommend_suppress, :not_relevant,
            :date_updated, :domain_source
        )
        """,
        records,
    )
    return len(records)


def load_stg_level_scale_anchors(conn: sqlite3.Connection, records: Sequence[Dict[str, Any]]) -> int:
    """Truncate and load level scale anchors staging. Returns count loaded."""
    if not records:
        return 0
    conn.execute("DELETE FROM stg_level_scale_anchors")
    conn.executemany(
        """
        INSERT INTO stg_level_scale_anchors (
            element_id, scale_id, anchor_value, anchor_description
        ) VALUES (
            :element_id, :scale_id, :anchor_value, :anchor_description
        )
        """,
        records,
    )
    return len(records)


def load_stg_scales_reference(conn: sqlite3.Connection, records: Sequence[Dict[str, Any]]) -> int:
    """Truncate and load scales reference staging. Returns count loaded."""
    if not records:
        return 0
    conn.execute("DELETE FROM stg_scales_reference")
    conn.executemany(
        """
        INSERT INTO stg_scales_reference (
            scale_id, scale_name, minimum, maximum
        ) VALUES (
            :scale_id, :scale_name, :minimum, :maximum
        )
        """,
        records,
    )
    return len(records)


def load_dim_scale_from_staging(conn: sqlite3.Connection) -> int:
    """Populate dim_scale only with IM and LV from staging. Replaces existing rows."""
    conn.execute("DELETE FROM dim_scale")
    conn.execute(
        """
        INSERT INTO dim_scale (scale_id, name, min_value, max_value, step)
        SELECT scale_id, scale_name, MIN(minimum), MAX(maximum), NULL
        FROM stg_scales_reference
        WHERE scale_id IN ('IM','LV')
        GROUP BY scale_id, scale_name
        """
    )
    cur = conn.execute("SELECT COUNT(*) FROM dim_scale")
    return int(cur.fetchone()[0])


def load_dim_element(conn: sqlite3.Connection) -> int:
    """Upsert dim_element from distinct element_ids in SKA staging.
    Returns number of distinct elements observed in staging.
    """
    # Count distinct elements across all SKA staging tables
    cur = conn.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT DISTINCT element_id, 'SKILL' AS domain FROM stg_skills
          UNION
          SELECT DISTINCT element_id, 'KNOWLEDGE' FROM stg_knowledge
          UNION
          SELECT DISTINCT element_id, 'ABILITY' FROM stg_abilities
        ) u
        """
    )
    total = cur.fetchone()[0]
    # Upsert into dim_element
    conn.execute(
        """
        INSERT OR REPLACE INTO dim_element (element_id, domain)
        SELECT element_id, domain FROM (
          SELECT DISTINCT element_id, 'SKILL' AS domain FROM stg_skills
          UNION
          SELECT DISTINCT element_id, 'KNOWLEDGE' FROM stg_knowledge
          UNION
          SELECT DISTINCT element_id, 'ABILITY' FROM stg_abilities
        ) s
        """
    )
    return int(total)


def load_dim_element_scale_anchor(conn: sqlite3.Connection) -> int:
    """Upsert anchors from staging into dim_element_scale (canonical anchor dim).
    Returns number of staging rows considered (may be fewer inserted due to PK).
    """
    cur = conn.execute("SELECT COUNT(*) FROM stg_level_scale_anchors")
    total = cur.fetchone()[0]
    conn.execute(
        """
        INSERT INTO dim_element_scale (element_id, scale_id, anchor_value, anchor_description)
        SELECT a.element_id, a.scale_id, a.anchor_value, a.anchor_description
        FROM stg_level_scale_anchors a
        JOIN dim_element e ON e.element_id = a.element_id
        JOIN dim_scale s   ON s.scale_id   = a.scale_id
        ON CONFLICT(element_id, scale_id, anchor_value) DO UPDATE SET
          anchor_description = excluded.anchor_description
        """
    )
    return int(total)


def load_fact_occupation_element_rating(conn: sqlite3.Connection) -> int:
    """Upsert fact_occupation_element_rating from SKA staging tables.
    Returns number of staging rows joined to dim_occupation (may update existing).
    """
    # Count rows to be inserted from all three staging tables after join to occupations
    cur = conn.execute(
        """
        SELECT (
          SELECT COUNT(*) FROM stg_skills s JOIN dim_occupation d ON d.onetsoc_code = s.onetsoc_code
        ) + (
          SELECT COUNT(*) FROM stg_knowledge k JOIN dim_occupation d ON d.onetsoc_code = k.onetsoc_code
        ) + (
          SELECT COUNT(*) FROM stg_abilities a JOIN dim_occupation d ON d.onetsoc_code = a.onetsoc_code
        ) AS total
        """
    )
    total = cur.fetchone()[0]
    # Upsert from skills
    conn.execute(
        """
        INSERT INTO fact_occupation_element_rating (
          occupation_id, element_id, scale_id, data_value, n, standard_error,
          lower_ci_bound, upper_ci_bound, recommend_suppress, not_relevant,
          date_updated, domain_source
        )
        SELECT d.occupation_id, s.element_id, s.scale_id, s.data_value, s.n, s.standard_error,
               s.lower_ci_bound, s.upper_ci_bound, s.recommend_suppress, s.not_relevant,
               s.date_updated, s.domain_source
        FROM stg_skills s
        JOIN dim_occupation d ON d.onetsoc_code = s.onetsoc_code
        ON CONFLICT(occupation_id, element_id, scale_id) DO UPDATE SET
          data_value = excluded.data_value,
          n = excluded.n,
          standard_error = excluded.standard_error,
          lower_ci_bound = excluded.lower_ci_bound,
          upper_ci_bound = excluded.upper_ci_bound,
          recommend_suppress = excluded.recommend_suppress,
          not_relevant = excluded.not_relevant,
          date_updated = excluded.date_updated,
          domain_source = excluded.domain_source
        """
    )
    # Upsert from knowledge
    conn.execute(
        """
        INSERT INTO fact_occupation_element_rating (
          occupation_id, element_id, scale_id, data_value, n, standard_error,
          lower_ci_bound, upper_ci_bound, recommend_suppress, not_relevant,
          date_updated, domain_source
        )
        SELECT d.occupation_id, k.element_id, k.scale_id, k.data_value, k.n, k.standard_error,
               k.lower_ci_bound, k.upper_ci_bound, k.recommend_suppress, k.not_relevant,
               k.date_updated, k.domain_source
        FROM stg_knowledge k
        JOIN dim_occupation d ON d.onetsoc_code = k.onetsoc_code
        ON CONFLICT(occupation_id, element_id, scale_id) DO UPDATE SET
          data_value = excluded.data_value,
          n = excluded.n,
          standard_error = excluded.standard_error,
          lower_ci_bound = excluded.lower_ci_bound,
          upper_ci_bound = excluded.upper_ci_bound,
          recommend_suppress = excluded.recommend_suppress,
          not_relevant = excluded.not_relevant,
          date_updated = excluded.date_updated,
          domain_source = excluded.domain_source
        """
    )
    # Upsert from abilities
    conn.execute(
        """
        INSERT INTO fact_occupation_element_rating (
          occupation_id, element_id, scale_id, data_value, n, standard_error,
          lower_ci_bound, upper_ci_bound, recommend_suppress, not_relevant,
          date_updated, domain_source
        )
        SELECT d.occupation_id, a.element_id, a.scale_id, a.data_value, a.n, a.standard_error,
               a.lower_ci_bound, a.upper_ci_bound, a.recommend_suppress, a.not_relevant,
               a.date_updated, a.domain_source
        FROM stg_abilities a
        JOIN dim_occupation d ON d.onetsoc_code = a.onetsoc_code
        ON CONFLICT(occupation_id, element_id, scale_id) DO UPDATE SET
          data_value = excluded.data_value,
          n = excluded.n,
          standard_error = excluded.standard_error,
          lower_ci_bound = excluded.lower_ci_bound,
          upper_ci_bound = excluded.upper_ci_bound,
          recommend_suppress = excluded.recommend_suppress,
          not_relevant = excluded.not_relevant,
          date_updated = excluded.date_updated,
          domain_source = excluded.domain_source
        """
    )
    return int(total)

def load_stg_invalid_ska(conn: sqlite3.Connection, records: Sequence[Dict[str, Any]]) -> int:
    """Truncate and load invalid SKA records into stg_invalid_ska for diagnostics."""
    conn.execute("DELETE FROM stg_invalid_ska")
    if not records:
        return 0
    cols = [
        "domain",
        "onetsoc_code",
        "element_id",
        "scale_id",
        "data_value",
        "n",
        "standard_error",
        "lower_ci_bound",
        "upper_ci_bound",
        "recommend_suppress",
        "not_relevant",
        "date_updated",
        "domain_source",
        "error_reason",
    ]
    placeholders = ",".join(":" + c for c in cols)
    # Ensure every record has the expected keys
    recs = []
    for r in records:
        rec = {k: (None if k not in r else r[k]) for k in cols}
        recs.append(rec)
    conn.executemany(
        f"INSERT INTO stg_invalid_ska ({','.join(cols)}) VALUES ({placeholders})",
        recs,
    )
    return len(recs)

__all__ = [
    "init_db",
    "load_stg_occupation",
    "load_dim_occupation",
    "load_dim_major_group",
    "load_stg_scales_reference",
    "load_stg_skills",
    "load_stg_knowledge",
    "load_stg_abilities",
    "load_stg_level_scale_anchors",
    "load_dim_element",
    "load_dim_element_scale_anchor",
    "load_fact_occupation_element_rating",
    "load_stg_invalid_ska",
]
