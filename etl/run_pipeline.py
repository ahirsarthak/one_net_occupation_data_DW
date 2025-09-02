import argparse
import os
import sqlite3

# extract → transform → load
from etl.extract import load_onet_records
from etl.transform import clean_occupation_records, split_and_clean_ska_records
from etl.load import (
    init_db,
    load_stg_occupation,
    load_stg_skills,
    load_stg_knowledge,
    load_stg_abilities,
    load_stg_level_scale_anchors,
    load_stg_scales_reference,
    load_dim_scale_from_staging,
    load_dim_occupation,
    load_dim_major_group,
    load_dim_element,
    load_dim_element_scale_anchor,
    load_fact_occupation_element_rating,
)
from etl.load import load_stg_invalid_ska
from etl.validate import validate_staging


def run(raw_dir: str, db_path: str, schema_path: str) -> None:
    # Extract
    raw_records = load_onet_records(raw_dir, "occupation")
    print(f"[ETL] Occupation rows extracted: {len(raw_records)}")
    skills = load_onet_records(raw_dir, "skills")
    knowledge = load_onet_records(raw_dir, "knowledge")
    abilities = load_onet_records(raw_dir, "abilities")
    scales_ref = load_onet_records(raw_dir, "scales_reference")
    anchors = load_onet_records(raw_dir, "level_scale_anchors")
    if skills:
        print(f"[ETL] Skills rows extracted: {len(skills)}")
    if knowledge:
        print(f"[ETL] Knowledge rows extracted: {len(knowledge)}")
    if abilities:
        print(f"[ETL] Abilities rows extracted: {len(abilities)}")
    if anchors:
        print(f"[ETL] Level scale anchors extracted: {len(anchors)}")
    if scales_ref:
        print(f"[ETL] Scales reference rows extracted: {len(scales_ref)}")

    # Transform
    records = clean_occupation_records(raw_records)
    print(f"[ETL] Occupation rows cleaned: {len(records)}")
    skills_clean, skills_invalid = split_and_clean_ska_records(skills, "skill") if skills else ([], [])
    knowledge_clean, knowledge_invalid = split_and_clean_ska_records(knowledge, "knowledge") if knowledge else ([], [])
    abilities_clean, abilities_invalid = split_and_clean_ska_records(abilities, "ability") if abilities else ([], [])

    # Initialize DB and Load
    init_db(db_path, schema_path)
    mg_csv = os.path.join(raw_dir, "soc_major_groups.csv")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        # Load Silver staging with cleaned records
        occ_loaded = load_stg_occupation(conn, records)
        print(f"[ETL] Loaded stg_occupation_data: {occ_loaded}")
        if skills_clean:
            sk_loaded = load_stg_skills(conn, skills_clean)
            print(f"[ETL] Loaded stg_skills: {sk_loaded}")
        if knowledge_clean:
            kn_loaded = load_stg_knowledge(conn, knowledge_clean)
            print(f"[ETL] Loaded stg_knowledge: {kn_loaded}")
        if abilities_clean:
            ab_loaded = load_stg_abilities(conn, abilities_clean)
            print(f"[ETL] Loaded stg_abilities: {ab_loaded}")
        # Persist invalid SKA rows for diagnostics
        invalid_rows = []
        if skills_invalid:
            invalid_rows.extend(skills_invalid)
            print(f"[ETL][INVALID] skills dropped: {len(skills_invalid)}")
        if knowledge_invalid:
            invalid_rows.extend(knowledge_invalid)
            print(f"[ETL][INVALID] knowledge dropped: {len(knowledge_invalid)}")
        if abilities_invalid:
            invalid_rows.extend(abilities_invalid)
            print(f"[ETL][INVALID] abilities dropped: {len(abilities_invalid)}")
        if invalid_rows:
            inv_loaded = load_stg_invalid_ska(conn, invalid_rows)
            print(f"[ETL][INVALID] persisted to stg_invalid_ska: {inv_loaded}")
        if anchors:
            an_loaded = load_stg_level_scale_anchors(conn, anchors)
            print(f"[ETL] Loaded stg_level_scale_anchors: {an_loaded}")
        if scales_ref:
            sr_loaded = load_stg_scales_reference(conn, scales_ref)
            print(f"[ETL] Loaded stg_scales_reference: {sr_loaded}")
            ds = load_dim_scale_from_staging(conn)
            print(f"[ETL] Upserted dim_scale (IM/LV only): {ds}")
        # Staging-only validations before building dims/fact
        s_errors, s_summary = validate_staging(conn)
        if s_errors:
            print("[VALIDATE:STAGING][ERRORS]")
            for e in s_errors:
                print(f" - {e}")
        if s_summary:
            print("[VALIDATE:STAGING][SUMMARY]")
            for k, v in s_summary:
                print(f" - {k}: {v}")
        if os.path.exists(mg_csv):
            mg_loaded = load_dim_major_group(conn, mg_csv)
            print(f"[ETL] Upserted dim_major_group: {mg_loaded}")
        dim_loaded = load_dim_occupation(conn, records)
        print(f"[ETL] Upserted dim_occupation: {dim_loaded}")
        elem_loaded = load_dim_element(conn)
        print(f"[ETL] Upserted dim_element: {elem_loaded}")
        if anchors:
            anc_loaded = load_dim_element_scale_anchor(conn)
            print(f"[ETL] Upserted dim_element_scale: {anc_loaded}")
        fact_loaded = load_fact_occupation_element_rating(conn)
        print(f"[ETL] Upserted fact_occupation_element_rating: {fact_loaded}")

    print(f"[ETL] Done. SQLite at {db_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ONET occupation ETL into SQLite")
    parser.add_argument("--raw_dir", default=os.path.join("data", "raw"))
    parser.add_argument("--db_path", default=os.path.join("warehouse", "onet.db"))
    parser.add_argument("--schema", default=os.path.join("warehouse", "schema.sql"))
    args = parser.parse_args()
    run(args.raw_dir, args.db_path, args.schema)
