import argparse
import glob
import os
import sqlite3
import csv


def run_all(db_path: str, queries_dir: str, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        for qpath in sorted(glob.glob(os.path.join(queries_dir, "*.sql"))):
            name = os.path.splitext(os.path.basename(qpath))[0]
            with open(qpath, "r", encoding="utf-8") as f:
                sql = f.read()
            cur = conn.execute(sql)
            rows = cur.fetchall()
            headers = [d[0] for d in cur.description]
            out_csv = os.path.join(out_dir, f"{name}.csv")
            with open(out_csv, "w", encoding="utf-8", newline="") as out:
                writer = csv.writer(out)
                writer.writerow(headers)
                writer.writerows(rows)
            print(f"[QUERY] {name}: {len(rows)} rows -> {out_csv}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run all SQL queries against the SQLite DB")
    parser.add_argument("--db_path", default=os.path.join("warehouse", "onet.db"))
    parser.add_argument("--queries_dir", default="queries")
    parser.add_argument("--out_dir", default=os.path.join("outputs", "queries"))
    args = parser.parse_args()
    run_all(args.db_path, args.queries_dir, args.out_dir)

