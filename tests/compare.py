"""Compare pipeline Parquet output with expected CSVs (reference.py output or dev-answer).

usage: python compare.py OUTPUT_DIR EXPECTED_DIR
"""
import sys

import duckdb

out, exp = sys.argv[1], sys.argv[2]
con = duckdb.connect()
ok = True
for t, key in [("user_daily", "user_id, day"), ("campaign_daily", "campaign_id, day")]:
    con.execute(f"CREATE OR REPLACE TABLE got AS SELECT * FROM read_parquet('{out}/{t}/*.parquet')")
    con.execute(f"CREATE OR REPLACE TABLE want AS SELECT * FROM read_csv('{exp}/{t}.csv', header=true, all_varchar=true)")
    cols = [r[0] for r in con.execute("DESCRIBE want").fetchall()]
    gcols = [r[0] for r in con.execute("DESCRIBE got").fetchall()]
    if sorted(cols) != sorted(gcols):
        print(t, "COLUMN MISMATCH", cols, gcols)
        ok = False
        continue
    dtype = dict((r[0], r[1]) for r in con.execute("DESCRIBE got").fetchall())
    if dtype["day"] != "DATE":
        print(t, "day is", dtype["day"])
        ok = False
    norm = ", ".join(f"coalesce(CAST({c} AS VARCHAR), '') AS {c}" for c in cols)
    a = f"SELECT {norm} FROM got"
    b = f"SELECT {norm} FROM want"
    extra = con.execute(f"SELECT count(*) FROM ({a} EXCEPT ALL {b})").fetchall()[0][0]
    missing = con.execute(f"SELECT count(*) FROM ({b} EXCEPT ALL {a})").fetchall()[0][0]
    n = con.execute("SELECT count(*) FROM want").fetchall()[0][0]
    print(f"{t}: expected {n} rows, extra {extra}, missing {missing}")
    if extra or missing:
        ok = False
        print(" sample extra:", con.execute(f"SELECT * FROM ({a} EXCEPT ALL {b}) ORDER BY {key} LIMIT 5").fetchall())
        print(" sample missing:", con.execute(f"SELECT * FROM ({b} EXCEPT ALL {a}) ORDER BY {key} LIMIT 5").fetchall())
print("PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
