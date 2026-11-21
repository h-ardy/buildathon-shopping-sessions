"""Fast synthetic data for timing only (not for correctness). usage: python mkbig.py OUT N_EVENTS"""
import json
import os
import sys

import duckdb

out, n = sys.argv[1], int(sys.argv[2])
users = n // 30
ws = 1790812800000  # 2026-10-01
con = duckdb.connect()
for d in ("events", "clicks", "links"):
    os.makedirs(f"{out}/{d}", exist_ok=True)
con.execute(f"""
CREATE TABLE e AS SELECT
  'e' || i AS event_id,
  'u' || (CAST(pow(random(), 2) * {users} AS BIGINT)) || '_' || (i % 3) AS user_id,
  {ws} + CAST(random() * 14 * 86400 AS BIGINT) * 1000 AS t,
  CASE WHEN random() < 0.97 THEN (CASE WHEN random() < 0.8 THEN 'view' ELSE 'cart' END) ELSE 'purchase' END AS event_type,
  'p' || CAST(random() * 50000 AS BIGINT) AS product_id,
  CAST(100 + random() * 50000 AS BIGINT) AS price_cents,
  i % 40 AS f
FROM range({n}) r(i)
""")
for f in range(40):
    con.execute(f"""COPY (SELECT event_id, CASE WHEN random() < 0.2 THEN '"' || user_id || '"' ELSE user_id END AS user_id,
      CASE WHEN random() < 0.6 THEN strftime(to_timestamp(t / 1000), '%Y-%m-%dT%H:%M:%SZ') ELSE CAST(t AS VARCHAR) END AS ts,
      strftime(to_timestamp(t / 1000 + 5), '%Y-%m-%dT%H:%M:%SZ') AS ingest_ts, event_type, product_id, price_cents
      FROM e WHERE f = {f}) TO '{out}/events/part-{f}.csv' (HEADER, QUOTE '', ESCAPE '')""")
con.execute(f"""COPY (SELECT 'k' || i AS click_id, 'u' || (CAST(pow(random(), 2) * {users} AS BIGINT)) || '_0' AS user_id,
   'c_' || CAST(random() * 2000 AS BIGINT) AS campaign_id,
   strftime(to_timestamp(({ws} - 7 * 86400000 + CAST(random() * 21 * 86400 AS BIGINT) * 1000) / 1000), '%Y-%m-%dT%H:%M:%SZ') AS ts
   FROM range({n // 5}) r(i)) TO '{out}/clicks/part-0.csv' (HEADER, QUOTE '', ESCAPE '')""")
con.execute(f"""COPY (SELECT 'u' || i || '_0' AS user_id_a, 'u' || i || '_' || (1 + i % 2) AS user_id_b FROM range({users}) r(i))
   TO '{out}/links/part-0.csv' (HEADER, QUOTE '', ESCAPE '')""")
con.execute(f"COPY (SELECT 'p' || i AS product_id, 'cat' || (i % 20) AS category FROM range(50000) r(i)) TO '{out}/products.csv' (HEADER)")
con.execute(f"COPY (SELECT 'c_' || i AS campaign_id, 'ch' || (i % 5) AS channel FROM range(2000) r(i)) TO '{out}/campaigns.csv' (HEADER)")
json.dump({"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}, open(f"{out}/manifest.json", "w"))
