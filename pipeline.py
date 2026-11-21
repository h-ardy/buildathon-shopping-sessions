"""Shopping Sessions pipeline (DuckDB + scipy).

Reads /data, writes /output/user_daily/ and /output/campaign_daily/ exactly as rules.md says.
"""
import glob
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import duckdb
import numpy as np
import pyarrow as pa
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:7.2f}s] {msg}", file=sys.stderr, flush=True)


DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 4))
MEM_GB = int(os.environ.get("MEM_GB", 16))

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
GAP_MS = 1_800_000

with open(os.path.join(DATA, "manifest.json")) as f:
    man = json.load(f)

os.makedirs(os.path.join(SCRATCH, 'duck_tmp'), exist_ok=True)
con = duckdb.connect()
con.execute(f"SET threads={NCPU}")
con.execute(f"SET memory_limit='{max(MEM_GB - 4, 2)}GB'")
con.execute(f"SET temp_directory='{os.path.join(SCRATCH, 'duck_tmp')}'")
con.execute("SET preserve_insertion_order=false")
con.execute("SET TimeZone='UTC'")

# ---------- helpers ----------
# One field: either wrapped whole in quotes, or no quote at all. No value has a comma.
FIELD = '("[^",]*"|[^",]*)'


def line_pattern(n):
    return "^" + ",".join([FIELD] * n) + "$"


def has_cr(path):
    with open(path, "rb") as f:
        while True:
            b = f.read(1 << 24)
            if not b:
                return False
            if b"\r" in b:
                return True


def split_files(sub):
    files = sorted(glob.glob(os.path.join(DATA, sub, "*.csv")))
    with ThreadPoolExecutor(max_workers=NCPU) as ex:
        flags = list(ex.map(has_cr, files))
    fast = [f for f, c in zip(files, flags) if not c]
    slow = [f for f, c in zip(files, flags) if c]
    return fast, slow


def sql_list(files):
    return "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"


def raw_fields(sub, names):
    """SQL for a relation with one VARCHAR column per name: the raw (still quoted) fields of every
    well-formed line, where well-formed means the right field count and quotes only around whole fields.
    Files with a \r use an exact line splitter (DuckDB's CSV reader would treat \r as a line end)."""
    n = len(names)
    fast, slow = split_files(sub)
    parts = []
    if fast:
        cols = "{" + ", ".join(f"'{c}': 'VARCHAR'" for c in names) + "}"
        okq = " AND ".join(
            f"({c} IS NULL OR NOT contains({c}, '\"') OR (length({c}) >= 2 AND starts_with({c}, '\"') "
            f"AND ends_with({c}, '\"') AND NOT contains({c}[2:-2], '\"')))"
            for c in names
        )
        parts.append(
            f"SELECT {', '.join(names)} FROM read_csv({sql_list(fast)}, header=true, quote='', escape='', "
            f"delim=',', auto_detect=false, columns={cols}, strict_mode=true, ignore_errors=true, "
            f"null_padding=false, new_line='\\n') WHERE {okq}"
        )
    if slow:
        pat = line_pattern(n)
        keys = ", ".join(f"'{c}'" for c in names)
        parts.append(
            f"SELECT {', '.join('r.' + c + ' AS ' + c for c in names)} FROM ("
            f"SELECT regexp_extract(l, '{pat}', [{keys}]) AS r FROM ("
            f"SELECT unnest(string_split(content, chr(10))[2:]) AS l FROM read_text({sql_list(slow)})"
            f") WHERE regexp_full_match(l, '{pat}'))"
        )
    if not parts:
        return "SELECT " + ", ".join(f"NULL::VARCHAR AS {c}" for c in names) + " WHERE false"
    return " UNION ALL ".join(parts)


def val(expr):
    # strip the syntax quotes, empty means null
    return f"NULLIF(trim({expr}, '\"'), '')"


def ts_to_ms(s):
    """SQL expression: the F1/F2/F3 string s -> epoch ms, NULL if bad."""
    iso = (
        f"regexp_full_match({s}, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}"
        f"(Z|[+-][0-9]{{2}}:[0-9]{{2}})')"
    )
    d = f"try_cast(substr({s},1,10) AS DATE)"
    hh = f"try_cast(substr({s},12,2) AS BIGINT)"
    mi = f"try_cast(substr({s},15,2) AS BIGINT)"
    ss = f"try_cast(substr({s},18,2) AS BIGINT)"
    oh = f"try_cast(substr({s},21,2) AS BIGINT)"
    om = f"try_cast(substr({s},24,2) AS BIGINT)"
    off = (
        f"(CASE WHEN length({s}) = 20 THEN 0 "
        f"WHEN {oh} < 24 AND {om} < 60 THEN "
        f"(CASE WHEN substr({s},20,1) = '+' THEN 1 ELSE -1 END) * ({oh}*3600000 + {om}*60000) "
        f"ELSE NULL END)"
    )
    iso_ms = (
        f"(CASE WHEN {d} IS NOT NULL AND {hh} < 24 AND {mi} < 60 AND {ss} < 60 THEN "
        f"date_diff('day', DATE '1970-01-01', {d}) * {DAY_MS} + {hh}*3600000 + {mi}*60000 + {ss}*1000 - {off} "
        f"ELSE NULL END)"
    )
    return (
        f"(CASE WHEN regexp_full_match({s}, '[0-9]+') THEN try_cast({s} AS BIGINT) "
        f"WHEN {iso} THEN {iso_ms} ELSE NULL END)"
    )


def iso_to_ms_literal(s):
    return con.execute(f"SELECT {ts_to_ms('?')}".replace("?", "'" + s + "'")).fetchall()[0][0]


WS = iso_to_ms_literal(man["window_start"])
WE = iso_to_ms_literal(man["window_end"])
CWS = WS - WEEK_MS
log(f"window {man['window_start']} .. {man['window_end']}")

# ---------- linked IDs ----------
con.execute(f"""
CREATE TEMP TABLE links AS
SELECT a, b FROM (
  SELECT {val("a")} AS a, {val("b")} AS b FROM ({raw_fields('links', ['a', 'b'])})
) WHERE a IS NOT NULL AND b IS NOT NULL
""")
con.execute("""
CREATE TEMP TABLE ids AS
SELECT id, (row_number() OVER () - 1)::BIGINT AS n FROM (SELECT a AS id FROM links UNION SELECT b FROM links)
""")
nid = con.execute("SELECT count(*) FROM ids").fetchall()[0][0]
edges = con.execute("""
SELECT ia.n AS x, ib.n AS y FROM links JOIN ids ia ON links.a = ia.id JOIN ids ib ON links.b = ib.id
""").fetchnumpy()
if nid:
    x = np.asarray(edges["x"], dtype=np.int64)
    y = np.asarray(edges["y"], dtype=np.int64)
    g = coo_matrix((np.ones(len(x), dtype=np.int8), (x, y)), shape=(nid, nid)).tocsr()
    _, labels = connected_components(g, directed=False)
else:
    labels = np.zeros(0, dtype=np.int64)
lab = pa.table({"n": np.arange(nid, dtype=np.int64), "lab": labels.astype(np.int64)})
con.register("lab_arrow", lab)
con.execute("""
CREATE TEMP TABLE pmap AS
SELECT id, min(id) OVER (PARTITION BY lab) AS person
FROM ids JOIN lab_arrow USING (n)
""")
con.unregister("lab_arrow")
log(f"links: {nid} ids")

# ---------- reference tables ----------
prod = os.path.join(DATA, "products.csv").replace("'", "''")
camp = os.path.join(DATA, "campaigns.csv").replace("'", "''")
con.execute(f"""
CREATE TEMP TABLE products AS SELECT * FROM read_csv('{prod}', header=true, all_varchar=true,
  columns={{'product_id':'VARCHAR','category':'VARCHAR'}}, auto_detect=false)
""")
con.execute(f"""
CREATE TEMP TABLE campaigns AS SELECT * FROM read_csv('{camp}', header=true, all_varchar=true,
  columns={{'campaign_id':'VARCHAR','channel':'VARCHAR'}}, auto_detect=false)
""")

# ---------- events ----------
con.execute(f"""
CREATE TEMP TABLE ev_raw AS
SELECT {val("e")} AS eid, {val("u")} AS uid, {val("t")} AS ts_s, {val("i")} AS its_s,
       {val("k")} AS et, {val("p")} AS pid, {val("c")} AS pr
FROM ({raw_fields('events', ['e', 'u', 't', 'i', 'k', 'p', 'c'])})
""")
log("events parsed")

con.execute(f"""
CREATE TEMP TABLE ev AS
WITH d AS (SELECT eid FROM ev_raw WHERE eid IS NOT NULL GROUP BY eid HAVING count(*) > 1),
kept AS (
  SELECT uid, ts_s, its_s, et, pid, pr FROM ev_raw ANTI JOIN d USING (eid) WHERE eid IS NOT NULL
  UNION ALL
  SELECT k.uid, k.ts_s, k.its_s, k.et, k.pid, k.pr FROM (
    SELECT arg_min(struct_pack(uid, ts_s, its_s, et, pid, pr), its_s) AS k
    FROM ev_raw SEMI JOIN d USING (eid) GROUP BY eid)
), typed AS (
  SELECT uid, {ts_to_ms('ts_s')} AS ts, epoch_ms(CAST(substr(its_s, 1, 19) AS TIMESTAMP)) AS its, et, pid,
         CASE WHEN et = 'purchase' AND regexp_full_match(pr, '[0-9]+')
                   AND length(ltrim(pr, '0')) BETWEEN 1 AND 8
              THEN try_cast(ltrim(pr, '0') AS BIGINT) ELSE NULL END AS price
  FROM kept
)
SELECT coalesce(pm.person, t.uid) AS person, t.ts, t.et,
       CASE WHEN t.et = 'purchase' THEN t.price ELSE 0 END AS price,
       coalesce(p.category, 'unknown') AS cat
FROM typed t
LEFT JOIN pmap pm ON pm.id = t.uid
LEFT JOIN products p ON p.product_id = t.pid
WHERE t.uid IS NOT NULL AND t.pid IS NOT NULL
  AND t.ts IS NOT NULL AND t.ts >= {WS} AND t.ts < {WE}
  AND t.et IN ('view', 'cart', 'purchase')
  AND (t.et <> 'purchase' OR (t.price BETWEEN 1 AND 10000000))
  AND t.its - t.ts <= {DAY_MS}
""")
con.execute("DROP TABLE ev_raw")
log("events cleaned")

# ---------- sessions ----------
con.execute(f"""
CREATE TEMP TABLE evs AS
SELECT person, ts, et, price, cat,
       sum(CASE WHEN gap > {GAP_MS} THEN 1 ELSE 0 END) OVER (PARTITION BY person ORDER BY ts RANGE UNBOUNDED PRECEDING) AS sid
FROM (SELECT *, ts - lag(ts) OVER (PARTITION BY person ORDER BY ts) AS gap FROM ev)
""")
con.execute("DROP TABLE ev")
con.execute(f"""
CREATE TEMP TABLE sessions AS
SELECT person, sid, min(ts) AS start,
       (DATE '1970-01-01' + CAST(min(ts) // {DAY_MS} AS INTEGER)) AS day,
       count(*) AS n_events,
       sum(CASE WHEN et = 'purchase' THEN price ELSE 0 END) AS rev,
       count(*) FILTER (WHERE et = 'purchase') AS n_purch
FROM evs GROUP BY person, sid
""")
log("sessions built")

# ---------- user_daily ----------
con.execute("""
CREATE TEMP TABLE topcat AS
SELECT person, day, cat FROM (
  SELECT s.person, s.day, e.cat, sum(e.price) AS r,
         row_number() OVER (PARTITION BY s.person, s.day ORDER BY sum(e.price) DESC, e.cat ASC) AS rn
  FROM evs e JOIN sessions s ON e.person = s.person AND e.sid = s.sid
  WHERE e.et = 'purchase'
  GROUP BY s.person, s.day, e.cat
) WHERE rn = 1
""")
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
con.execute(f"""
COPY (
  SELECT u.person AS user_id, u.day, u.sessions, u.events, u.revenue_cents, t.cat AS top_category
  FROM (SELECT person, day, count(*)::BIGINT AS sessions, sum(n_events)::BIGINT AS events,
               sum(rev)::BIGINT AS revenue_cents
        FROM sessions GROUP BY person, day) u
  LEFT JOIN topcat t ON t.person = u.person AND t.day = u.day
) TO '{os.path.join(OUT, 'user_daily', 'part-0.parquet')}' (FORMAT parquet)
""")
if os.environ.get("DEBUG"):
    con.execute(f"COPY evs TO '{SCRATCH}/evs.parquet' (FORMAT parquet)")
con.execute("DROP TABLE evs")
log("user_daily written")

# ---------- clicks ----------
con.execute(f"""
CREATE TEMP TABLE clicks AS
WITH raw AS (
  SELECT {val("c")} AS cid, {val("u")} AS uid, {val("k")} AS camp, {val("t")} AS ts_s
  FROM ({raw_fields('clicks', ['c', 'u', 'k', 't'])})
), one AS (
  SELECT cid, any_value(uid) AS uid, any_value(camp) AS camp, any_value(ts_s) AS ts_s
  FROM raw WHERE cid IS NOT NULL GROUP BY cid
), typed AS (
  SELECT cid, uid, camp, {ts_to_ms('ts_s')} AS ts FROM one
)
SELECT coalesce(pm.person, t.uid) AS person, t.cid, t.camp, t.ts
FROM typed t
JOIN campaigns c ON c.campaign_id = t.camp
LEFT JOIN pmap pm ON pm.id = t.uid
WHERE t.uid IS NOT NULL AND t.ts IS NOT NULL AND t.ts >= {CWS} AND t.ts < {WE}
""")
log("clicks cleaned")

# ---------- attribution ----------
con.execute(f"""
CREATE TEMP TABLE credit AS
WITH c1 AS (
  SELECT person, ts, arg_min(camp, cid) AS camp FROM clicks GROUP BY person, ts
)
SELECT s.person, s.start, s.day, s.rev, s.n_purch, c1.ts AS cts,
       CASE WHEN c1.ts IS NOT NULL AND s.start - c1.ts <= {WEEK_MS} THEN c1.camp ELSE 'organic' END AS camp
FROM sessions s ASOF LEFT JOIN c1 ON s.person = c1.person AND s.start >= c1.ts
""")
if os.environ.get("DEBUG"):
    con.execute(f"COPY credit TO '{SCRATCH}/credit.parquet' (FORMAT parquet)")
log("credit done")

# ---------- assists ----------
con.execute(f"""
CREATE TEMP TABLE sess_g AS
SELECT person, start, day, row_number() OVER (ORDER BY person, start, sid)::BIGINT AS g FROM sessions
""")
con.execute(f"""
CREATE TEMP TABLE isl AS
WITH c2 AS (SELECT DISTINCT person, camp, ts FROM clicks),
f AS (SELECT *, CASE WHEN ts - lag(ts) OVER (PARTITION BY person, camp ORDER BY ts) <= {WEEK_MS} THEN 0 ELSE 1 END AS nw FROM c2),
k AS (SELECT *, sum(nw) OVER (PARTITION BY person, camp ORDER BY ts ROWS UNBOUNDED PRECEDING) AS grp FROM f)
SELECT person, camp, min(ts) AS lo, max(ts) + {WEEK_MS} AS hi FROM k GROUP BY person, camp, grp
""")
con.execute("""
CREATE TEMP TABLE assist AS
WITH a AS (
  SELECT i.person, i.camp, i.lo, i.hi, s.g AS g1 FROM isl i ASOF LEFT JOIN sess_g s ON i.person = s.person AND i.lo <= s.start
), b AS (
  SELECT a.camp, a.g1, s.g AS g2 FROM a ASOF LEFT JOIN sess_g s ON a.person = s.person AND a.hi >= s.start
), r AS (
  SELECT camp, unnest(range(g1, g2 + 1)) AS g FROM b WHERE g1 IS NOT NULL AND g2 IS NOT NULL AND g1 <= g2
)
SELECT r.camp, s.day, count(*)::BIGINT AS assisted FROM r JOIN sess_g s USING (g) GROUP BY r.camp, s.day
""")
log("assists done")

con.execute(f"""
COPY (
  WITH cr AS (
    SELECT camp, day, count(*)::BIGINT AS sessions, count(*) FILTER (WHERE n_purch > 0)::BIGINT AS purchasing_sessions,
           sum(rev)::BIGINT AS revenue_cents
    FROM credit GROUP BY camp, day
  )
  SELECT coalesce(cr.camp, a.camp) AS campaign_id,
         CASE WHEN coalesce(cr.camp, a.camp) = 'organic' THEN 'organic' ELSE c.channel END AS channel,
         coalesce(cr.day, a.day) AS day,
         coalesce(cr.sessions, 0)::BIGINT AS sessions,
         coalesce(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
         coalesce(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
         coalesce(a.assisted, 0)::BIGINT AS assisted_sessions
  FROM cr FULL OUTER JOIN assist a ON cr.camp = a.camp AND cr.day = a.day
  LEFT JOIN campaigns c ON c.campaign_id = coalesce(cr.camp, a.camp)
) TO '{os.path.join(OUT, 'campaign_daily', 'part-0.parquet')}' (FORMAT parquet)
""")
log("campaign_daily written; done")
