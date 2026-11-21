"""Slow, literal reading of rules.md. Used only to cross-check pipeline.py on small data.

usage: python reference.py DATA_DIR OUT_DIR   (writes user_daily.csv, campaign_daily.csv)
"""
import calendar
import csv
import glob
import json
import os
import re
import sys
from collections import defaultdict

DAY = 86_400_000
WEEK = 7 * DAY


def read_lines(sub, data):
    for path in sorted(glob.glob(os.path.join(data, sub, "*.csv"))):
        with open(path, "rb") as f:
            raw = f.read().decode("utf-8")
        parts = raw.split("\n")
        if raw.endswith("\n"):
            parts = parts[:-1]  # the last \n ends the last line
        for line in parts[1:]:
            yield line


def parse(line, n):
    if line == "":
        return None
    fields = line.split(",")
    if len(fields) != n:
        return None
    out = []
    for x in fields:
        if '"' in x:
            if len(x) >= 2 and x[0] == '"' and x[-1] == '"' and '"' not in x[1:-1]:
                x = x[1:-1]
            else:
                return None
        out.append(x if x != "" else None)
    return out


DIG = set("0123456789")


def isdig(s):
    return len(s) > 0 and all(c in DIG for c in s)


def ts_ms(s):
    if s is None:
        return None
    if isdig(s):
        v = int(s)
        return v if v < 2**63 else None
    m = re.fullmatch(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|([+-])([0-9]{2}):([0-9]{2}))", s)
    if not m:
        return None
    y, mo, d, h, mi, se = (int(m.group(i)) for i in range(1, 7))
    if not (1 <= mo <= 12) or y < 1:
        return None
    if not (1 <= d <= calendar.monthrange(y, mo)[1]):
        return None
    if h > 23 or mi > 59 or se > 59:
        return None
    off = 0
    if m.group(7) != "Z":
        oh, om = int(m.group(9)), int(m.group(10))
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000 * (1 if m.group(8) == "+" else -1)
    days = (calendar.timegm((y, mo, d, 0, 0, 0)) // 86400)
    return days * DAY + ((h * 60 + mi) * 60 + se) * 1000 - off


def main(data, out):
    man = json.load(open(os.path.join(data, "manifest.json")))
    ws, we = ts_ms(man["window_start"]), ts_ms(man["window_end"])

    # links: union find
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for line in read_lines("links", data):
        r = parse(line, 2)
        if r is None or r[0] is None or r[1] is None:
            continue
        a, b = find(r[0]), find(r[1])
        if a != b:
            parent[a] = b
    groups = defaultdict(list)
    for x in list(parent):
        groups[find(x)].append(x)
    name = {}
    for members in groups.values():
        nm = min(members, key=lambda s: s.encode())
        for x in members:
            name[x] = nm

    def person(u):
        return name.get(u, u)

    prods = {}
    with open(os.path.join(data, "products.csv"), newline="") as f:
        for row in csv.DictReader(f):
            prods[row["product_id"]] = row["category"]
    camps = {}
    with open(os.path.join(data, "campaigns.csv"), newline="") as f:
        for row in csv.DictReader(f):
            camps[row["campaign_id"]] = row["channel"]

    # events
    kept = {}
    for line in read_lines("events", data):
        r = parse(line, 7)
        if r is None or r[0] is None:
            continue
        its = ts_ms(r[3])
        if r[0] not in kept or its < kept[r[0]][0]:
            kept[r[0]] = (its, r)
    evs = defaultdict(list)
    for its, r in kept.values():
        eid, uid, ts_s, _, et, pid, pr = r
        ts = ts_ms(ts_s)
        if uid is None or ts is None or not (ws <= ts < we) or et not in ("view", "cart", "purchase") or pid is None:
            continue
        price = 0
        if et == "purchase":
            if pr is None or not isdig(pr):
                continue
            price = int(pr)
            if not (1 <= price <= 10_000_000):
                continue
        if its - ts > DAY:
            continue
        evs[person(uid)].append((ts, et, price, prods.get(pid, "unknown")))

    sessions = []  # (person, start, events list)
    for p, lst in evs.items():
        lst.sort(key=lambda e: e[0])
        cur = None
        prev = None
        for e in lst:
            if cur is None or e[0] - prev > 1_800_000:
                cur = [p, e[0], []]
                sessions.append(cur)
            cur[2].append(e)
            prev = e[0]

    # clicks
    cl = {}
    for line in read_lines("clicks", data):
        r = parse(line, 4)
        if r is None or r[0] is None:
            continue
        cl.setdefault(r[0], r)
    pclicks = defaultdict(list)
    for cid, uid, camp, ts_s in cl.values():
        ts = ts_ms(ts_s)
        if uid is None or ts is None or not (ws - WEEK <= ts < we) or camp not in camps:
            continue
        pclicks[person(uid)].append((ts, cid, camp))

    ud = defaultdict(lambda: [0, 0, 0, defaultdict(int)])
    cd = defaultdict(lambda: [0, 0, 0, 0])
    for p, start, lst in sessions:
        day = start // DAY
        u = ud[(p, day)]
        u[0] += 1
        u[1] += len(lst)
        rev = sum(e[2] for e in lst if e[1] == "purchase")
        u[2] += rev
        npurch = sum(1 for e in lst if e[1] == "purchase")
        for e in lst:
            if e[1] == "purchase":
                u[3][e[3]] += e[2]
        q = [c for c in pclicks.get(p, []) if c[0] <= start and start - c[0] <= WEEK]
        if q:
            best = sorted(q, key=lambda c: (-c[0], c[1].encode()))[0]
            camp = best[2]
        else:
            camp = "organic"
        c = cd[(camp, day)]
        c[0] += 1
        c[1] += 1 if npurch else 0
        c[2] += rev
        for cp in set(c2[2] for c2 in q):
            cd[(cp, day)][3] += 1

    import datetime

    def dstr(d):
        return (datetime.date(1970, 1, 1) + datetime.timedelta(days=d)).isoformat()

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "user_daily.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
        for (p, d), (s, e, r, cats) in ud.items():
            top = ""
            if cats:
                top = sorted(cats.items(), key=lambda kv: (-kv[1], kv[0].encode()))[0][0]
            w.writerow([p, dstr(d), s, e, r, top])
    with open(os.path.join(out, "campaign_daily.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"])
        for (cp, d), v in cd.items():
            w.writerow([cp, "organic" if cp == "organic" else camps[cp], dstr(d), *v])


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
