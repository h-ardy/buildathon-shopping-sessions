"""Make a small, very dirty dataset that hits the edge cases in rules.md.

usage: python mkdata.py OUT_DIR SEED [N_PEOPLE]
"""
import datetime
import json
import os
import random
import sys

DAY = 86_400_000
WEEK = 7 * DAY

out, seed = sys.argv[1], int(sys.argv[2])
npeople = int(sys.argv[3]) if len(sys.argv) > 3 else 60
R = random.Random(seed)

start_day = datetime.date(2028, 2, 20) if seed % 2 else datetime.date(2026, 10, 1)
ws = int(datetime.datetime(start_day.year, start_day.month, start_day.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)
we = ws + 14 * DAY


def iso(ms, form=None):
    if form == "F1":
        ms -= ms % 1000
    form = form or R.choice(["F1", "F1", "F2", "F3"])
    if form == "F3" or ms % 1000:
        s = str(ms)
        return ("000" + s) if R.random() < 0.1 else s
    dt = datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc)
    if form == "F1":
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    off = R.choice([-600, -240, -30, 0, 0, 330, 720])
    loc = dt + datetime.timedelta(minutes=off)
    sign = "-" if off < 0 else "+"
    if off == 0 and R.random() < 0.5:
        sign = "-"
    return loc.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{abs(off)//60:02d}:{abs(off)%60:02d}"


BAD_TS = ["", "2026-10-03T23:58:10.5Z", "2026-10-03t23:58:10Z", "2026-10-03 23:58:10Z", "2026-10-03T23:58:10",
          "1791071890000.0", "1.79e12", "-5", "2026-13-03T23:58:10Z", "2026-02-30T00:00:00Z", "2027-02-29T00:00:00Z",
          "2026-10-03T24:00:00Z", "2026-10-03T23:58:60Z", "2026-10-03T19:58:10+24:00", "2026-10-03T19:58:10+05:60",
          "2026-10-03T19:58:10+0200", "２５０", "86400000", "99999999999999999999999", "2026-10-03T23:58:10z"]


def q(v):
    return f'"{v}"' if R.random() < 0.2 else v


products = [f"p{i}" for i in range(40)]
cats = [f"cat{i}" for i in range(6)]
campaigns = [f"c_{i}" for i in range(12)]

# people and ids
people = []
links = []
for i in range(npeople):
    k = R.choice([1, 1, 1, 2, 2, 3, 5])
    if i < 2:
        k = 40  # long chain
    ids = [f"u{i}_{j}" for j in range(k)]
    if R.random() < 0.3:
        ids.append(f" u{i}x")  # leading space id
    order = ids[:]
    R.shuffle(order)
    for a, b in zip(order, order[1:]):
        links.append((a, b) if R.random() < 0.5 else (b, a))
    if R.random() < 0.1:
        links.append((ids[0], ids[0]))
    if R.random() < 0.1:
        links.append((f"zz_unused{i}", ids[0]))
    people.append(ids)
# shared device joins a few people
dev = "devX"
for i in range(3, 9):
    links.append((dev, people[i][0]))
links += [(a, b) for a, b in R.sample(links, len(links) // 10)]  # repeats
links += [(b, a) for a, b in R.sample(links, len(links) // 10)]

events = []
clicks = []
eid = 0
cid = 0
for ids in people:
    t = R.randint(ws - DAY, we)
    for _ in range(R.randint(1, 30)):
        gap = R.choice([1000, 60_000, 1_799_999, 1_800_000, 1_800_001, 3 * 3600_000, 0, 20 * 3600_000])
        t += gap
        uid = R.choice(ids)
        et = R.choice(["view"] * 6 + ["cart"] * 2 + ["purchase"] * 3)
        pid = R.choice(products + ["pX", "P1", ""])
        pr = str(R.randint(1, 50_000)) if et == "purchase" else R.choice(["", "12", "abc"])
        if et == "purchase" and R.random() < 0.1:
            pr = R.choice(["0", "-5", "1.5", "", "10000001", "10000000", "0250", "00000000000000001"])
        ingest = t + R.choice([0, 5000, DAY, DAY + 1000, 3 * DAY, -5000])
        eid += 1
        e = [f"e{eid}", uid, iso(t), iso(ingest, "F1"), et, pid, pr]
        if R.random() < 0.05:
            e[2] = R.choice(BAD_TS)
        if R.random() < 0.02:
            e[4] = R.choice(["Purchase", "cart ", "click"])
        if R.random() < 0.02:
            e[1] = ""
        if R.random() < 0.01:
            e[1] = e[1].upper()
        events.append(e)
        if R.random() < 0.1:  # duplicates, later ingest; sometimes different user
            for k in range(R.randint(1, 3)):
                d = list(e)
                d[3] = iso(ingest + R.randint(1000, 600_000), "F1")
                if R.random() < 0.3:
                    d[1] = R.choice(R.choice(people))
                if R.random() < 0.3:
                    d[2] = iso(t)  # same instant, maybe another form
                events.append(d)
        if R.random() < 0.02:  # earliest copy bad, later copy good
            d = list(e)
            e[2] = "bad"
            d[3] = iso(ingest + 1000, "F1")
            events.append(d)
        # clicks around
        if R.random() < 0.4:
            ct = t - R.choice([0, 1, -1, 1000, WEEK, WEEK + 1, 3 * DAY, 9 * DAY])
            for _ in range(R.choice([1, 1, 2])):
                cid += 1
                c = [f"k{cid:05d}", R.choice(ids), R.choice(campaigns + ["C_1", "c_999"]), iso(ct)]
                if R.random() < 0.03:
                    c[3] = R.choice(BAD_TS)
                clicks.append(c)
                if R.random() < 0.1:
                    clicks.append(list(c))

R.shuffle(events)
R.shuffle(clicks)
R.shuffle(links)


def dump(sub, header, rows, nfiles, no_cr=frozenset()):
    os.makedirs(os.path.join(out, sub), exist_ok=True)
    chunks = [[] for _ in range(nfiles)]
    for r in rows:
        chunks[R.randrange(nfiles)].append(r)
    for i, ch in enumerate(chunks):
        with open(os.path.join(out, sub, f"part-{i}.csv"), "w", newline="") as f:
            f.write(header + "\n")
            for r in ch:
                line = ",".join(q(v) for v in r)
                x = R.random()
                if x < 0.01 and not os.environ.get("NOCR") and r[0] not in no_cr:
                    line = line + "\r"
                elif x < 0.015:
                    line = line + ",extra"
                elif x < 0.02:
                    line = '"' + line
                elif x < 0.025:
                    line = ""
                elif x < 0.03:
                    line = line.replace(",", ',x"y,', 1) if "," in line else line
                f.write(line + "\n")


dump("events", "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents", events, 4)
from collections import Counter
_dup = frozenset(k for k, n in Counter(c[0] for c in clicks).items() if n > 1)
dump("clicks", "click_id,user_id,campaign_id,ts", clicks, 3, _dup)
dump("links", "user_id_a,user_id_b", links, 3)
with open(os.path.join(out, "products.csv"), "w") as f:
    f.write("product_id,category\n")
    for p in products:
        f.write(f"{p},{R.choice(cats)}\n")
with open(os.path.join(out, "campaigns.csv"), "w") as f:
    f.write("campaign_id,channel\n")
    for c in campaigns:
        f.write(f"{c},{R.choice(['search','social','email','display','video'])}\n")


def z(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


json.dump({"window_start": z(ws), "window_end": z(we)}, open(os.path.join(out, "manifest.json"), "w"))
