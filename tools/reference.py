#!/usr/bin/env python3
"""Slow, straightforward reference implementation of rules.md, for cross-checking.
Usage: reference.py DATA_DIR OUT_DIR  (writes user_daily.csv and campaign_daily.csv)"""
import csv, glob, json, os, re, sys, datetime
from collections import defaultdict

DATA, OUT = sys.argv[1], sys.argv[2]
DAY = 86_400_000
WEEK = 7 * DAY


def lines(folder):
    for path in sorted(glob.glob(os.path.join(DATA, folder, "*.csv"))):
        with open(path, "rb") as f:
            raw = f.read()
        parts = raw.split(b"\n")
        if raw.endswith(b"\n"):
            parts = parts[:-1]
        for line in parts[1:]:
            yield line


def fields(line, n):
    fs = line.split(b",")
    if len(fs) != n:
        return None
    out = []
    for f in fs:
        if b'"' in f:
            if len(f) >= 2 and f[0:1] == b'"' and f[-1:] == b'"' and b'"' not in f[1:-1]:
                out.append(f[1:-1])
            else:
                return None
        else:
            out.append(f)
    return out


F1 = re.compile(rb"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z$")
F2 = re.compile(rb"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})([+-])(\d{2}):(\d{2})$")
F3 = re.compile(rb"^[0-9]+$")


def civil(y, mo, d, h, mi, s):
    try:
        dt = datetime.datetime(y, mo, d, h, mi, s, tzinfo=datetime.timezone.utc)
    except ValueError:
        return None
    return int(dt.timestamp()) * 1000


def ts(v):
    # re's $ also matches before a trailing \n; values never hold \n (split on it)
    m = F1.match(v)
    if m and len(v) == 20:
        g = [int(x) for x in m.groups()]
        if g[0] < 1:
            return None
        return civil(*g)
    m = F2.match(v)
    if m and len(v) == 25:
        g = m.groups()
        oh, om = int(g[7]), int(g[8])
        if oh > 23 or om > 59:
            return None
        y = int(g[0])
        if y < 1:
            return None
        base = civil(y, *[int(x) for x in g[1:6]])
        if base is None:
            return None
        off = (oh * 60 + om) * 60000
        return base - off if g[6] == b"+" else base + off
    if F3.match(v) and len(v) > 0 and v.isdigit() and all(48 <= c <= 57 for c in v):
        x = int(v)
        return x if x < 2**63 else None
    return None


man = json.load(open(os.path.join(DATA, "manifest.json")))
WS = ts(man["window_start"].encode())
WE = ts(man["window_end"].encode())

products = {}
for line in lines(".") if False else []:
    pass
with open(os.path.join(DATA, "products.csv"), "rb") as f:
    for line in f.read().split(b"\n")[1:]:
        r = fields(line, 2)
        if r and r[0]:
            products[r[0]] = r[1]
campaigns = {}
with open(os.path.join(DATA, "campaigns.csv"), "rb") as f:
    for line in f.read().split(b"\n")[1:]:
        r = fields(line, 2)
        if r and r[0]:
            campaigns[r[0]] = r[1]

# links
parent = {}


def find(x):
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x


for line in lines("links"):
    r = fields(line, 2)
    if not r or not r[0] or not r[1]:
        continue
    a, b = r
    parent.setdefault(a, a)
    parent.setdefault(b, b)
    ra, rb = find(a), find(b)
    if ra != rb:
        parent[ra] = rb
groups = defaultdict(list)
for x in parent:
    groups[find(x)].append(x)
name = {}
for root, members in groups.items():
    n = min(members)
    for m in members:
        name[m] = n


def person(u):
    return name.get(u, u)


# events
best = {}
for line in lines("events"):
    r = fields(line, 7)
    if not r or not r[0]:
        continue
    ing = ts(r[3])
    if r[0] not in best or ing < best[r[0]][0]:
        best[r[0]] = (ing, r)
events = defaultdict(list)
for eid, (ing, r) in best.items():
    _, uid, t, _, typ, pid, price = r
    t = ts(t)
    if not uid or t is None or not (WS <= t < WE) or typ not in (b"view", b"cart", b"purchase") or not pid:
        continue
    p = 0
    if typ == b"purchase":
        if not price or not all(48 <= c <= 57 for c in price):
            continue
        p = int(price)
        if not (1 <= p <= 10_000_000):
            continue
    if ing - t > DAY:
        continue
    events[person(uid)].append((t, typ, p, products.get(pid, b"unknown")))

clicks = defaultdict(list)
for line in lines("clicks"):
    r = fields(line, 4)
    if not r or not r[0]:
        continue
    cid, uid, camp, t = r
    t = ts(t)
    if not uid or t is None or not (WS - WEEK <= t < WE) or camp not in campaigns:
        continue
    clicks[person(uid)].append((t, cid, camp))

ud = {}
cd = defaultdict(lambda: [0, 0, 0, 0])
for per, evs in events.items():
    evs.sort(key=lambda e: e[0])
    sessions = []
    for e in evs:
        if sessions and e[0] - sessions[-1][-1][0] <= 1_800_000:
            sessions[-1].append(e)
        else:
            sessions.append([e])
    cks = clicks.get(per, [])
    for s in sessions:
        start = s[0][0]
        day = start // DAY
        rev = sum(e[2] for e in s if e[1] == b"purchase")
        purch = any(e[1] == b"purchase" for e in s)
        k = ud.setdefault((per, day), [0, 0, 0, defaultdict(int)])
        k[0] += 1
        k[1] += len(s)
        k[2] += rev
        for e in s:
            if e[1] == b"purchase":
                k[3][e[3]] += e[2]
        q = [c for c in cks if c[0] <= start and start - c[0] <= WEEK]
        if q:
            mt = max(c[0] for c in q)
            camp = min(c for c in q if c[0] == mt)[2] if False else sorted([c for c in q if c[0] == mt], key=lambda c: c[1])[0][2]
        else:
            camp = b"organic"
        a = cd[(camp, day)]
        a[0] += 1
        a[1] += purch
        a[2] += rev
        for c in set(c[2] for c in q):
            cd[(c, day)][3] += 1


def date(d):
    return (datetime.date(1970, 1, 1) + datetime.timedelta(days=d)).isoformat()


os.makedirs(OUT, exist_ok=True)
with open(os.path.join(OUT, "user_daily.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
    for (per, day), (s, n, rev, cats) in ud.items():
        top = ""
        if cats:
            top = sorted(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0][0].decode()
        w.writerow([per.decode(), date(day), s, n, rev, top])
with open(os.path.join(OUT, "campaign_daily.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"])
    for (camp, day), a in cd.items():
        ch = b"organic" if camp == b"organic" else campaigns[camp]
        w.writerow([camp.decode(), ch.decode(), date(day), a[0], a[1], a[2], a[3]])
