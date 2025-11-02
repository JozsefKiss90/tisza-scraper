
# history_search.py
import argparse, sqlite3, csv, sys
from datetime import datetime, timedelta
from pathlib import Path
from rapidfuzz import fuzz

DB_PATH = "news.sqlite"

def to_ts(dt): return int(dt.timestamp())

def parse_date(s):
    return int(datetime.fromisoformat(s).timestamp())

def fts_query(conn, q, ts_from=None, ts_to=None, limit=200, order="bm25"):
    args = []
    date_filter = ""
    if ts_from is not None:
        date_filter += " AND i.ts >= ?"
        args.append(ts_from)
    if ts_to is not None:
        date_filter += " AND i.ts < ?"
        args.append(ts_to)

    sql = f"""
    SELECT i.title, i.link, i.label, round(i.label_score,3) as label_score, datetime(i.ts,'unixepoch') as date, 
           bm25(items_fts) as rank, i.cluster_id, substr(i.content,1,400) as snippet
    FROM items i
    JOIN items_fts ON items_fts.rowid = i.rowid
    WHERE items_fts MATCH ?
    {date_filter}
    ORDER BY {"rank" if order=="bm25" else "i.ts DESC"}
    LIMIT ?
    """
    return conn.execute(sql, [q, *args, limit]).fetchall()

def fuzzy_filter(rows, q, thresh=70):
    out = []
    for r in rows:
        title, link, label, score, date, rank, cid, snippet = r
        s = f"{title} {snippet or ''}"
        if fuzz.partial_ratio(q, s) >= thresh:
            out.append(r)
    return out

def export_csv(rows, path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    import csv
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["date","title","link","label","label_score","cluster_id","rank","snippet"])
        for title, link, label, score, date, rank, cid, snippet in rows:
            w.writerow([date, title, link, label, score, cid, rank, snippet])
    print(f"✅ Exportálva: {path}  ({len(rows)} sor)")

def main():
    ap = argparse.ArgumentParser(description="Történeti keresés az adatbázisban (FTS5 + BM25 + fuzzy)")
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--q", required=True, help='Keresőkifejezés, pl. "Lázár János"')
    ap.add_argument("--from", dest="date_from", help="Kezdő dátum (YYYY-MM-DD)")
    ap.add_argument("--to", dest="date_to", help="Záró dátum (YYYY-MM-DD, kizáró)")
    ap.add_argument("--years", type=int, help="Hány évre visszamenőleg (pl. 10) ha nincs from/to")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--order", choices=["bm25","time"], default="bm25")
    ap.add_argument("--fuzzy", action="store_true", help="Fuzzy utószűrés (név- és kifejezés-variációkhoz)")
    ap.add_argument("--export", help="CSV export útvonala (pl. export/lazar_10ev.csv)")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)

    if args.date_from or args.date_to:
        ts_from = parse_date(args.date_from) if args.date_from else None
        ts_to = parse_date(args.date_to) if args.date_to else None
    else:
        if not args.years:
            args.years = 10
        now = datetime.now()
        ts_to = to_ts(now)
        try:
            ts_from = to_ts(now.replace(year=now.year - args.years))
        except ValueError:
            ts_from = to_ts(now - timedelta(days=365*args.years))

    rows = fts_query(conn, args.q, ts_from, ts_to, args.limit, args.order)

    if args.fuzzy:
        rows = fuzzy_filter(rows, args.q)

    if not rows:
        print("⚠️ Nincs találat.")
        return

    for i,(title, link, label, score, date, rank, cid, snippet) in enumerate(rows, 1):
        print(f"{i:02d}. [{date}] {title}")
        print(f"    🏷️  {label or '—'} ({score if score is not None else '—'})   🌀 {cid}   🔢 bm25={round(rank,2) if rank is not None else '—'}")
        print(f"    🔗 {link}")
        print(f"    🧾 {snippet}\n")

    if args.export:
        export_csv(rows, args.export)

if __name__ == "__main__":
    main()
