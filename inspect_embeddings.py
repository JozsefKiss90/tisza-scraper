# inspect_embeddings.py
import argparse
import sqlite3
from textwrap import shorten

DB_PATH = "news.sqlite"

def list_articles(conn, label=None, cluster=None, limit=15, order="score"):
    q = "SELECT title, link, label, label_score, cluster_id, datetime(ts,'unixepoch'), substr(content,1,400) FROM items"
    filters = []
    args = []
    if label:
        filters.append("label = ?")
        args.append(label)
    if cluster is not None:
        filters.append("cluster_id = ?")
        args.append(cluster)
    if filters:
        q += " WHERE " + " AND ".join(filters)
    if order == "score":
        q += " ORDER BY label_score DESC"
    elif order == "time":
        q += " ORDER BY ts DESC"
    q += f" LIMIT {limit}"
    return conn.execute(q, args).fetchall()

def list_clusters(conn):
    q = """
    SELECT cluster_id, COUNT(*),
           MAX(datetime(ts,'unixepoch')),
           (SELECT cluster_summary FROM items i2 WHERE i2.cluster_id=i1.cluster_id AND i2.cluster_summary IS NOT NULL LIMIT 1)
    FROM items i1
    GROUP BY cluster_id
    ORDER BY cluster_id;
    """
    return conn.execute(q).fetchall()

def main():
    ap = argparse.ArgumentParser(description="Embedding-alapú cikkböngésző")
    ap.add_argument("--db", default=DB_PATH, help="Adatbázis elérési útja")
    ap.add_argument("--label", choices=["kormánypárti", "ellenzéki", "semleges"], help="Szűrés címkére")
    ap.add_argument("--cluster", type=int, help="Szűrés klaszter ID-re")
    ap.add_argument("--limit", type=int, default=10, help="Megjelenített cikkek száma")
    ap.add_argument("--order", choices=["score", "time"], default="score", help="Rendezés módja")
    ap.add_argument("--clusters", action="store_true", help="Csak klaszterek összefoglalóinak listázása")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)

    if args.clusters:
        print("\n📊 Klaszter-összefoglalók:\n")
        for cid, count, lastdate, summary in list_clusters(conn):
            print(f"🌀 Klaszter {cid} – {count} cikk (utolsó: {lastdate})")
            if summary:
                print("   Összefoglaló:", shorten(summary, 500, placeholder="…"))
            print()
        return

    rows = list_articles(conn, label=args.label, cluster=args.cluster, limit=args.limit, order=args.order)
    if not rows:
        print("Nincs találat a megadott szűrőkre.")
        return

    print(f"\n📰 Legfrissebb / legrelevánsabb {len(rows)} cikk az adatbázisból:\n")
    for i, (title, link, lab, score, cid, ts, preview) in enumerate(rows, 1):
        print(f"{i:02d}. {title}")
        print(f"    📅 {ts}   🏷️  {lab or '—'} ({score:.2f})   🌀 klaszter: {cid}")
        print(f"    🔗 {link}")
        print(f"    🧾 {shorten(preview or '', width=300, placeholder='…')}\n")

if __name__ == "__main__":
    main()
